#include <amqp_topic_transceiver/AMQPTopicTransmitter.h>
#include <topic_tools/shape_shifter.h>
#include <amqp_topic_transceiver/utils.h>
#include <blosc.h>

DEFINE_LOGGER_VARIABLES

namespace amqp_topic_transceiver
{
AMQPTopicTransmitter::AMQPTopicTransmitter(ros::NodeHandle node_handle, ros::NodeHandle private_node_handle)
  : nh_(node_handle), private_nh_(private_node_handle)
{
  _setStreamName(ros::this_node::getName());
  _initLogger();

  private_nh_.param<std::string>("server_url", server_url_, "localhost");
  private_nh_.param<int>("server_port", server_port_, 5672);
  private_nh_.param<std::string>("exchange", exchange_, "ros_topic_transmitter");
  private_nh_.param<int>("queue_size", queue_size_, 1);
  private_nh_.param<std::string>("server_user", server_user_, "guest");
  private_nh_.param<std::string>("server_password", server_password_, "guest");
  private_nh_.param<float>("metadata_retransmission_period_seconds", metadata_retransmission_period_seconds, 3.0);
  private_nh_.param<bool>("use_compression", use_compression_, true);
  std::string compression_algorithm;
  private_nh_.param<std::string>("compression_algorithm", compression_algorithm, "zstd");
  private_nh_.param<int>("compression_level", compression_level_, 5);

  std::vector<std::string> topics;
  private_nh_.param<std::vector<std::string> >("topics", topics, {});

  dyn_param_server_.reset(new dynamic_reconfigure::Server<amqp_topic_transceiver::AMQPTopicTransmitter_configConfig>(
      guard_dyn_param_server_recursive_mutex_, private_nh_));
  dyn_param_server_->setCallback([this](auto&& config, auto&& level) { reconfigureRequest(config, level); });

  blosc_init();
  auto rcode = blosc_set_compressor(compression_algorithm.c_str());
  if (rcode < 0)
  {
    LOG_ERR("Error setting compressor. Does it really exist?");
  }

  amqp_socket_t* socket = nullptr;

  conn = amqp_new_connection();

  socket = amqp_tcp_socket_new(conn);
  if (socket == nullptr)
  {
    die("creating TCP socket");
  }

  int status = amqp_socket_open(socket, server_url_.c_str(), server_port_);
  if (status != 0)
  {
    die("opening TCP socket");
  }

  die_on_amqp_error(amqp_login(conn,
                               "/",
                               AMQP_DEFAULT_MAX_CHANNELS,
                               AMQP_DEFAULT_FRAME_SIZE,
                               0,
                               AMQP_SASL_METHOD_PLAIN,
                               server_user_.c_str(),
                               server_password_.c_str()),
                    "Logging in");
  amqp_channel_open(conn, 1);
  die_on_amqp_error(amqp_get_rpc_reply(conn), "Opening channel");

  amqp_exchange_declare(
      conn, 1, amqp_cstring_bytes(exchange_.c_str()), amqp_cstring_bytes("fanout"), 0, 0, 0, 0, amqp_empty_table);

  for (const auto& topic : topics)
  {
    LOG_INF("Subscribing to topic " << topic);
    auto sub = node_handle.subscribe<topic_tools::ShapeShifter>(
        topic,
        queue_size_,
        [this, topic](const auto& msg) { processMessage(topic, msg); },
        ros::VoidConstPtr(),
        ros::TransportHints().tcpNoDelay());
    subs_.emplace(topic, sub);
  }
}

AMQPTopicTransmitter::~AMQPTopicTransmitter()
{
  die_on_amqp_error(amqp_channel_close(conn, 1, AMQP_REPLY_SUCCESS), "Closing channel");
  die_on_amqp_error(amqp_connection_close(conn, AMQP_REPLY_SUCCESS), "Closing connection");
  die_on_error(amqp_destroy_connection(conn), "Ending connection");
}

class Wrapper
{
public:
  explicit Wrapper(char* buf)
  {
    buf_ = buf;
  }

  char* advance(int size)
  {
    return buf_;
  }

private:
  char* buf_;
};

size_t AMQPTopicTransmitter::compress_buffer(const char* buf, const char** buf2, size_t size)
{
  if (!use_compression_)
  {
    *buf2 = buf;
    return size;
  }
  LOG_DEB("Compression input size: " << size);
  *buf2 = new char[size];
  int csize =
      blosc_compress(compression_level_, BLOSC_BITSHUFFLE, sizeof(double), size, buf, const_cast<char*>(*buf2), size);
  if (csize == 0)
  {
    LOG_DEB("Buffer is uncompressible.  Giving up.");
    delete[] * buf2;
    *buf2 = buf;
    return size;
  }
  if (csize < 0)
  {
    LOG_ERR("Compression error.  Error code: " << csize);
    delete[] * buf2;
    *buf2 = buf;
    return size;
  }

  LOG_DEB("Compression: " << size << " -> " << csize << " (" << ((1. * size) / csize) << ")");
  return csize;
}

void AMQPTopicTransmitter::processMessage(const std::string& topic,
                                          const ros::MessageEvent<topic_tools::ShapeShifter>& msg_event)
{
  const auto& msg = msg_event.getConstMessage();
  const auto& connection_header = msg_event.getConnectionHeaderPtr();
  const auto& md5sum = msg->getMD5Sum();
  auto& info = subs_.find(topic)->second;

  auto now = ros::Time::now();
  if (md5sum != info.last_md5sum ||
      (now - info.last_metadata_transmit).toSec() > metadata_retransmission_period_seconds)
  {
    const auto& datatype = msg->getDataType();
    const auto& def = msg->getMessageDefinition();
    bool latch = false;
    if (connection_header)
    {
      auto it = connection_header->find("latching");
      if ((it != connection_header->end()) && (it->second == "1"))
      {
        LOG_DEB("input topic " << topic << " is latched; latching output topic to match");
        latch = true;
      }
    }

    size_t buf_size = 3 * sizeof(uint32_t) + md5sum.size() + datatype.size() + def.size() + 1;
    char* buf = new char[buf_size];

    auto* len_ptr = reinterpret_cast<uint32_t*>(&buf[0]);
    len_ptr[0] = md5sum.size();
    len_ptr[1] = datatype.size();
    len_ptr[2] = def.size();

    auto* data_ptr = &buf[0] + 3 * sizeof(uint32_t);
    memcpy(data_ptr, md5sum.c_str(), md5sum.size());
    memcpy(data_ptr + len_ptr[0], datatype.c_str(), datatype.size());
    memcpy(data_ptr + len_ptr[0] + len_ptr[1], def.c_str(), def.size());
    buf[buf_size - 1] = static_cast<char>(latch);

    const char* buf_compressed;
    size_t size_compressed = compress_buffer(buf, &buf_compressed, buf_size);

    amqp_bytes_t data = { .len = size_compressed, .bytes = const_cast<char*>(buf_compressed) };
    LOG_DEB("Sending AMQP metadata message with body size: " << size_compressed);

    const char* exchange = exchange_.c_str();
    const char* routingkey = topic.c_str();

    amqp_basic_properties_t props;
    props._flags = AMQP_BASIC_CONTENT_TYPE_FLAG;  // AMQP_BASIC_DELIVERY_MODE_FLAG;
    props.content_type = amqp_cstring_bytes("application/message-metadata");
    // props.delivery_mode = 2;  // persistent
    die_on_error(
        amqp_basic_publish(conn, 1, amqp_cstring_bytes(exchange), amqp_cstring_bytes(routingkey), 0, 0, &props, data),
        "Publishing metadata");

    delete[] buf;
    info.metadata_sent = true;
    info.last_md5sum = md5sum;
    info.last_metadata_transmit = now;
  }

  const char* exchange = exchange_.c_str();
  const char* routingkey = topic.c_str();

  char* buf = new char[msg->size()];
  size_t size = msg->size();
  Wrapper wrap(buf);
  msg->write(wrap);
  const char* buf_compressed;
  size_t size_compressed = compress_buffer(buf, &buf_compressed, size);
  const amqp_bytes_t bytes = { .len = size_compressed, .bytes = const_cast<char*>(buf_compressed) };
  LOG_DEB("Sending AMQP ROS message with body size: " << size_compressed);

  // LOG_DEB(msg->getMD5Sum() << " | " << msg->getDataType() << " | " << msg->getMessageDefinition())

  amqp_basic_properties_t props;
  props._flags = AMQP_BASIC_CONTENT_TYPE_FLAG;
  props.content_type = amqp_cstring_bytes("application/octet-string");
  die_on_error(
      amqp_basic_publish(conn, 1, amqp_cstring_bytes(exchange), amqp_cstring_bytes(routingkey), 0, 0, &props, bytes),
      "Publishing");

  delete[] buf;
}

void AMQPTopicTransmitter::reconfigureRequest(AMQPTopicTransmitter_configConfig& new_config, uint32_t level)
{
  auto lvl = static_cast<aduulm_logger::LoggerLevel>(new_config.log_level);
  _setLogLevel(lvl);
  LOG_DEB("Switched log level to " << lvl);
}

DEFINE_LOGGER_CLASS_INTERFACE_IMPLEMENTATION(AMQPTopicTransmitter)

}  // namespace amqp_topic_transceiver
