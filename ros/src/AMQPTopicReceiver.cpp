#include <amqp_topic_transceiver/AMQPTopicReceiver.h>
#include <topic_tools/shape_shifter.h>
#include <amqp_topic_transceiver/utils.h>
#include <blosc.h>

DEFINE_LOGGER_VARIABLES

namespace amqp_topic_transceiver
{
AMQPTopicReceiver::AMQPTopicReceiver(ros::NodeHandle node_handle, ros::NodeHandle private_node_handle)
  : nh_(node_handle), private_nh_(private_node_handle)
{
  _setStreamName(ros::this_node::getName());
  _initLogger();
  _setLogLevel(aduulm_logger::LoggerLevel::Debug);

  private_nh_.param<std::string>("server_url", server_url_, "localhost");
  private_nh_.param<int>("server_port", server_port_, 5672);
  private_nh_.param<std::string>("exchange", exchange_, "ros_topic_transmitter");
  private_nh_.param<int>("queue_size", queue_size_, 1);
  private_nh_.param<std::string>("server_user", server_user_, "guest");
  private_nh_.param<std::string>("server_password", server_password_, "guest");
  private_nh_.param<std::string>("topic_suffix", topic_suffix_, "");
  private_nh_.param<bool>("use_compression", use_compression_, true);
  private_nh_.param<int>("decompression_buffer_size", decompression_buffer_size_, 50 * 1024);

  dyn_param_server_.reset(new dynamic_reconfigure::Server<amqp_topic_transceiver::AMQPTopicReceiver_configConfig>(
      guard_dyn_param_server_recursive_mutex_, private_nh_));
  dyn_param_server_->setCallback([this](auto&& config, auto&& level) { reconfigureRequest(config, level); });

  blosc_init();
  auto rcode = blosc_set_compressor("zstd");
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
}

AMQPTopicReceiver::~AMQPTopicReceiver()
{
  die_on_amqp_error(amqp_channel_close(conn, 1, AMQP_REPLY_SUCCESS), "Closing channel");
  die_on_amqp_error(amqp_connection_close(conn, AMQP_REPLY_SUCCESS), "Closing connection");
  die_on_error(amqp_destroy_connection(conn), "Ending connection");
}

class Wrapper
{
public:
  Wrapper(const char* buf, int size)
  {
    buf_ = buf;
    size_ = size;
  }

  [[nodiscard]] int getLength() const
  {
    return size_;
  }
  const char* getData()
  {
    return buf_;
  }

private:
  const char* buf_;
  int size_;
};

static inline std::string b2s(const amqp_bytes_t& bytes)
{
  return std::string(static_cast<const char*>(bytes.bytes), bytes.len);
}

// static inline std::string str2hex(const std::string &input) {
//   std::stringstream ss;
//
//   ss << std::hex << std::setfill('0');
//   for (const char c : input) {
//     ss << std::setw(2) << static_cast<unsigned int>(static_cast<unsigned char>(c));
//   }
//
//   return ss.str();
// }

int AMQPTopicReceiver::decompress_buffer(const char* buf, const char** buf2, size_t size)
{
  if (!use_compression_)
  {
    *buf2 = buf;
    return size;
  }
  LOG_DEB("Decompression input size: " << size);
  int dsize = decompression_buffer_size_;
  *buf2 = new char[dsize];
  dsize = blosc_decompress(buf, const_cast<char*>(*buf2), dsize);
  if (dsize <= 0)
  {
    LOG_DEB("Decompression error.  Error code: " << dsize);
    LOG_DEB("Assuming data was not compressed in the first place.");
    delete[] * buf2;
    *buf2 = buf;
    return size;
  }
  LOG_DEB("Decompression output size: " << dsize);
  return dsize;
}

bool AMQPTopicReceiver::run()
{
  amqp_bytes_t exchange = amqp_cstring_bytes(exchange_.c_str());
  amqp_bytes_t routingkey = amqp_cstring_bytes("");
  amqp_bytes_t queuename = amqp_cstring_bytes("");

  auto* res = amqp_queue_declare(conn, 1, queuename, 0, 0, 0, 1, amqp_empty_table);
  queuename = res->queue;
  die_on_amqp_error(amqp_get_rpc_reply(conn), "Declaring queue");

  amqp_queue_bind(conn, 1, queuename, exchange, routingkey, amqp_empty_table);
  die_on_amqp_error(amqp_get_rpc_reply(conn), "Binding queue to exchange");

  amqp_basic_consume(conn, 1, queuename, amqp_empty_bytes, 0, 1, 0, amqp_empty_table);
  die_on_amqp_error(amqp_get_rpc_reply(conn), "Consuming");
  LOG_DEB("Start consuming");

  while (ros::ok() && !ros::isShuttingDown())
  {
    ros::spinOnce();

    amqp_envelope_t envelope;
    amqp_maybe_release_buffers(conn);

    struct timeval timeout = { .tv_sec = 0, .tv_usec = 500000 };
    auto consume_res = amqp_consume_message(conn, &envelope, &timeout, 0);

    if (AMQP_RESPONSE_NORMAL != consume_res.reply_type)
    {
      continue;
    }
    size_t buf_size = envelope.message.body.len;
    LOG_DEB("Received a message with " << buf_size << " bytes!");

    // LOG_DEB("Delivery " << envelope.delivery_tag << ", exchange " << b2s(envelope.exchange) << " routingkey " <<
    // b2s(envelope.routing_key)); LOG_DEB("Message body: " << str2hex(b2s(envelope.message.body)));

    std::string content_type = b2s(envelope.message.properties.content_type);
    std::string topic = b2s(envelope.routing_key);

    LOG_DEB("message content type: " << content_type);
    if (content_type == "application/message-metadata")
    {
      LOG_DEB("Received topic metadata for topic " << topic);
      const auto* buf_compressed = static_cast<const char*>(envelope.message.body.bytes);
      const char* buf;
      decompress_buffer(buf_compressed, &buf, envelope.message.body.len);
      const auto* len_ptr = reinterpret_cast<const uint32_t*>(buf);
      const char* data_ptr = buf + 3 * sizeof(uint32_t);

      auto md5sum = std::string(data_ptr, len_ptr[0]);
      auto datatype = std::string(data_ptr + len_ptr[0], len_ptr[1]);
      auto definition = std::string(data_ptr + len_ptr[0] + len_ptr[1], len_ptr[2]);
      bool latch = buf[buf_size - 1] != 0;
      if (buf != buf_compressed)
      {
        delete[] buf;
      }

      auto info_entry = pubs_.find(topic);
      if (info_entry != pubs_.end() && info_entry->second.md5sum == md5sum && latch == info_entry->second.latch)
      {
        continue;
      }
      ros::AdvertiseOptions opts(topic + topic_suffix_, queue_size_, md5sum, datatype, definition);
      opts.latch = latch;
      auto pub = nh_.advertise(opts);
      LOG_DEB("Advertising topic " << topic << topic_suffix_);
      info_entry = pubs_.emplace(topic, pub).first;
      info_entry->second.md5sum = md5sum;
      info_entry->second.datatype = datatype;
      info_entry->second.definition = definition;
      info_entry->second.latch = latch;
      info_entry->second.msg.morph(md5sum, datatype, definition, std::string(latch ? "true" : "false"));
    }
    else
    {
      LOG_DEB("Got message data on topic " << topic);

      auto info_entry = pubs_.find(topic);
      if (info_entry == pubs_.end())
      {
        continue;
      }
      LOG_DEB("Publishing on topic " << topic << topic_suffix_);
      const auto* buf_compressed = static_cast<const char*>(envelope.message.body.bytes);
      const char* buf;
      auto size = decompress_buffer(buf_compressed, &buf, envelope.message.body.len);
      auto& msg = info_entry->second.msg;
      Wrapper wrap(buf, size);
      msg.read(wrap);
      if (buf != buf_compressed)
      {
        delete[] buf;
      }

      info_entry->second.pub.publish(msg);
    }

    amqp_destroy_envelope(&envelope);
  }
  // amqp_basic_cancel(conn, 1, NULL);
  return true;
}

void AMQPTopicReceiver::reconfigureRequest(AMQPTopicReceiver_configConfig& new_config, uint32_t level)
{
  auto lvl = static_cast<aduulm_logger::LoggerLevel>(new_config.log_level);
  _setLogLevel(lvl);
  LOG_DEB("Switched log level to " << lvl);
}

DEFINE_LOGGER_CLASS_INTERFACE_IMPLEMENTATION(AMQPTopicReceiver)

}  // namespace amqp_topic_transceiver
