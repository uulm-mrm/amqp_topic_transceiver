#include <amqp_topic_transceiver/AMQPTopicReceiver.h>
#include <topic_tools/shape_shifter.h>
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

  client_ = std::make_shared<AMQPClient>(
      server_url_ + ":" + std::to_string(server_port_), exchange_, server_user_, server_password_);

  container_ = std::make_shared<proton::container>(*client_);
  container_thread_ = std::make_shared<std::thread>([&]() { container_->run(); });

  receiver_thread_ = std::make_shared<std::thread>([&]() {
    while (true)
    {
      auto msg = client_->receive();
      if (!msg)
      {
        break;
      }
      // std::cout << "received \"" << msg->body() << '"' << std::endl;
      handleMessage(*msg);
    }
  });
}

AMQPTopicReceiver::~AMQPTopicReceiver()
{
  client_->close();
  container_->stop();
  receiver_thread_->join();
  container_thread_->join();
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

void AMQPTopicReceiver::handleMessage(const proton::message& message)
{
  LOG_DEB("got a valid message");
  auto body_type = message.body().type();
  auto topic = message.to();
  auto content_type = message.content_type();
  LOG_DEB("Got message for " << topic << ", encoding type " << body_type);

  std::string content;
  if (body_type == proton::BINARY)
  {
    proton::binary content_bin;
    proton::get(message.body(), content_bin);
    content = content_bin;
  }
  else if (body_type == proton::STRING)
  {
    proton::get(message.body(), content);
  }
  else
  {
    LOG_ERR_THROTTLE(5.0, "Got unknown body encoding " << body_type << "! Skipping message!");
    return;
  }

  size_t buf_size = content.size();
  LOG_DEB("Received a message with " << buf_size << " bytes!");

  LOG_DEB("message content type: " << content_type);
  if (content_type == "application/message-metadata")
  {
    LOG_DEB("Received topic metadata for topic " << topic);
    const auto* buf_compressed = static_cast<const char*>(content.c_str());
    const char* buf;
    decompress_buffer(buf_compressed, &buf, buf_size);
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
      return;
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
      return;
    }
    LOG_DEB("Publishing on topic " << topic << topic_suffix_);
    const auto* buf_compressed = static_cast<const char*>(content.c_str());
    const char* buf;
    auto size = decompress_buffer(buf_compressed, &buf, buf_size);
    auto& msg = info_entry->second.msg;
    Wrapper wrap(buf, size);
    msg.read(wrap);
    if (buf != buf_compressed)
    {
      delete[] buf;
    }

    info_entry->second.pub.publish(msg);
  }
}

void AMQPTopicReceiver::reconfigureRequest(AMQPTopicReceiver_configConfig& new_config, uint32_t level)
{
  auto lvl = static_cast<aduulm_logger::LoggerLevel>(new_config.log_level);
  _setLogLevel(lvl);
  LOG_DEB("Switched log level to " << lvl);
}

DEFINE_LOGGER_CLASS_INTERFACE_IMPLEMENTATION(AMQPTopicReceiver)

}  // namespace amqp_topic_transceiver
