#include <amqp_topic_transceiver/AMQPTopicReceiver.h>
#include <blosc.h>
#include <rcl/types.h>
#include <rclcpp/serialized_message.hpp>
#include <rmw/types.h>
#include <aduulm_logger/aduulm_logger.hpp>


namespace amqp_topic_transceiver
{
AMQPTopicReceiver::AMQPTopicReceiver(const std::string& name) : rclcpp_lifecycle::LifecycleNode(std::move(name))
{
  declare_parameter("log_level", 3);
  declare_parameter("server_url", "localhost");
  declare_parameter("server_port", 5672);
  declare_parameter("exchange", "ros_topic_transmitter");
  declare_parameter("queue_size", 1);
  declare_parameter("server_user", "guest");
  declare_parameter("server_password", "guest");
  declare_parameter("topic_suffix", "");
  declare_parameter("use_compression", true);
  declare_parameter("decompression_buffer_size", 50 * 1024);

  int log_level = get_parameter("log_level").as_int();
  server_url_ = get_parameter("server_url").as_string();
  server_port_ = get_parameter("server_port").as_int();
  exchange_ = get_parameter("exchange").as_string();
  queue_size_ = get_parameter("queue_size").as_int();
  server_user_ = get_parameter("server_user").as_string();
  server_password_ = get_parameter("server_password").as_string();
  topic_suffix_ = get_parameter("topic_suffix").as_string();
  use_compression_ = get_parameter("use_compression").as_bool();

  aduulm_logger::setLogLevel(static_cast<aduulm_logger::LoggerLevel>(log_level));
  LOG_DEB("Log level set to " << log_level);

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

CallbackReturn AMQPTopicReceiver::on_configure(const rclcpp_lifecycle::State& /*previous_state*/)
{
  return CallbackReturn::SUCCESS;
}

CallbackReturn AMQPTopicReceiver::on_activate(const rclcpp_lifecycle::State& /*previous_state*/)
{
  /* for (auto &info : pubs_) { */
  /*   info.second.pub->on_activate(); */
  /* } */

  return CallbackReturn::SUCCESS;
}

CallbackReturn AMQPTopicReceiver::on_deactivate(const rclcpp_lifecycle::State& /*previous_state*/)
{
  /* for (auto &info : pubs_) { */
  /*   info.second.pub->on_deactivate(); */
  /* } */

  return CallbackReturn::SUCCESS;
}

int AMQPTopicReceiver::decompress_buffer(const char* buf, const char** buf2, size_t size)
{
  if (!use_compression_)
  {
    *buf2 = buf;
    return size;
  }
  LOG_DEB("Decompression input size: " << size);
  size_t nbytes;
  int ret = blosc_cbuffer_validate(buf, size, &nbytes);
  if (ret >= 0) {
    char *tmp = new char[nbytes];
    *buf2 = tmp;
    auto dsize = blosc_decompress(buf, tmp, nbytes);
    assert(dsize <= nbytes);
    LOG_DEB("Decompression output size: " << dsize);
    if (dsize >= 0) {
      return dsize;
    }
    LOG_ERR("Decompression error.  Error code: " << dsize);
    delete[] tmp;
  }
  LOG_DEB("Assuming data was not compressed in the first place.");
  *buf2 = buf;
  return size;
}

static size_t deserialize(const char *buf, size_t cnt, char& value)
{
  value = buf[cnt];
  return cnt+1;
}

static size_t deserialize(const char *buf, size_t cnt, size_t& value)
{
  const auto *buf_ = reinterpret_cast<const size_t*>(&buf[cnt]);
  value = *buf_;
  return cnt+sizeof(size_t);
}

static size_t deserialize(const char *buf, size_t cnt, rmw_time_t& value)
{
  const auto *buf_ = reinterpret_cast<const uint64_t*>(&buf[cnt]);
  value.sec = buf_[0];
  value.nsec = buf_[1];
  return cnt+2*sizeof(size_t);
}

/* static size_t deserialize(const char *buf, size_t cnt, std::string &value, size_t size) */
/* { */
/*   value = std::string(&buf[cnt], size); */
/*   return cnt+value.size(); */
/* } */

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
  LOG_DEB("Got body content");

  auto info_entry = pubs_.find(topic);
  const std::string& metadata = proton::get<std::string>(message.properties().get("metadata"));
  const std::string& topic_type = proton::get<std::string>(message.properties().get("topic_type"));
  LOG_DEB("topic_type: " << topic_type);

  if (info_entry == pubs_.end() || metadata != info_entry->second.metadata) {
    LOG_DEB("Received topic metadata for topic " << topic);
    size_t buf_size = metadata.size();
    const auto* buf_compressed = static_cast<const char*>(metadata.c_str());
    const char* buf;
    decompress_buffer(buf_compressed, &buf, buf_size);
    char version;
    size_t cnt = 0;
    size_t history;
    size_t reliability;
    size_t durability;
    size_t liveliness;
    char avoid_conventions;
    rmw_qos_profile_t qos;
    cnt = deserialize(buf, cnt, version);
    LOG_DEB("version: " << static_cast<int>(version));
    assert(version == 1);
    cnt = deserialize(buf, cnt, history);
    cnt = deserialize(buf, cnt, qos.depth);
    cnt = deserialize(buf, cnt, reliability);
    cnt = deserialize(buf, cnt, durability);
    cnt = deserialize(buf, cnt, qos.deadline);
    cnt = deserialize(buf, cnt, qos.lifespan);
    cnt = deserialize(buf, cnt, liveliness);
    cnt = deserialize(buf, cnt, qos.liveliness_lease_duration);
    cnt = deserialize(buf, cnt, avoid_conventions);
    const size_t total_info_size = 1 + 5*sizeof(size_t) + 3*2*sizeof(size_t) + 1;
    assert(cnt == total_info_size);

    qos.history = static_cast<typeof(qos.history)>(history);
    qos.reliability = static_cast<typeof(qos.reliability)>(reliability);
    qos.durability = static_cast<typeof(qos.durability)>(durability);
    qos.liveliness = static_cast<typeof(qos.liveliness)>(liveliness);
    qos.avoid_ros_namespace_conventions = static_cast<bool>(avoid_conventions);
    LOG_DEB("history: " << history);
    LOG_DEB("depth: " << qos.depth);
    LOG_DEB("reliability: " << reliability);
    LOG_DEB("durability: " << durability);
    LOG_DEB("deadline: " << qos.deadline.sec << " " << qos.deadline.nsec);
    LOG_DEB("lifespan: " << qos.lifespan.sec << " " << qos.lifespan.nsec);
    LOG_DEB("liveliness: " << liveliness);
    LOG_DEB("lease duration: " << qos.liveliness_lease_duration.sec << " " << qos.liveliness_lease_duration.nsec);
    LOG_DEB("avoid: " << qos.avoid_ros_namespace_conventions);
    rclcpp::QoS qos_(1);
    qos_.get_rmw_qos_profile() = qos;
    if (buf != buf_compressed) {
      delete[] buf;
    }

    if (info_entry != pubs_.end() && qos_ == info_entry->second.qos)
    {
      return;
    }
    if (info_entry != pubs_.end())
    {
      info_entry->second.pub.reset();
    }
    auto pub = create_generic_publisher(topic + topic_suffix_, topic_type, qos_);
    info_entry = pubs_.emplace(std::piecewise_construct, std::forward_as_tuple(topic), std::forward_as_tuple(pub, qos_)).first;
    info_entry->second.metadata = metadata;
  }

  size_t buf_size = content.size();

  LOG_DEB("Got message data on topic " << topic << " with type " << topic_type);
  LOG_DEB("Publishing on topic " << topic << topic_suffix_);
  const auto* buf_compressed = static_cast<const char*>(content.c_str());
  char* buf;
  auto size = decompress_buffer(buf_compressed, (const char **)&buf, buf_size);
  LOG_DEB("Received a message with " << buf_size << " / " << size << " bytes!");
  if (buf == buf_compressed) {
    // we need to copy the buffer in this case because SerializedMessage will attempt to free it
    char *tmp = new char[size];
    memcpy(tmp, buf_compressed, size);
    buf = tmp;
  }
  rcl_serialized_message_t msg_;
  msg_.buffer = reinterpret_cast<unsigned char*>(buf);
  msg_.buffer_length = size;
  msg_.buffer_capacity = size;
  msg_.allocator = rcutils_get_default_allocator();

  /* std::stringstream ss2; */
  /* for (int i = 10; i > 0; i--) { */
  /*   ss2 << (int)msg_.buffer[size-i] << " "; */
  /* } */
  /* LOG_DEB("Last 10 bytes: " << ss2.str()); */
  /* LOG_DEB("Last byte: " << (int)msg_.buffer[size-1]); */

  // Sanity check: last byte should be a zero for ROS messages
  if (msg_.buffer[size-1] != 0) {
    throw std::runtime_error("Decoding result invalid!");
  }

  // SerializedMessage should take care of deleting the buffer
  rclcpp::SerializedMessage msg(std::move(msg_));

  info_entry->second.pub->publish(msg);
}

}  // namespace amqp_topic_transceiver
