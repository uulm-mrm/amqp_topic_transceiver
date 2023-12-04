#include <amqp_topic_transceiver/AMQPTopicTransceiver.h>
#include <blosc.h>
#include <aduulm_logger/aduulm_logger.hpp>
#include <uuid/uuid.h>
#include <vector>

namespace amqp_topic_transceiver
{
AMQPTopicTransceiver::AMQPTopicTransceiver(const std::string &name)
  : rclcpp_lifecycle::LifecycleNode(name)
{
  declare_parameter("log_level", 3);
  declare_parameter("server_url", "localhost:5672");
  declare_parameter("exchange", "ros_topic_transmitter");
  declare_parameter("queue_size", 1);
  declare_parameter("server_user", "guest");
  declare_parameter("server_password", "guest");
  declare_parameter("topic_suffix", "");
  declare_parameter("use_compression", true);
  declare_parameter("compression_algorithm", "zstd");
  declare_parameter("compression_level", 5);
  declare_parameter<std::vector<std::string>>("topics_to_transmit", std::vector<std::string>{});
  declare_parameter<std::vector<std::string>>("topics_to_receive", std::vector<std::string>{});

  int log_level = get_parameter("log_level").as_int();
  server_url_ = get_parameter("server_url").as_string();
  exchange_ = get_parameter("exchange").as_string();
  queue_size_ = get_parameter("queue_size").as_int();
  server_user_ = get_parameter("server_user").as_string();
  server_password_ = get_parameter("server_password").as_string();
  use_compression_ = get_parameter("use_compression").as_bool();
  topic_suffix_ = get_parameter("topic_suffix").as_string();
  auto compression_algorithm = get_parameter("compression_algorithm").as_string();
  compression_level_ = get_parameter("compression_level").as_int();
  topics_to_transmit_ = get_parameter("topics_to_transmit").as_string_array();
  topics_to_receive_ = get_parameter("topics_to_receive").as_string_array();

  // Remove empty topics (could be passed due to ROS2 launch file quirks)
  std::erase(topics_to_receive_, "");
  std::erase(topics_to_transmit_, "");

  aduulm_logger::setLogLevel(static_cast<aduulm_logger::LoggerLevel>(log_level));

  uuid_t uuid;
  uuid_generate(uuid);
  std::array<char, 37> uuid_str;
  uuid_unparse(uuid, uuid_str.data());
  uuid_ = std::string(uuid_str.data());

  blosc_init();
  auto rcode = blosc_set_compressor(compression_algorithm.c_str());
  if (rcode < 0)
  {
    LOG_ERR("Error setting compressor. Does it really exist?");
  }

  std::stringstream filter;
  filter << "NOT(sender = '" << uuid_ << "')";
  if (!topics_to_receive_.empty()) {
    filter << " AND (";
    bool first = true;
    for (const auto &topic : topics_to_receive_)
    {
      if (!first) {
        first = false;
        filter << " OR ";
      }
      filter << "topic = '" << topic << "'";
    }
    filter << ")";
  }
  client_ = std::make_shared<mrm::v2x_amqp_connector_lib::AMQPClient>(
      server_url_, exchange_, exchange_, server_user_, server_password_, filter.str());

  receiver_thread_ = std::make_shared<std::thread>([&]() -> void {
    while (true)
    {
      auto msg = client_->receive();
      if (!msg)
      {
        if (client_->is_closing())
        {
          break;
        }
        // wait for reconnection
        using namespace std::chrono_literals;
        std::this_thread::sleep_for(200ms);
        continue;
      }
      handleMessage(*msg);
    }
  });

  using namespace std::chrono_literals;
  discovery_timer_ = create_wall_timer(1000ms, std::bind(&AMQPTopicTransceiver::create_subscriptions, this));

  create_subscriptions();
}

AMQPTopicTransceiver::~AMQPTopicTransceiver()
{
  client_.reset();
  receiver_thread_->join();
}

void AMQPTopicTransceiver::create_subscriptions()
{
  LOG_INF("Timer callback");
  for (const auto& topic : topics_to_transmit_)
  {
    LOG_INF("Topic " << topic);
    if (subs_.find(topic) != subs_.end()) {
      LOG_DEB("Topic " << topic << " already subscribed");
      continue;
    }

    // borrowed this from topic_tool base_node, who borrowed it from domain bridge
    // https://github.com/ros-tooling/topic_tools/blob/main/topic_tools/src/tool_base_node.cpp
    // https://github.com/ros2/domain_bridge/blob/main/src/domain_bridge/wait_for_graph_events.hpp

    std::vector<rclcpp::TopicEndpointInfo> endpoint_info_vec = get_publishers_info_by_topic(topic);
    std::size_t num_endpoints = endpoint_info_vec.size();
    if (num_endpoints < 1U) {
      LOG_DEB("No endpoints for topic " << topic);
      continue;
    }
    auto topic_type = endpoint_info_vec[0].topic_type();

    // Initialize QoS
    rclcpp::QoS qos{10};
    // Default reliability and durability to value of first endpoint
    qos.reliability(endpoint_info_vec[0].qos_profile().reliability());
    qos.durability(endpoint_info_vec[0].qos_profile().durability());
    // Always use automatic liveliness
    qos.liveliness(rclcpp::LivelinessPolicy::Automatic);

    // Reliability and durability policies can cause trouble with enpoint matching
    // Count number of "reliable" publishers and number of "transient local" publishers
    std::size_t reliable_count = 0U;
    std::size_t transient_local_count = 0U;
    // For duration-based policies, note the largest value to ensure matching all publishers
    rclcpp::Duration max_deadline(0, 0U);
    rclcpp::Duration max_lifespan(0, 0U);
    for (const auto & info : endpoint_info_vec) {
      const auto & profile = info.qos_profile();
      if (profile.reliability() == rclcpp::ReliabilityPolicy::Reliable) {
        reliable_count++;
      }
      if (profile.durability() == rclcpp::DurabilityPolicy::TransientLocal) {
        transient_local_count++;
      }
      if (profile.deadline() > max_deadline) {
        max_deadline = profile.deadline();
      }
      if (profile.lifespan() > max_lifespan) {
        max_lifespan = profile.lifespan();
      }
    }

    // If not all publishers have a "reliable" policy, then use a "best effort" policy
    // and print a warning
    if (reliable_count > 0U && reliable_count != num_endpoints) {
      qos.best_effort();
      LOG_WARN("Some, but not all, publishers on topic " << topic << " offer 'reliable' reliability. Falling back to 'best effort' reliability in order to connect to all publishers.");
    }

    // If not all publishers have a "transient local" policy, then use a "volatile" policy
    // and print a warning
    if (transient_local_count > 0U && transient_local_count != num_endpoints) {
      qos.durability_volatile();
      LOG_WARN("Some, but not all, publishers on topic " << topic << " offer 'transient local' durability. Falling back to 'volatile' durability in order to connect to all publishers.");
    }

    qos.deadline(max_deadline);
    qos.lifespan(max_lifespan);

    LOG_INF("Subscribing to topic " << topic);
    auto sub = create_generic_subscription(
        topic,
        topic_type,
        qos,
        [this, topic](const auto& msg) { processMessage(topic, msg); });
    subs_.emplace(std::piecewise_construct, std::forward_as_tuple(topic), std::forward_as_tuple(sub, topic_type, qos));
  }
}

size_t AMQPTopicTransceiver::compress_buffer(const char* buf, const char** buf2, size_t size)
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

static size_t serialize(char *buf, size_t cnt, char value)
{
  buf[cnt] = value;
  return cnt+1;
}

static size_t serialize(char *buf, size_t cnt, size_t value)
{
  auto *buf_ = reinterpret_cast<size_t*>(&buf[cnt]);
  *buf_ = value;
  return cnt+sizeof(size_t);
}

static size_t serialize(char *buf, size_t cnt, rmw_time_t value)
{
  auto *buf_ = reinterpret_cast<uint64_t*>(&buf[cnt]);
  buf_[0] = value.sec;
  buf_[1] = value.nsec;
  return cnt+2*sizeof(size_t);
}

/* static size_t serialize(char *buf, size_t cnt, std::string value) */
/* { */
/*   memcpy(&buf[cnt], value.c_str(), value.size()); */
/*   return cnt+value.size(); */
/* } */

int AMQPTopicTransceiver::decompress_buffer(const char* buf, const char** buf2, size_t size)
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

void AMQPTopicTransceiver::processMessage(const std::string& topic,
                                          const std::shared_ptr<rclcpp::SerializedMessage>& msg)
{
  auto& info = subs_.find(topic)->second;
  const auto& topic_type = info.topic_type;
  auto now = get_clock()->now();

  if (info.metadata.empty())
  {
    const auto &qos = info.qos;
    const auto &qos_raw = qos.get_rmw_qos_profile();
    const auto history = qos_raw.history;
    const auto depth = qos_raw.depth;
    const auto reliability = qos_raw.reliability;
    const auto durability = qos_raw.durability;
    const auto& deadline = qos_raw.deadline;
    const auto& lifespan = qos_raw.lifespan;
    const auto& liveliness = qos_raw.liveliness;
    const auto& liveliness_lease = qos_raw.liveliness_lease_duration;
    const bool avoid_conventions = qos_raw.avoid_ros_namespace_conventions;
    const size_t total_info_size = 1 + 5*sizeof(size_t) + 3*2*sizeof(size_t) + 1;

    size_t buf_size = total_info_size;
    char* buf = new char[buf_size];
    char version = 1;
    size_t cnt = 0;
    cnt = serialize(buf, cnt, version);
    cnt = serialize(buf, cnt, static_cast<size_t>(history));
    cnt = serialize(buf, cnt, depth);
    cnt = serialize(buf, cnt, static_cast<size_t>(reliability));
    cnt = serialize(buf, cnt, static_cast<size_t>(durability));
    cnt = serialize(buf, cnt, deadline);
    cnt = serialize(buf, cnt, lifespan);
    cnt = serialize(buf, cnt, static_cast<size_t>(liveliness));
    cnt = serialize(buf, cnt, liveliness_lease);
    cnt = serialize(buf, cnt, static_cast<char>(avoid_conventions));
    LOG_DEB(cnt << " vs. " << buf_size);
    assert(cnt == buf_size);
    LOG_DEB("history: " << history);
    LOG_DEB("depth: " << depth);
    LOG_DEB("reliability: " << reliability);
    LOG_DEB("durability: " << durability);
    LOG_DEB("deadline: " << deadline.sec << " " << deadline.nsec);
    LOG_DEB("lifespan: " << lifespan.sec << " " << lifespan.nsec);
    LOG_DEB("liveliness: " << liveliness);
    LOG_DEB("lease duration: " << liveliness_lease.sec << " " << liveliness_lease.nsec);
    LOG_DEB("avoid: " << avoid_conventions);
    const auto *buf2 = reinterpret_cast<const size_t*>(&buf[1]);
    for (int i = 0; i < (buf_size-1)/sizeof(size_t); i++) {
      LOG_DEB(i << " -> " << buf2[i]);
    }

    const char* buf_compressed;
    size_t size_compressed = compress_buffer(buf, &buf_compressed, buf_size);

    info.metadata = std::string(buf_compressed, size_compressed);
  }

  const char *buf = reinterpret_cast<char*>(msg->get_rcl_serialized_message().buffer);
  const char *buf_compressed;
  size_t size_compressed = compress_buffer(buf, &buf_compressed, msg->size());
  LOG_DEB("Sending AMQP ROS message with body size: " << size_compressed);

  const size_t message_ttl_ms = 500;

  proton::message message;
  message.body() = proton::binary(std::string(const_cast<char*>(buf_compressed), size_compressed));
  message.properties().put("metadata", info.metadata);
  message.properties().put("topic_type", topic_type);
  message.properties().put("topic", topic);
  message.properties().put("sender", uuid_);
  message.to(topic);
  message.content_type("application/octet-string");
  message.expiry_time(proton::timestamp(proton::timestamp::now().milliseconds() + message_ttl_ms));
  message.ttl(proton::duration(message_ttl_ms));

  auto ret = client_->send(message);
  if (buf_compressed != buf)
  {
    delete[] buf_compressed;
  }
  if (!ret)
  {
    LOG_ERR("Error sending message for " << topic << "...");
    return;
  }
}

void AMQPTopicTransceiver::handleMessage(const proton::message& message)
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
