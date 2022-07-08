#include <amqp_topic_transceiver/AMQPTopicTransmitter.h>
#include <blosc.h>
#include <tuple>
#include <utility>
#include <aduulm_logger/aduulm_logger.hpp>

namespace amqp_topic_transceiver
{
AMQPTopicTransmitter::AMQPTopicTransmitter(const std::string &name)
  : rclcpp_lifecycle::LifecycleNode(name)
{
  declare_parameter("log_level", 3);
  declare_parameter("server_url", "localhost");
  declare_parameter("server_port", 5672);
  declare_parameter("exchange", "ros_topic_transmitter");
  declare_parameter("queue_size", 1);
  declare_parameter("server_user", "guest");
  declare_parameter("server_password", "guest");
  declare_parameter("use_compression", true);
  declare_parameter("compression_algorithm", "zstd");
  declare_parameter("compression_level", 5);
  declare_parameter<std::vector<std::string>>("topics", {});

  int log_level = get_parameter("log_level").as_int();
  server_url_ = get_parameter("server_url").as_string();
  server_port_ = get_parameter("server_port").as_int();
  exchange_ = get_parameter("exchange").as_string();
  queue_size_ = get_parameter("queue_size").as_int();
  server_user_ = get_parameter("server_user").as_string();
  server_password_ = get_parameter("server_password").as_string();
  use_compression_ = get_parameter("use_compression").as_bool();
  auto compression_algorithm = get_parameter("compression_algorithm").as_string();
  compression_level_ = get_parameter("compression_level").as_int();
  topics_ = get_parameter("topics").as_string_array();

  aduulm_logger::setLogLevel(static_cast<aduulm_logger::LoggerLevel>(log_level));

  blosc_init();
  auto rcode = blosc_set_compressor(compression_algorithm.c_str());
  if (rcode < 0)
  {
    LOG_ERR("Error setting compressor. Does it really exist?");
  }

  client_ = std::make_shared<AMQPClient>(
      server_url_ + ":" + std::to_string(server_port_), exchange_, server_user_, server_password_);

  container_ = std::make_shared<proton::container>(*client_);
  container_thread_ = std::make_shared<std::thread>([&]() { container_->run(); });

  using namespace std::chrono_literals;
  discovery_timer_ = create_wall_timer(1000ms, std::bind(&AMQPTopicTransmitter::create_subscriptions, this));

  create_subscriptions();
}

AMQPTopicTransmitter::~AMQPTopicTransmitter()
{
  client_->close();
  container_->stop();
  container_thread_->join();
}

void AMQPTopicTransmitter::create_subscriptions()
{
  LOG_INF("Timer callback");
  for (const auto& topic : topics_)
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
    if (num_endpoints < 1u) {
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
    std::size_t reliable_count = 0u;
    std::size_t transient_local_count = 0u;
    // For duration-based policies, note the largest value to ensure matching all publishers
    rclcpp::Duration max_deadline(0, 0u);
    rclcpp::Duration max_lifespan(0, 0u);
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
    if (reliable_count > 0u && reliable_count != num_endpoints) {
      qos.best_effort();
      LOG_WARN("Some, but not all, publishers on topic " << topic << " offer 'reliable' reliability. Falling back to 'best effort' reliability in order to connect to all publishers.");
    }

    // If not all publishers have a "transient local" policy, then use a "volatile" policy
    // and print a warning
    if (transient_local_count > 0u && transient_local_count != num_endpoints) {
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

void AMQPTopicTransmitter::processMessage(const std::string& topic,
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

}  // namespace amqp_topic_transceiver
