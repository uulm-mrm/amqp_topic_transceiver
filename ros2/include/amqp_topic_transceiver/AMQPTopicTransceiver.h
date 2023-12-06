#ifndef AMQP_TOPIC_TRANSCEIVER_AMQPTOPICTRANSMITTER_H
#define AMQP_TOPIC_TRANSCEIVER_AMQPTOPICTRANSMITTER_H

#include <rclcpp/node_interfaces/node_graph_interface.hpp>
#include <rclcpp/rclcpp.hpp>
#include <rclcpp/generic_publisher.hpp>
#include <rclcpp/serialized_message.hpp>
#include <rclcpp_lifecycle/lifecycle_node.hpp>

#include <thread>
#include <aduulm_logger/aduulm_logger.hpp>
#include <v2x_amqp_connector_lib/v2x_amqp_connector_lib.h>

namespace amqp_topic_transceiver
{
struct TopicSubscriberInfoContainer
{
  explicit TopicSubscriberInfoContainer(rclcpp::GenericSubscription::SharedPtr sub_,
                                        const std::string& topic_type_,
                                        const rclcpp::QoS& qos_)
    : sub(sub_), topic_type(topic_type_), qos(qos_)
  {
  }

  rclcpp::GenericSubscription::SharedPtr sub;
  std::string topic_type;
  rclcpp::QoS qos;
  std::string metadata;
  rclcpp::Time last_metadata_transmit;
};

struct TopicPublisherInfoContainer
{
  explicit TopicPublisherInfoContainer(rclcpp::GenericPublisher::SharedPtr pub_, rclcpp::QoS qos_)
    : pub(pub_), qos(qos_)
  {
  }

  rclcpp::GenericPublisher::SharedPtr pub;
  rclcpp::QoS qos;
  std::string metadata;
};

class AMQPTopicTransceiver : public rclcpp_lifecycle::LifecycleNode
{
public:
  explicit AMQPTopicTransceiver(const std::string& name);
  ~AMQPTopicTransceiver() override;

protected:
  void handleMessage(const proton::message& message);
  void processMessage(const std::string& topic, const std::shared_ptr<rclcpp::SerializedMessage>& msg);
  void create_subscriptions();
  size_t compress_buffer(const char* buf, const char** buf2, size_t size);
  int decompress_buffer(const char* buf, const char** buf2, size_t size);

  rclcpp::TimerBase::SharedPtr discovery_timer_;
  std::map<std::string, TopicSubscriberInfoContainer> subs_;
  std::map<std::string, TopicPublisherInfoContainer> pubs_;

  std::string server_url_;
  std::string uuid_;
  std::string server_user_;
  std::string server_password_;
  std::string exchange_;
  std::string topic_suffix_;
  int queue_size_;
  float metadata_retransmission_period_seconds;
  bool use_compression_;
  int compression_level_;
  std::vector<std::string> topics_to_transmit_;
  std::vector<std::string> topics_to_receive_;

  std::shared_ptr<mrm::v2x_amqp_connector_lib::AMQPClient> client_;
  std::shared_ptr<std::thread> receiver_thread_;
};

}  // namespace amqp_topic_transceiver

#endif  // AMQP_TOPIC_TRANSCEIVER_AMQPTOPICTRANSMITTER_H
