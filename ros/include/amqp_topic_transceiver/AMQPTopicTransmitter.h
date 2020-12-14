#ifndef AMQP_TOPIC_TRANSCEIVER_AMQPTOPICTRANSMITTER_H
#define AMQP_TOPIC_TRANSCEIVER_AMQPTOPICTRANSMITTER_H

#include <ros/console.h>
#include <ros/ros.h>
#include <topic_tools/shape_shifter.h>

#include <amqp.h>
#include <amqp_tcp_socket.h>

// Dynamic reconfigure and cfg
#include "aduulm_logger/aduulm_logger.hpp"
#include "amqp_topic_transceiver/AMQPTopicTransmitter_configConfig.h"
#include <dynamic_reconfigure/server.h>

namespace amqp_topic_transceiver
{
struct TopicSubscriberInfoContainer
{
  explicit TopicSubscriberInfoContainer(const ros::Subscriber& sub) : sub(sub)
  {
  }

  ros::Subscriber sub;
  bool metadata_sent{ false };
  std::string last_md5sum;
  ros::Time last_metadata_transmit;
};

class AMQPTopicTransmitter
{
public:
  AMQPTopicTransmitter(ros::NodeHandle, ros::NodeHandle);
  ~AMQPTopicTransmitter();

  DEFINE_LOGGER_CLASS_INTERFACE_HEADER

private:
  void reconfigureRequest(AMQPTopicTransmitter_configConfig& new_config, uint32_t level);

  void processMessage(const std::string& topic, const ros::MessageEvent<topic_tools::ShapeShifter>& msg_event);

  std::map<std::string, TopicSubscriberInfoContainer> subs_;

  ros::NodeHandle nh_;
  ros::NodeHandle private_nh_;
  amqp_connection_state_t conn;

  std::string server_url_;
  int server_port_;
  std::string server_user_;
  std::string server_password_;
  std::string exchange_;
  int queue_size_;
  float metadata_retransmission_period_seconds;

  boost::recursive_mutex guard_dyn_param_server_recursive_mutex_;
  std::shared_ptr<dynamic_reconfigure::Server<AMQPTopicTransmitter_configConfig> > dyn_param_server_;
};
}  // namespace amqp_topic_transceiver

#endif  // AMQP_TOPIC_TRANSCEIVER_AMQPTOPICTRANSMITTER_H
