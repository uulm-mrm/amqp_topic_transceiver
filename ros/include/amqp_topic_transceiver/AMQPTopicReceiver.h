#ifndef AMQP_TOPIC_TRANSCEIVER_AMQPTOPICTRANSMITTER_H
#define AMQP_TOPIC_TRANSCEIVER_AMQPTOPICTRANSMITTER_H

#include <ros/console.h>
#include <ros/ros.h>
#include <topic_tools/shape_shifter.h>

#include <amqp.h>
#include <amqp_tcp_socket.h>

// Dynamic reconfigure and cfg
#include "aduulm_logger/aduulm_logger.hpp"
#include "amqp_topic_transceiver/AMQPTopicReceiver_configConfig.h"
#include <dynamic_reconfigure/server.h>

namespace amqp_topic_transceiver
{
struct TopicPublisherInfoContainer
{
  explicit TopicPublisherInfoContainer(const ros::Publisher& pub) : pub(pub)
  {
  }

  ros::Publisher pub;
  std::string md5sum;
  std::string datatype;
  std::string definition;
  bool latch;
  topic_tools::ShapeShifter msg;
};

class AMQPTopicReceiver
{
public:
  AMQPTopicReceiver(ros::NodeHandle, ros::NodeHandle);
  ~AMQPTopicReceiver();

  bool run();

  DEFINE_LOGGER_CLASS_INTERFACE_HEADER

private:
  void reconfigureRequest(AMQPTopicReceiver_configConfig& new_config, uint32_t level);

  std::map<std::string, TopicPublisherInfoContainer> pubs_;

  std::string suffix_;

  std::string server_url_;
  int server_port_;
  std::string server_user_;
  std::string server_password_;
  std::string exchange_;
  int queue_size_;
  std::string topic_suffix_;

  ros::NodeHandle nh_;
  ros::NodeHandle private_nh_;
  amqp_connection_state_t conn;

  boost::recursive_mutex guard_dyn_param_server_recursive_mutex_;
  std::shared_ptr<dynamic_reconfigure::Server<AMQPTopicReceiver_configConfig> > dyn_param_server_;
};
}  // namespace amqp_topic_transceiver

#endif  // AMQP_TOPIC_TRANSCEIVER_AMQPTOPICTRANSMITTER_H
