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
class AMQPTopicReceiver
{
public:
  AMQPTopicReceiver(ros::NodeHandle, ros::NodeHandle);
  ~AMQPTopicReceiver();

  bool run();

  DEFINE_LOGGER_CLASS_INTERFACE_HEADER

private:
  void reconfigureRequest(AMQPTopicReceiver_configConfig& new_config, uint32_t level);

private:
  ros::Publisher pub_;
  bool publisher_created;

  ros::NodeHandle nh_;
  ros::NodeHandle private_nh_;
  amqp_connection_state_t conn;

  dynamic_reconfigure::Server<AMQPTopicReceiver_configConfig> dyn_param_server_;
};
}  // namespace amqp_topic_transceiver

#endif  // AMQP_TOPIC_TRANSCEIVER_AMQPTOPICTRANSMITTER_H
