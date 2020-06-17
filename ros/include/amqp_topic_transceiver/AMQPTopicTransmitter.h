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

namespace amqp_topic_transceiver {
class AMQPTopicTransmitter {
public:
  AMQPTopicTransmitter(ros::NodeHandle, ros::NodeHandle);
  ~AMQPTopicTransmitter();

  DEFINE_LOGGER_CLASS_INTERFACE_HEADER

private:
  void reconfigureRequest(AMQPTopicTransmitter_configConfig &new_config,
                          uint32_t level);

private:
  void
  processMessage(const ros::MessageEvent<topic_tools::ShapeShifter> &msg_event);

  ros::Subscriber sub_;
  bool metadata_sent;

  ros::NodeHandle nh_;
  ros::NodeHandle private_nh_;
  amqp_connection_state_t conn;

  std::string last_md5sum;

  dynamic_reconfigure::Server<AMQPTopicTransmitter_configConfig>
      dyn_param_server_;
};
} // namespace amqp_topic_transceiver

#endif // AMQP_TOPIC_TRANSCEIVER_AMQPTOPICTRANSMITTER_H
