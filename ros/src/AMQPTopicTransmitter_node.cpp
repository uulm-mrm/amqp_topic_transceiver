#include <aduulm_logger/aduulm_logger.hpp>
#include <amqp_topic_transceiver/AMQPTopicTransmitter.h>

int main(int argc, char** argv)
{
  ros::init(argc, argv, "AMQPTopicTransmitter");

  amqp_topic_transceiver::AMQPTopicTransmitter AMQPTopicTransmitter_node(ros::NodeHandle(), ros::NodeHandle("~"));

  ros::spin();
  return 0;
}
