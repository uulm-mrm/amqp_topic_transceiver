#include <aduulm_logger/aduulm_logger.hpp>
#include <amqp_topic_transceiver/AMQPTopicReceiver.h>

int main(int argc, char** argv)
{
  ros::init(argc, argv, "AMQPTopicReceiver");

  amqp_topic_transceiver::AMQPTopicReceiver AMQPTopicReceiver_node(ros::NodeHandle(), ros::NodeHandle("~"));

  // ros::spin();
  AMQPTopicReceiver_node.run();
  return 0;
}
