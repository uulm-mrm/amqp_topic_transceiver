#include <amqp_topic_transceiver/AMQPTopicTransceiver.h>
#include <aduulm_logger/aduulm_logger.hpp>

DEFINE_LOGGER_VARIABLES

int main(int argc, char** argv)
{
  rclcpp::init(argc, argv);

  auto node = std::make_shared<amqp_topic_transceiver::AMQPTopicTransceiver>("AMQPTopicTransceiver");

  rclcpp::executors::SingleThreadedExecutor exe;
  exe.add_node(node->get_node_base_interface());

  exe.spin();

  rclcpp::shutdown();
  return 0;
}
