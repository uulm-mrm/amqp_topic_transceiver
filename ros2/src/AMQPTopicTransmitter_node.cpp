#include <amqp_topic_transceiver/AMQPTopicTransmitter.h>
#include <aduulm_logger/aduulm_logger.hpp>

DEFINE_LOGGER_VARIABLES

int main(int argc, char** argv)
{
  rclcpp::init(argc, argv);

  auto node = std::make_shared<amqp_topic_transceiver::AMQPTopicTransmitter>("AMQPTopicTransmitter");

  using CallbackReturn = rclcpp_lifecycle::node_interfaces::LifecycleNodeInterface::CallbackReturn;
  CallbackReturn configure_return;
  node->configure(configure_return);
  if (CallbackReturn::SUCCESS == configure_return)
  {
    node->activate();
  }

  rclcpp::executors::SingleThreadedExecutor exe;
  exe.add_node(node->get_node_base_interface());

  exe.spin();

  rclcpp::shutdown();
  return 0;
}
