#ifndef AMQP_TOPIC_TRANSCEIVER_AMQPTOPICRECEIVER_H
#define AMQP_TOPIC_TRANSCEIVER_AMQPTOPICRECEIVER_H

#include <chrono>
#include <iostream>
#include <memory>

#include <rclcpp/rclcpp.hpp>
#include <rclcpp/generic_publisher.hpp>
#include <rclcpp_lifecycle/lifecycle_node.hpp>

#include <proton/connection.hpp>
#include <proton/connection_options.hpp>
#include <proton/reconnect_options.hpp>
#include <proton/container.hpp>
#include <proton/message.hpp>
#include <proton/messaging_handler.hpp>
#include <proton/receiver.hpp>
#include <condition_variable>
#include <queue>
#include <utility>
#include <thread>
#include <aduulm_logger/aduulm_logger.hpp>

namespace amqp_topic_transceiver
{
using CallbackReturn = rclcpp_lifecycle::node_interfaces::LifecycleNodeInterface::CallbackReturn;

struct TopicPublisherInfoContainer
{
  explicit TopicPublisherInfoContainer(rclcpp::GenericPublisher::SharedPtr pub_, rclcpp::QoS qos_) : pub(pub_), qos(qos_)
  {
  }

  rclcpp::GenericPublisher::SharedPtr pub;
  rclcpp::QoS qos;
  std::string metadata;
};

class AMQPClient;

class AMQPTopicReceiver : public rclcpp_lifecycle::LifecycleNode
{
public:
  AMQPTopicReceiver(const std::string &name);
  ~AMQPTopicReceiver();


private:
  int decompress_buffer(const char* buf, const char** buf2, size_t size);
  void handleMessage(const proton::message& message);

  CallbackReturn on_configure(const rclcpp_lifecycle::State& /*previous_state*/) override;
  CallbackReturn on_activate(const rclcpp_lifecycle::State& /*previous_state*/) override;
  CallbackReturn on_deactivate(const rclcpp_lifecycle::State& /*previous_state*/) override;

  std::map<std::string, TopicPublisherInfoContainer> pubs_;

  std::string suffix_;

  std::string server_url_;
  int server_port_;
  std::string server_user_;
  std::string server_password_;
  std::string exchange_;
  int queue_size_;
  std::string topic_suffix_;
  bool use_compression_;
  int decompression_buffer_size_;

  std::shared_ptr<AMQPClient> client_;
  std::shared_ptr<proton::container> container_;
  std::shared_ptr<std::thread> container_thread_;
  std::shared_ptr<std::thread> receiver_thread_;
};

class AMQPClient : public proton::messaging_handler
{
  // Invariant
  const std::string url_;
  const std::string address_;
  const std::string user_;
  const std::string pw_;

  // Shared by proton and user threads, protected by lock_
  std::mutex lock_;
  std::queue<proton::message> messages_;
  std::condition_variable messages_ready_;
  std::optional<proton::receiver> receiver_;
  bool closing_ = false;

public:
  AMQPClient(std::string url, std::string address, std::string user, std::string pw)
    : url_(std::move(url)), address_(std::move(address)), user_(std::move(user)), pw_(std::move(pw))
  {
    LOG_DEB("AMQPClient initialized");
  }
  // Thread safe
  std::optional<proton::message> receive()
  {
    std::unique_lock<std::mutex> l(lock_);
    while (messages_.empty() && !closing_)
    {
      messages_ready_.wait(l);
    }
    if (closing_)
    {
      return {};
    }
    auto msg = std::move(messages_.front());
    messages_.pop();
    return { msg };
  }
  // Thread safe
  void close()
  {
    std::lock_guard<std::mutex> l(lock_);
    closing_ = true;
    messages_ready_.notify_all();
  }

private:
  std::optional<proton::connection> connection_;

  // == messaging_handler overrides, only called in proton handler thread
  // Note: this example creates a connection when the container starts.
  // To create connections after the container has started, use
  // container::connect().
  // See @ref multithreaded_client_flow_control.cpp for an example.
  void on_container_start(proton::container& cont) override
  {
    proton::reconnect_options ro;
    proton::connection_options co;
    if (user_ == "anonymous")
    {
      co.sasl_enabled(true);
      co.sasl_allowed_mechs("ANONYMOUS");
      co.sasl_allow_insecure_mechs(true);
    }
    else if (!user_.empty())
    {
      co.sasl_enabled(true);
      co.sasl_allow_insecure_mechs(true);
      co.user(user_);
      co.password(pw_);
    }
    else
    {
      co.sasl_enabled(false);
    }
    ro.max_delay(proton::duration(2000));
    co.idle_timeout(proton::duration(5000));
    co.reconnect(ro);
    connection_ = cont.connect(url_, co);
    LOG_DEB("on_container_start done");
  }
  void open_receivers()
  {
    if (!connection_) {
      LOG_ERR("No connection, but trying to create receiver!");
      return;
    }
    LOG_INF("Opening receiver...");
    connection_->open_receiver(address_);
  }
  void on_connection_open(proton::connection& conn) override
  {
    LOG_DEB("on_connection_open start");
    if (!conn.reconnected() || !receiver_)
    {
      open_receivers();
    }
    else
    {
      LOG_DEB("reconnected.");
    }
    LOG_DEB("on_connection_open done");
  }
  void on_message(proton::delivery& dlv, proton::message& msg) override
  {
    LOG_DEB("got a message");
    std::lock_guard<std::mutex> l(lock_);
    messages_.push(msg);
    messages_ready_.notify_all();
  }
  void on_error(const proton::error_condition& e) override
  {
    LOG_ERR("unexpected error: " << e);
    LOG_ERR("error name: " << e.name());
    if (e.name() == "amqp:internal-error")
    {
      // in this case, the automatic reconnect will fail.
      // thus, we shut down the node and wait for ROS to respawn the node
      LOG_ERR("interal error");
      exit(1);
    }
  }
  void on_transport_close(proton::transport& tp) override
  {
    LOG_WARN("transport closed");
    if (receiver_) {
      receiver_->close();
    }
  }
  void on_transport_error(proton::transport& tp) override
  {
    LOG_WARN("transport error");
    if (receiver_) {
      receiver_->close();
    }
  }
  void on_connection_close(proton::connection& conn) override
  {
    LOG_WARN("connection closed");
    if (receiver_) {
      receiver_->close();
    }
  }
  void on_connection_error(proton::connection& conn) override
  {
    LOG_WARN("connection closed due to error: " << conn.error());
    if (receiver_) {
      receiver_->close();
    }
  }
  void on_receiver_open(proton::receiver& r) override
  {
    LOG_WARN("receiver open");
    receiver_ = r;
  }
  void on_receiver_close(proton::receiver& r) override
  {
    LOG_WARN("receiver closed");
    using namespace std::chrono_literals;
    std::this_thread::sleep_for(500ms);
    open_receivers();
  }
  void on_receiver_error(proton::receiver& r) override
  {
    LOG_WARN("receiver closed with error");
  }
  void on_receiver_detach(proton::receiver& r) override
  {
    LOG_WARN("receiver detached/closed");
    if (receiver_) {
      receiver_->close();
    }
  }
};
}  // namespace amqp_topic_transceiver

#endif  // AMQP_TOPIC_TRANSCEIVER_AMQPTOPICRECEIVER_H
