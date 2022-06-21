#ifndef AMQP_TOPIC_TRANSCEIVER_AMQPTOPICTRANSMITTER_H
#define AMQP_TOPIC_TRANSCEIVER_AMQPTOPICTRANSMITTER_H

#include <rclcpp/node_interfaces/node_graph_interface.hpp>
#include <rclcpp/rclcpp.hpp>
#include <rclcpp/generic_publisher.hpp>
#include <rclcpp/serialized_message.hpp>
#include <rclcpp_lifecycle/lifecycle_node.hpp>

#include <condition_variable>
#include <proton/connection.hpp>
#include <proton/connection_options.hpp>
#include <proton/reconnect_options.hpp>
#include <proton/work_queue.hpp>
#include <proton/container.hpp>
#include <proton/message.hpp>
#include <proton/messaging_handler.hpp>
#include <proton/sender.hpp>
#include <utility>
#include <thread>
#include <aduulm_logger/aduulm_logger.hpp>

namespace amqp_topic_transceiver
{
struct TopicSubscriberInfoContainer
{
  explicit TopicSubscriberInfoContainer(rclcpp::GenericSubscription::SharedPtr sub_, const std::string &topic_type_, const rclcpp::QoS &qos_) : sub(sub_), topic_type(topic_type_), qos(qos_)
  {
  }

  rclcpp::GenericSubscription::SharedPtr sub;
  std::string topic_type;
  rclcpp::QoS qos;
  std::string metadata;
  rclcpp::Time last_metadata_transmit;
};

class AMQPClient;

class AMQPTopicTransmitter : public rclcpp_lifecycle::LifecycleNode
{
public:
  AMQPTopicTransmitter(const std::string &name);
  ~AMQPTopicTransmitter();


private:
  void processMessage(const std::string& topic, const std::shared_ptr<rclcpp::SerializedMessage> &msg);
  void create_subscriptions();
  size_t compress_buffer(const char* buf, const char** buf2, size_t size);

  rclcpp::TimerBase::SharedPtr discovery_timer_;
  std::map<std::string, TopicSubscriberInfoContainer> subs_;

  std::string server_url_;
  int server_port_;
  std::string server_user_;
  std::string server_password_;
  std::string exchange_;
  int queue_size_;
  float metadata_retransmission_period_seconds;
  bool use_compression_;
  int compression_level_;
  std::vector<std::string> topics_;

  std::shared_ptr<AMQPClient> client_;
  std::shared_ptr<proton::container> container_;
  std::shared_ptr<std::thread> container_thread_;
};

class AMQPClient : public proton::messaging_handler
{
  // Invariant
  const std::string url_;
  const std::string address_;
  const std::string user_;
  const std::string pw_;

  std::optional<proton::sender> sender_;
  // Shared by proton and user threads, protected by lock_
  std::mutex lock_;
  proton::work_queue* work_queue_;
  std::condition_variable sender_ready_;
  bool sender_connected_ = false;
  rclcpp::Clock clock_;

public:
  AMQPClient(std::string url, std::string address, std::string user, std::string pw)
    : url_(std::move(url))
    , address_(std::move(address))
    , user_(std::move(user))
    , pw_(std::move(pw))
    , work_queue_(nullptr)
  {
    LOG_DEB("AMQPClient initialized");
  }
  // Thread safe
  bool send(const proton::message& msg)
  {
    if (!sender_connected_)
    {
      LOG_WARN_THROTTLE(5., "sender not connected");
      return false;
    }
    // Copy the message, we cannot pass it by reference since it
    // will be used in another thread.
    work_queue()->add([=]() {
      if (!sender_)
      {
        LOG_DEB("sender not yet created");
        return;
      }
      LOG_DEB("sending message");
      sender_->send(msg);
    });
    return true;
  }
  // Thread safe
  void close()
  {
    if (sender_connected_)
    {
      work_queue()->add([this]() { sender_->connection().close(); });
    }
  }

private:
  proton::work_queue* work_queue()
  {
    // Wait till work_queue_ and sender_ are initialized.
    std::unique_lock<std::mutex> l(lock_);
    while (work_queue_ == nullptr)
    {
      sender_ready_.wait(l);
    }
    return work_queue_;
  }
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
    co.reconnect(ro);
    cont.connect(url_, co);
    LOG_DEB("on_container_start done");
  }
  void on_connection_open(proton::connection& conn) override
  {
    LOG_DEB("on_connection_open start");
    if (!conn.reconnected() || !sender_)
    {
      // Do initial per-connection setup here.
      // Open initial senders/receivers if needed.
      conn.open_sender(address_);
    }
    else
    {
      LOG_DEB("reconnected.");
    }
    LOG_DEB("on_connection_open done");
  }
  void on_sender_open(proton::sender& s) override
  {
    LOG_DEB("on_sender_open");
    // sender_ and work_queue_ must be set atomically
    std::lock_guard<std::mutex> l(lock_);
    sender_ = s;
    work_queue_ = &s.work_queue();
    sender_connected_ = true;
    LOG_DEB("on_sender_open done");
    sender_ready_.notify_all();
  }
  void on_error(const proton::error_condition& e) override
  {
    LOG_ERR("unexpected error: " << e);
    if (e.name() == "amqp:internal-error")
    {
      // in this case, the automatic reconnect will fail.
      // thus, we shut down the node and wait for ROS to respawn the node
      LOG_ERR("interal error, shutting down node");
      exit(1);
    }
  }
  void on_transport_close(proton::transport& tp) override
  {
    LOG_WARN("transport closed");
    // sender_->connection().open();
  }
  void on_transport_error(proton::transport& tp) override
  {
    LOG_WARN("transport error");
  }
  void on_connection_close(proton::connection& conn) override
  {
    LOG_WARN("connection closed");
    /* sender_.close(); */
  }
  void on_connection_error(proton::connection& conn) override
  {
    LOG_WARN("connection closed due to error: " << conn.error());
    /* sender_.close(); */
  }
  void on_sender_close(proton::sender& s) override
  {
    LOG_WARN("sender closed");
    /* sender_connected_ = false; */
    /* sender_.close(); */
  }
  void on_sender_error(proton::sender& s) override
  {
    LOG_WARN("sender closed with error");
    /* sender_connected_ = false; */
    /* sender_.close(); */
  }
  void on_sender_detach(proton::sender& s) override
  {
    LOG_WARN("sender detached/closed");
    /* sender_connected_ = false; */
    /* sender_.close(); */
  }
};

}  // namespace amqp_topic_transceiver

#endif  // AMQP_TOPIC_TRANSCEIVER_AMQPTOPICTRANSMITTER_H
