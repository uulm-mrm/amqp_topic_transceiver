#include <amqp_topic_transceiver/AMQPTopicReceiver.h>
#include <topic_tools/shape_shifter.h>

void die(const char* fmt, ...)
{
  va_list ap;
  va_start(ap, fmt);
  vfprintf(stderr, fmt, ap);
  va_end(ap);
  fprintf(stderr, "\n");
  exit(1);
}

void die_on_error(int x, char const* context)
{
  if (x < 0)
  {
    fprintf(stderr, "%s: %s\n", context, amqp_error_string2(x));
    exit(1);
  }
}

void die_on_amqp_error(amqp_rpc_reply_t x, char const* context)
{
  switch (x.reply_type)
  {
    case AMQP_RESPONSE_NORMAL:
      return;

    case AMQP_RESPONSE_NONE:
      fprintf(stderr, "%s: missing RPC reply type!\n", context);
      break;

    case AMQP_RESPONSE_LIBRARY_EXCEPTION:
      fprintf(stderr, "%s: %s\n", context, amqp_error_string2(x.library_error));
      break;

    case AMQP_RESPONSE_SERVER_EXCEPTION:
      switch (x.reply.id)
      {
        case AMQP_CONNECTION_CLOSE_METHOD:
        {
          auto* m = static_cast<amqp_connection_close_t*>(x.reply.decoded);
          fprintf(stderr,
                  "%s: server connection error %uh, message: %.*s\n",
                  context,
                  m->reply_code,
                  static_cast<int>(m->reply_text.len),
                  static_cast<char*>(m->reply_text.bytes));
          break;
        }
        case AMQP_CHANNEL_CLOSE_METHOD:
        {
          auto* m = static_cast<amqp_channel_close_t*>(x.reply.decoded);
          fprintf(stderr,
                  "%s: server channel error %uh, message: %.*s\n",
                  context,
                  m->reply_code,
                  static_cast<int>(m->reply_text.len),
                  static_cast<char*>(m->reply_text.bytes));
          break;
        }
        default:
          fprintf(stderr, "%s: unknown server error, method id 0x%08X\n", context, x.reply.id);
          break;
      }
      break;
  }

  exit(1);
}

DEFINE_LOGGER_VARIABLES

namespace amqp_topic_transceiver
{
AMQPTopicReceiver::AMQPTopicReceiver(ros::NodeHandle node_handle, ros::NodeHandle private_node_handle)
  : private_nh_(private_node_handle), nh_(node_handle)
{
  _setStreamName(ros::this_node::getName());
  _initLogger();
  _setLogLevel(aduulm_logger::LoggerLevel::Debug);

  private_nh_.param<std::string>("server_url", server_url_, "localhost");
  private_nh_.param<int>("server_port", server_port_, 5672);
  private_nh_.param<std::string>("exchange", exchange_, "ros_topic_transmitter");
  private_nh_.param<int>("queue_size", queue_size_, 1);
  private_nh_.param<std::string>("server_user", server_user_, "guest");
  private_nh_.param<std::string>("server_password", server_password_, "guest");
  private_nh_.param<std::string>("topic_suffix", topic_suffix_, "");

  dyn_param_server_.reset(new dynamic_reconfigure::Server<amqp_topic_transceiver::AMQPTopicReceiver_configConfig>(
      guard_dyn_param_server_recursive_mutex_, private_nh_));
  dyn_param_server_->setCallback([this](auto&& config, auto&& level) { reconfigureRequest(config, level); });

  amqp_socket_t* socket = nullptr;

  conn = amqp_new_connection();

  socket = amqp_tcp_socket_new(conn);
  if (socket == nullptr)
  {
    die("creating TCP socket");
  }

  int status = amqp_socket_open(socket, server_url_.c_str(), server_port_);
  if (status != 0)
  {
    die("opening TCP socket");
  }

  die_on_amqp_error(amqp_login(conn,
                               "/",
                               AMQP_DEFAULT_MAX_CHANNELS,
                               AMQP_DEFAULT_FRAME_SIZE,
                               0,
                               AMQP_SASL_METHOD_PLAIN,
                               server_user_.c_str(),
                               server_password_.c_str()),
                    "Logging in");
  amqp_channel_open(conn, 1);
  die_on_amqp_error(amqp_get_rpc_reply(conn), "Opening channel");

  amqp_exchange_declare(
      conn, 1, amqp_cstring_bytes(exchange_.c_str()), amqp_cstring_bytes("fanout"), 0, 0, 0, 0, amqp_empty_table);
}

AMQPTopicReceiver::~AMQPTopicReceiver()
{
  die_on_amqp_error(amqp_channel_close(conn, 1, AMQP_REPLY_SUCCESS), "Closing channel");
  die_on_amqp_error(amqp_connection_close(conn, AMQP_REPLY_SUCCESS), "Closing connection");
  die_on_error(amqp_destroy_connection(conn), "Ending connection");
}

class Wrapper
{
public:
  Wrapper(const char* buf, int size)
  {
    this->buf = buf;
    this->size = size;
  }

  [[nodiscard]] int getLength() const
  {
    return size;
  }
  const char* getData()
  {
    return buf;
  }

private:
  const char* buf;
  int size;
};

bool AMQPTopicReceiver::run()
{
  const char* exchange = exchange_.c_str();
  const char* routingkey = "";
  amqp_bytes_t queuename = amqp_cstring_bytes("");

  auto* res = amqp_queue_declare(conn, 1, queuename, 0, 0, 0, 1, amqp_empty_table);
  queuename = res->queue;
  die_on_amqp_error(amqp_get_rpc_reply(conn), "Declaring queue");

  amqp_queue_bind(conn, 1, queuename, amqp_cstring_bytes(exchange), amqp_cstring_bytes(""), amqp_empty_table);
  die_on_amqp_error(amqp_get_rpc_reply(conn), "Binding queue to exchange");

  amqp_basic_consume(conn, 1, queuename, amqp_empty_bytes, 0, 1, 0, amqp_empty_table);
  die_on_amqp_error(amqp_get_rpc_reply(conn), "Consuming");
  LOG_DEB("Start consuming");

  while (ros::ok() && !ros::isShuttingDown())
  {
    ros::spinOnce();

    amqp_envelope_t envelope;
    amqp_maybe_release_buffers(conn);

    struct timeval timeout = { .tv_sec = 0, .tv_usec = 200 };
    auto res = amqp_consume_message(conn, &envelope, &timeout, 0);

    if (AMQP_RESPONSE_NORMAL != res.reply_type)
    {
      continue;
    }
    size_t buf_size = envelope.message.body.len;
    LOG_DEB("Received a message with " << buf_size << " bytes!");

    // printf("Delivery %u, exchange %.*s routingkey %.*s\n",
    //        (unsigned)envelope.delivery_tag, (int)envelope.exchange.len,
    //        (char *)envelope.exchange.bytes, (int)envelope.routing_key.len,
    //        (char *)envelope.routing_key.bytes);
    // printf("----\n");
    //
    auto& ct = envelope.message.properties.content_type;
    std::string content_type = std::string(static_cast<char*>(ct.bytes), ct.len);
    auto& t = envelope.routing_key;
    std::string topic = std::string(static_cast<char*>(t.bytes), t.len);

    LOG_DEB("message content type: " << content_type);
    if (content_type == "application/message-metadata")
    {
      LOG_DEB("Received topic metadata for topic " << topic);
      const char* buf = static_cast<const char*>(envelope.message.body.bytes);
      const auto* len_ptr = reinterpret_cast<const uint32_t*>(buf);
      const char* data_ptr = buf + 3 * sizeof(uint32_t);
      // std::cout << len_ptr[0] << std::endl;
      // std::cout << len_ptr[1] << std::endl;
      // std::cout << len_ptr[2] << std::endl;

      auto md5sum = std::string(data_ptr, len_ptr[0]);
      auto datatype = std::string(data_ptr + len_ptr[0], len_ptr[1]);
      auto definition = std::string(data_ptr + len_ptr[0] + len_ptr[1], len_ptr[2]);
      bool latch = buf[buf_size - 1] != 0;

      auto info_entry = pubs_.find(topic);
      if (info_entry != pubs_.end() && info_entry->second.md5sum == md5sum && latch == info_entry->second.latch)
      {
        continue;
      }
      ros::AdvertiseOptions opts(topic + topic_suffix_, queue_size_, md5sum, datatype, definition);
      opts.latch = latch;
      auto pub = nh_.advertise(opts);
      LOG_DEB("Advertising topic " << topic << topic_suffix_);
      info_entry = pubs_.emplace(topic, pub).first;
      info_entry->second.md5sum = md5sum;
      info_entry->second.datatype = datatype;
      info_entry->second.definition = definition;
      info_entry->second.latch = latch;
      info_entry->second.msg.morph(md5sum, datatype, definition, std::string(latch ? "true" : "false"));
    }
    else
    {
      // std::cout << md5sum << " | " << datatype << " | " << def << " | " <<
      // (int)latch << std::endl;

      // printf("Delivery %u, exchange %.*s routingkey %.*s\n",
      //        (unsigned)envelope.delivery_tag, (int)envelope.exchange.len,
      //        (char *)envelope.exchange.bytes, (int)envelope.routing_key.len,
      //        (char *)envelope.routing_key.bytes);
      // printf("----\n");

      // std::cout << std::string((char *)envelope.message.body.bytes,
      // envelope.message.body.len) << std::endl;
      LOG_DEB("Got message data on topic " << topic);

      auto info_entry = pubs_.find(topic);
      if (info_entry == pubs_.end())
      {
        continue;
      }
      LOG_DEB("Publishing on topic " << topic << topic_suffix_);
      auto* buf = static_cast<char*>(envelope.message.body.bytes);
      auto size = envelope.message.body.len;
      auto& msg = info_entry->second.msg;
      Wrapper wrap(buf, size);
      msg.read(wrap);

      info_entry->second.pub.publish(msg);
    }

    amqp_destroy_envelope(&envelope);
  }
  // amqp_basic_cancel(conn, 1, NULL);
  return true;
}

void AMQPTopicReceiver::reconfigureRequest(AMQPTopicReceiver_configConfig& new_config, uint32_t level)
{
  auto lvl = static_cast<aduulm_logger::LoggerLevel>(new_config.log_level);
  _setLogLevel(lvl);
  LOG_DEB("Switched log level to " << lvl);
}

DEFINE_LOGGER_CLASS_INTERFACE_IMPLEMENTATION(AMQPTopicReceiver)

}  // namespace amqp_topic_transceiver
