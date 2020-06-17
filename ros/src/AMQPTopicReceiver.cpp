#include <amqp_topic_transceiver/AMQPTopicReceiver.h>
#include <topic_tools/shape_shifter.h>

void die(const char *fmt, ...) {
  va_list ap;
  va_start(ap, fmt);
  vfprintf(stderr, fmt, ap);
  va_end(ap);
  fprintf(stderr, "\n");
  exit(1);
}

void die_on_error(int x, char const *context) {
  if (x < 0) {
    fprintf(stderr, "%s: %s\n", context, amqp_error_string2(x));
    exit(1);
  }
}

void die_on_amqp_error(amqp_rpc_reply_t x, char const *context) {
  switch (x.reply_type) {
  case AMQP_RESPONSE_NORMAL:
    return;

  case AMQP_RESPONSE_NONE:
    fprintf(stderr, "%s: missing RPC reply type!\n", context);
    break;

  case AMQP_RESPONSE_LIBRARY_EXCEPTION:
    fprintf(stderr, "%s: %s\n", context, amqp_error_string2(x.library_error));
    break;

  case AMQP_RESPONSE_SERVER_EXCEPTION:
    switch (x.reply.id) {
    case AMQP_CONNECTION_CLOSE_METHOD: {
      amqp_connection_close_t *m = (amqp_connection_close_t *)x.reply.decoded;
      fprintf(stderr, "%s: server connection error %uh, message: %.*s\n",
              context, m->reply_code, (int)m->reply_text.len,
              (char *)m->reply_text.bytes);
      break;
    }
    case AMQP_CHANNEL_CLOSE_METHOD: {
      amqp_channel_close_t *m = (amqp_channel_close_t *)x.reply.decoded;
      fprintf(stderr, "%s: server channel error %uh, message: %.*s\n", context,
              m->reply_code, (int)m->reply_text.len,
              (char *)m->reply_text.bytes);
      break;
    }
    default:
      fprintf(stderr, "%s: unknown server error, method id 0x%08X\n", context,
              x.reply.id);
      break;
    }
    break;
  }

  exit(1);
}

DEFINE_LOGGER_VARIABLES

namespace amqp_topic_transceiver {

AMQPTopicReceiver::AMQPTopicReceiver(ros::NodeHandle node_handle,
                                     ros::NodeHandle private_node_handle)
    : private_nh_(private_node_handle), dyn_param_server_(private_nh_),
      nh_(node_handle), publisher_created(false) {
  _setStreamName(ros::this_node::getName());
  _initLogger();

  const char *hostname = "localhost";
  int port = 5672;

  amqp_socket_t *socket = NULL;

  conn = amqp_new_connection();

  socket = amqp_tcp_socket_new(conn);
  if (!socket) {
    die("creating TCP socket");
  }

  int status = amqp_socket_open(socket, hostname, port);
  if (status) {
    die("opening TCP socket");
  }

  die_on_amqp_error(amqp_login(conn, "/", 0, 131072, 0, AMQP_SASL_METHOD_PLAIN,
                               "guest", "guest"),
                    "Logging in");
  amqp_channel_open(conn, 1);
  die_on_amqp_error(amqp_get_rpc_reply(conn), "Opening channel");
}

AMQPTopicReceiver::~AMQPTopicReceiver() {
  die_on_amqp_error(amqp_channel_close(conn, 1, AMQP_REPLY_SUCCESS),
                    "Closing channel");
  die_on_amqp_error(amqp_connection_close(conn, AMQP_REPLY_SUCCESS),
                    "Closing connection");
  die_on_error(amqp_destroy_connection(conn), "Ending connection");
}

class Wrapper {
public:
  Wrapper(const char *buf, int size) {
    this->buf = buf;
    this->size = size;
  }

  int getLength() { return size; }
  const char *getData() { return buf; }

private:
  const char *buf;
  int size;
};

bool AMQPTopicReceiver::run() {
  std::string md5sum;
  std::string datatype;
  std::string def;
  bool latch;
  {
    amqp_bytes_t queuename = amqp_cstring_bytes("hello_metadata");

    {
      amqp_queue_declare_ok_t *r =
          amqp_queue_declare(conn, 1, queuename, 0, 0, 0, 1, amqp_empty_table);
      die_on_amqp_error(amqp_get_rpc_reply(conn), "Declaring queue");
    }

    const char *exchange = "";
    const char *routingkey = "hello_metadata";

    auto consumer_tag = amqp_cstring_bytes("metadata_consumer");
    amqp_basic_consume(conn, 1, queuename, consumer_tag, 0, 1, 0,
                       amqp_empty_table);
    die_on_amqp_error(amqp_get_rpc_reply(conn), "Consuming");

    for (;;) {
      ros::spinOnce();
      if (!ros::ok() || ros::isShuttingDown()) {
        break;
      }

      amqp_rpc_reply_t res;
      amqp_envelope_t envelope;

      amqp_maybe_release_buffers(conn);

      struct timeval timeout = {.tv_sec = 0, .tv_usec = 200};
      res = amqp_consume_message(conn, &envelope, &timeout, 0);

      if (AMQP_RESPONSE_NORMAL != res.reply_type) {
        continue;
      }

      // printf("Delivery %u, exchange %.*s routingkey %.*s\n",
      //        (unsigned)envelope.delivery_tag, (int)envelope.exchange.len,
      //        (char *)envelope.exchange.bytes, (int)envelope.routing_key.len,
      //        (char *)envelope.routing_key.bytes);
      // printf("----\n");

      size_t buf_size = envelope.message.body.len;
      // std::cout << buf_size << std::endl;
      const char *buf = (const char *)envelope.message.body.bytes;
      const uint32_t *len_ptr = (const uint32_t *)buf;
      const char *data_ptr = buf + 3 * sizeof(uint32_t);
      // std::cout << len_ptr[0] << std::endl;
      // std::cout << len_ptr[1] << std::endl;
      // std::cout << len_ptr[2] << std::endl;

      md5sum = std::string(data_ptr, len_ptr[0]);
      datatype = std::string(data_ptr + len_ptr[0], len_ptr[1]);
      def = std::string(data_ptr + len_ptr[0] + len_ptr[1], len_ptr[2]);
      latch = buf[buf_size - 1];

      // std::cout << md5sum << " | " << datatype << " | " << def << " | " <<
      // (int)latch << std::endl;

      amqp_destroy_envelope(&envelope);

      break;
    }
    amqp_basic_cancel(conn, 1, consumer_tag);
  }
  if (!ros::ok() || ros::isShuttingDown()) {
    return false;
  }

  amqp_bytes_t queuename = amqp_cstring_bytes("hello");

  {
    amqp_queue_declare_ok_t *r =
        amqp_queue_declare(conn, 1, queuename, 0, 0, 0, 1, amqp_empty_table);
    die_on_amqp_error(amqp_get_rpc_reply(conn), "Declaring queue");
  }

  const char *exchange = "";
  const char *routingkey = "hello";

  auto consumer_tag = amqp_cstring_bytes("msg_consumer");
  amqp_basic_consume(conn, 1, queuename, consumer_tag, 0, 1, 0,
                     amqp_empty_table);
  die_on_amqp_error(amqp_get_rpc_reply(conn), "Consuming");

  for (;;) {
    ros::spinOnce();
    if (!ros::ok() || ros::isShuttingDown()) {
      break;
    }

    amqp_rpc_reply_t res;
    amqp_envelope_t envelope;

    amqp_maybe_release_buffers(conn);

    struct timeval timeout = {.tv_sec = 0, .tv_usec = 200};
    res = amqp_consume_message(conn, &envelope, &timeout, 0);

    if (AMQP_RESPONSE_NORMAL != res.reply_type) {
      continue;
    }

    // printf("Delivery %u, exchange %.*s routingkey %.*s\n",
    //        (unsigned)envelope.delivery_tag, (int)envelope.exchange.len,
    //        (char *)envelope.exchange.bytes, (int)envelope.routing_key.len,
    //        (char *)envelope.routing_key.bytes);
    // printf("----\n");

    // std::cout << std::string((char *)envelope.message.body.bytes,
    // envelope.message.body.len) << std::endl;

    topic_tools::ShapeShifter msg;
    auto buf = (char *)envelope.message.body.bytes;
    auto size = envelope.message.body.len;
    Wrapper wrap(buf, size);
    msg.read(wrap);
    msg.morph(md5sum, datatype, def, std::string(latch ? "true" : "false"));

    if (!publisher_created) {
      pub_ = msg.advertise(nh_, "/msg2", 10);
      publisher_created = true;
    }
    pub_.publish(msg);

    amqp_destroy_envelope(&envelope);
  }
  amqp_basic_cancel(conn, 1, consumer_tag);
}

void AMQPTopicReceiver::reconfigureRequest(
    AMQPTopicReceiver_configConfig &new_config, uint32_t level) {
  auto lvl = (aduulm_logger::LoggerLevel)new_config.log_level;
  _setLogLevel(lvl);
  LOG_DEB("Switched log level to " << lvl);
}

DEFINE_LOGGER_CLASS_INTERFACE_IMPLEMENTATION(AMQPTopicReceiver)

} // namespace amqp_topic_transceiver
