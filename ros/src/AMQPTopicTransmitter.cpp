#include <amqp_topic_transceiver/AMQPTopicTransmitter.h>
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

AMQPTopicTransmitter::AMQPTopicTransmitter(ros::NodeHandle node_handle,
                                           ros::NodeHandle private_node_handle)
    : private_nh_(private_node_handle), dyn_param_server_(private_nh_),
      nh_(node_handle), metadata_sent(false) {
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

  sub_ = node_handle.subscribe("/cameras/spu4_cam1/image_compressed/compressed",
                               10, &AMQPTopicTransmitter::processMessage, this,
                               ros::TransportHints().tcpNoDelay());
}

AMQPTopicTransmitter::~AMQPTopicTransmitter() {
  die_on_amqp_error(amqp_channel_close(conn, 1, AMQP_REPLY_SUCCESS),
                    "Closing channel");
  die_on_amqp_error(amqp_connection_close(conn, AMQP_REPLY_SUCCESS),
                    "Closing connection");
  die_on_error(amqp_destroy_connection(conn), "Ending connection");
}

class Wrapper {
public:
  Wrapper(char *buf) { this->buf = buf; }

  char *advance(int size) { return buf; }

private:
  char *buf;
};

void AMQPTopicTransmitter::processMessage(
    const ros::MessageEvent<topic_tools::ShapeShifter> &msg_event) {
  boost::shared_ptr<topic_tools::ShapeShifter const> const &msg =
      msg_event.getConstMessage();
  boost::shared_ptr<const ros::M_string> const &connection_header =
      msg_event.getConnectionHeaderPtr();

  auto md5sum = msg->getMD5Sum();

  if (md5sum != last_md5sum) {
    amqp_bytes_t queuename = amqp_cstring_bytes("hello_metadata");

    auto datatype = msg->getDataType();
    auto def = msg->getMessageDefinition();
    bool latch = false;
    if (connection_header) {
      ros::M_string::const_iterator it = connection_header->find("latching");
      if ((it != connection_header->end()) && (it->second == "1")) {
        ROS_DEBUG("input topic is latched; latching output topic to match");
        latch = true;
      }
    }

    size_t buf_size =
        3 * sizeof(uint32_t) + md5sum.size() + datatype.size() + def.size() + 1;
    char *buf = new char[buf_size];

    auto len_ptr = (uint32_t *)(&buf[0]);
    len_ptr[0] = md5sum.size();
    len_ptr[1] = datatype.size();
    len_ptr[2] = def.size();

    auto data_ptr = &buf[0] + 3 * sizeof(uint32_t);
    memcpy(data_ptr, md5sum.c_str(), md5sum.size());
    memcpy(data_ptr + len_ptr[0], datatype.c_str(), datatype.size());
    memcpy(data_ptr + len_ptr[0] + len_ptr[1], def.c_str(), def.size());
    buf[buf_size - 1] = (char)latch;

    amqp_bytes_t data = {.len = buf_size, .bytes = buf};

    {
      amqp_queue_declare_ok_t *r =
          amqp_queue_declare(conn, 1, queuename, 0, 0, 0, 1, amqp_empty_table);
      die_on_amqp_error(amqp_get_rpc_reply(conn), "Declaring metadata queue");
    }

    const char *exchange = "";
    const char *routingkey = "hello_metadata";

    amqp_basic_properties_t props;
    props._flags = AMQP_BASIC_DELIVERY_MODE_FLAG;
    props.delivery_mode = 2; // persistent
    die_on_error(amqp_basic_publish(conn, 1, amqp_cstring_bytes(exchange),
                                    amqp_cstring_bytes(routingkey), 0, 0,
                                    &props, data),
                 "Publishing metadata");

    delete[] buf;
    metadata_sent = true;
    last_md5sum = md5sum;
  }

  amqp_bytes_t queuename = amqp_cstring_bytes("hello");

  {
    amqp_queue_declare_ok_t *r =
        amqp_queue_declare(conn, 1, queuename, 0, 0, 0, 1, amqp_empty_table);
    die_on_amqp_error(amqp_get_rpc_reply(conn), "Declaring queue");
  }

  const char *exchange = "";
  const char *routingkey = "hello";

  char *buf = new char[msg->size()];
  // std::cout << "Publishing Msg with size: " << msg->size() << std::endl;
  Wrapper wrap(buf);
  msg->write(wrap);
  const amqp_bytes_t bytes = {.len = msg->size(), .bytes = buf};

  // std::cout << msg->getMD5Sum() << " | " << msg->getDataType() << " | " <<
  // msg->getMessageDefinition() << std::endl;

  amqp_basic_properties_t props;
  props._flags = 0;
  // props._flags = AMQP_BASIC_DELIVERY_MODE_FLAG;
  // props.delivery_mode = 2; // persistent
  die_on_error(amqp_basic_publish(conn, 1, amqp_cstring_bytes(exchange),
                                  amqp_cstring_bytes(routingkey), 0, 0, &props,
                                  bytes),
               "Publishing");

  delete[] buf;
}

void AMQPTopicTransmitter::reconfigureRequest(
    AMQPTopicTransmitter_configConfig &new_config, uint32_t level) {
  auto lvl = (aduulm_logger::LoggerLevel)new_config.log_level;
  _setLogLevel(lvl);
  LOG_DEB("Switched log level to " << lvl);
}

DEFINE_LOGGER_CLASS_INTERFACE_IMPLEMENTATION(AMQPTopicTransmitter)

} // namespace amqp_topic_transceiver
