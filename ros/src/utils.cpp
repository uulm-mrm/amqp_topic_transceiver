#include <amqp_topic_transceiver/utils.h>
#include <cstdio>
#include <cstdlib>

void die(const char* fmt)
{
  fprintf(stderr, "%s", fmt);
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
