#pragma once
#include <amqp.h>
#include <cstdarg>

void die(const char* fmt);
void die_on_error(int x, char const* context);
void die_on_amqp_error(amqp_rpc_reply_t x, char const* context);
