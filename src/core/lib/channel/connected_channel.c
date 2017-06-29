/*
 *
 * Copyright 2015 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

#include "src/core/lib/channel/connected_channel.h"

#include <stdarg.h>
#include <stdio.h>
#include <string.h>

#include <grpc/byte_buffer.h>
#include <grpc/slice_buffer.h>
#include <grpc/support/alloc.h>
#include <grpc/support/log.h>
#include "src/core/lib/profiling/timers.h"
#include "src/core/lib/support/string.h"
#include "src/core/lib/transport/transport.h"

#define MAX_BUFFER_LENGTH 8192

typedef struct connected_channel_channel_data {
  grpc_transport *transport;
} channel_data;

typedef struct connected_channel_call_data {
  grpc_call_combiner *call_combiner;
  // Closures used for returning results on the call combiner.
  grpc_closure on_complete[6];  // Max number of pending batches.
  size_t num_on_completes;
  grpc_closure recv_initial_metadata_ready;
  grpc_closure recv_message_ready;
} call_data;

static void intercepted_closure_run(grpc_exec_ctx *exec_ctx, void *arg,
                                    grpc_error *error) {
  grpc_closure *original_closure = arg;
  GRPC_CLOSURE_RUN(exec_ctx, original_closure, GRPC_ERROR_REF(error));
}

/* We perform a small hack to locate transport data alongside the connected
   channel data in call allocations, to allow everything to be pulled in minimal
   cache line requests */
#define TRANSPORT_STREAM_FROM_CALL_DATA(calld) ((grpc_stream *)((calld) + 1))
#define CALL_DATA_FROM_TRANSPORT_STREAM(transport_stream) \
  (((call_data *)(transport_stream)) - 1)

/* Intercept a call operation and either push it directly up or translate it
   into transport stream operations */
static void con_start_transport_stream_op_batch(
    grpc_exec_ctx *exec_ctx, grpc_call_element *elem,
    grpc_transport_stream_op_batch *op) {
  call_data *calld = elem->call_data;
  channel_data *chand = elem->channel_data;
  GRPC_CALL_LOG_OP(GPR_INFO, elem, op);
  if (op->recv_initial_metadata) {
    op->payload->recv_initial_metadata.recv_initial_metadata_ready =
        GRPC_CLOSURE_INIT(
            &calld->recv_initial_metadata_ready, intercepted_closure_run,
            op->payload->recv_initial_metadata.recv_initial_metadata_ready,
            &calld->call_combiner->scheduler);
gpr_log(GPR_INFO, "INTERCEPTING recv_initial_metadata: closure=%p call_combiner=%p", op->payload->recv_initial_metadata.recv_initial_metadata_ready, calld->call_combiner);
  }
  if (op->recv_message) {
    op->payload->recv_message.recv_message_ready = GRPC_CLOSURE_INIT(
        &calld->recv_message_ready, intercepted_closure_run,
        op->payload->recv_message.recv_message_ready,
        &calld->call_combiner->scheduler);
gpr_log(GPR_INFO, "INTERCEPTING recv_message: closure=%p call_combiner=%p", op->payload->recv_message.recv_message_ready, calld->call_combiner);
  }
  op->on_complete = GRPC_CLOSURE_INIT(
      &calld->on_complete[calld->num_on_completes++], intercepted_closure_run,
      op->on_complete, &calld->call_combiner->scheduler);
gpr_log(GPR_INFO, "INTERCEPTING on_complete: closure=%p call_combiner=%p", op->on_complete, calld->call_combiner);
  grpc_transport_perform_stream_op(exec_ctx, chand->transport,
                                   TRANSPORT_STREAM_FROM_CALL_DATA(calld), op);
gpr_log(GPR_INFO, "STOPPING call_combiner %p", calld->call_combiner);
  grpc_call_combiner_stop(exec_ctx, calld->call_combiner);
}

static void con_start_transport_op(grpc_exec_ctx *exec_ctx,
                                   grpc_channel_element *elem,
                                   grpc_transport_op *op) {
  channel_data *chand = elem->channel_data;
  grpc_transport_perform_op(exec_ctx, chand->transport, op);
}

/* Constructor for call_data */
static grpc_error *init_call_elem(grpc_exec_ctx *exec_ctx,
                                  grpc_call_element *elem,
                                  const grpc_call_element_args *args) {
  call_data *calld = elem->call_data;
  channel_data *chand = elem->channel_data;
  calld->call_combiner = args->call_combiner;
  int r = grpc_transport_init_stream(
      exec_ctx, chand->transport, TRANSPORT_STREAM_FROM_CALL_DATA(calld),
      &args->call_stack->refcount, args->server_transport_data, args->arena);
  return r == 0 ? GRPC_ERROR_NONE
                : GRPC_ERROR_CREATE_FROM_STATIC_STRING(
                      "transport stream initialization failed");
}

static void set_pollset_or_pollset_set(grpc_exec_ctx *exec_ctx,
                                       grpc_call_element *elem,
                                       grpc_polling_entity *pollent) {
  call_data *calld = elem->call_data;
  channel_data *chand = elem->channel_data;
  grpc_transport_set_pops(exec_ctx, chand->transport,
                          TRANSPORT_STREAM_FROM_CALL_DATA(calld), pollent);
}

/* Destructor for call_data */
static void destroy_call_elem(grpc_exec_ctx *exec_ctx, grpc_call_element *elem,
                              const grpc_call_final_info *final_info,
                              grpc_closure *then_schedule_closure) {
  call_data *calld = elem->call_data;
  channel_data *chand = elem->channel_data;
  grpc_transport_destroy_stream(exec_ctx, chand->transport,
                                TRANSPORT_STREAM_FROM_CALL_DATA(calld),
                                then_schedule_closure);
}

/* Constructor for channel_data */
static grpc_error *init_channel_elem(grpc_exec_ctx *exec_ctx,
                                     grpc_channel_element *elem,
                                     grpc_channel_element_args *args) {
  channel_data *cd = (channel_data *)elem->channel_data;
  GPR_ASSERT(args->is_last);
  cd->transport = NULL;
  return GRPC_ERROR_NONE;
}

/* Destructor for channel_data */
static void destroy_channel_elem(grpc_exec_ctx *exec_ctx,
                                 grpc_channel_element *elem) {
  channel_data *cd = (channel_data *)elem->channel_data;
  if (cd->transport) {
    grpc_transport_destroy(exec_ctx, cd->transport);
  }
}

static char *con_get_peer(grpc_exec_ctx *exec_ctx, grpc_call_element *elem) {
  channel_data *chand = elem->channel_data;
  return grpc_transport_get_peer(exec_ctx, chand->transport);
}

/* No-op. */
static void con_get_channel_info(grpc_exec_ctx *exec_ctx,
                                 grpc_channel_element *elem,
                                 const grpc_channel_info *channel_info) {}

const grpc_channel_filter grpc_connected_filter = {
    con_start_transport_stream_op_batch,
    con_start_transport_op,
    sizeof(call_data),
    init_call_elem,
    set_pollset_or_pollset_set,
    destroy_call_elem,
    sizeof(channel_data),
    init_channel_elem,
    destroy_channel_elem,
    con_get_peer,
    con_get_channel_info,
    "connected",
};

static void bind_transport(grpc_channel_stack *channel_stack,
                           grpc_channel_element *elem, void *t) {
  channel_data *cd = (channel_data *)elem->channel_data;
  GPR_ASSERT(elem->filter == &grpc_connected_filter);
  GPR_ASSERT(cd->transport == NULL);
  cd->transport = t;

  /* HACK(ctiller): increase call stack size for the channel to make space
     for channel data. We need a cleaner (but performant) way to do this,
     and I'm not sure what that is yet.
     This is only "safe" because call stacks place no additional data after
     the last call element, and the last call element MUST be the connected
     channel. */
  channel_stack->call_stack_size += grpc_transport_stream_size(t);
}

bool grpc_add_connected_filter(grpc_exec_ctx *exec_ctx,
                               grpc_channel_stack_builder *builder,
                               void *arg_must_be_null) {
  GPR_ASSERT(arg_must_be_null == NULL);
  grpc_transport *t = grpc_channel_stack_builder_get_transport(builder);
  GPR_ASSERT(t != NULL);
  return grpc_channel_stack_builder_append_filter(
      builder, &grpc_connected_filter, bind_transport, t);
}

grpc_stream *grpc_connected_channel_get_stream(grpc_call_element *elem) {
  call_data *calld = elem->call_data;
  return TRANSPORT_STREAM_FROM_CALL_DATA(calld);
}
