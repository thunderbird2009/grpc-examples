/*
 *
 * Copyright 2018 gRPC authors.
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

#include <grpc/support/port_platform.h>

#include <stdint.h>

#include "src/core/ext/filters/client_channel/health/health_check_client.h"

#include "pb_decode.h"
#include "pb_encode.h"
#include "src/core/ext/filters/client_channel/health/health.pb.h"
#include "src/core/lib/gprpp/mutex_lock.h"
#include "src/core/lib/transport/error_utils.h"
#include "src/core/lib/transport/status_metadata.h"

#define HEALTH_CHECK_INITIAL_CONNECT_BACKOFF_SECONDS 1
#define HEALTH_CHECK_RECONNECT_BACKOFF_MULTIPLIER 1.6
#define HEALTH_CHECK_RECONNECT_MAX_BACKOFF_SECONDS 120
#define HEALTH_CHECK_RECONNECT_JITTER 0.2

namespace grpc_core {

//
// HealthCheckClient
//

HealthCheckClient::HealthCheckClient(
    const char* service_name,
    RefCountedPtr<ConnectedSubchannel> connected_subchannel,
    grpc_pollset_set* interested_parties)
    : service_name_(service_name),
      connected_subchannel_(std::move(connected_subchannel)),
      interested_parties_(interested_parties),
      retry_backoff_(
          BackOff::Options()
              .set_initial_backoff(
                  HEALTH_CHECK_INITIAL_CONNECT_BACKOFF_SECONDS * 1000)
              .set_multiplier(HEALTH_CHECK_RECONNECT_BACKOFF_MULTIPLIER)
              .set_jitter(HEALTH_CHECK_RECONNECT_JITTER)
              .set_max_backoff(HEALTH_CHECK_RECONNECT_MAX_BACKOFF_SECONDS *
                               1000)) {
  StartCall();
}

void HealthCheckClient::NotifyOnHealthChange(grpc_connectivity_state* state,
                                             grpc_closure* closure) {
  MutexLock lock(&mu_);
  GPR_ASSERT(notify_state_ == nullptr);
  if (*state != state_) {
    *state = state_;
    GRPC_CLOSURE_SCHED(closure, GRPC_ERROR_NONE);
    return;
  }
  notify_state_ = state;
  on_health_changed_ = closure;
}

void HealthCheckClient::SetHealthStatus(grpc_connectivity_state state) {
  MutexLock lock(&mu_);
  SetHealthStatusLocked(state);
}

void HealthCheckClient::SetHealthStatusLocked(grpc_connectivity_state state) {
  if (notify_state_ != nullptr && *notify_state_ != state) {
    *notify_state_ = state;
    GRPC_CLOSURE_SCHED(on_health_changed_, GRPC_ERROR_NONE);
  }
  state_ = state;
}

void HealthCheckClient::Orphan() {
  {
    MutexLock lock(&mu_);
    if (on_health_changed_ != nullptr) {
      GRPC_CLOSURE_SCHED(on_health_changed_, GRPC_ERROR_CANCELLED);
      notify_state_ = nullptr;
      on_health_changed_ = nullptr;
    }
    shutting_down_ = true;
    call_state_.reset();
    if (retry_timer_callback_pending_) {
      grpc_timer_cancel(&retry_timer_);
    }
  }
  Unref(DEBUG_LOCATION, "orphan");
}

void HealthCheckClient::StartCall() {
  MutexLock lock(&mu_);
  StartCallLocked();
}

void HealthCheckClient::StartCallLocked() {
  if (shutting_down_) return;
  GPR_ASSERT(call_state_ == nullptr);
  SetHealthStatusLocked(GRPC_CHANNEL_CONNECTING);
  call_state_ = MakeOrphanable<CallState>(Ref(), interested_parties_);
// FIXME
#if 0
  if (grpc_lb_glb_trace.enabled()) {
    gpr_log(GPR_INFO,
            "[grpclb %p] Query for backends (lb_channel: %p, lb_calld: %p)",
            this, lb_channel_, lb_calld_.get());
  }
#endif
  call_state_->StartCall();
}

void HealthCheckClient::StartRetryTimer() {
  MutexLock lock(&mu_);
  SetHealthStatusLocked(GRPC_CHANNEL_TRANSIENT_FAILURE);
  grpc_millis next_try = retry_backoff_.NextAttemptTime();
// FIXME
#if 0
  if (grpc_lb_glb_trace.enabled()) {
    gpr_log(GPR_INFO, "[grpclb %p] Connection to LB server lost...", this);
    grpc_millis timeout = next_try - ExecCtx::Get()->Now();
    if (timeout > 0) {
      gpr_log(GPR_INFO, "[grpclb %p] ... retry_timer_active in %" PRId64 "ms.",
              this, timeout);
    } else {
      gpr_log(GPR_INFO, "[grpclb %p] ... retry_timer_active immediately.",
              this);
    }
  }
#endif
  // Ref for callback, tracked manually.
  Ref(DEBUG_LOCATION, "health_retry_timer").release();
  GRPC_CLOSURE_INIT(&retry_timer_callback_, OnRetryTimer, this,
                    grpc_schedule_on_exec_ctx);
  retry_timer_callback_pending_ = true;
  grpc_timer_init(&retry_timer_, next_try, &retry_timer_callback_);
}

void HealthCheckClient::OnRetryTimer(void* arg, grpc_error* error) {
  HealthCheckClient* self = static_cast<HealthCheckClient*>(arg);
  {
    MutexLock lock(&self->mu_);
    self->retry_timer_callback_pending_ = false;
    if (!self->shutting_down_ && error == GRPC_ERROR_NONE &&
        self->call_state_ == nullptr) {
// FIXME
#if 0
      if (grpc_lb_glb_trace.enabled()) {
        gpr_log(GPR_INFO, "[grpclb %p] Restarting call to LB server",
                grpclb_policy);
      }
#endif
      self->StartCallLocked();
    }
  }
  self->Unref(DEBUG_LOCATION, "health_retry_timer");
}

//
// protobuf helpers
//

namespace {

void EncodeRequest(const char* service_name,
                   ManualConstructor<SliceBufferByteStream>* send_message) {
  grpc_health_v1_HealthCheckRequest request_struct;
  request_struct.has_service = true;
  snprintf(request_struct.service, sizeof(request_struct.service), "%s",
           service_name);
  pb_ostream_t ostream;
  memset(&ostream, 0, sizeof(ostream));
  pb_encode(&ostream, grpc_health_v1_HealthCheckRequest_fields,
            &request_struct);
  grpc_slice request_slice = grpc_slice_malloc(ostream.bytes_written);
  ostream = pb_ostream_from_buffer(GRPC_SLICE_START_PTR(request_slice),
                                   GRPC_SLICE_LENGTH(request_slice));
  GPR_ASSERT(pb_encode(&ostream, grpc_health_v1_HealthCheckRequest_fields,
                       &request_struct) != 0);
  grpc_slice_buffer slice_buffer;
  grpc_slice_buffer_init(&slice_buffer);
  send_message->Init(&slice_buffer, 0);
  grpc_slice_buffer_destroy(&slice_buffer);
}

// Returns true if healthy.
bool DecodeResponse(grpc_slice_buffer* slice_buffer) {
  // If message is empty, assume unhealthy.
// FIXME: log this
  if (slice_buffer->length == 0) return false;
  // Concatenate the slices to form a single string.
  UniquePtr<uint8_t> recv_message_deleter;
  uint8_t* recv_message;
  if (slice_buffer->count == 1) {
    recv_message = GRPC_SLICE_START_PTR(slice_buffer->slices[0]);
  } else {
    recv_message = static_cast<uint8_t*>(gpr_malloc(slice_buffer->length));
    recv_message_deleter.reset(recv_message);
    size_t offset = 0;
    for (size_t i = 0; i < slice_buffer->count; ++i) {
      memcpy(recv_message + offset,
             GRPC_SLICE_START_PTR(slice_buffer->slices[i]),
             GRPC_SLICE_LENGTH(slice_buffer->slices[i]));
      offset += GRPC_SLICE_LENGTH(slice_buffer->slices[i]);
    }
  }
  // Deserialize message.
  grpc_health_v1_HealthCheckResponse response_struct;
  pb_istream_t istream =
      pb_istream_from_buffer(recv_message, slice_buffer->length);
  if (!pb_decode(&istream, grpc_health_v1_HealthCheckResponse_fields,
                 &response_struct)) {
    // Can't parse message; assume unhealthy.
// FIXME: log this
    return false;
  }
  if (!response_struct.has_status) {
    // Field not present; assume unhealthy.
// FIXME: log this
    return false;
  }
  return response_struct.status ==
         grpc_health_v1_HealthCheckResponse_ServingStatus_SERVING;
}

}  // namespace

//
// HealthCheckClient::CallState
//

HealthCheckClient::CallState::CallState(
    RefCountedPtr<HealthCheckClient> health_check_client,
    grpc_pollset_set* interested_parties)
    : health_check_client_(std::move(health_check_client)),
      pollent_(grpc_polling_entity_create_from_pollset_set(interested_parties)),
      arena_(gpr_arena_create(health_check_client_->connected_subchannel_
                                  ->GetInitialCallSizeEstimate(0))) {
  memset(&call_combiner_, 0, sizeof(call_combiner_));
  grpc_call_combiner_init(&call_combiner_);
  memset(context_, 0, sizeof(context_));
  gpr_atm_rel_store(&seen_response_, static_cast<gpr_atm>(0));
}

HealthCheckClient::CallState::~CallState() {
  GRPC_SUBCHANNEL_CALL_UNREF(call_, "call_ended");
  gpr_arena_destroy(arena_);
  grpc_call_combiner_destroy(&call_combiner_);
}

void HealthCheckClient::CallState::Orphan() {
  grpc_call_combiner_cancel(&call_combiner_, GRPC_ERROR_CANCELLED);
  Cancel();
}

void HealthCheckClient::CallState::StartCall() {
  ConnectedSubchannel::CallArgs args = {
      &pollent_,
      GRPC_MDSTR_SLASH_GRPC_DOT_HEALTH_DOT_V1_DOT_HEALTH_SLASH_WATCH,
      gpr_now(GPR_CLOCK_MONOTONIC),  // start_time
      GRPC_MILLIS_INF_FUTURE,        // deadline
      arena_,
      context_,
      &call_combiner_,
      0,  // parent_data_size
  };
  grpc_error* error =
      health_check_client_->connected_subchannel_->CreateCall(args, &call_);
  if (error != GRPC_ERROR_NONE) {
// FIXME: log
    GRPC_ERROR_UNREF(error);
    // Schedule instead of running directly, since we must not be
    // holding health_check_client_->mu_ when CallEnded() is called.
    GRPC_CLOSURE_SCHED(
        GRPC_CLOSURE_INIT(&batch_.handler_private.closure, CallEndedRetry,
                          this, grpc_schedule_on_exec_ctx),
        GRPC_ERROR_NONE);
    return;
  }
  // Initialize payload and batch.
  memset(&payload_, 0, sizeof(payload_));
  memset(&batch_, 0, sizeof(batch_));
  batch_.payload = &payload_;
  // on_complete callback takes ref, handled manually.
  Ref(DEBUG_LOCATION, "on_complete").release();
  batch_.on_complete = GRPC_CLOSURE_INIT(&on_complete_, OnComplete, this,
                                         grpc_schedule_on_exec_ctx);
  // Add send_initial_metadata op.
  grpc_metadata_batch_init(&send_initial_metadata_);
  payload_.send_initial_metadata.send_initial_metadata =
      &send_initial_metadata_;
  batch_.send_initial_metadata = true;
  // Add send_message op.
  EncodeRequest(health_check_client_->service_name_, &send_message_);
  payload_.send_message.send_message.reset(send_message_.get());
  batch_.send_message = true;
  // Add send_trailing_metadata op.
  grpc_metadata_batch_init(&send_trailing_metadata_);
  payload_.send_trailing_metadata.send_trailing_metadata =
      &send_trailing_metadata_;
  batch_.send_trailing_metadata = true;
  // Add recv_initial_metadata op.
  grpc_metadata_batch_init(&recv_initial_metadata_);
  payload_.recv_initial_metadata.recv_initial_metadata =
      &recv_initial_metadata_;
  // recv_initial_metadata_ready callback takes ref, handled manually.
  Ref(DEBUG_LOCATION, "recv_initial_metadata_ready").release();
  payload_.recv_initial_metadata.recv_initial_metadata_ready =
      GRPC_CLOSURE_INIT(&recv_initial_metadata_ready_, RecvInitialMetadataReady,
                        this, grpc_schedule_on_exec_ctx);
  batch_.recv_initial_metadata = true;
  // Add recv_message op.
  payload_.recv_message.recv_message = &recv_message_;
  // recv_message callback takes ref, handled manually.
  Ref(DEBUG_LOCATION, "recv_message_ready").release();
  payload_.recv_message.recv_message_ready = GRPC_CLOSURE_INIT(
      &recv_message_ready_, RecvMessageReady, this, grpc_schedule_on_exec_ctx);
  batch_.recv_message = true;
  // Start batch.
  grpc_subchannel_call_process_op(call_, &batch_);
  // Initialize recv_trailing_metadata batch.
  memset(&recv_trailing_metadata_batch_, 0,
         sizeof(recv_trailing_metadata_batch_));
  recv_trailing_metadata_batch_.payload = &payload_;
  // Add recv_trailing_metadata op.
  grpc_metadata_batch_init(&recv_trailing_metadata_);
  payload_.recv_trailing_metadata.recv_trailing_metadata =
      &recv_trailing_metadata_;
  // This callback signals the end of the call, so it relies on the
  // initial ref instead of taking a new ref.  When it's invoked, the
  // initial ref is released.
  payload_.recv_trailing_metadata.recv_trailing_metadata_ready =
      GRPC_CLOSURE_INIT(&recv_trailing_metadata_ready_,
                        RecvTrailingMetadataReady, this,
                        grpc_schedule_on_exec_ctx);
  recv_trailing_metadata_batch_.recv_trailing_metadata = true;
  // Start recv_trailing_metadata batch.
  grpc_subchannel_call_process_op(call_, &recv_trailing_metadata_batch_);
}

void HealthCheckClient::CallState::OnCancelComplete(void* arg,
                                                    grpc_error* error) {
  HealthCheckClient::CallState* self =
      static_cast<HealthCheckClient::CallState*>(arg);
  GRPC_CALL_COMBINER_STOP(&self->call_combiner_, "health_cancel");
  self->Unref(DEBUG_LOCATION, "cancel");
}

void HealthCheckClient::CallState::StartCancel(void* arg, grpc_error* error) {
  HealthCheckClient::CallState* self =
      static_cast<HealthCheckClient::CallState*>(arg);
  auto* batch = grpc_make_transport_stream_op(
      GRPC_CLOSURE_CREATE(OnCancelComplete, self, grpc_schedule_on_exec_ctx));
  batch->cancel_stream = true;
  batch->payload->cancel_stream.cancel_error = GRPC_ERROR_CANCELLED;
  grpc_subchannel_call_process_op(self->call_, batch);
}

void HealthCheckClient::CallState::Cancel() {
  if (call_ != nullptr) {
    Ref(DEBUG_LOCATION, "cancel").release();
    GRPC_CALL_COMBINER_START(
        &call_combiner_,
        GRPC_CLOSURE_CREATE(StartCancel, this, grpc_schedule_on_exec_ctx),
        GRPC_ERROR_NONE, "health_cancel");
  }
}

void HealthCheckClient::CallState::OnComplete(void* arg, grpc_error* error) {
  HealthCheckClient::CallState* self =
      static_cast<HealthCheckClient::CallState*>(arg);
  GRPC_CALL_COMBINER_STOP(&self->call_combiner_, "on_complete");
  grpc_metadata_batch_destroy(&self->send_initial_metadata_);
  grpc_metadata_batch_destroy(&self->send_trailing_metadata_);
  self->Unref(DEBUG_LOCATION, "on_complete");
}

void HealthCheckClient::CallState::RecvInitialMetadataReady(void* arg,
                                                            grpc_error* error) {
  HealthCheckClient::CallState* self =
      static_cast<HealthCheckClient::CallState*>(arg);
  GRPC_CALL_COMBINER_STOP(&self->call_combiner_, "recv_initial_metadata_ready");
  grpc_metadata_batch_destroy(&self->recv_initial_metadata_);
  self->Unref(DEBUG_LOCATION, "recv_initial_metadata_ready");
}

void HealthCheckClient::CallState::DoneReadingRecvMessage(grpc_error* error) {
  recv_message_.reset();
  if (error != GRPC_ERROR_NONE) {
    Cancel();
  } else {
    const bool healthy = DecodeResponse(&recv_message_buffer_);
    const grpc_connectivity_state state =
        healthy ? GRPC_CHANNEL_READY : GRPC_CHANNEL_TRANSIENT_FAILURE;
    health_check_client_->SetHealthStatus(state);
    gpr_atm_rel_store(&seen_response_, static_cast<gpr_atm>(1));
  }
  grpc_slice_buffer_destroy(&recv_message_buffer_);
  Unref(DEBUG_LOCATION, "recv_message_ready");
}

grpc_error* HealthCheckClient::CallState::PullSliceFromRecvMessage() {
  grpc_slice slice;
  grpc_error* error = recv_message_->Pull(&slice);
  if (error == GRPC_ERROR_NONE) {
    grpc_slice_buffer_add(&recv_message_buffer_, slice);
  }
  return error;
}

void HealthCheckClient::CallState::ContinueReadingRecvMessage() {
  while (recv_message_->Next(SIZE_MAX, &recv_message_ready_)) {
    grpc_error* error = PullSliceFromRecvMessage();
    if (error != GRPC_ERROR_NONE) {
      DoneReadingRecvMessage(error);
      return;
    }
    if (recv_message_buffer_.length == recv_message_->length()) {
      DoneReadingRecvMessage(GRPC_ERROR_NONE);
      break;
    }
  }
}

void HealthCheckClient::CallState::OnByteStreamNext(void* arg,
                                                    grpc_error* error) {
  HealthCheckClient::CallState* self =
      static_cast<HealthCheckClient::CallState*>(arg);
  if (error != GRPC_ERROR_NONE) {
    self->DoneReadingRecvMessage(error);
    return;
  }
  error = self->PullSliceFromRecvMessage();
  if (error != GRPC_ERROR_NONE) {
    self->DoneReadingRecvMessage(error);
    return;
  }
  if (self->recv_message_buffer_.length == self->recv_message_->length()) {
    self->DoneReadingRecvMessage(GRPC_ERROR_NONE);
  } else {
    self->ContinueReadingRecvMessage();
  }
}

void HealthCheckClient::CallState::RecvMessageReady(void* arg,
                                                    grpc_error* error) {
  HealthCheckClient::CallState* self =
      static_cast<HealthCheckClient::CallState*>(arg);
  GRPC_CALL_COMBINER_STOP(&self->call_combiner_, "recv_message_ready");
  grpc_slice_buffer_init(&self->recv_message_buffer_);
  GRPC_CLOSURE_INIT(&self->recv_message_ready_, OnByteStreamNext, self,
                    grpc_schedule_on_exec_ctx);
  self->ContinueReadingRecvMessage();
  // Ref will continue to be held until we finish draining the byte stream.
}

void HealthCheckClient::CallState::RecvTrailingMetadataReady(
    void* arg, grpc_error* error) {
  HealthCheckClient::CallState* self =
      static_cast<HealthCheckClient::CallState*>(arg);
  GRPC_CALL_COMBINER_STOP(&self->call_combiner_,
                          "recv_trailing_metadata_ready");
  // Get call status.
  grpc_status_code status = GRPC_STATUS_UNKNOWN;
  if (error != GRPC_ERROR_NONE) {
    grpc_error_get_status(error, GRPC_MILLIS_INF_FUTURE, &status,
                          nullptr /* slice */, nullptr /* http_error */,
                          nullptr /* error_string */);
  } else if (self->recv_trailing_metadata_.idx.named.grpc_status != nullptr) {
    status = grpc_get_status_code_from_metadata(
        self->recv_trailing_metadata_.idx.named.grpc_status->md);
  }
  // Clean up.
  grpc_metadata_batch_destroy(&self->recv_trailing_metadata_);
  // For status unimplemented, give up and assume always healthy.
  bool retry = true;
  if (status == GRPC_STATUS_UNIMPLEMENTED) {
// FIXME: log and add subchannel trace event
    self->health_check_client_->SetHealthStatus(GRPC_CHANNEL_READY);
    retry = false;
  }
  self->CallEnded(retry);
}

void HealthCheckClient::CallState::CallEndedRetry(void* arg,
                                                  grpc_error* error) {
  HealthCheckClient::CallState* self =
      static_cast<HealthCheckClient::CallState*>(arg);
  self->CallEnded(true /* retry */);
}

void HealthCheckClient::CallState::CallEnded(bool retry) {
  // If this CallState is still in use, this call ended because of a failure,
  // so we need to stop using it and optionally create a new one.
  // Otherwise, we have deliberately ended this call, and no further action
  // is required.
  if (this == health_check_client_->call_state_.get()) {
    health_check_client_->call_state_.reset();
    if (retry) {
      GPR_ASSERT(!health_check_client_->shutting_down_);
      if (static_cast<bool>(gpr_atm_acq_load(&seen_response_))) {
        // If the call fails after we've gotten a successful response, reset
        // the backoff and restart the call immediately.
        health_check_client_->retry_backoff_.Reset();
        health_check_client_->StartCall();
      } else {
        // If the call failed without receiving any messages, retry later.
        health_check_client_->StartRetryTimer();
      }
    }
  }
  Unref(DEBUG_LOCATION, "call_ended");
}

}  // namespace grpc_core
