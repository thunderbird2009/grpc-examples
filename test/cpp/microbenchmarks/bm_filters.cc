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

#include <benchmark/benchmark.h>
#include <string.h>
#include <sstream>

#include <grpc/grpc.h>
#include <grpc/support/alloc.h>
#include <grpc/support/string_util.h>
#include <grpcpp/channel.h>
#include <grpcpp/support/channel_arguments.h>

#include "src/core/ext/filters/client_channel/client_channel.h"
#include "src/core/ext/filters/deadline/deadline_filter.h"
#include "src/core/ext/filters/http/client/http_client_filter.h"
#include "src/core/ext/filters/http/message_compress/message_compress_filter.h"
#include "src/core/ext/filters/http/server/http_server_filter.h"
#include "src/core/ext/filters/load_reporting/server_load_reporting_filter.h"
#include "src/core/ext/filters/message_size/message_size_filter.h"
#include "src/core/lib/channel/channel_stack.h"
#include "src/core/lib/channel/connected_channel.h"
#include "src/core/lib/gprpp/manual_constructor.h"
#include "src/core/lib/iomgr/call_combiner.h"
#include "src/core/lib/profiling/timers.h"
#include "src/core/lib/surface/channel.h"
#include "src/core/lib/transport/transport_impl.h"

#include "src/cpp/client/create_channel_internal.h"
#include "src/proto/grpc/testing/echo.grpc.pb.h"
#include "test/cpp/microbenchmarks/helpers.h"
#include "test/cpp/util/test_config.h"

#define CALL_ELEMS_FROM_STACK(stk)     \
  ((grpc_call_element*)((char*)(stk) + \
                        ROUND_UP_TO_ALIGNMENT_SIZE(sizeof(grpc_call_stack))))

/* Given a size, round up to the next multiple of sizeof(void*) */
#define ROUND_UP_TO_ALIGNMENT_SIZE(x) \
  (((x) + GPR_MAX_ALIGNMENT - 1u) & ~(GPR_MAX_ALIGNMENT - 1u))

auto& force_library_initialization = Library::get();

static void FilterDestroy(void* arg, grpc_error* error) { gpr_free(arg); }

static void DoNothing(void* arg, grpc_error* error) {}

class FakeClientChannelFactory : public grpc_client_channel_factory {
 public:
  FakeClientChannelFactory() { vtable = &vtable_; }

 private:
  static void NoRef(grpc_client_channel_factory* factory) {}
  static void NoUnref(grpc_client_channel_factory* factory) {}
  static grpc_subchannel* CreateSubchannel(grpc_client_channel_factory* factory,
                                           const grpc_subchannel_args* args) {
    return nullptr;
  }
  static grpc_channel* CreateClientChannel(grpc_client_channel_factory* factory,
                                           const char* target,
                                           grpc_client_channel_type type,
                                           const grpc_channel_args* args) {
    return nullptr;
  }

  static const grpc_client_channel_factory_vtable vtable_;
};

const grpc_client_channel_factory_vtable FakeClientChannelFactory::vtable_ = {
    NoRef, NoUnref, CreateSubchannel, CreateClientChannel};

static grpc_arg StringArg(const char* key, const char* value) {
  grpc_arg a;
  a.type = GRPC_ARG_STRING;
  a.key = const_cast<char*>(key);
  a.value.string = const_cast<char*>(value);
  return a;
}

enum FixtureFlags : uint32_t {
  CHECKS_NOT_LAST = 1,
  REQUIRES_TRANSPORT = 2,
};

template <const grpc_channel_filter* kFilter, uint32_t kFlags>
struct Fixture {
  const grpc_channel_filter* filter = kFilter;
  const uint32_t flags = kFlags;
};

namespace dummy_filter {

static void StartTransportStreamOp(grpc_call_element* elem,
                                   grpc_transport_stream_op_batch* op) {}

static void StartTransportOp(grpc_channel_element* elem,
                             grpc_transport_op* op) {}

static grpc_error* InitCallElem(grpc_call_element* elem,
                                const grpc_call_element_args* args) {
  return GRPC_ERROR_NONE;
}

static void SetPollsetOrPollsetSet(grpc_call_element* elem,
                                   grpc_polling_entity* pollent) {}

static void DestroyCallElem(grpc_call_element* elem,
                            const grpc_call_final_info* final_info,
                            grpc_closure* then_sched_closure) {}

grpc_error* InitChannelElem(grpc_channel_element* elem,
                            grpc_channel_element_args* args) {
  return GRPC_ERROR_NONE;
}

void DestroyChannelElem(grpc_channel_element* elem) {}

void GetChannelInfo(grpc_channel_element* elem,
                    const grpc_channel_info* channel_info) {}

static const grpc_channel_filter dummy_filter = {StartTransportStreamOp,
                                                 StartTransportOp,
                                                 0,
                                                 InitCallElem,
                                                 SetPollsetOrPollsetSet,
                                                 DestroyCallElem,
                                                 0,
                                                 InitChannelElem,
                                                 DestroyChannelElem,
                                                 GetChannelInfo,
                                                 "dummy_filter"};

}  // namespace dummy_filter

namespace dummy_transport {

/* Memory required for a single stream element - this is allocated by upper
   layers and initialized by the transport */
size_t sizeof_stream; /* = sizeof(transport stream) */

/* name of this transport implementation */
const char* name;

/* implementation of grpc_transport_init_stream */
int InitStream(grpc_transport* self, grpc_stream* stream,
               grpc_stream_refcount* refcount, const void* server_data,
               gpr_arena* arena) {
  return 0;
}

/* implementation of grpc_transport_set_pollset */
void SetPollset(grpc_transport* self, grpc_stream* stream,
                grpc_pollset* pollset) {}

/* implementation of grpc_transport_set_pollset */
void SetPollsetSet(grpc_transport* self, grpc_stream* stream,
                   grpc_pollset_set* pollset_set) {}

/* implementation of grpc_transport_perform_stream_op */
void PerformStreamOp(grpc_transport* self, grpc_stream* stream,
                     grpc_transport_stream_op_batch* op) {
  GRPC_CLOSURE_SCHED(op->on_complete, GRPC_ERROR_NONE);
}

/* implementation of grpc_transport_perform_op */
void PerformOp(grpc_transport* self, grpc_transport_op* op) {}

/* implementation of grpc_transport_destroy_stream */
void DestroyStream(grpc_transport* self, grpc_stream* stream,
                   grpc_closure* then_sched_closure) {}

/* implementation of grpc_transport_destroy */
void Destroy(grpc_transport* self) {}

/* implementation of grpc_transport_get_endpoint */
grpc_endpoint* GetEndpoint(grpc_transport* self) { return nullptr; }

static const grpc_transport_vtable dummy_transport_vtable = {
    0,          "dummy_http2", InitStream,
    SetPollset, SetPollsetSet, PerformStreamOp,
    PerformOp,  DestroyStream, Destroy,
    GetEndpoint};

static grpc_transport dummy_transport = {&dummy_transport_vtable};

}  // namespace dummy_transport

class NoOp {
 public:
  class Op {
   public:
    Op(NoOp* p, grpc_call_stack* s) {}
    void Finish() {}
  };
};

class SendEmptyMetadata {
 public:
  SendEmptyMetadata() {
    memset(&op_, 0, sizeof(op_));
    op_.on_complete = GRPC_CLOSURE_INIT(&closure_, DoNothing, nullptr,
                                        grpc_schedule_on_exec_ctx);
    op_.send_initial_metadata = true;
    op_.payload = &op_payload_;
  }

  class Op {
   public:
    Op(SendEmptyMetadata* p, grpc_call_stack* s) {
      grpc_metadata_batch_init(&batch_);
      p->op_payload_.send_initial_metadata.send_initial_metadata = &batch_;
    }
    void Finish() { grpc_metadata_batch_destroy(&batch_); }

   private:
    grpc_metadata_batch batch_;
  };

 private:
  const gpr_timespec deadline_ = gpr_inf_future(GPR_CLOCK_MONOTONIC);
  const gpr_timespec start_time_ = gpr_now(GPR_CLOCK_MONOTONIC);
  const grpc_slice method_ = grpc_slice_from_static_string("/foo/bar");
  grpc_transport_stream_op_batch op_;
  grpc_transport_stream_op_batch_payload op_payload_;
  grpc_closure closure_;
};

// Test a filter in isolation. Fixture specifies the filter 
// under test (use the Fixture<> template to specify this), and TestOp defines 
// some unit of work to perform on said filter.
template <class Fixture, class TestOp>
static void BM_IsolatedFilter(benchmark::State& state) {
  TrackCounters track_counters;
  Fixture fixture;
  std::ostringstream label;

  std::vector<grpc_arg> args;
  FakeClientChannelFactory fake_client_channel_factory;
  args.push_back(grpc_client_channel_factory_create_channel_arg(
      &fake_client_channel_factory));
  args.push_back(StringArg(GRPC_ARG_SERVER_URI, "localhost"));

  grpc_channel_args channel_args = {args.size(), &args[0]};

  std::vector<const grpc_channel_filter*> filters;
  if (fixture.filter != nullptr) {
    filters.push_back(fixture.filter);
  }
  if (fixture.flags & CHECKS_NOT_LAST) {
    filters.push_back(&dummy_filter::dummy_filter);
    label << " #has_dummy_filter";
  }

  grpc_core::ExecCtx exec_ctx;
  size_t channel_size = grpc_channel_stack_size(
      filters.data(), filters.size());
  grpc_channel_stack* channel_stack =
      static_cast<grpc_channel_stack*>(gpr_zalloc(channel_size));
  GPR_ASSERT(GRPC_LOG_IF_ERROR(
      "channel_stack_init",
      grpc_channel_stack_init(1, FilterDestroy, channel_stack, &filters[0],
                              filters.size(), &channel_args,
                              fixture.flags & REQUIRES_TRANSPORT
                                  ? &dummy_transport::dummy_transport
                                  : nullptr,
                              "CHANNEL", channel_stack)));
  grpc_core::ExecCtx::Get()->Flush();
  grpc_call_stack* call_stack =
      static_cast<grpc_call_stack*>(gpr_zalloc(channel_stack->call_stack_size));
  grpc_millis deadline = GRPC_MILLIS_INF_FUTURE;
  gpr_timespec start_time = gpr_now(GPR_CLOCK_MONOTONIC);
  grpc_slice method = grpc_slice_from_static_string("/foo/bar");
  grpc_call_final_info final_info;
  TestOp test_op_data;
  grpc_call_element_args call_args;
  call_args.call_stack = call_stack;
  call_args.server_transport_data = nullptr;
  call_args.context = nullptr;
  call_args.path = method;
  call_args.start_time = start_time;
  call_args.deadline = deadline;
  const int kArenaSize = 4096;
  call_args.arena = gpr_arena_create(kArenaSize);
  while (state.KeepRunning()) {
    GPR_TIMER_SCOPE("BenchmarkCycle", 0);
    GRPC_ERROR_UNREF(
        grpc_call_stack_init(channel_stack, 1, DoNothing, nullptr, &call_args));
    typename TestOp::Op op(&test_op_data, call_stack);
    grpc_call_stack_destroy(call_stack, &final_info, nullptr);
    op.Finish();
    grpc_core::ExecCtx::Get()->Flush();
    // recreate arena every 64k iterations to avoid oom
    if (0 == (state.iterations() & 0xffff)) {
      gpr_arena_destroy(call_args.arena);
      call_args.arena = gpr_arena_create(kArenaSize);
    }
  }
  gpr_arena_destroy(call_args.arena);
  grpc_channel_stack_destroy(channel_stack);

  gpr_free(channel_stack);
  gpr_free(call_stack);

  state.SetLabel(label.str());
  track_counters.Finish(state);
}

typedef Fixture<nullptr, 0> NoFilter;
BENCHMARK_TEMPLATE(BM_IsolatedFilter, NoFilter, NoOp);
typedef Fixture<&dummy_filter::dummy_filter, 0> DummyFilter;
BENCHMARK_TEMPLATE(BM_IsolatedFilter, DummyFilter, NoOp);
BENCHMARK_TEMPLATE(BM_IsolatedFilter, DummyFilter, SendEmptyMetadata);
typedef Fixture<&grpc_client_channel_filter, 0> ClientChannelFilter;
BENCHMARK_TEMPLATE(BM_IsolatedFilter, ClientChannelFilter, NoOp);
typedef Fixture<&grpc_message_compress_filter, CHECKS_NOT_LAST> CompressFilter;
BENCHMARK_TEMPLATE(BM_IsolatedFilter, CompressFilter, NoOp);
BENCHMARK_TEMPLATE(BM_IsolatedFilter, CompressFilter, SendEmptyMetadata);
typedef Fixture<&grpc_client_deadline_filter, CHECKS_NOT_LAST>
    ClientDeadlineFilter;
BENCHMARK_TEMPLATE(BM_IsolatedFilter, ClientDeadlineFilter, NoOp);
BENCHMARK_TEMPLATE(BM_IsolatedFilter, ClientDeadlineFilter, SendEmptyMetadata);
typedef Fixture<&grpc_server_deadline_filter, CHECKS_NOT_LAST>
    ServerDeadlineFilter;
BENCHMARK_TEMPLATE(BM_IsolatedFilter, ServerDeadlineFilter, NoOp);
BENCHMARK_TEMPLATE(BM_IsolatedFilter, ServerDeadlineFilter, SendEmptyMetadata);
typedef Fixture<&grpc_http_client_filter, CHECKS_NOT_LAST | REQUIRES_TRANSPORT>
    HttpClientFilter;
BENCHMARK_TEMPLATE(BM_IsolatedFilter, HttpClientFilter, NoOp);
BENCHMARK_TEMPLATE(BM_IsolatedFilter, HttpClientFilter, SendEmptyMetadata);
typedef Fixture<&grpc_http_server_filter, CHECKS_NOT_LAST> HttpServerFilter;
BENCHMARK_TEMPLATE(BM_IsolatedFilter, HttpServerFilter, NoOp);
BENCHMARK_TEMPLATE(BM_IsolatedFilter, HttpServerFilter, SendEmptyMetadata);
typedef Fixture<&grpc_message_size_filter, CHECKS_NOT_LAST> MessageSizeFilter;
BENCHMARK_TEMPLATE(BM_IsolatedFilter, MessageSizeFilter, NoOp);
BENCHMARK_TEMPLATE(BM_IsolatedFilter, MessageSizeFilter, SendEmptyMetadata);
typedef Fixture<&grpc_server_load_reporting_filter, CHECKS_NOT_LAST>
    LoadReportingFilter;
BENCHMARK_TEMPLATE(BM_IsolatedFilter, LoadReportingFilter, NoOp);
BENCHMARK_TEMPLATE(BM_IsolatedFilter, LoadReportingFilter, SendEmptyMetadata);

// Test a filter's call stack init in isolation. Fixture specifies the filter 
// under test (use the Fixture<> template to specify this)
template <class Fixture>
static void BM_CallStackInit(benchmark::State& state) {
  TrackCounters track_counters;
  Fixture fixture;
  std::ostringstream label;

  std::vector<grpc_arg> args;
  FakeClientChannelFactory fake_client_channel_factory;
  args.push_back(grpc_client_channel_factory_create_channel_arg(
      &fake_client_channel_factory));
  args.push_back(StringArg(GRPC_ARG_SERVER_URI, "localhost"));

  grpc_channel_args channel_args = {args.size(), &args[0]};

  std::vector<const grpc_channel_filter*> filters;
  if (fixture.filter != nullptr) {
    filters.push_back(fixture.filter);
    if (fixture.flags & CHECKS_NOT_LAST) {
      filters.push_back(&dummy_filter::dummy_filter);
    } else {
      filters.insert(filters.begin(), &dummy_filter::dummy_filter);
    }
  }


  grpc_core::ExecCtx exec_ctx;
  size_t channel_size = grpc_channel_stack_size(
      filters.data(), filters.size());
  grpc_channel_stack* channel_stack =
      static_cast<grpc_channel_stack*>(gpr_zalloc(channel_size));
  GPR_ASSERT(GRPC_LOG_IF_ERROR(
      "channel_stack_init",
      grpc_channel_stack_init(1, FilterDestroy, channel_stack, &filters[0],
                              filters.size(), &channel_args,
                              fixture.flags & REQUIRES_TRANSPORT
                                  ? &dummy_transport::dummy_transport
                                  : nullptr,
                              "CHANNEL", channel_stack)));
  grpc_core::ExecCtx::Get()->Flush();
  grpc_call_stack* call_stack =
      static_cast<grpc_call_stack*>(gpr_zalloc(channel_stack->call_stack_size));
  grpc_millis deadline = GRPC_MILLIS_INF_FUTURE;
  gpr_timespec start_time = gpr_now(GPR_CLOCK_MONOTONIC);
  grpc_slice method = grpc_slice_from_static_string("/foo/bar");
  grpc_call_final_info final_info;
  grpc_call_element_args call_args;
  call_args.call_stack = call_stack;
  call_args.server_transport_data = nullptr;
  call_args.context = nullptr;
  call_args.path = method;
  call_args.start_time = start_time;
  call_args.deadline = deadline;
  const int kArenaSize = 4096;
  call_args.arena = gpr_arena_create(kArenaSize);
  while (state.KeepRunning()) {
    GPR_TIMER_SCOPE("BenchmarkCycle", 0);
    GRPC_ERROR_UNREF(
        grpc_call_stack_init(channel_stack, 1, DoNothing, nullptr, &call_args));
    grpc_call_stack_destroy(call_stack, &final_info, nullptr);
    // recreate arena every 64k iterations to avoid oom
    if (0 == (state.iterations() & 0xffff)) {
      gpr_arena_destroy(call_args.arena);
      call_args.arena = gpr_arena_create(kArenaSize);
    }
  }
  gpr_arena_destroy(call_args.arena);
  grpc_channel_stack_destroy(channel_stack);

  gpr_free(channel_stack);
  gpr_free(call_stack);

  state.SetLabel(label.str());
  track_counters.Finish(state);
}

BENCHMARK_TEMPLATE(BM_CallStackInit, NoFilter);
BENCHMARK_TEMPLATE(BM_CallStackInit, DummyFilter);
BENCHMARK_TEMPLATE(BM_CallStackInit, ClientChannelFilter);
BENCHMARK_TEMPLATE(BM_CallStackInit, CompressFilter);
BENCHMARK_TEMPLATE(BM_CallStackInit, ClientDeadlineFilter);
BENCHMARK_TEMPLATE(BM_CallStackInit, ServerDeadlineFilter);
BENCHMARK_TEMPLATE(BM_CallStackInit, HttpClientFilter);
BENCHMARK_TEMPLATE(BM_CallStackInit, HttpServerFilter);
BENCHMARK_TEMPLATE(BM_CallStackInit, MessageSizeFilter);
BENCHMARK_TEMPLATE(BM_CallStackInit, LoadReportingFilter);

// Test a filter's start_transport_stream_op_batch in isolation. Fixture 
// specifies the filter under test (use the Fixture<> template to specify this).
template <class Fixture>
static void BM_StartTransportStreamOpBatch(benchmark::State& state) {
  TrackCounters track_counters;
  Fixture fixture;
  std::ostringstream label;

  std::vector<grpc_arg> args;
  FakeClientChannelFactory fake_client_channel_factory;
  args.push_back(grpc_client_channel_factory_create_channel_arg(
      &fake_client_channel_factory));
  args.push_back(StringArg(GRPC_ARG_SERVER_URI, "localhost"));

  grpc_channel_args channel_args = {args.size(), &args[0]};

  std::vector<const grpc_channel_filter*> filters;
  if (fixture.filter != nullptr) {
    filters.push_back(fixture.filter);
    if (fixture.flags & CHECKS_NOT_LAST) {
      filters.push_back(&dummy_filter::dummy_filter);
    } else {
      // Add another dummy filter so that all the benchmarked filters 
      // have a dummy filter on the stack. For consistency. 
      filters.insert(filters.begin(), &dummy_filter::dummy_filter);
    }
  }

  grpc_core::ExecCtx exec_ctx;
  size_t channel_size = grpc_channel_stack_size(
      filters.data(), filters.size());
  grpc_channel_stack* channel_stack =
      static_cast<grpc_channel_stack*>(gpr_zalloc(channel_size));
  GPR_ASSERT(GRPC_LOG_IF_ERROR(
      "channel_stack_init",
      grpc_channel_stack_init(1, FilterDestroy, channel_stack, filters.data(),
                              filters.size(), &channel_args,
                              fixture.flags & REQUIRES_TRANSPORT
                                  ? &dummy_transport::dummy_transport
                                  : nullptr,
                              "CHANNEL", channel_stack)));
  grpc_core::ExecCtx::Get()->Flush();
  grpc_call_stack* call_stack =
      static_cast<grpc_call_stack*>(gpr_zalloc(channel_stack->call_stack_size));
  grpc_millis deadline = GRPC_MILLIS_INF_FUTURE;
  gpr_timespec start_time = gpr_now(GPR_CLOCK_MONOTONIC);
  grpc_slice method = grpc_slice_from_static_string("/foo/bar");
  grpc_call_final_info final_info;
  grpc_call_element_args call_args;
  call_args.call_stack = call_stack;
  call_args.server_transport_data = nullptr;
  call_args.context = nullptr;
  call_args.path = method;
  call_args.start_time = start_time;
  call_args.deadline = deadline;
  const int kArenaSize = 4096;
  call_args.arena = gpr_arena_create(kArenaSize);
  
  while(state.KeepRunning()) {
    GPR_TIMER_SCOPE("BenchmarkCycle", 0);
    memset(call_stack, 0, channel_stack->call_stack_size);
    GRPC_ERROR_UNREF(
      grpc_call_stack_init(channel_stack, 1, DoNothing, nullptr, &call_args));

    /* Create new payload */
    grpc_transport_stream_op_batch_payload payload;
    memset(&payload, 0, sizeof(grpc_transport_stream_op_batch_payload));
    grpc_metadata_batch metadata_batch_send_init;
    grpc_metadata_batch metadata_batch_recv_init;
    grpc_metadata_batch metadata_batch_send_trailing;
    grpc_metadata_batch metadata_batch_recv_trailing;
    grpc_metadata_batch_init(&metadata_batch_send_init);
    grpc_metadata_batch_init(&metadata_batch_recv_init);
    grpc_metadata_batch_init(&metadata_batch_send_trailing);
    grpc_metadata_batch_init(&metadata_batch_recv_trailing);
    payload.send_initial_metadata.send_initial_metadata = &metadata_batch_send_init;

    payload.send_trailing_metadata.send_trailing_metadata = &metadata_batch_send_trailing;
    payload.recv_initial_metadata.recv_initial_metadata = &metadata_batch_recv_init;
    uint32_t recv_flags = 0;
    payload.recv_initial_metadata.recv_flags = &recv_flags;

    gpr_atm peer_address_atm;
    payload.recv_initial_metadata.peer_string = &peer_address_atm;
    std::string peer_address_string = "Unknown.";
    gpr_atm_rel_store(payload.recv_initial_metadata.peer_string, 
                  (gpr_atm)gpr_strdup(peer_address_string.data()));

    payload.recv_trailing_metadata.recv_trailing_metadata = &metadata_batch_recv_trailing;

    grpc_core::OrphanablePtr<grpc_core::ByteStream> op;
    payload.recv_message.recv_message = &op;

    grpc_transport_stream_stats stats;
    memset(&stats, 0, sizeof(grpc_transport_stream_stats));
    payload.collect_stats.collect_stats = &stats;

    grpc_core::ManualConstructor<grpc_core::SliceBufferByteStream> 
        byte_stream_send;
         grpc_slice_buffer sb2;
    grpc_slice_buffer_init(&sb2); 
    byte_stream_send.Init(&sb2, 0);
    payload.send_message.send_message.reset(byte_stream_send.get());

    grpc_slice_buffer sb;
    grpc_slice_buffer_init(&sb);
    grpc_core::SliceBufferByteStream* sbs = grpc_core::New<grpc_core::SliceBufferByteStream>(&sb, 0);
    payload.recv_message.recv_message->reset(sbs);

    /* Create new batch with all 6 ops */
    grpc_transport_stream_op_batch batch;
    memset(&batch, 0, sizeof(grpc_transport_stream_op_batch));
    batch.payload = &payload;
    batch.send_initial_metadata = true;
    batch.send_trailing_metadata = true;
    batch.send_message = true;
    batch.recv_initial_metadata = true;
    batch.recv_message = true;
    batch.recv_trailing_metadata = true;
    batch.collect_stats = true;
    
    grpc_call_element* call_elem = CALL_ELEMS_FROM_STACK(call_args.call_stack);

    if (fixture.filter != nullptr) {      
      fixture.filter->start_transport_stream_op_batch(call_elem, &batch);
    }

    GRPC_CLOSURE_RUN(batch.on_complete, GRPC_ERROR_NONE);
    GRPC_CLOSURE_RUN(batch.payload->recv_initial_metadata.recv_initial_metadata_ready, GRPC_ERROR_NONE);
    GRPC_CLOSURE_RUN(batch.payload->recv_message.recv_message_ready, GRPC_ERROR_NONE);
  }

  grpc_call_stack_destroy(call_stack, &final_info, nullptr);
  grpc_core::ExecCtx::Get()->Flush();

  grpc_channel_stack_destroy(channel_stack);
  gpr_arena_destroy(call_args.arena);

  gpr_free(channel_stack);
  gpr_free(call_stack);

  state.SetLabel(label.str());
  track_counters.Finish(state);
}
BENCHMARK_TEMPLATE(BM_StartTransportStreamOpBatch, NoFilter);
BENCHMARK_TEMPLATE(BM_StartTransportStreamOpBatch, DummyFilter);
BENCHMARK_TEMPLATE(BM_StartTransportStreamOpBatch, CompressFilter);
BENCHMARK_TEMPLATE(BM_StartTransportStreamOpBatch, ClientDeadlineFilter);
BENCHMARK_TEMPLATE(BM_StartTransportStreamOpBatch, ServerDeadlineFilter);
BENCHMARK_TEMPLATE(BM_StartTransportStreamOpBatch, HttpClientFilter);
BENCHMARK_TEMPLATE(BM_StartTransportStreamOpBatch, HttpServerFilter);
BENCHMARK_TEMPLATE(BM_StartTransportStreamOpBatch, MessageSizeFilter);
BENCHMARK_TEMPLATE(BM_StartTransportStreamOpBatch, LoadReportingFilter);

// Some distros have RunSpecifiedBenchmarks under the benchmark namespace,
// and others do not. This allows us to support both modes.
namespace benchmark {
void RunTheBenchmarksNamespaced() { RunSpecifiedBenchmarks(); }
}  // namespace benchmark

int main(int argc, char** argv) {
  ::benchmark::Initialize(&argc, argv);
  ::grpc::testing::InitTest(&argc, &argv, false);
  benchmark::RunTheBenchmarksNamespaced();
  return 0;
}
