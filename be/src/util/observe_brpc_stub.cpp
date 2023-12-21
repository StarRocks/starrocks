#include "util/observe_brpc_stub.h"

#include <atomic>

namespace starrocks {
class ClosureWrapper : public google::protobuf::Closure {
public:
    ClosureWrapper(int method_idx_, CallMetrics& metrics_, google::protobuf::Closure* closure)
            : method_idx(method_idx_), metrics(metrics_), _closure(closure) {
        metrics.metrics[method_idx]++;
    }

    void Run() noexcept override {
        metrics.metrics[method_idx]--;
        _closure->Run();
        delete this;
    }

private:
    int method_idx;
    CallMetrics& metrics;
    google::protobuf::Closure* _closure;
};
// auto closure = new ClosureWrapper(0, _metrics, done);
//

ObservePBackendService_Stub::ObservePBackendService_Stub(CallMetrics& metrics, google::protobuf::RpcChannel* channel,
                                                         google::protobuf::Service::ChannelOwnership ownership)
        : doris::PBackendService_Stub(channel, ownership), _metrics(metrics) {}

void ObservePBackendService_Stub::transmit_data(google::protobuf::RpcController* controller,
                                                const ::starrocks::PTransmitDataParams* request,
                                                ::starrocks::PTransmitDataResult* response,
                                                ::google::protobuf::Closure* done) {
    auto closure = new ClosureWrapper(0, _metrics, done);
    channel()->CallMethod(descriptor()->method(0), controller, request, response, closure);
}
void ObservePBackendService_Stub::exec_plan_fragment(google::protobuf::RpcController* controller,
                                                     const ::starrocks::PExecPlanFragmentRequest* request,
                                                     ::starrocks::PExecPlanFragmentResult* response,
                                                     ::google::protobuf::Closure* done) {
    auto closure = new ClosureWrapper(1, _metrics, done);
    channel()->CallMethod(descriptor()->method(1), controller, request, response, closure);
}
void ObservePBackendService_Stub::exec_batch_plan_fragments(google::protobuf::RpcController* controller,
                                                            const ::starrocks::PExecBatchPlanFragmentsRequest* request,
                                                            ::starrocks::PExecBatchPlanFragmentsResult* response,
                                                            ::google::protobuf::Closure* done) {
    auto closure = new ClosureWrapper(2, _metrics, done);
    channel()->CallMethod(descriptor()->method(2), controller, request, response, closure);
}
void ObservePBackendService_Stub::cancel_plan_fragment(google::protobuf::RpcController* controller,
                                                       const ::starrocks::PCancelPlanFragmentRequest* request,
                                                       ::starrocks::PCancelPlanFragmentResult* response,
                                                       ::google::protobuf::Closure* done) {
    auto closure = new ClosureWrapper(3, _metrics, done);

    channel()->CallMethod(descriptor()->method(3), controller, request, response, closure);
}
void ObservePBackendService_Stub::fetch_data(google::protobuf::RpcController* controller,
                                             const ::starrocks::PFetchDataRequest* request,
                                             ::starrocks::PFetchDataResult* response,
                                             ::google::protobuf::Closure* done) {
    auto closure = new ClosureWrapper(4, _metrics, done);

    channel()->CallMethod(descriptor()->method(4), controller, request, response, closure);
}
void ObservePBackendService_Stub::tablet_writer_open(google::protobuf::RpcController* controller,
                                                     const ::starrocks::PTabletWriterOpenRequest* request,
                                                     ::starrocks::PTabletWriterOpenResult* response,
                                                     ::google::protobuf::Closure* done) {
    auto closure = new ClosureWrapper(5, _metrics, done);

    channel()->CallMethod(descriptor()->method(5), controller, request, response, closure);
}
void ObservePBackendService_Stub::tablet_writer_add_batch(google::protobuf::RpcController* controller,
                                                          const ::starrocks::PTabletWriterAddBatchRequest* request,
                                                          ::starrocks::PTabletWriterAddBatchResult* response,
                                                          ::google::protobuf::Closure* done) {
    auto closure = new ClosureWrapper(6, _metrics, done);

    channel()->CallMethod(descriptor()->method(6), controller, request, response, closure);
}
void ObservePBackendService_Stub::tablet_writer_cancel(google::protobuf::RpcController* controller,
                                                       const ::starrocks::PTabletWriterCancelRequest* request,
                                                       ::starrocks::PTabletWriterCancelResult* response,
                                                       ::google::protobuf::Closure* done) {
    auto closure = new ClosureWrapper(7, _metrics, done);

    channel()->CallMethod(descriptor()->method(7), controller, request, response, closure);
}
void ObservePBackendService_Stub::trigger_profile_report(google::protobuf::RpcController* controller,
                                                         const ::starrocks::PTriggerProfileReportRequest* request,
                                                         ::starrocks::PTriggerProfileReportResult* response,
                                                         ::google::protobuf::Closure* done) {
    auto closure = new ClosureWrapper(8, _metrics, done);

    channel()->CallMethod(descriptor()->method(8), controller, request, response, closure);
}
void ObservePBackendService_Stub::get_info(google::protobuf::RpcController* controller,
                                           const ::starrocks::PProxyRequest* request,
                                           ::starrocks::PProxyResult* response, ::google::protobuf::Closure* done) {
    auto closure = new ClosureWrapper(9, _metrics, done);
    channel()->CallMethod(descriptor()->method(9), controller, request, response, closure);
}
void ObservePBackendService_Stub::get_pulsar_info(google::protobuf::RpcController* controller,
                                                  const ::starrocks::PPulsarProxyRequest* request,
                                                  ::starrocks::PPulsarProxyResult* response,
                                                  ::google::protobuf::Closure* done) {
    auto closure = new ClosureWrapper(10, _metrics, done);
    channel()->CallMethod(descriptor()->method(10), controller, request, response, closure);
}
void ObservePBackendService_Stub::transmit_chunk(google::protobuf::RpcController* controller,
                                                 const ::starrocks::PTransmitChunkParams* request,
                                                 ::starrocks::PTransmitChunkResult* response,
                                                 ::google::protobuf::Closure* done) {
    auto closure = new ClosureWrapper(11, _metrics, done);
    channel()->CallMethod(descriptor()->method(11), controller, request, response, closure);
}
void ObservePBackendService_Stub::transmit_chunk_via_http(google::protobuf::RpcController* controller,
                                                          const ::starrocks::PHttpRequest* request,
                                                          ::starrocks::PTransmitChunkResult* response,
                                                          ::google::protobuf::Closure* done) {
    auto closure = new ClosureWrapper(12, _metrics, done);
    channel()->CallMethod(descriptor()->method(12), controller, request, response, closure);
}
void ObservePBackendService_Stub::tablet_writer_add_chunk(google::protobuf::RpcController* controller,
                                                          const ::starrocks::PTabletWriterAddChunkRequest* request,
                                                          ::starrocks::PTabletWriterAddBatchResult* response,
                                                          ::google::protobuf::Closure* done) {
    auto closure = new ClosureWrapper(13, _metrics, done);
    channel()->CallMethod(descriptor()->method(13), controller, request, response, closure);
}
void ObservePBackendService_Stub::tablet_writer_add_chunks(google::protobuf::RpcController* controller,
                                                           const ::starrocks::PTabletWriterAddChunksRequest* request,
                                                           ::starrocks::PTabletWriterAddBatchResult* response,
                                                           ::google::protobuf::Closure* done) {
    auto closure = new ClosureWrapper(14, _metrics, done);
    channel()->CallMethod(descriptor()->method(14), controller, request, response, closure);
}
void ObservePBackendService_Stub::tablet_writer_add_segment(google::protobuf::RpcController* controller,
                                                            const ::starrocks::PTabletWriterAddSegmentRequest* request,
                                                            ::starrocks::PTabletWriterAddSegmentResult* response,
                                                            ::google::protobuf::Closure* done) {
    auto closure = new ClosureWrapper(15, _metrics, done);
    channel()->CallMethod(descriptor()->method(15), controller, request, response, closure);
}
void ObservePBackendService_Stub::transmit_runtime_filter(google::protobuf::RpcController* controller,
                                                          const ::starrocks::PTransmitRuntimeFilterParams* request,
                                                          ::starrocks::PTransmitRuntimeFilterResult* response,
                                                          ::google::protobuf::Closure* done) {
    auto closure = new ClosureWrapper(16, _metrics, done);
    channel()->CallMethod(descriptor()->method(16), controller, request, response, closure);
}
void ObservePBackendService_Stub::execute_command(google::protobuf::RpcController* controller,
                                                  const ::starrocks::ExecuteCommandRequestPB* request,
                                                  ::starrocks::ExecuteCommandResultPB* response,
                                                  ::google::protobuf::Closure* done) {
    auto closure = new ClosureWrapper(17, _metrics, done);
    channel()->CallMethod(descriptor()->method(17), controller, request, response, closure);
}
} // namespace starrocks