#pragma once

#include <atomic>

#include "gen_cpp/doris_internal_service.pb.h"

namespace starrocks {
struct CallMetrics {
    CallMetrics(size_t max_size) : metrics(max_size) {
        for (auto& metric : metrics) {
            metric = {};
        }
    }
    std::vector<std::atomic_int32_t> metrics;
};

class ObservePBackendService_Stub : public doris::PBackendService_Stub {
public:
    ObservePBackendService_Stub(CallMetrics& metrics, google::protobuf::RpcChannel* channel,
                                google::protobuf::Service::ChannelOwnership ownership);
    ~ObservePBackendService_Stub() override = default;

    // implements PBackendService ------------------------------------------

    void transmit_data(::google::protobuf::RpcController* controller, const ::starrocks::PTransmitDataParams* request,
                       ::starrocks::PTransmitDataResult* response, ::google::protobuf::Closure* done) override;
    void exec_plan_fragment(::google::protobuf::RpcController* controller,
                            const ::starrocks::PExecPlanFragmentRequest* request,
                            ::starrocks::PExecPlanFragmentResult* response, ::google::protobuf::Closure* done) override;
    void exec_batch_plan_fragments(::google::protobuf::RpcController* controller,
                                   const ::starrocks::PExecBatchPlanFragmentsRequest* request,
                                   ::starrocks::PExecBatchPlanFragmentsResult* response,
                                   ::google::protobuf::Closure* done) override;
    void cancel_plan_fragment(::google::protobuf::RpcController* controller,
                              const ::starrocks::PCancelPlanFragmentRequest* request,
                              ::starrocks::PCancelPlanFragmentResult* response,
                              ::google::protobuf::Closure* done) override;
    void fetch_data(::google::protobuf::RpcController* controller, const ::starrocks::PFetchDataRequest* request,
                    ::starrocks::PFetchDataResult* response, ::google::protobuf::Closure* done) override;
    void tablet_writer_open(::google::protobuf::RpcController* controller,
                            const ::starrocks::PTabletWriterOpenRequest* request,
                            ::starrocks::PTabletWriterOpenResult* response, ::google::protobuf::Closure* done) override;
    void tablet_writer_add_batch(::google::protobuf::RpcController* controller,
                                 const ::starrocks::PTabletWriterAddBatchRequest* request,
                                 ::starrocks::PTabletWriterAddBatchResult* response,
                                 ::google::protobuf::Closure* done) override;
    void tablet_writer_cancel(::google::protobuf::RpcController* controller,
                              const ::starrocks::PTabletWriterCancelRequest* request,
                              ::starrocks::PTabletWriterCancelResult* response,
                              ::google::protobuf::Closure* done) override;
    void trigger_profile_report(::google::protobuf::RpcController* controller,
                                const ::starrocks::PTriggerProfileReportRequest* request,
                                ::starrocks::PTriggerProfileReportResult* response,
                                ::google::protobuf::Closure* done) override;
    void get_info(::google::protobuf::RpcController* controller, const ::starrocks::PProxyRequest* request,
                  ::starrocks::PProxyResult* response, ::google::protobuf::Closure* done) override;
    void get_pulsar_info(::google::protobuf::RpcController* controller, const ::starrocks::PPulsarProxyRequest* request,
                         ::starrocks::PPulsarProxyResult* response, ::google::protobuf::Closure* done) override;
    void transmit_chunk(::google::protobuf::RpcController* controller, const ::starrocks::PTransmitChunkParams* request,
                        ::starrocks::PTransmitChunkResult* response, ::google::protobuf::Closure* done) override;
    void transmit_chunk_via_http(::google::protobuf::RpcController* controller,
                                 const ::starrocks::PHttpRequest* request, ::starrocks::PTransmitChunkResult* response,
                                 ::google::protobuf::Closure* done) override;
    void tablet_writer_add_chunk(::google::protobuf::RpcController* controller,
                                 const ::starrocks::PTabletWriterAddChunkRequest* request,
                                 ::starrocks::PTabletWriterAddBatchResult* response,
                                 ::google::protobuf::Closure* done) override;
    void tablet_writer_add_chunks(::google::protobuf::RpcController* controller,
                                  const ::starrocks::PTabletWriterAddChunksRequest* request,
                                  ::starrocks::PTabletWriterAddBatchResult* response,
                                  ::google::protobuf::Closure* done) override;
    void tablet_writer_add_segment(::google::protobuf::RpcController* controller,
                                   const ::starrocks::PTabletWriterAddSegmentRequest* request,
                                   ::starrocks::PTabletWriterAddSegmentResult* response,
                                   ::google::protobuf::Closure* done) override;
    void transmit_runtime_filter(::google::protobuf::RpcController* controller,
                                 const ::starrocks::PTransmitRuntimeFilterParams* request,
                                 ::starrocks::PTransmitRuntimeFilterResult* response,
                                 ::google::protobuf::Closure* done) override;
    void execute_command(::google::protobuf::RpcController* controller,
                         const ::starrocks::ExecuteCommandRequestPB* request,
                         ::starrocks::ExecuteCommandResultPB* response, ::google::protobuf::Closure* done) override;

private:
    CallMetrics& _metrics;
};
} // namespace starrocks