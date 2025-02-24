// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <memory>
#include <mutex>

#include "common/status.h"
#include "gen_cpp/internal_service.pb.h"
#include "service/brpc.h"

namespace starrocks {

class PInternalService_RecoverableStub : public PInternalService,
                                         public std::enable_shared_from_this<PInternalService_RecoverableStub> {
public:
    PInternalService_RecoverableStub(const butil::EndPoint& endpoint);
    ~PInternalService_RecoverableStub();

    Status reset_channel(const std::string& protocol = "");

#ifdef BE_TEST
    PInternalService_Stub* stub() { return _stub.get(); }
#endif

    // implements PInternalService ------------------------------------------

    void tablet_writer_open(::google::protobuf::RpcController* controller,
                            const ::starrocks::PTabletWriterOpenRequest* request,
                            ::starrocks::PTabletWriterOpenResult* response, ::google::protobuf::Closure* done);
    void tablet_writer_cancel(::google::protobuf::RpcController* controller,
                              const ::starrocks::PTabletWriterCancelRequest* request,
                              ::starrocks::PTabletWriterCancelResult* response, ::google::protobuf::Closure* done);
    void transmit_chunk(::google::protobuf::RpcController* controller, const ::starrocks::PTransmitChunkParams* request,
                        ::starrocks::PTransmitChunkResult* response, ::google::protobuf::Closure* done);
    void transmit_chunk_via_http(::google::protobuf::RpcController* controller,
                                 const ::starrocks::PHttpRequest* request, ::starrocks::PTransmitChunkResult* response,
                                 ::google::protobuf::Closure* done);
    void tablet_writer_add_chunk(::google::protobuf::RpcController* controller,
                                 const ::starrocks::PTabletWriterAddChunkRequest* request,
                                 ::starrocks::PTabletWriterAddBatchResult* response, ::google::protobuf::Closure* done);
    void tablet_writer_add_chunks(::google::protobuf::RpcController* controller,
                                  const ::starrocks::PTabletWriterAddChunksRequest* request,
                                  ::starrocks::PTabletWriterAddBatchResult* response,
                                  ::google::protobuf::Closure* done);
    void tablet_writer_add_chunk_via_http(::google::protobuf::RpcController* controller,
                                          const ::starrocks::PHttpRequest* request,
                                          ::starrocks::PTabletWriterAddBatchResult* response,
                                          ::google::protobuf::Closure* done);
    void tablet_writer_add_chunks_via_http(::google::protobuf::RpcController* controller,
                                           const ::starrocks::PHttpRequest* request,
                                           ::starrocks::PTabletWriterAddBatchResult* response,
                                           ::google::protobuf::Closure* done);
    void tablet_writer_add_segment(::google::protobuf::RpcController* controller,
                                   const ::starrocks::PTabletWriterAddSegmentRequest* request,
                                   ::starrocks::PTabletWriterAddSegmentResult* response,
                                   ::google::protobuf::Closure* done);
    void load_diagnose(::google::protobuf::RpcController* controller, const ::starrocks::PLoadDiagnoseRequest* request,
                       ::starrocks::PLoadDiagnoseResult* response, ::google::protobuf::Closure* done) override;
    void transmit_runtime_filter(::google::protobuf::RpcController* controller,
                                 const ::starrocks::PTransmitRuntimeFilterParams* request,
                                 ::starrocks::PTransmitRuntimeFilterResult* response,
                                 ::google::protobuf::Closure* done);
    void local_tablet_reader_multi_get(::google::protobuf::RpcController* controller,
                                       const ::starrocks::PTabletReaderMultiGetRequest* request,
                                       ::starrocks::PTabletReaderMultiGetResult* response,
                                       ::google::protobuf::Closure* done);
    void execute_command(::google::protobuf::RpcController* controller,
                         const ::starrocks::ExecuteCommandRequestPB* request,
                         ::starrocks::ExecuteCommandResultPB* response, ::google::protobuf::Closure* done);
    void process_dictionary_cache(::google::protobuf::RpcController* controller,
                                  const ::starrocks::PProcessDictionaryCacheRequest* request,
                                  ::starrocks::PProcessDictionaryCacheResult* response,
                                  ::google::protobuf::Closure* done);

private:
    std::shared_ptr<starrocks::PInternalService_Stub> _stub;
    const butil::EndPoint _endpoint;
    int64_t _connection_group = 0;
    std::mutex _mutex;
    GOOGLE_DISALLOW_EVIL_CONSTRUCTORS(PInternalService_RecoverableStub);
};

} // namespace starrocks
