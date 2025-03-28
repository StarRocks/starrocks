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

#include "util/internal_service_recoverable_stub.h"

#include <utility>

#include "common/config.h"

namespace starrocks {

class RecoverableClosure : public ::google::protobuf::Closure {
public:
    RecoverableClosure(std::shared_ptr<starrocks::PInternalService_RecoverableStub> stub,
                       ::google::protobuf::RpcController* controller, ::google::protobuf::Closure* done)
            : _stub(std::move(std::move(stub))), _controller(controller), _done(done) {}

    void Run() override {
        auto* cntl = static_cast<brpc::Controller*>(_controller);
        if (cntl->Failed() && cntl->ErrorCode() == EHOSTDOWN) {
            auto st = _stub->reset_channel();
            if (!st.ok()) {
                LOG(WARNING) << "Fail to reset channel: " << st.to_string();
            }
        }
        _done->Run();
        delete this;
    }

private:
    std::shared_ptr<starrocks::PInternalService_RecoverableStub> _stub;
    ::google::protobuf::RpcController* _controller;
    ::google::protobuf::Closure* _done;
};

PInternalService_RecoverableStub::PInternalService_RecoverableStub(const butil::EndPoint& endpoint)
        : _endpoint(endpoint) {}

PInternalService_RecoverableStub::~PInternalService_RecoverableStub() = default;

Status PInternalService_RecoverableStub::reset_channel(const std::string& protocol) {
    std::lock_guard<std::mutex> l(_mutex);
    brpc::ChannelOptions options;
    options.connect_timeout_ms = config::rpc_connect_timeout_ms;
    if (protocol == "http") {
        options.protocol = protocol;
    } else {
        // http does not support these.
        options.connection_type = config::brpc_connection_type;
        options.connection_group = std::to_string(_connection_group++);
    }
    options.max_retry = 3;
    std::unique_ptr<brpc::Channel> channel(new brpc::Channel());
    if (channel->Init(_endpoint, &options)) {
        LOG(WARNING) << "Fail to init channel " << _endpoint;
        return Status::InternalError("Fail to init channel");
    }
    _stub = std::make_shared<PInternalService_Stub>(channel.release(), google::protobuf::Service::STUB_OWNS_CHANNEL);
    return Status::OK();
}

void PInternalService_RecoverableStub::tablet_writer_open(::google::protobuf::RpcController* controller,
                                                          const ::starrocks::PTabletWriterOpenRequest* request,
                                                          ::starrocks::PTabletWriterOpenResult* response,
                                                          ::google::protobuf::Closure* done) {
    auto closure = new RecoverableClosure(shared_from_this(), controller, done);
    _stub->tablet_writer_open(controller, request, response, closure);
}

void PInternalService_RecoverableStub::tablet_writer_cancel(::google::protobuf::RpcController* controller,
                                                            const ::starrocks::PTabletWriterCancelRequest* request,
                                                            ::starrocks::PTabletWriterCancelResult* response,
                                                            ::google::protobuf::Closure* done) {
    auto closure = new RecoverableClosure(shared_from_this(), controller, done);
    _stub->tablet_writer_cancel(controller, request, response, closure);
}

void PInternalService_RecoverableStub::transmit_chunk(::google::protobuf::RpcController* controller,
                                                      const ::starrocks::PTransmitChunkParams* request,
                                                      ::starrocks::PTransmitChunkResult* response,
                                                      ::google::protobuf::Closure* done) {
    auto closure = new RecoverableClosure(shared_from_this(), controller, done);
    _stub->transmit_chunk(controller, request, response, closure);
}

void PInternalService_RecoverableStub::transmit_chunk_via_http(::google::protobuf::RpcController* controller,
                                                               const ::starrocks::PHttpRequest* request,
                                                               ::starrocks::PTransmitChunkResult* response,
                                                               ::google::protobuf::Closure* done) {
    auto closure = new RecoverableClosure(shared_from_this(), controller, done);
    _stub->transmit_chunk_via_http(controller, request, response, closure);
}

void PInternalService_RecoverableStub::tablet_writer_add_chunk(::google::protobuf::RpcController* controller,
                                                               const ::starrocks::PTabletWriterAddChunkRequest* request,
                                                               ::starrocks::PTabletWriterAddBatchResult* response,
                                                               ::google::protobuf::Closure* done) {
    auto closure = new RecoverableClosure(shared_from_this(), controller, done);
    _stub->tablet_writer_add_chunk(controller, request, response, closure);
}

void PInternalService_RecoverableStub::tablet_writer_add_chunks(
        ::google::protobuf::RpcController* controller, const ::starrocks::PTabletWriterAddChunksRequest* request,
        ::starrocks::PTabletWriterAddBatchResult* response, ::google::protobuf::Closure* done) {
    auto closure = new RecoverableClosure(shared_from_this(), controller, done);
    _stub->tablet_writer_add_chunks(controller, request, response, closure);
}

void PInternalService_RecoverableStub::tablet_writer_add_chunk_via_http(
        ::google::protobuf::RpcController* controller, const ::starrocks::PHttpRequest* request,
        ::starrocks::PTabletWriterAddBatchResult* response, ::google::protobuf::Closure* done) {
    auto closure = new RecoverableClosure(shared_from_this(), controller, done);
    _stub->tablet_writer_add_chunk_via_http(controller, request, response, closure);
}

void PInternalService_RecoverableStub::tablet_writer_add_chunks_via_http(
        ::google::protobuf::RpcController* controller, const ::starrocks::PHttpRequest* request,
        ::starrocks::PTabletWriterAddBatchResult* response, ::google::protobuf::Closure* done) {
    auto closure = new RecoverableClosure(shared_from_this(), controller, done);
    _stub->tablet_writer_add_chunks_via_http(controller, request, response, closure);
}

void PInternalService_RecoverableStub::tablet_writer_add_segment(
        ::google::protobuf::RpcController* controller, const ::starrocks::PTabletWriterAddSegmentRequest* request,
        ::starrocks::PTabletWriterAddSegmentResult* response, ::google::protobuf::Closure* done) {
    auto closure = new RecoverableClosure(shared_from_this(), controller, done);
    _stub->tablet_writer_add_segment(controller, request, response, closure);
}

void PInternalService_RecoverableStub::load_diagnose(::google::protobuf::RpcController* controller,
                                                     const ::starrocks::PLoadDiagnoseRequest* request,
                                                     ::starrocks::PLoadDiagnoseResult* response,
                                                     ::google::protobuf::Closure* done) {
    auto closure = new RecoverableClosure(shared_from_this(), controller, done);
    _stub->load_diagnose(controller, request, response, closure);
}

void PInternalService_RecoverableStub::transmit_runtime_filter(::google::protobuf::RpcController* controller,
                                                               const ::starrocks::PTransmitRuntimeFilterParams* request,
                                                               ::starrocks::PTransmitRuntimeFilterResult* response,
                                                               ::google::protobuf::Closure* done) {
    auto closure = new RecoverableClosure(shared_from_this(), controller, done);
    _stub->transmit_runtime_filter(controller, request, response, closure);
}

void PInternalService_RecoverableStub::local_tablet_reader_multi_get(
        ::google::protobuf::RpcController* controller, const ::starrocks::PTabletReaderMultiGetRequest* request,
        ::starrocks::PTabletReaderMultiGetResult* response, ::google::protobuf::Closure* done) {
    auto closure = new RecoverableClosure(shared_from_this(), controller, done);
    _stub->local_tablet_reader_multi_get(controller, request, response, closure);
}

void PInternalService_RecoverableStub::execute_command(::google::protobuf::RpcController* controller,
                                                       const ::starrocks::ExecuteCommandRequestPB* request,
                                                       ::starrocks::ExecuteCommandResultPB* response,
                                                       ::google::protobuf::Closure* done) {
    auto closure = new RecoverableClosure(shared_from_this(), controller, done);
    _stub->execute_command(controller, request, response, closure);
}

void PInternalService_RecoverableStub::process_dictionary_cache(
        ::google::protobuf::RpcController* controller, const ::starrocks::PProcessDictionaryCacheRequest* request,
        ::starrocks::PProcessDictionaryCacheResult* response, ::google::protobuf::Closure* done) {
    auto closure = new RecoverableClosure(shared_from_this(), controller, done);
    _stub->process_dictionary_cache(controller, request, response, closure);
}

} // namespace starrocks