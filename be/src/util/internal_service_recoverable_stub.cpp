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

#include <functional>

#include "common/config.h"

namespace starrocks {

PInternalService_RecoverableStub::PInternalService_RecoverableStub(const butil::EndPoint& endpoint,
                                                                   std::string protocol)
        : _endpoint(endpoint), _protocol(std::move(protocol)) {}

PInternalService_RecoverableStub::~PInternalService_RecoverableStub() = default;

Status PInternalService_RecoverableStub::reset_channel(int64_t next_connection_group) {
    if (next_connection_group == 0) {
        next_connection_group = _connection_group.load() + 1;
    }
    if (next_connection_group != _connection_group + 1) {
        // need to take int64_t overflow into consideration
        return Status::OK();
    }
    brpc::ChannelOptions options;
    options.connect_timeout_ms = config::rpc_connect_timeout_ms;
    if (!_protocol.empty()) {
        options.protocol = _protocol;
    }
    if (_protocol != "http") {
        // http does not support these.
        options.connection_type = config::brpc_connection_type;
        options.connection_group = std::to_string(next_connection_group);
    }
    options.max_retry = 3;
    std::unique_ptr<brpc::Channel> channel(new brpc::Channel());
    if (channel->Init(_endpoint, &options)) {
        LOG(WARNING) << "Fail to init channel " << _endpoint;
        return Status::InternalError("Fail to init channel");
    }
    auto ptr = std::make_unique<PInternalService_Stub>(channel.release(), google::protobuf::Service::STUB_OWNS_CHANNEL);
    std::unique_lock l(_mutex);
    if (next_connection_group == _connection_group.load() + 1) {
        // prevent the underlying _stub been reset again by the same epoch calls
        ++_connection_group;
        _stub.reset(ptr.release());
    }
    return Status::OK();
}

Status PInternalService_RecoverableStub::check_health() {
#ifdef BE_TEST
    if (_endpoint.port == 125) {
        LOG(INFO) << "BE_TEST mode: Port 125 simulated as unhealthy for " << _endpoint;
        return Status::InternalError("Simulated failure for port 111");
    }
    LOG(INFO) << "BE_TEST mode: Endpoint " << _endpoint << " simulated as healthy";
    return Status::OK();
#endif

    // extract host
    std::string host = butil::endpoint2str(_endpoint).c_str();
    size_t colon_pos = host.find(':');
    if (colon_pos != std::string::npos) {
        host = host.substr(0, colon_pos);
    }

    // generate endpoint
    butil::EndPoint http_endpoint;
    if (butil::str2endpoint((host + ":" + std::to_string(config::be_http_port)).c_str(), &http_endpoint) != 0) {
        LOG(WARNING) << "Invalid HTTP endpoint: " << host << ":" << config::be_http_port;
        return Status::InternalError("Invalid HTTP endpoint");
    }

    // generate channel options
    brpc::ChannelOptions options;
    options.connect_timeout_ms = config::rpc_connect_timeout_ms;
    options.timeout_ms = config::rpc_connect_timeout_ms;
    options.max_retry = 3;
    options.protocol = "http";

    // init channel
    brpc::Channel channel;
    if (channel.Init(http_endpoint, &options) != 0) {
        LOG(WARNING) << "Failed to init HTTP channel for " << http_endpoint;
        return Status::InternalError("Failed to init HTTP channel");
    }

    // generate controller
    brpc::Controller cntl;
    cntl.http_request().set_method(brpc::HTTP_METHOD_GET);
    cntl.http_request().uri() = "/api/health";

    // trigger health API
    channel.CallMethod(nullptr, &cntl, nullptr, nullptr, nullptr);

    // get and check response
    if (cntl.Failed()) {
        LOG(WARNING) << "Health check failed for endpoint " << _endpoint << ", error: " << cntl.ErrorText();
        return Status::InternalError("Health check request failed");
    }
    int status_code = cntl.http_response().status_code();
    if (status_code == 200) {
        LOG(INFO) << "Health check passed for endpoint " << _endpoint << " (HTTP " << status_code << ")";
        return Status::OK();
    } else {
        LOG(WARNING) << "Health check failed for endpoint " << _endpoint << ", HTTP status: " << status_code;
        return Status::InternalError("Health check failed");
    }
}

void PInternalService_RecoverableStub::tablet_writer_open(::google::protobuf::RpcController* controller,
                                                          const ::starrocks::PTabletWriterOpenRequest* request,
                                                          ::starrocks::PTabletWriterOpenResult* response,
                                                          ::google::protobuf::Closure* done) {
    auto closure = new RecoverableClosureType(shared_from_this(), controller, done);
    stub()->tablet_writer_open(controller, request, response, closure);
}

void PInternalService_RecoverableStub::tablet_writer_cancel(::google::protobuf::RpcController* controller,
                                                            const ::starrocks::PTabletWriterCancelRequest* request,
                                                            ::starrocks::PTabletWriterCancelResult* response,
                                                            ::google::protobuf::Closure* done) {
    auto closure = new RecoverableClosureType(shared_from_this(), controller, done);
    stub()->tablet_writer_cancel(controller, request, response, closure);
}

void PInternalService_RecoverableStub::transmit_chunk(::google::protobuf::RpcController* controller,
                                                      const ::starrocks::PTransmitChunkParams* request,
                                                      ::starrocks::PTransmitChunkResult* response,
                                                      ::google::protobuf::Closure* done) {
    auto closure = new RecoverableClosureType(shared_from_this(), controller, done);
    stub()->transmit_chunk(controller, request, response, closure);
}

void PInternalService_RecoverableStub::transmit_chunk_via_http(::google::protobuf::RpcController* controller,
                                                               const ::starrocks::PHttpRequest* request,
                                                               ::starrocks::PTransmitChunkResult* response,
                                                               ::google::protobuf::Closure* done) {
    auto closure = new RecoverableClosure(shared_from_this(), controller, done);
    stub()->transmit_chunk_via_http(controller, request, response, closure);
}

void PInternalService_RecoverableStub::tablet_writer_add_chunk(::google::protobuf::RpcController* controller,
                                                               const ::starrocks::PTabletWriterAddChunkRequest* request,
                                                               ::starrocks::PTabletWriterAddBatchResult* response,
                                                               ::google::protobuf::Closure* done) {
    auto closure = new RecoverableClosureType(shared_from_this(), controller, done);
    stub()->tablet_writer_add_chunk(controller, request, response, closure);
}

void PInternalService_RecoverableStub::tablet_writer_add_chunks(
        ::google::protobuf::RpcController* controller, const ::starrocks::PTabletWriterAddChunksRequest* request,
        ::starrocks::PTabletWriterAddBatchResult* response, ::google::protobuf::Closure* done) {
    auto closure = new RecoverableClosureType(shared_from_this(), controller, done);
    stub()->tablet_writer_add_chunks(controller, request, response, closure);
}

void PInternalService_RecoverableStub::tablet_writer_add_chunk_via_http(
        ::google::protobuf::RpcController* controller, const ::starrocks::PHttpRequest* request,
        ::starrocks::PTabletWriterAddBatchResult* response, ::google::protobuf::Closure* done) {
    auto closure = new RecoverableClosureType(shared_from_this(), controller, done);
    stub()->tablet_writer_add_chunk_via_http(controller, request, response, closure);
}

void PInternalService_RecoverableStub::tablet_writer_add_chunks_via_http(
        ::google::protobuf::RpcController* controller, const ::starrocks::PHttpRequest* request,
        ::starrocks::PTabletWriterAddBatchResult* response, ::google::protobuf::Closure* done) {
    auto closure = new RecoverableClosureType(shared_from_this(), controller, done);
    stub()->tablet_writer_add_chunks_via_http(controller, request, response, closure);
}

void PInternalService_RecoverableStub::tablet_writer_add_segment(
        ::google::protobuf::RpcController* controller, const ::starrocks::PTabletWriterAddSegmentRequest* request,
        ::starrocks::PTabletWriterAddSegmentResult* response, ::google::protobuf::Closure* done) {
    auto closure = new RecoverableClosureType(shared_from_this(), controller, done);
    stub()->tablet_writer_add_segment(controller, request, response, closure);
}

void PInternalService_RecoverableStub::get_load_replica_status(google::protobuf::RpcController* controller,
                                                               const PLoadReplicaStatusRequest* request,
                                                               PLoadReplicaStatusResult* response,
                                                               google::protobuf::Closure* done) {
    auto closure = new RecoverableClosureType(shared_from_this(), controller, done);
    stub()->get_load_replica_status(controller, request, response, closure);
}

void PInternalService_RecoverableStub::load_diagnose(::google::protobuf::RpcController* controller,
                                                     const ::starrocks::PLoadDiagnoseRequest* request,
                                                     ::starrocks::PLoadDiagnoseResult* response,
                                                     ::google::protobuf::Closure* done) {
    auto closure = new RecoverableClosureType(shared_from_this(), controller, done);
    stub()->load_diagnose(controller, request, response, closure);
}

void PInternalService_RecoverableStub::transmit_runtime_filter(::google::protobuf::RpcController* controller,
                                                               const ::starrocks::PTransmitRuntimeFilterParams* request,
                                                               ::starrocks::PTransmitRuntimeFilterResult* response,
                                                               ::google::protobuf::Closure* done) {
    auto closure = new RecoverableClosureType(shared_from_this(), controller, done);
    stub()->transmit_runtime_filter(controller, request, response, closure);
}

void PInternalService_RecoverableStub::local_tablet_reader_multi_get(
        ::google::protobuf::RpcController* controller, const ::starrocks::PTabletReaderMultiGetRequest* request,
        ::starrocks::PTabletReaderMultiGetResult* response, ::google::protobuf::Closure* done) {
    auto closure = new RecoverableClosureType(shared_from_this(), controller, done);
    stub()->local_tablet_reader_multi_get(controller, request, response, closure);
}

void PInternalService_RecoverableStub::execute_command(::google::protobuf::RpcController* controller,
                                                       const ::starrocks::ExecuteCommandRequestPB* request,
                                                       ::starrocks::ExecuteCommandResultPB* response,
                                                       ::google::protobuf::Closure* done) {
    auto closure = new RecoverableClosureType(shared_from_this(), controller, done);
    stub()->execute_command(controller, request, response, closure);
}

void PInternalService_RecoverableStub::process_dictionary_cache(
        ::google::protobuf::RpcController* controller, const ::starrocks::PProcessDictionaryCacheRequest* request,
        ::starrocks::PProcessDictionaryCacheResult* response, ::google::protobuf::Closure* done) {
    auto closure = new RecoverableClosureType(shared_from_this(), controller, done);
    stub()->process_dictionary_cache(controller, request, response, closure);
}

void PInternalService_RecoverableStub::fetch_datacache(::google::protobuf::RpcController* controller,
                                                       const ::starrocks::PFetchDataCacheRequest* request,
                                                       ::starrocks::PFetchDataCacheResponse* response,
                                                       ::google::protobuf::Closure* done) {
    stub()->fetch_datacache(controller, request, response, nullptr);
}

} // namespace starrocks
