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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/service/internal_service.h

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include "common/status.h"
#include "gen_cpp/doris_internal_service.pb.h"
#include "gen_cpp/internal_service.pb.h"
#include "service/internal_service.h"

namespace brpc {
class Controller;
}

namespace starrocks {

class ExecEnv;

template <typename T>
class BackendInternalServiceImpl : public PInternalServiceImplBase<T> {
public:
    BackendInternalServiceImpl(ExecEnv* exec_env) : PInternalServiceImplBase<T>(exec_env) {}

    void tablet_writer_open(google::protobuf::RpcController* controller, const PTabletWriterOpenRequest* request,
                            PTabletWriterOpenResult* response, google::protobuf::Closure* done) override;

    void tablet_writer_add_batch(google::protobuf::RpcController* controller,
                                 const PTabletWriterAddBatchRequest* request, PTabletWriterAddBatchResult* response,
                                 google::protobuf::Closure* done) override;

    void tablet_writer_add_chunk(google::protobuf::RpcController* controller,
                                 const PTabletWriterAddChunkRequest* request, PTabletWriterAddBatchResult* response,
                                 google::protobuf::Closure* done) override;

    void tablet_writer_add_chunks(google::protobuf::RpcController* controller,
                                  const PTabletWriterAddChunksRequest* request, PTabletWriterAddBatchResult* response,
                                  google::protobuf::Closure* done) override;

    void tablet_writer_add_segment(google::protobuf::RpcController* controller,
                                   const PTabletWriterAddSegmentRequest* request,
                                   PTabletWriterAddSegmentResult* response, google::protobuf::Closure* done) override;

    void tablet_writer_cancel(google::protobuf::RpcController* controller, const PTabletWriterCancelRequest* request,
                              PTabletWriterCancelResult* response, google::protobuf::Closure* done) override;

    void local_tablet_reader_open(google::protobuf::RpcController* controller, const PTabletReaderOpenRequest* request,
                                  PTabletReaderOpenResult* response, google::protobuf::Closure* done) override;
    void local_tablet_reader_close(google::protobuf::RpcController* controller,
                                   const PTabletReaderCloseRequest* request, PTabletReaderCloseResult* response,
                                   google::protobuf::Closure* done) override;
    void local_tablet_reader_multi_get(google::protobuf::RpcController* controller,
                                       const PTabletReaderMultiGetRequest* request,
                                       PTabletReaderMultiGetResult* response, google::protobuf::Closure* done) override;
    void local_tablet_reader_scan_open(google::protobuf::RpcController* controller,
                                       const PTabletReaderScanOpenRequest* request,
                                       PTabletReaderScanOpenResult* response, google::protobuf::Closure* done) override;
    void local_tablet_reader_scan_get_next(google::protobuf::RpcController* controller,
                                           const PTabletReaderScanGetNextRequest* request,
                                           PTabletReaderScanGetNextResult* response,
                                           google::protobuf::Closure* done) override;
};

} // namespace starrocks
