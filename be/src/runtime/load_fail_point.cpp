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

#include "runtime/load_fail_point.h"

#ifdef FIU_ENABLE
#include "gutil/strings/join.h"
#include "service/backend_options.h"
#include "util/uid_util.h"
#endif

namespace starrocks::load::failpoint {

// ===================================== Network-related fail points
// These fail points simulate RPC failures during data loading

// Simulates RPC failure when issuing a tablet_writer_open.
// Supported in both shared-nothing and shared-data architectures.
DEFINE_FAIL_POINT(load_tablet_writer_open);

// Simulates RPC failure when issuing a tablet_writer_add_chunks
// Supported in both shared-nothing and shared-data architectures.
DEFINE_FAIL_POINT(load_tablet_writer_add_chunks);

// Simulates RPC failure when issuing a tablet_writer_add_segment.
// Only supported in shared-nothing architecture.
DEFINE_FAIL_POINT(load_tablet_writer_add_segment);

// Simulates RPC failure when issuing a tablet_writer_cancel.
// Supported in both shared-nothing and shared-data architectures.
DEFINE_FAIL_POINT(load_tablet_writer_cancel);

// ===================================== I/O-related fail points
// These fail points simulate I/O failures during data loading

// Simulates failure when flushing memtable to storage (disk or S3).
// Supported in both shared-nothing and shared-data architectures.
DEFINE_FAIL_POINT(load_memtable_flush);

// Simulates failure when flushing segment to disk on secondary replicas.
// Only supported in shared-nothing architecture with replicated storage.
DEFINE_FAIL_POINT(load_segment_flush);

// Simulates failure during primary key index preloading in delta writer.
// Supported in both shared-nothing and shared-data architectures.
DEFINE_FAIL_POINT(load_pk_preload);

// Simulates failure during transaction commit at the end of loading.
// Supported in both shared-nothing and shared-data architectures.
DEFINE_FAIL_POINT(load_commit_txn);

#ifdef FIU_ENABLE

#define LOG_FP(name) LOG(INFO) << "load_failpoint: " << #name
#define LOG_BRPC_FP(name, remote_host, request)                                         \
    LOG_FP(name) << ", remote_ip: " << remote_host << ", txn_id: " << request->txn_id() \
                 << ", load_id: " << print_id(request->id())

#define BRPC_ERROR_MSG(name, remote_host, txn_id)                                                                    \
    fmt::format("{} failpoint triggered failure, rpc: {} -> {}, txn_id: {}", #name, BackendOptions::get_localhost(), \
                remote_host, txn_id)
#define IO_ERROR_MSG(name, txn_id, tablet_id)                                               \
    fmt::format("{} failpoint triggered failure, be: {}, txn_id: {}, tablet_id: {}", #name, \
                BackendOptions::get_localhost(), txn_id, tablet_id)

void tablet_writer_open_fp_action(const std::string& remote_host, RefCountClosure<PTabletWriterOpenResult>* closure,
                                  PTabletWriterOpenRequest* request) {
    std::string tablet_ids = JoinMapped(
            request->tablets(), [](const PTabletWithPartition& tablet) { return std::to_string(tablet.tablet_id()); },
            ",");
    LOG_BRPC_FP(load_tablet_writer_open, remote_host, request)
            << ", send_id: " << request->sender_id() << ", tablet_ids: " << tablet_ids;
    closure->cntl.SetFailed(BRPC_ERROR_MSG(load_tablet_writer_open, remote_host, request->txn_id()));
}

void tablet_writer_add_chunks_fp_action(const std::string& remote_host,
                                        ReusableClosure<PTabletWriterAddBatchResult>* closure,
                                        PTabletWriterAddChunksRequest* request) {
    DCHECK(request->requests().size() > 0);
    LOG_BRPC_FP(load_tablet_writer_add_chunks, remote_host, request->mutable_requests(0))
            << ", send_id: " << request->mutable_requests(0)->sender_id();
    closure->cntl.SetFailed(
            BRPC_ERROR_MSG(load_tablet_writer_add_chunks, remote_host, request->mutable_requests(0)->txn_id()));
}

void tablet_writer_add_segment_fp_action(const std::string& remote_host,
                                         ReusableClosure<PTabletWriterAddSegmentResult>* closure,
                                         PTabletWriterAddSegmentRequest* request) {
    LOG_BRPC_FP(load_tablet_writer_add_segment, remote_host, request) << ", tablet_id: " << request->tablet_id();
    closure->cntl.SetFailed(BRPC_ERROR_MSG(load_tablet_writer_add_segment, remote_host, request->txn_id()) +
                            ", tablet_id: " + std::to_string(request->tablet_id()));
}

void tablet_writer_cancel_fp_action(const std::string& remote_host, ::google::protobuf::Closure* closure,
                                    brpc::Controller* cntl, PTabletWriterCancelRequest* request) {
    std::string tablet_ids;
    if (!request->tablet_ids().empty()) {
        tablet_ids = JoinElements(request->tablet_ids(), ",");
    }
    LOG_BRPC_FP(load_tablet_writer_cancel, remote_host, request)
            << ", send_id: " << request->sender_id() << ", tablet_ids: (" << tablet_ids
            << "), reason: " << request->reason();
    cntl->SetFailed(BRPC_ERROR_MSG(load_tablet_writer_cancel, remote_host, request->txn_id()));
}

Status memtable_flush_fp_action(int64_t txn_id, int64_t tablet_id) {
    LOG_FP(load_memtable_flush) << ", txn_id: " << txn_id << ", tablet_id: " << tablet_id;
    return Status::IOError(IO_ERROR_MSG(load_memtable_flush, txn_id, tablet_id));
}

Status segment_flush_fp_action(int64_t txn_id, int64_t tablet_id) {
    LOG_FP(load_segment_flush) << ", txn_id: " << txn_id << ", tablet_id: " << tablet_id;
    return Status::IOError(IO_ERROR_MSG(load_segment_flush, txn_id, tablet_id));
}

Status pk_preload_fp_action(int64_t txn_id, int64_t tablet_id) {
    LOG_FP(load_pk_preload) << ", txn_id: " << txn_id << ", tablet_id: " << tablet_id;
    return Status::IOError(IO_ERROR_MSG(load_pk_preload, txn_id, tablet_id));
}

Status commit_txn_fp_action(int64_t txn_id, int64_t tablet_id) {
    LOG_FP(load_commit_txn) << ", txn_id: " << txn_id << ", tablet_id: " << tablet_id;
    return Status::IOError(IO_ERROR_MSG(load_commit_txn, txn_id, tablet_id));
}

#endif

} // namespace starrocks::load::failpoint