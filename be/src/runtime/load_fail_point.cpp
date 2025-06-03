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

DEFINE_FAIL_POINT(load_tablet_writer_open);
DEFINE_FAIL_POINT(load_tablet_writer_add_chunks);
DEFINE_FAIL_POINT(load_tablet_writer_add_segment);
DEFINE_FAIL_POINT(load_tablet_writer_cancel);
DEFINE_FAIL_POINT(load_commit_txn);
DEFINE_FAIL_POINT(load_memtable_flush);
DEFINE_FAIL_POINT(load_segment_flush);
DEFINE_FAIL_POINT(load_pk_preload);

#ifdef FIU_ENABLE

#define LOG_FP(name) LOG(INFO) << "load_failpoint: " << #name
#define LOG_BRPC_FP(name, cntl, request)                                                        \
    LOG_FP(name) << ", remote_ip: " << get_remote_ip(cntl) << ", txn_id: " << request->txn_id() \
                 << ", load_id: " << print_id(request->id())

#define BRPC_ERROR_MSG(name, txn_id) \
    fmt::format("{} failpoint triggered failure, be: {}, txn_id: {}", #name, BackendOptions::get_localhost(), txn_id)
#define IO_ERROR_MSG(name, txn_id, tablet_id)                                               \
    fmt::format("{} failpoint triggered failure, be: {}, txn_id: {}, tablet_id: {}", #name, \
                BackendOptions::get_localhost(), txn_id, tablet_id)

std::string get_remote_ip(const brpc::Controller& cntl) {
    return butil::ip2str(cntl.remote_side().ip).c_str();
}

void tablet_writer_open_fp_action(RefCountClosure<PTabletWriterOpenResult>* closure,
                                  PTabletWriterOpenRequest* request) {
    std::string tablet_ids = JoinMapped(
            request->tablets(), [](const PTabletWithPartition& tablet) { return std::to_string(tablet.tablet_id()); },
            ",");
    LOG_BRPC_FP(load_tablet_writer_open, closure->cntl, request)
            << ", send_id: " << request->sender_id() << ", tablet_ids: " << tablet_ids;
    closure->cntl.SetFailed(BRPC_ERROR_MSG(load_tablet_writer_open, request->txn_id()));
}

void tablet_writer_add_chunks_fp_action(ReusableClosure<PTabletWriterAddBatchResult>* closure,
                                        PTabletWriterAddChunksRequest* request) {
    CHECK(request->requests().size() > 0);
    LOG_BRPC_FP(load_tablet_writer_add_chunks, closure->cntl, request->mutable_requests(0))
            << ", send_id: " << request->mutable_requests(0)->sender_id();
    closure->cntl.SetFailed(BRPC_ERROR_MSG(load_tablet_writer_add_chunks, request->mutable_requests(0)->txn_id()));
}

void tablet_writer_add_segment_fp_action(ReusableClosure<PTabletWriterAddSegmentResult>* closure,
                                         PTabletWriterAddSegmentRequest* request) {
    LOG_BRPC_FP(load_tablet_writer_add_segment, closure->cntl, request) << ", tablet_id: " << request->tablet_id();
    closure->cntl.SetFailed(BRPC_ERROR_MSG(load_tablet_writer_add_segment, request->txn_id()));
}

void tablet_writer_cancel_fp_action(::google::protobuf::Closure* closure, brpc::Controller* cntl,
                                    PTabletWriterCancelRequest* request) {
    std::string tablet_ids;
    if (!request->tablet_ids().empty()) {
        tablet_ids = JoinElements(request->tablet_ids(), ",");
    }
    LOG_BRPC_FP(load_tablet_writer_cancel, *cntl, request) << ", send_id: " << request->sender_id() << ", tablet_ids: ("
                                                           << tablet_ids << "), reason: " << request->reason();
    cntl->SetFailed(BRPC_ERROR_MSG(load_tablet_writer_cancel, request->txn_id()));
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