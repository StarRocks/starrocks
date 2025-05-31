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
#include "util/uid_util.h"
#endif

namespace starrocks::load::failpoint {

DEFINE_FAIL_POINT(TABLET_WRITER_OPEN);
DEFINE_FAIL_POINT(TABLET_WRITER_ADD_CHUNKS);
DEFINE_FAIL_POINT(TABLET_WRITER_ADD_SEGMENT);
DEFINE_FAIL_POINT(TABLET_WRITER_CANCEL);
DEFINE_FAIL_POINT(COMMIT_TXN);
DEFINE_FAIL_POINT(MEMTABLE_FLUSH);
DEFINE_FAIL_POINT(SEGMENT_FLUSH);
DEFINE_FAIL_POINT(PK_PRELOAD);

#ifdef FIU_ENABLE

#define LOG_FP(name) LOG(INFO) << "load_failpoint: " << name
#define LOG_BRPC_FP(name, cntl, request)                                                        \
    LOG_FP(name) << ", remote_ip: " << get_remote_ip(cntl) << ", txn_id: " << request->txn_id() \
                 << ", load_id: " << print_id(request->id())

#define ERROR_MSG(name) fmt::format("{} failpoint triggered failure", name)

std::string get_remote_ip(const brpc::Controller& cntl) {
    return butil::ip2str(cntl.remote_side().ip).c_str();
}

void tablet_writer_open_fp_action(RefCountClosure<PTabletWriterOpenResult>* closure,
                                  PTabletWriterOpenRequest* request) {
    std::string tablet_ids = JoinMapped(
            request->tablets(), [](const PTabletWithPartition& tablet) { return std::to_string(tablet.tablet_id()); },
            ",");
    LOG_BRPC_FP(TABLET_WRITER_OPEN, closure->cntl, request) << ", tablet_ids: " << tablet_ids;
    closure->cntl.SetFailed(ERROR_MSG(TABLET_WRITER_OPEN));
    closure->Run();
}

void tablet_writer_add_chunks_fp_action(ReusableClosure<PTabletWriterAddBatchResult>* closure,
                                        PTabletWriterAddChunksRequest* request) {
    CHECK(request->requests().size() > 0);
    LOG_BRPC_FP(TABLET_WRITER_ADD_CHUNKS, closure->cntl, request->mutable_requests(0));
    closure->cntl.SetFailed(ERROR_MSG(TABLET_WRITER_ADD_CHUNKS));
    closure->Run();
}

void tablet_writer_add_segment_fp_action(ReusableClosure<PTabletWriterAddSegmentResult>* closure,
                                         PTabletWriterAddSegmentRequest* request) {
    LOG_BRPC_FP(TABLET_WRITER_ADD_SEGMENT, closure->cntl, request) << ", tablet_id: " << request->tablet_id();
    closure->cntl.SetFailed(ERROR_MSG(TABLET_WRITER_ADD_SEGMENT));
    closure->Run();
}

void tablet_writer_cancel_fp_action(::google::protobuf::Closure* closure, brpc::Controller* cntl,
                                    PTabletWriterCancelRequest* request) {
    std::string tablet_ids;
    if (request->has_tablet_id()) {
        tablet_ids = std::to_string(request->tablet_id());
    } else if (!request->tablet_ids().empty()) {
        tablet_ids = JoinElements(request->tablet_ids(), ",");
    }
    LOG_BRPC_FP(TABLET_WRITER_CANCEL, *cntl, request)
            << ", tablet_ids: (" << tablet_ids << "), reason: " << request->reason();
    cntl->SetFailed(ERROR_MSG(TABLET_WRITER_CANCEL));
    closure->Run();
}

Status memtable_flush_fp_action(int64_t txn_id, int64_t tablet_id) {
    LOG_FP(MEMTABLE_FLUSH) << ", txn_id: " << txn_id << ", tablet_id: " << tablet_id;
    return Status::IOError(ERROR_MSG(MEMTABLE_FLUSH));
}

Status segment_flush_fp_action(int64_t txn_id, int64_t tablet_id) {
    LOG_FP(SEGMENT_FLUSH) << ", txn_id: " << txn_id << ", tablet_id: " << tablet_id;
    return Status::IOError(ERROR_MSG(SEGMENT_FLUSH));
}

Status pk_preload_fp_action(int64_t txn_id, int64_t tablet_id) {
    LOG_FP(PK_PRELOAD) << ", txn_id: " << txn_id << ", tablet_id: " << tablet_id;
    return Status::IOError(ERROR_MSG(PK_PRELOAD));
}

Status commit_txn_fp_action(int64_t txn_id, int64_t tablet_id) {
    LOG_FP(COMMIT_TXN) << ", txn_id: " << txn_id << ", tablet_id: " << tablet_id;
    return Status::IOError(ERROR_MSG(COMMIT_TXN));
}

#endif

} // namespace starrocks::load::failpoint