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
#include "gutil/strings/join.h"
#include "util/uid_util.h"

#define LOG_BRPC_FP(name, closure, request)                                                    \
    LOG(INFO) << "load_failpoint: " << name << ", remote_ip: " << get_remote_ip(closure->cntl) \
              << ", txn_id: " << request->txn_id() << ", load_id: " << print_id(request->id())

std::string get_remote_ip(const brpc::Controller& cntl) {
    return butil::ip2str(cntl.remote_side().ip).c_str();
}

void tablet_writer_open_fp_action(RefCountClosure<PTabletWriterOpenResult>* closure,
                                  PTabletWriterOpenRequest* request) {
    std::string tablet_ids = JoinMapped(
            request->tablets(), [](const PTabletWithPartition& tablet) { return std::to_string(tablet.tablet_id()); },
            ",");
    LOG_BRPC_FP(TABLET_WRITER_OPEN, closure, request) << ", tablet_ids: " << tablet_ids;
    closure->cntl.SetFailed("failpoint triggered failure");
    closure->Run();
}

void tablet_writer_add_chunks_fp_action(ReusableClosure<PTabletWriterAddBatchResult>* closure,
                                        PTabletWriterAddChunksRequest* request) {
    CHECK(request->requests().size() > 0);
    LOG_BRPC_FP(TABLET_WRITER_ADD_CHUNKS, closure, request->mutable_requests(0));
    closure->cntl.SetFailed("failpoint triggered failure");
    closure->Run();
}

void tablet_writer_add_segment_fp_action(ReusableClosure<PTabletWriterAddSegmentResult>* closure,
                                         PTabletWriterAddSegmentRequest* request) {
    LOG_BRPC_FP(TABLET_WRITER_ADD_SEGMENT, closure, request) << ", tablet_id: " << request->tablet_id();
    closure->cntl.SetFailed("failpoint triggered failure");
    closure->Run();
}

void tablet_writer_cancel_fp_action(RefCountClosure<PTabletWriterCancelResult>* closure,
                                    PTabletWriterCancelRequest* request) {
    std::string tablet_ids;
    if (request->has_tablet_id()) {
        tablet_ids = std::to_string(request->tablet_id());
    } else if (!request->tablet_ids().empty()) {
        tablet_ids = JoinElements(request->tablet_ids(), ",");
    }
    LOG_BRPC_FP(TABLET_WRITER_CANCEL, closure, request)
            << ", tablet_ids: (" << tablet_ids << "), reason: " << request->reason();
    closure->cntl.SetFailed("failpoint triggered failure");
    closure->Run();
}
#endif

} // namespace starrocks::load::failpoint