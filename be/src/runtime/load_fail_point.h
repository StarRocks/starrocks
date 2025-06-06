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

#include "util/failpoint/fail_point.h"

#ifdef FIU_ENABLE
#include "gen_cpp/internal_service.pb.h"
#include "util/ref_count_closure.h"
#include "util/reusable_closure.h"
#endif

namespace starrocks::load::failpoint {

// All fail points are defined as global variables in load_fail_point.cpp. Each fail point
// represents a specific failure scenario that can be triggered during data loading operations.
// The file contains detailed documentation for each fail point's purpose.
//
// Below are the action handlers for each fail point. Each handler is named after its
// corresponding fail point with an "_fp_action" suffix. These handlers implement the
// specific failure behavior when a fail point is triggered.

#ifdef FIU_ENABLE
void tablet_writer_open_fp_action(const std::string& remote_host, RefCountClosure<PTabletWriterOpenResult>* closure,
                                  PTabletWriterOpenRequest* request);
void tablet_writer_add_chunks_fp_action(const std::string& remote_host,
                                        ReusableClosure<PTabletWriterAddBatchResult>* closure,
                                        PTabletWriterAddChunksRequest* request);
void tablet_writer_add_segment_fp_action(const std::string& remote_host,
                                         ReusableClosure<PTabletWriterAddSegmentResult>* closure,
                                         PTabletWriterAddSegmentRequest* request);
void tablet_writer_cancel_fp_action(const std::string& remote_host, ::google::protobuf::Closure* closure,
                                    brpc::Controller* cntl, PTabletWriterCancelRequest* request);

Status memtable_flush_fp_action(int64_t txn_id, int64_t tablet_id);
Status segment_flush_fp_action(int64_t txn_id, int64_t tablet_id);
Status pk_preload_fp_action(int64_t txn_id, int64_t tablet_id);
Status commit_txn_fp_action(int64_t txn_id, int64_t tablet_id);

#define TABLET_WRITER_OPEN_FP_ACTION(remote_host, closure, request) \
    ::starrocks::load::failpoint::tablet_writer_open_fp_action(remote_host, closure, &request)
#define TABLET_WRITER_ADD_CHUNKS_FP_ACTION(remote_host, closure, request) \
    ::starrocks::load::failpoint::tablet_writer_add_chunks_fp_action(remote_host, closure, &request)
#define TABLET_WRITER_ADD_SEGMENT_FP_ACTION(remote_host, closure, request) \
    ::starrocks::load::failpoint::tablet_writer_add_segment_fp_action(remote_host, closure, &request)
#define TABLET_WRITER_CANCEL_FP_ACTION(remote_host, closure, cntl, request) \
    ::starrocks::load::failpoint::tablet_writer_cancel_fp_action(remote_host, closure, &cntl, &request)
#define MEMTABLE_FLUSH_FP_ACTION(txn_id, tablet_id) \
    return ::starrocks::load::failpoint::memtable_flush_fp_action(txn_id, tablet_id)
#define SEGMENT_FLUSH_FP_ACTION(txn_id, tablet_id) \
    return ::starrocks::load::failpoint::segment_flush_fp_action(txn_id, tablet_id)
#define PK_PRELOAD_FP_ACTION(txn_id, tablet_id) ::starrocks::load::failpoint::pk_preload_fp_action(txn_id, tablet_id)
#define COMMIT_TXN_FP_ACTION(txn_id, tablet_id) ::starrocks::load::failpoint::commit_txn_fp_action(txn_id, tablet_id)
#else
#define TABLET_WRITER_OPEN_FP_ACTION(remote_host, closure, request)
#define TABLET_WRITER_ADD_CHUNKS_FP_ACTION(remote_host, closure, request)
#define TABLET_WRITER_ADD_SEGMENT_FP_ACTION(remote_host, closure, request)
#define TABLET_WRITER_CANCEL_FP_ACTION(remote_host, closure, request)
#define MEMTABLE_FLUSH_FP_ACTION(txn_id, tablet_id)
#define SEGMENT_FLUSH_FP_ACTION(txn_id, tablet_id)
#define PK_PRELOAD_FP_ACTION(txn_id, tablet_id)
#define COMMIT_TXN_FP_ACTION(txn_id, tablet_id)
#endif

} // namespace starrocks::load::failpoint