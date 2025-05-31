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

#define FP_NAME(name) FP_PREFIX + name

static const std::string FP_PREFIX = "load::";
static const std::string TABLET_WRITER_OPEN = FP_NAME("tablet_writer_open");
static const std::string TABLET_WRITER_ADD_CHUNKS = FP_NAME("tablet_writer_add_chunks");
static const std::string TABLET_WRITER_ADD_SEGMENT = FP_NAME("tablet_writer_add_segment");
static const std::string TABLET_WRITER_CANCEL = FP_NAME("tablet_writer_cancel");
static const std::string MEMTABLE_FLUSH = FP_NAME("memtable_flush");
static const std::string SEGMENT_FLUSH = FP_NAME("segment_flush");
static const std::string PK_PRELOAD = FP_NAME("pk_preload");
static const std::string COMMIT_TXN = FP_NAME("commit_txn");

#ifdef FIU_ENABLE
void tablet_writer_open_fp_action(RefCountClosure<PTabletWriterOpenResult>* closure, PTabletWriterOpenRequest* request);
void tablet_writer_add_chunks_fp_action(ReusableClosure<PTabletWriterAddBatchResult>* closure,
                                        PTabletWriterAddChunksRequest* request);
void tablet_writer_add_segment_fp_action(ReusableClosure<PTabletWriterAddSegmentResult>* closure,
                                         PTabletWriterAddSegmentRequest* request);
void tablet_writer_cancel_fp_action(::google::protobuf::Closure* closure, brpc::Controller* cntl,
                                    PTabletWriterCancelRequest* request);

Status memtable_flush_fp_action(int64_t txn_id, int64_t tablet_id);
Status segment_flush_fp_action(int64_t txn_id, int64_t tablet_id);

#define TABLET_WRITER_OPEN_FP_ACTION(closure, request) \
    ::starrocks::load::failpoint::tablet_writer_open_fp_action(closure, &request);
#define TABLET_WRITER_ADD_CHUNKS_FP_ACTION(closure, request) \
    ::starrocks::load::failpoint::tablet_writer_add_chunks_fp_action(closure, &request);
#define TABLET_WRITER_ADD_SEGMENT_FP_ACTION(closure, request) \
    ::starrocks::load::failpoint::tablet_writer_add_segment_fp_action(closure, &request);
#define TABLET_WRITER_CANCEL_FP_ACTION(closure, cntl, request) \
    ::starrocks::load::failpoint::tablet_writer_cancel_fp_action(closure, &cntl, &request);
#define MEMTABLE_FLUSH_FP_ACTION(txn_id, tablet_id) \
    { return ::starrocks::load::failpoint::memtable_flush_fp_action(txn_id, tablet_id); }
#define SEGMENT_FLUSH_FP_ACTION(txn_id, tablet_id) \
    { return ::starrocks::load::failpoint::segment_flush_fp_action(txn_id, tablet_id); }
#else
#define TABLET_WRITER_OPEN_FP_ACTION(closure, request)
#define TABLET_WRITER_ADD_CHUNKS_FP_ACTION(closure, request)
#define TABLET_WRITER_ADD_SEGMENT_FP_ACTION(closure, request)
#define TABLET_WRITER_CANCEL_FP_ACTION(closure, request)
#define MEMTABLE_FLUSH_FP_ACTION(txn_id, tablet_id)
#define SEGMENT_FLUSH_FP_ACTION(txn_id, tablet_id)
#endif

} // namespace starrocks::load::failpoint