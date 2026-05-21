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
#include <vector>

#include "common/status.h"
#include "gen_cpp/lake_types.pb.h"
#include "storage/rowset/segment_file_info.h"
#include "storage/tablet_schema.h"

namespace starrocks {
class FileSystem;
} // namespace starrocks

namespace starrocks::lake {
class TabletManager;
} // namespace starrocks::lake

namespace starrocks::secondary_sorted {

// Shared entry point for the three hook sites:
//   - DeltaWriterImpl::finish_with_txnlog (load path)
//   - HorizontalCompactionTask::execute   (horizontal compaction)
//   - VerticalCompactionTask::execute     (vertical compaction)
//
// Behaviour:
//   1. Short-circuit and return Status::OK() with empty `out_pbs` if
//      `config::enable_secondary_index_write` is false or no PocIndexDef
//      is registered for `tablet_id`.
//   2. Otherwise, open the freshly-written segments via Segment::open()
//      and invoke SecondaryIndexWriter::build() per index def. Append
//      the resulting PB entries to `out_pbs`.
//   3. Errors propagate up so the caller can fail the rowset commit.
//
// `seg_file_infos[i].path` is the basename inside the Lake fileset, as
// recorded by the tablet writer. We pair them with sequential seg_ids
// 0..N-1, matching the encoded_pos space used by the writer.
Status maybe_build_secondary_indexes(int64_t tablet_id, int64_t txn_id, const TabletSchemaCSPtr& source_schema,
                                     const std::vector<SegmentFileInfo>& seg_file_infos,
                                     std::shared_ptr<FileSystem> fs, lake::TabletManager* tablet_mgr,
                                     std::vector<SecondaryIndexFilePB>* out_pbs);

} // namespace starrocks::secondary_sorted
