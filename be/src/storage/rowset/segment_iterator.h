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
#include <optional>
#include <vector>

#include "common/statusor.h"
#include "storage/chunk_iterator.h"
#include "storage/options.h"
#include "storage/range.h"
#include "storage/seek_range.h"

namespace starrocks {
class Segment;

class ColumnPredicate;
class Schema;
class SegmentReadOptions;

ChunkIteratorPtr new_segment_iterator(const std::shared_ptr<Segment>& segment, const Schema& schema,
                                      const SegmentReadOptions& options);
ChunkIteratorPtr new_raw_segment_iterator(const std::shared_ptr<Segment>& segment, const Schema& schema,
                                          const SegmentReadOptions& options);
Status reset_raw_segment_iterator(const ChunkIteratorPtr& iter, const SegmentReadOptions& options);
StatusOr<SparseRange<>> new_segment_iterator_for_execution_pruning(const std::shared_ptr<Segment>& segment,
                                                                   const Schema& schema,
                                                                   const SegmentReadOptions& options);
StatusOr<SparseRange<>> new_segment_iterator_for_prepare_pruning(const std::shared_ptr<Segment>& segment,
                                                                 const Schema& schema,
                                                                 const SegmentReadOptions& options);

StatusOr<SparseRange<>> get_segment_scan_range_by_key_ranges(const std::shared_ptr<Segment>& segment,
                                                             const std::vector<SeekRange>& ranges,
                                                             const LakeIOOptions& lake_io_opts);
StatusOr<std::vector<std::optional<Range<>>>> get_segment_rowid_ranges_by_seek_ranges(
        const std::shared_ptr<Segment>& segment, const std::vector<SeekRange>& ranges,
        const LakeIOOptions& lake_io_opts);
StatusOr<std::optional<Range<>>> get_segment_rowid_range_by_seek_range(const std::shared_ptr<Segment>& segment,
                                                                       const SeekRange& range,
                                                                       const LakeIOOptions& lake_io_opts);

} // namespace starrocks
