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
#include "storage/olap_common.h"
#include "storage/range.h"

namespace starrocks {
class Segment;
class SeekRange;

class ColumnPredicate;
class Schema;
class SegmentReadOptions;
struct LakeIOOptions;

ChunkIteratorPtr new_segment_iterator(const std::shared_ptr<Segment>& segment, const Schema& schema,
                                      const SegmentReadOptions& options);

// Resolve a key-space SeekRange to the corresponding rowid range within |segment|.
// Wraps the lookup machinery of SegmentIterator so callers outside the
// iterator scan path can convert a TabletRangePB-derived SeekRange into a
// contiguous [lower, upper) rowid window.
// Returns std::nullopt when the range is empty on this segment.
StatusOr<std::optional<Range<rowid_t>>> segment_seek_range_to_rowid_range(const std::shared_ptr<Segment>& segment,
                                                                          const SeekRange& range,
                                                                          const LakeIOOptions& lake_io_opts);

} // namespace starrocks
