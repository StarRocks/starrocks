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

#include <vector>

#include "column/chunk.h"
#include "common/statusor.h"
#include "gen_cpp/Types_types.h"
#include "gen_cpp/types.pb.h"
#include "storage/lake/sst_seek_range.h"
#include "storage/seek_range.h"
#include "storage/tablet_schema.h"

namespace starrocks::lake {
class TabletRangeHelper {
public:
    /**
     * @brief Create a SeekRange from TabletRangePB.
     *
     * @note IMPORTANT:
     *  - If `mem_pool` is nullptr, the returned SeekRange may contain Slices that point
     *    directly to the string data within `tablet_range_pb` for string-like columns.
     *    The caller MUST ensure that `tablet_range_pb` outlives the returned SeekRange
     *    to avoid dangling pointers.
     *  - If `mem_pool` is non-null, the string data will be copied into `mem_pool`, and
     *    the caller MUST ensure that `mem_pool` outlives the returned SeekRange.
     *
     * @param tablet_range_pb The protobuf source of the range.
     * @param tablet_schema The schema used to parse types.
     * @param mem_pool Optional memory pool used to allocate string data for Slices.
     * @return StatusOr<SeekRange> A SeekRange referencing data owned either by
     *         `tablet_range_pb` or `mem_pool`, depending on `mem_pool`.
     */
    static StatusOr<SeekRange> create_seek_range_from(const TabletRangePB& tablet_range_pb,
                                                      const TabletSchemaCSPtr& tablet_schema, MemPool* mem_pool);

    static StatusOr<SstSeekRange> create_sst_seek_range_from(const TabletRangePB& tablet_range_pb,
                                                             const TabletSchemaCSPtr& tablet_schema);

    static StatusOr<TabletRangePB> convert_t_range_to_pb_range(const TTabletRange& t_range);

private:
    // check if the tablet range is closedOpen
    static Status _validate_tablet_range(const TabletRangePB& tablet_range_pb);
};

} // namespace starrocks::lake
