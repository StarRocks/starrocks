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
    static StatusOr<MutableColumns> get_lower_boundaries_from(const std::vector<TTabletRange>& tablet_ranges,
                                                              size_t num_columns);

    /**
     * @brief Create a SeekRange from TabletRangePB.
     * 
     * @note IMPORTANT: The returned SeekRange contains Slices that point directly 
     * to the string data within `tablet_range_pb` if the column type is string. The
     * caller MUST ensure that  `tablet_range_pb` outlives the returned SeekRange to
     * avoid dangling pointers.
     *
     * @param tablet_range_pb The protobuf source of the range.
     * @param tablet_schema The schema used to parse types.
     * @return StatusOr<SeekRange> A SeekRange referencing data in tablet_range_pb.
     */
    static StatusOr<SeekRange> create_seek_range_from(const TabletRangePB& tablet_range_pb,
                                                      const TabletSchemaCSPtr& tablet_schema);

    static StatusOr<SstSeekRange> create_sst_seek_range_from(const TabletRangePB& tablet_range_pb,
                                                             const TabletSchemaCSPtr& tablet_schema);

private:
    static Status _parse_string_to_datum(const TypeDescriptor& type_desc, const std::string& value_str, Datum* datum);
};

} // namespace starrocks::lake
