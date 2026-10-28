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

#include <array>
#include <cstdint>
#include <memory>

#include "common/statusor.h"
#include "gen_cpp/PlanNodes_types.h"
#include "gen_cpp/types.pb.h"
#include "storage/seek_range.h"
#include "storage/tablet_schema.h"

namespace starrocks {

// Immutable, query-only ownership interval. This is not a persisted tablet range.
struct RangeGroupTask {
    int32_t group_id;
    TabletRangePB key_range;
    std::array<int32_t, 3> sort_key_unique_ids;
};
using RangeGroupTaskPtr = std::shared_ptr<const RangeGroupTask>;

StatusOr<RangeGroupTaskPtr> decode_range_group_task(const TGroupedRangeTask& wire);
Status validate_range_group_schema(const RangeGroupTask& task, const TabletSchema& schema);
StatusOr<SeekRange> decode_range_group_seek(const RangeGroupTask& task, const TabletSchemaCSPtr& schema);

} // namespace starrocks
