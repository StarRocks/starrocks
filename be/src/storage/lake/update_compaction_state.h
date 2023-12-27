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

#include <string>
#include <unordered_map>

#include "common/status.h"
#include "storage/lake/types_fwd.h"
#include "storage/olap_common.h"

namespace starrocks {

namespace lake {
class Rowset;

class CompactionState {
public:
    CompactionState(Rowset* rowset);
    ~CompactionState();

    CompactionState(const CompactionState&) = delete;
    CompactionState& operator=(const CompactionState&) = delete;

    Status load_segments(Rowset* rowset, const TabletSchema& tablet_schema, uint32_t segment_id);
    void release_segments(uint32_t segment_id);

    std::vector<ColumnUniquePtr> pk_cols;

private:
    Status _load_segments(Rowset* rowset, const TabletSchema& tablet_schema, uint32_t segment_id);
};

} // namespace lake

} // namespace starrocks
