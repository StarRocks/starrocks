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
#include <string>
#include <vector>

#include "common/status.h"
#include "common/statusor.h"
#include "gen_cpp/lake_types.pb.h"
#include "gen_cpp/types.pb.h"

namespace starrocks {
class TabletSegmentId;
}

namespace starrocks::lake {

// One resolved IDG entry as seen by readers: an .idx file (or GIN dir) on a
// segment, the (col_uid, index_type) keys it carries, and the alter_version
// at which it was created. dropped_keys are NOT returned here; the loader
// strips them before returning entries (see LakeIndexDeltaGroupLoader::load).
struct IndexDeltaGroupEntry {
    // Active keys after applying tombstones.
    struct Key {
        int32_t col_unique_id = 0;
        IndexType index_type = INDEX_UNKNOWN;
    };

    std::vector<Key> keys;
    std::string index_file;
    int64_t version = 0;
    std::string encryption_meta;
    bool shared_file = false;
    int64_t file_size = 0;
};

using IndexDeltaGroupList = std::vector<IndexDeltaGroupEntry>;
using IndexDeltaGroupListPtr = std::shared_ptr<IndexDeltaGroupList>;

// Read-side abstraction. Implementations resolve segment_id -> IDG list,
// filtering by visible version and dropping tombstoned keys.
class IndexDeltaGroupLoader {
public:
    virtual ~IndexDeltaGroupLoader() = default;

    // Load all IDG entries on `tsid` whose `version <= query_version`,
    // sorted from newest to oldest. Tombstoned keys (per dropped_keys) are
    // stripped from each entry; an entry whose keys all became empty is
    // omitted. Returns OK with empty `out` if the segment has no IDG.
    virtual Status load(const TabletSegmentId& tsid, int64_t query_version, IndexDeltaGroupList* out) = 0;
};

} // namespace starrocks::lake
