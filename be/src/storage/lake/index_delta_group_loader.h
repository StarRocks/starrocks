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

#include "storage/lake/index_delta_group.h"
#include "storage/lake/tablet_metadata.h"

namespace starrocks::lake {

// Lake (shared-data) implementation of IndexDeltaGroupLoader. Reads IDG
// entries from `TabletMetadataPB.idg_meta` carried by `_tablet_metadata`.
// Mirrors `LakeDeltaColumnGroupLoader` for IDG / .idx instead of DCG / .cols.
//
// Construction is cheap: only stores a TabletMetadataPtr alias. Lookups walk
// the per-segment entry list, filter by version + dropped_keys, and project
// into `IndexDeltaGroupEntry`.
class LakeIndexDeltaGroupLoader : public IndexDeltaGroupLoader {
public:
    explicit LakeIndexDeltaGroupLoader(TabletMetadataPtr tablet_metadata)
            : _tablet_metadata(std::move(tablet_metadata)) {}

    Status load(const TabletSegmentId& tsid, int64_t query_version, IndexDeltaGroupList* out) override;

private:
    TabletMetadataPtr _tablet_metadata;
};

} // namespace starrocks::lake
