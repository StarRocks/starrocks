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

#include "storage/compaction.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/rowset_factory.h"

namespace starrocks {

struct MergeConfig {
    CompactionAlgorithm algorithm = HORIZONTAL_COMPACTION;
};

// heap based rowset merger used for updatable tablet's compaction

Status compaction_merge_rowsets(Tablet& tablet, int64_t version, const vector<RowsetSharedPtr>& rowsets,
                                RowsetWriter* writer, const MergeConfig& cfg,
                                const starrocks::TabletSchemaCSPtr& cur_tablet_schema = nullptr);

} // namespace starrocks
