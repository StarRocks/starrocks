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

#include "common/statusor.h"
#include "storage/compaction_utils.h"

namespace starrocks::lake {

class Rowset;
using RowsetPtr = std::shared_ptr<Rowset>;
class Tablet;
using TabletPtr = std::shared_ptr<Tablet>;
class CompactionPolicy;
using CompactionPolicyPtr = std::shared_ptr<CompactionPolicy>;
class TabletMetadataPB;
class TabletManager;

// Compaction policy for lake tablet
class CompactionPolicy {
public:
    explicit CompactionPolicy(TabletPtr tablet) : _tablet(std::move(tablet)) {}
    virtual ~CompactionPolicy() = default;

    virtual StatusOr<std::vector<RowsetPtr>> pick_rowsets(int64_t version) = 0;
    virtual StatusOr<CompactionAlgorithm> choose_compaction_algorithm(const std::vector<RowsetPtr>& rowsets);

    static StatusOr<CompactionPolicyPtr> create_compaction_policy(TabletPtr tablet);

protected:
    TabletPtr _tablet;
};

double compaction_score(TabletManager* tablet_mgr, const TabletMetadataPB& metadata);

} // namespace starrocks::lake
