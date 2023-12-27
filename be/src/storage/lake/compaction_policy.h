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
class CompactionPolicy;
using CompactionPolicyPtr = std::shared_ptr<CompactionPolicy>;
class TabletMetadataPB;
class TabletManager;

// Compaction policy for lake tablet
class CompactionPolicy {
public:
    virtual ~CompactionPolicy();

    virtual StatusOr<std::vector<RowsetPtr>> pick_rowsets() = 0;

    virtual StatusOr<CompactionAlgorithm> choose_compaction_algorithm(const std::vector<RowsetPtr>& rowsets);

    static StatusOr<CompactionPolicyPtr> create(TabletManager* tablet_mgr,
                                                std::shared_ptr<const TabletMetadataPB> tablet_metadata);

protected:
    explicit CompactionPolicy(TabletManager* tablet_mgr, std::shared_ptr<const TabletMetadataPB> tablet_metadata)
            : _tablet_mgr(tablet_mgr), _tablet_metadata(std::move(tablet_metadata)) {
        CHECK(_tablet_mgr != nullptr) << "tablet_mgr is null";
        CHECK(_tablet_metadata != nullptr) << "tablet metadata is null";
    }

    TabletManager* _tablet_mgr;
    std::shared_ptr<const TabletMetadataPB> _tablet_metadata;
};

double compaction_score(TabletManager* tablet_mgr, const std::shared_ptr<const TabletMetadataPB>& metadata);

} // namespace starrocks::lake
