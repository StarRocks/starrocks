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

#include <optional>
#include <shared_mutex>
#include <unordered_map>
#include <vector>

#include "compute_env/query/global_late_materialization_context.h"
#include "gen_cpp/PlanNodes_types.h"
#include "storage/lake/types_fwd.h"
#include "storage/rowset/base_rowset.h"

namespace starrocks {

class LakeScanLazyMaterializationContext : public GlobalLateMaterilizationContext {
public:
    void capture_rowsets(int32_t tablet_id, int64_t version, const std::vector<BaseRowsetSharedPtr>& rowsets);

    lake::RowsetPtr get_rowset(int32_t tablet_id, int32_t dynamic_rssid, int32_t* segment_idx) const;
    lake::RowsetPtr get_rowset(const std::vector<lake::RowsetPtr>& rowsets, int32_t drssid, int32_t* segment_idx) const;

    int64_t get_rowsets_version(int32_t tablet_id) const {
        std::shared_lock lock(_mutex);
        return _versions.at(tablet_id);
    }

    const TLakeScanNode& scan_node() const { return *_thrift_lake_scan_node; }
    void set_scan_node(const TLakeScanNode& node);

private:
    mutable std::shared_mutex _mutex;
    std::unordered_map<int32_t, std::vector<lake::RowsetPtr>> _rowsets;
    std::unordered_map<int32_t, int64_t> _versions;
    std::optional<TLakeScanNode> _thrift_lake_scan_node;
};

} // namespace starrocks
