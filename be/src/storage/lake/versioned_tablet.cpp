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

#include "storage/lake/versioned_tablet.h"

#include "storage/lake/tablet_metadata.h"
#include "storage/tablet_schema_map.h"
#include "storage/lake/rowset.h"
#include "storage/lake/tablet.h"

namespace starrocks::lake {

VersionedTablet::TabletSchemaPtr VersionedTablet::get_schema() const {
    return GlobalTabletSchemaMap::Instance()->emplace(_metadata->schema()).first;
}

StatusOr<VersionedTablet::RowsetList> VersionedTablet::get_rowsets() const {
    std::vector<RowsetPtr> rowsets;
    rowsets.reserve(_metadata->rowsets_size());
    Tablet tablet(_tablet_mgr, _metadata->id());
    for (int i = 0, size = _metadata->rowsets_size(); i < size; ++i) {
        const auto& rowset_metadata = _metadata->rowsets(i);
        auto rowset = std::make_shared<Rowset>(tablet, std::make_shared<const RowsetMetadata>(rowset_metadata), i);
        rowsets.emplace_back(std::move(rowset));
    }
    return rowsets;
}

} // namespace starrocks::lake