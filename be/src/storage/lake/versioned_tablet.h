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
#include <vector>

#include "common/statusor.h"

namespace starrocks {
class TabletSchema;
}

namespace starrocks::lake {

class Rowset;
class TabletManager;
class TabletMetadataPB;

// A tablet contains shards of data. There can be multiple versions of the same
// tablet. This class represents a specific version of a tablet.
class VersionedTablet {
    using RowsetPtr = std::shared_ptr<Rowset>;
    using RowsetList = std::vector<RowsetPtr>;
    using TabletMetadataPtr = std::shared_ptr<const TabletMetadataPB>;
    using TabletSchemaPtr = std::shared_ptr<const TabletSchema>;

public:
    // |tablet_mgr| cannot be nullptr and must outlive this VersionedTablet.
    // |metadata| cannot be nullptr.
    explicit VersionedTablet(TabletManager* tablet_mgr, TabletMetadataPtr metadata)
            : _tablet_mgr(tablet_mgr), _metadata(std::move(metadata)) {}

    const TabletMetadataPtr& metadata() const { return _metadata; }

    TabletSchemaPtr get_schema() const;

    StatusOr<RowsetList> get_rowsets() const;

private:
    TabletManager* _tablet_mgr;
    TabletMetadataPtr _metadata;
};

} // namespace starrocks::lake
