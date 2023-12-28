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
class Schema;
} // namespace starrocks

namespace starrocks::lake {

class Rowset;
class TabletManager;
class TabletMetadataPB;
class TabletWriter;
class TabletReader;
enum WriterType : int;

// A tablet contains shards of data. There can be multiple versions of the same
// tablet. This class represents a specific version of a tablet.
class VersionedTablet {
    using RowsetPtr = std::shared_ptr<Rowset>;
    using RowsetList = std::vector<RowsetPtr>;
    using TabletMetadataPtr = std::shared_ptr<const TabletMetadataPB>;
    using TabletSchemaPtr = std::shared_ptr<const TabletSchema>;

public:
    // Default constructor. After construction, valid() is false
    VersionedTablet() : _tablet_mgr(nullptr), _metadata() {}

    // |tablet_mgr| cannot be nullptr and must outlive this VersionedTablet.
    // |metadata| cannot be nullptr.
    explicit VersionedTablet(TabletManager* tablet_mgr, TabletMetadataPtr metadata)
            : _tablet_mgr(tablet_mgr), _metadata(std::move(metadata)) {}

    bool valid() const { return _metadata != nullptr; }

    // Same as metadata()->id()
    int64_t id() const;

    // Same as metadata()->version()
    int64_t version() const;

    const TabletMetadataPtr& metadata() const { return _metadata; }

    TabletSchemaPtr get_schema() const;

    // Prerequisite: valid() == true
    RowsetList get_rowsets() const;

    // `segment_max_rows` is used in vertical writer
    StatusOr<std::unique_ptr<TabletWriter>> new_writer(WriterType type, int64_t txn_id,
                                                       uint32_t max_rows_per_segment = 0);

    StatusOr<std::unique_ptr<TabletReader>> new_reader(Schema schema);

    TabletManager* tablet_manager() const { return _tablet_mgr; }

    bool has_delete_predicates() const;

private:
    TabletManager* _tablet_mgr;
    TabletMetadataPtr _metadata;
};

} // namespace starrocks::lake
