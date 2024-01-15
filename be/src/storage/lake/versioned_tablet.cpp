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

#include "storage/lake/pk_tablet_writer.h"
#include "storage/lake/rowset.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/lake/tablet_reader.h"
#include "storage/lake/tablet_writer.h"
#include "storage/tablet_schema_map.h"

namespace starrocks::lake {

VersionedTablet::TabletSchemaPtr VersionedTablet::get_schema() const {
    return GlobalTabletSchemaMap::Instance()->emplace(_metadata->schema()).first;
}

int64_t VersionedTablet::id() const {
    return _metadata->id();
}

int64_t VersionedTablet::version() const {
    return _metadata->version();
}

StatusOr<std::unique_ptr<TabletWriter>> VersionedTablet::new_writer(WriterType type, int64_t txn_id,
                                                                    uint32_t max_rows_per_segment) {
    std::unique_ptr<TabletWriter> tablet_writer;
    auto tablet_schema = get_schema();
    if (tablet_schema->keys_type() == KeysType::PRIMARY_KEYS) {
        if (type == kHorizontal) {
            tablet_writer = std::make_unique<HorizontalPkTabletWriter>(id(), tablet_schema, txn_id);
        } else {
            DCHECK(type == kVertical);
            tablet_writer = std::make_unique<VerticalPkTabletWriter>(id(), tablet_schema, txn_id, max_rows_per_segment);
        }
    } else {
        if (type == kHorizontal) {
            tablet_writer = std::make_unique<HorizontalGeneralTabletWriter>(id(), tablet_schema, txn_id);
        } else {
            DCHECK(type == kVertical);
            tablet_writer =
                    std::make_unique<VerticalGeneralTabletWriter>(id(), tablet_schema, txn_id, max_rows_per_segment);
        }
    }
    tablet_writer->set_location_provider(_tablet_mgr->location_provider());
    return tablet_writer;
}

StatusOr<std::unique_ptr<TabletReader>> VersionedTablet::new_reader(Schema schema) {
    return std::make_unique<TabletReader>(_tablet_mgr, _metadata, std::move(schema));
}

bool VersionedTablet::has_delete_predicates() const {
    for (const auto& rowset : _metadata->rowsets()) {
        if (rowset.has_delete_predicate()) {
            return true;
        }
    }
    return false;
}

std::vector<RowsetPtr> VersionedTablet::get_rowsets() const {
    return Rowset::get_rowsets(_tablet_mgr, _metadata);
}

} // namespace starrocks::lake