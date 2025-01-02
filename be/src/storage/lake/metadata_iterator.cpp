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

#include "storage/lake/metadata_iterator.h"

#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/lake/txn_log.h"

namespace starrocks::lake {

template <>
StatusOr<TabletMetadataPtr> MetadataIterator<TabletMetadataPtr>::get_metadata_from_tablet_manager(
        const std::string& path) {
    ASSIGN_OR_RETURN(auto tablet_metadata, _manager->get_tablet_metadata(path, false));

    if (tablet_metadata->gtid() < _max_gtid) {
        return Status::NotFound("no more element");
    }

    _max_gtid = tablet_metadata->gtid();

    if (tablet_metadata->id() == _tablet_id) {
        return tablet_metadata;
    }

    // Handle tablet initial metadata
    auto metadata = std::make_shared<TabletMetadata>(*tablet_metadata);
    metadata->set_id(_tablet_id);
    return metadata;
}

template <>
StatusOr<TxnLogPtr> MetadataIterator<TxnLogPtr>::get_metadata_from_tablet_manager(const std::string& path) {
    return _manager->get_txn_log(path, false);
}
} // namespace starrocks::lake
