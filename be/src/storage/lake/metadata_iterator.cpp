// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "storage/lake/metadata_iterator.h"

#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/lake/txn_log.h"

namespace starrocks::lake {

template <>
StatusOr<TabletMetadataPtr> MetadataIterator<TabletMetadataPtr>::get_metadata_from_tablet_manager(
        const std::string& path) {
    return _manager->get_tablet_metadata(path, false);
}

template <>
StatusOr<TxnLogPtr> MetadataIterator<TxnLogPtr>::get_metadata_from_tablet_manager(const std::string& path) {
    return _manager->get_txn_log(path, false);
}
} // namespace starrocks::lake
