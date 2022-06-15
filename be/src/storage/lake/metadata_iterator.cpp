// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "storage/lake/metadata_iterator.h"

#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/lake/txn_log.h"

namespace starrocks::lake {

template <>
StatusOr<TabletMetadataPtr> MetadataIterator<TabletMetadataPtr>::get_metadata_from_tablet_manager(
        const std::string& group, const std::string& path) {
    return _manager->get_tablet_metadata(group, path);
}

template <>
StatusOr<TxnLogPtr> MetadataIterator<TxnLogPtr>::get_metadata_from_tablet_manager(const std::string& group,
                                                                                  const std::string& path) {
    return _manager->get_txn_log(group, path);
}
} // namespace starrocks::lake
