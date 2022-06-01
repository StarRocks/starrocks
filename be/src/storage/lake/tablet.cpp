// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "storage/lake/tablet.h"

#include "storage/lake/metadata_iterator.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/txn_log.h"

namespace starrocks::lake {

Status Tablet::put_metadata(const TabletMetadata& metadata) {
    return _mgr->put_tablet_metadata(_group, metadata);
}

Status Tablet::put_metadata(TabletMetadataPtr metadata) {
    return _mgr->put_tablet_metadata(_group, metadata);
}

StatusOr<TabletMetadataPtr> Tablet::get_metadata(int64_t version) {
    return _mgr->get_tablet_metadata(_group, _id, version);
}

StatusOr<MetadataIterator> Tablet::list_metadata() {
    return _mgr->list_tablet_metadata(_group, _id);
}

Status Tablet::delete_metadata(int64_t version) {
    return _mgr->delete_tablet_metadata(_group, _id, version);
}

Status Tablet::delete_metadata() {
    return Status::NotSupported("Tablet::delete_metadata");
}

Status Tablet::put_txn_log(const TxnLog& log) {
    // TODO: Check log.tablet_id() == _id
    return _mgr->put_txn_log(_group, log);
}

Status Tablet::put_txn_log(TxnLogPtr log) {
    // TODO: Check log.tablet_id() == _id
    return _mgr->put_txn_log(_group, log);
}

StatusOr<TxnLogPtr> Tablet::get_txn_log(int64_t txn_id) {
    return _mgr->get_txn_log(_group, _id, txn_id);
}

Status Tablet::delete_txn_log(int64_t txn_id) {
    return _mgr->delete_txn_log(_group, _id, txn_id);
}

} // namespace starrocks::lake
