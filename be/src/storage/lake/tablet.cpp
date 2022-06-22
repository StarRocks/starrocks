// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "storage/lake/tablet.h"

#include "storage/lake/general_tablet_writer.h"
#include "storage/lake/metadata_iterator.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_schema_cache.h"
#include "storage/lake/txn_log.h"
#include "storage/tablet_schema_map.h"

namespace starrocks::lake {

Status Tablet::put_metadata(const TabletMetadata& metadata) {
    return _mgr->put_tablet_metadata(_group, metadata);
}

Status Tablet::put_metadata(TabletMetadataPtr metadata) {
    return _mgr->put_tablet_metadata(_group, std::move(metadata));
}

StatusOr<TabletMetadataPtr> Tablet::get_metadata(int64_t version) {
    return _mgr->get_tablet_metadata(_group, _id, version);
}

StatusOr<TabletMetadataIter> Tablet::list_metadata() {
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
    return _mgr->put_txn_log(_group, std::move(log));
}

StatusOr<TxnLogPtr> Tablet::get_txn_log(int64_t txn_id) {
    return _mgr->get_txn_log(_group, _id, txn_id);
}

Status Tablet::delete_txn_log(int64_t txn_id) {
    return _mgr->delete_txn_log(_group, _id, txn_id);
}

StatusOr<std::unique_ptr<TabletWriter>> Tablet::new_writer() {
    // TODO: check tablet type
    return std::make_unique<GeneralTabletWriter>(*this);
}

StatusOr<std::shared_ptr<const TabletSchema>> Tablet::get_schema() {
    auto ptr = GlobalTabletSchemaCache::Instance()->lookup_schema_cache(_id);
    RETURN_IF(ptr != nullptr, ptr);

    ASSIGN_OR_RETURN(TabletMetadataIter metadata_iter, _mgr->list_tablet_metadata(_group, _id));
    if (!metadata_iter.has_next()) {
        return Status::NotFound(fmt::format("tablet {} metadata not found", _id));
    }
    ASSIGN_OR_RETURN(auto metadata, metadata_iter.next());
    ptr = GlobalTabletSchemaMap::Instance()->emplace(metadata->schema()).first;
    if (ptr == nullptr) {
        return Status::InternalError(fmt::format("tablet schema {} failed to emplace in TabletSchemaMap", _id));
    }
    (void)GlobalTabletSchemaCache::Instance()->fill_schema_cache(_id, ptr, metadata->schema().SpaceUsedLong());
    return ptr;
}

std::string Tablet::metadata_path(int64_t version) const {
    return _mgr->tablet_metadata_path(_group, _id, version);
}

std::string Tablet::txn_log_path(int64_t txn_id) const {
    return _mgr->txn_log_path(_group, _id, txn_id);
}

} // namespace starrocks::lake
