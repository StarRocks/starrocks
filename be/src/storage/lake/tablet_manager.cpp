// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "storage/lake/tablet_manager.h"

#include "storage/lake/metadata_iterator.h"
#include "storage/lake/tablet.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/lake/txnlog.h"
#include "util/lru_cache.h"

namespace starrocks::lake {

TabletManager::TabletManager(Starlet* starlet, int64_t cache_capacity)
        : _starlet(starlet), _metacache(new_lru_cache(cache_capacity)) {}

Status TabletManager::create_tablet(const TCreateTabletReq& req) {
    (void)req;
    return Status::NotSupported("TabletManager::create_tablet");
}

StatusOr<Tablet> TabletManager::get_tablet(int64_t tablet_id) {
    (void)tablet_id;
    return Status::NotSupported("TabletManager::get_tablet");
}

Status TabletManager::drop_tablet(int64_t tablet_id) {
    (void)tablet_id;
    return Status::NotSupported("TabletManager::drop_tablet");
}

Status TabletManager::put_tablet_metadata(const std::string& group, const TabletMetadata& metadata) {
    (void)group;
    (void)metadata;
    return Status::NotSupported("TabletManager::put_tablet_metadata");
}

StatusOr<TabletMetadata> TabletManager::get_tablet_metadata(const std::string& group, int64_t tablet_id,
                                                            int64_t version) {
    (void)group;
    (void)tablet_id;
    (void)version;
    return Status::NotSupported("TabletManager::get_tablet_metadata");
}

Status TabletManager::delete_tablet_metadata(const std::string& group, int64_t tablet_id, int64_t version) {
    (void)group;
    (void)tablet_id;
    (void)version;
    return Status::NotSupported("TabletManager::delete_tablet_metadata");
}

StatusOr<MetadataIterator> TabletManager::list_tablet_metadata(const std::string& group) {
    (void)group;
    return Status::NotSupported("TabletManager::list_tablet_metadata");
}

StatusOr<MetadataIterator> TabletManager::list_tablet_metadata(const std::string& group, int64_t tablet_id) {
    (void)group;
    (void)tablet_id;
    return Status::NotSupported("TabletManager::list_tablet_metadata");
}

StatusOr<TxnLog> TabletManager::get_txnlog(const std::string& group, int64_t tablet_id, int64_t txn_id) {
    (void)group;
    (void)tablet_id;
    (void)txn_id;
    return Status::NotSupported("TabletManager::get_txnlog");
}

Status TabletManager::put_txnlog(const std::string& group, int64_t tablet_id, const TxnLog& log) {
    (void)group;
    (void)tablet_id;
    (void)log;
    return Status::NotSupported("TabletManager::put_txnlog");
}

Status TabletManager::delete_txnlog(const std::string& group, int64_t tablet_id, int64_t txn_id) {
    (void)group;
    (void)tablet_id;
    (void)txn_id;
    return Status::NotSupported("TabletManager::delete_txnlog");
}

StatusOr<MetadataIterator> TabletManager::list_txnlog(const std::string& group) {
    (void)group;
    return Status::NotSupported("TabletManager::list_txnlog");
}

StatusOr<MetadataIterator> TabletManager::list_txnlog(const std::string& group, int64_t tablet_id) {
    (void)group;
    (void)tablet_id;
    return Status::NotSupported("TabletManager::list_txnlog");
}

} // namespace starrocks::lake
