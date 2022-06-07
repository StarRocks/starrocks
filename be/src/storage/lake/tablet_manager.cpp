// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "storage/lake/tablet_manager.h"

#include "storage/lake/group_assigner.h"
#include "storage/lake/metadata_iterator.h"
#include "storage/lake/tablet.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/lake/txn_log.h"
#include "util/lru_cache.h"

namespace starrocks::lake {

TabletManager::TabletManager(GroupAssigner* group_assigner, int64_t cache_capacity)
        : _group_assigner(group_assigner), _metacache(new_lru_cache(cache_capacity)) {}

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

Status TabletManager::put_tablet_metadata(const std::string& group, TabletMetadataPtr metadata) {
    (void)group;
    (void)metadata;
    return Status::NotSupported("TabletManager::put_tablet_metadata");
}

StatusOr<TabletMetadataPtr> TabletManager::get_tablet_metadata(const std::string& group, int64_t tablet_id,
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

StatusOr<TxnLogPtr> TabletManager::get_txn_log(const std::string& group, int64_t tablet_id, int64_t txn_id) {
    (void)group;
    (void)tablet_id;
    (void)txn_id;
    return Status::NotSupported("TabletManager::get_txn_log");
}

Status TabletManager::put_txn_log(const std::string& group, const TxnLog& log) {
    (void)group;
    (void)log;
    return Status::NotSupported("TabletManager::put_txn_log");
}

Status TabletManager::put_txn_log(const std::string& group, TxnLogPtr log) {
    (void)group;
    (void)log;
    return Status::NotSupported("TabletManager::put_txn_log");
}

Status TabletManager::delete_txn_log(const std::string& group, int64_t tablet_id, int64_t txn_id) {
    (void)group;
    (void)tablet_id;
    (void)txn_id;
    return Status::NotSupported("TabletManager::delete_txn_log");
}

StatusOr<MetadataIterator> TabletManager::list_txn_log(const std::string& group) {
    (void)group;
    return Status::NotSupported("TabletManager::list_txn_log");
}

StatusOr<MetadataIterator> TabletManager::list_txn_log(const std::string& group, int64_t tablet_id) {
    (void)group;
    (void)tablet_id;
    return Status::NotSupported("TabletManager::list_txn_log");
}

} // namespace starrocks::lake
