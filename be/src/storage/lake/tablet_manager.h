// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "common/statusor.h"
#include "storage/lake/metadata_iterator.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/lake/txn_log.h"
#include "util/lru_cache.h"

namespace starrocks {
class Cache;
class TCreateTabletReq;
} // namespace starrocks

namespace starrocks::lake {

class GroupAssigner;
class Tablet;
template <typename T>
class MetadataIterator;
using TabletMetadataIter = MetadataIterator<TabletMetadataPtr>;
using TxnLogIter = MetadataIterator<TxnLogPtr>;

class TabletManager {
    friend class Tablet;

public:
    // Does NOT take the ownership of |group_assigner| and |group_assigner| must outlive
    // this TabletManager.
    // |cache_capacity| is the max number of bytes can be used by the
    // metadata cache.
    explicit TabletManager(GroupAssigner* group_assigner, int64_t cache_capacity);

    Status create_tablet(const TCreateTabletReq& req);

    StatusOr<Tablet> get_tablet(int64_t tablet_id);

    Status drop_tablet(int64_t tablet_id);

    Status put_tablet_metadata(const std::string& group, const TabletMetadata& metadata);
    Status put_tablet_metadata(const std::string& group, TabletMetadataPtr metadata);
    StatusOr<TabletMetadataPtr> get_tablet_metadata(const std::string& group, int64_t tablet_id, int64_t version);
    StatusOr<TabletMetadataPtr> get_tablet_metadata(const std::string& group, const std::string& path);
    StatusOr<TabletMetadataIter> list_tablet_metadata(const std::string& group);
    StatusOr<TabletMetadataIter> list_tablet_metadata(const std::string& group, int64_t tablet_id);
    Status delete_tablet_metadata(const std::string& group, int64_t tablet_id, int64_t version);

    Status put_txn_log(const std::string& group, const TxnLog& log);
    Status put_txn_log(const std::string& group, TxnLogPtr log);
    StatusOr<TxnLogPtr> get_txn_log(const std::string& group, int64_t tablet_id, int64_t txn_id);
    StatusOr<TxnLogPtr> get_txn_log(const std::string& group, const std::string& path);
    StatusOr<TxnLogIter> list_txn_log(const std::string& group);
    StatusOr<TxnLogIter> list_txn_log(const std::string& group, int64_t tablet_id);
    Status delete_txn_log(const std::string& group, int64_t tablet_id, int64_t txn_id);

    GroupAssigner* TEST_set_group_assigner(GroupAssigner* value) {
        auto ret = _group_assigner;
        _group_assigner = value;
        return ret;
    }

private:
    std::string tablet_metadata_path(const std::string& group, int64_t tablet_id, int64_t verson);
    std::string tablet_metadata_path(const std::string& group, const std::string& metadata_path);
    std::string tablet_metadata_cache_key(int64_t tablet_id, int64_t verson);
    StatusOr<TabletMetadataPtr> get_tablet_metadata(const std::string& metadata_path);

    std::string txn_log_path(const std::string& group, int64_t tablet_id, int64_t txn_id);
    std::string txn_log_path(const std::string& group, const std::string& txnlog_path);
    std::string txn_log_cache_key(int64_t tablet_id, int64_t txn_id);
    StatusOr<TxnLogPtr> get_txn_log(const std::string& txnlog_path);

    bool fill_metacache(const std::string& key, void* ptr, int size);
    TabletMetadataPtr lookup_tablet_metadata(const std::string& key);
    TxnLogPtr lookup_txn_log(const std::string& key);
    void erase_metacache(const std::string& key);

    GroupAssigner* _group_assigner;
    std::unique_ptr<Cache> _metacache;
};

} // namespace starrocks::lake
