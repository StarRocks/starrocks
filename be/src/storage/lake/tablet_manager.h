// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "common/statusor.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/lake/txn_log.h"

namespace starrocks {
class Cache;
class TCreateTabletReq;
} // namespace starrocks

namespace starrocks::lake {

class GroupAssigner;
class MetadataIterator;
class Tablet;

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

private:
    Status put_tablet_metadata(const std::string& group, const TabletMetadata& metadata);
    Status put_tablet_metadata(const std::string& group, TabletMetadataPtr metadata);
    StatusOr<TabletMetadataPtr> get_tablet_metadata(const std::string& group, int64_t tablet_id, int64_t version);
    Status delete_tablet_metadata(const std::string& group, int64_t tablet_id, int64_t version);
    StatusOr<MetadataIterator> list_tablet_metadata(const std::string& group);
    StatusOr<MetadataIterator> list_tablet_metadata(const std::string& group, int64_t tablet_id);

    Status put_txn_log(const std::string& group, const TxnLog& log);
    Status put_txn_log(const std::string& group, TxnLogPtr log);
    StatusOr<TxnLogPtr> get_txn_log(const std::string& group, int64_t tablet_id, int64_t txn_id);
    Status delete_txn_log(const std::string& group, int64_t tablet_id, int64_t txn_id);
    StatusOr<MetadataIterator> list_txn_log(const std::string& group);
    StatusOr<MetadataIterator> list_txn_log(const std::string& group, int64_t tablet_id);

    GroupAssigner* _group_assigner;
    std::unique_ptr<Cache> _metacache;
};

} // namespace starrocks::lake
