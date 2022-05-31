// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "common/statusor.h"

namespace staros::starlet {
class Starlet;
} // namespace staros::starlet

namespace starrocks {
class Cache;
class TCreateTabletReq;
} // namespace starrocks

namespace starrocks::lake {

class MetadataIterator;
class Tablet;
class TabletMetadata;
class TxnLog;

class TabletManager {
    friend class Tablet;
    using Starlet = staros::starlet::Starlet;

public:
    // Does NOT take the ownership of |starlet| and |starlet| must outlive
    // this TabletManager.
    // |cache_capacity| is the max number of bytes can be used by the
    // metadata cache.
    explicit TabletManager(Starlet* starlet, int64_t cache_capacity);

    Status create_tablet(const TCreateTabletReq& req);

    StatusOr<Tablet> get_tablet(int64_t tablet_id);

    Status drop_tablet(int64_t tablet_id);

private:
    Status put_tablet_metadata(const std::string& group, const TabletMetadata& metadata);
    StatusOr<TabletMetadata> get_tablet_metadata(const std::string& group, int64_t tablet_id, int64_t version);
    Status delete_tablet_metadata(const std::string& group, int64_t tablet_id, int64_t version);
    StatusOr<MetadataIterator> list_tablet_metadata(const std::string& group);
    StatusOr<MetadataIterator> list_tablet_metadata(const std::string& group, int64_t tablet_id);

    StatusOr<TxnLog> get_txn_log(const std::string& group, int64_t tablet_id, int64_t txn_id);
    Status put_txn_log(const std::string& group, const TxnLog& log);
    Status delete_txn_log(const std::string& group, int64_t tablet_id, int64_t txn_id);
    StatusOr<MetadataIterator> list_txn_log(const std::string& group);
    StatusOr<MetadataIterator> list_txn_log(const std::string& group, int64_t tablet_id);

    Starlet* _starlet;
    std::unique_ptr<Cache> _metacache;
};

} // namespace starrocks::lake
