// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "common/statusor.h"
#include "storage/lake/metadata_iterator.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/lake/txn_log.h"
#include "storage/tablet_schema.h"
#include "util/lru_cache.h"

namespace starrocks {
class Cache;
class TCreateTabletReq;
} // namespace starrocks

namespace starrocks::lake {

class CompactionTask;
class LocationProvider;
class Tablet;
template <typename T>
class MetadataIterator;
using TabletMetadataIter = MetadataIterator<TabletMetadataPtr>;
using TxnLogIter = MetadataIterator<TxnLogPtr>;
using TabletSchemaPtr = std::shared_ptr<const starrocks::TabletSchema>;
using CompactionTaskPtr = std::shared_ptr<CompactionTask>;

class TabletManager {
    friend class Tablet;

public:
    // Does NOT take the ownership of |location_provider| and |location_provider| must outlive
    // this TabletManager.
    // |cache_capacity| is the max number of bytes can be used by the
    // metadata cache.
    explicit TabletManager(LocationProvider* location_provider, int64_t cache_capacity);

    Status create_tablet(const TCreateTabletReq& req);

    StatusOr<Tablet> get_tablet(int64_t tablet_id);

    Status drop_tablet(int64_t tablet_id);

    Status publish_version(int64_t tablet_id, int64_t base_version, int64_t new_version, const int64_t* txns,
                           int txns_size);

    StatusOr<CompactionTaskPtr> compact(int64_t tablet_id, int64_t version, int64_t txn_id);

    Status put_tablet_metadata(const std::string& root, const TabletMetadata& metadata);
    Status put_tablet_metadata(const std::string& root, TabletMetadataPtr metadata);
    StatusOr<TabletMetadataPtr> get_tablet_metadata(const std::string& root, int64_t tablet_id, int64_t version);
    StatusOr<TabletMetadataPtr> get_tablet_metadata(const std::string& root, const std::string& path);
    StatusOr<TabletMetadataIter> list_tablet_metadata(int64_t tablet_id, bool filter_tablet);
    Status delete_tablet_metadata(const std::string& root, int64_t tablet_id, int64_t version);

    Status put_txn_log(const std::string& root, const TxnLog& log);
    Status put_txn_log(const std::string& root, TxnLogPtr log);
    StatusOr<TxnLogPtr> get_txn_log(const std::string& root, int64_t tablet_id, int64_t txn_id);
    StatusOr<TxnLogPtr> get_txn_log(const std::string& root, const std::string& path);
    StatusOr<TxnLogIter> list_txn_log(int64_t tablet_id, bool filter_tablet);
    Status delete_txn_log(const std::string& root, int64_t tablet_id, int64_t txn_id);

    void prune_metacache();

    LocationProvider* TEST_set_location_provider(LocationProvider* value) {
        auto ret = _location_provider;
        _location_provider = value;
        return ret;
    }

private:
    std::string tablet_metadata_path(const std::string& root, int64_t tablet_id, int64_t verson);
    std::string tablet_metadata_path(const std::string& root, const std::string& metadata_path);
    std::string tablet_metadata_cache_key(int64_t tablet_id, int64_t verson);
    StatusOr<TabletMetadataPtr> load_tablet_metadata(const std::string& metadata_path, int64_t tablet_id);

    std::string txn_log_path(const std::string& root, int64_t tablet_id, int64_t txn_id);
    std::string txn_log_path(const std::string& root, const std::string& txnlog_path);
    std::string txn_log_cache_key(int64_t tablet_id, int64_t txn_id);
    StatusOr<TxnLogPtr> load_txn_log(const std::string& txnlog_path, int64_t tablet_id);

    bool fill_metacache(const std::string& key, void* ptr, int size);
    TabletMetadataPtr lookup_tablet_metadata(const std::string& key);
    TxnLogPtr lookup_txn_log(const std::string& key);
    void erase_metacache(const std::string& key);

    std::string tablet_schema_cache_key(int64_t tablet_id);
    TabletSchemaPtr lookup_tablet_schema(const std::string& key);

    std::string path_assemble(const std::string& path, int64_t tablet_id);

    LocationProvider* _location_provider;
    std::unique_ptr<Cache> _metacache;
};

} // namespace starrocks::lake
