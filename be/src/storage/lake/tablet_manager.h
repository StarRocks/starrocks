// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <variant>

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

    Status put_tablet_metadata(const TabletMetadata& metadata);

    Status put_tablet_metadata(TabletMetadataPtr metadata);

    StatusOr<TabletMetadataPtr> get_tablet_metadata(int64_t tablet_id, int64_t version);

    StatusOr<TabletMetadataPtr> get_tablet_metadata(const std::string& path, bool fill_cache = true);

    StatusOr<TabletMetadataIter> list_tablet_metadata(int64_t tablet_id, bool filter_tablet);

    Status delete_tablet_metadata(int64_t tablet_id, int64_t version);

    Status put_txn_log(const TxnLog& log);

    Status put_txn_log(TxnLogPtr log);

    StatusOr<TxnLogPtr> get_txn_log(int64_t tablet_id, int64_t txn_id);

    StatusOr<TxnLogPtr> get_txn_log(const std::string& path, bool fill_cache = true);

    StatusOr<TxnLogIter> list_txn_log(int64_t tablet_id, bool filter_tablet);

    Status delete_txn_log(int64_t tablet_id, int64_t txn_id);

    void prune_metacache();

    // TODO: remove this method
    LocationProvider* TEST_set_location_provider(LocationProvider* value) {
        auto ret = _location_provider;
        _location_provider = value;
        return ret;
    }

private:
    using CacheValue = std::variant<TabletMetadataPtr, TxnLogPtr, TabletSchemaPtr>;

    static std::string tablet_schema_cache_key(int64_t tablet_id);

    static void cache_value_deleter(const CacheKey& /*key*/, void* value) { delete static_cast<CacheValue*>(value); }

    std::string tablet_root_location(int64_t tablet_id) const;
    std::string tablet_metadata_location(int64_t tablet_id, int64_t version) const;
    std::string txn_log_location(int64_t tablet_id, int64_t txn_id) const;
    std::string segment_location(int64_t tablet_id, std::string_view segment_name) const;

    StatusOr<TabletMetadataPtr> load_tablet_metadata(const std::string& metadata_location);
    StatusOr<TxnLogPtr> load_txn_log(const std::string& txn_log_location);

    StatusOr<TabletSchemaPtr> get_tablet_schema(int64_t tablet_id);

    bool fill_metacache(const std::string& key, CacheValue* ptr, int size);
    TabletMetadataPtr lookup_tablet_metadata(const std::string& key);
    TxnLogPtr lookup_txn_log(const std::string& key);
    void erase_metacache(const std::string& key);

    TabletSchemaPtr lookup_tablet_schema(const std::string& key);

    LocationProvider* _location_provider;
    std::unique_ptr<Cache> _metacache;
};

} // namespace starrocks::lake
