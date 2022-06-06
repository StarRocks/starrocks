// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "storage/lake/tablet_manager.h"

#include "fmt/format.h"
#include "fs/fs.h"
#include "gen_cpp/AgentService_types.h"
#include "gen_cpp/olap_file.pb.h"
#include "gen_cpp/starlake.pb.h"
#include "gutil/strings/util.h"
#include "storage/lake/group_assigner.h"
#include "storage/lake/metadata_iterator.h"
#include "storage/lake/tablet.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/lake/txn_log.h"
#include "storage/metadata_util.h"
#include "util/lru_cache.h"
#include "util/raw_container.h"

namespace starrocks::lake {

TabletManager::TabletManager(GroupAssigner* group_assigner, int64_t cache_capacity)
        : _group_assigner(group_assigner), _metacache(new_lru_cache(cache_capacity)) {}

// path $bucket:/$ServiceID/$GroupID/tbl_${TabletID}_${version}
std::string TabletManager::tablet_metadata_path(const std::string& group, int64_t tablet_id, int64_t verson) {
    if (group.back() != '/') {
        return fmt::format("{}/tbl_{:016X}_{:016X}", group, tablet_id, verson);
    } else {
        return fmt::format("{}tbl_{:016X}_{:016X}", group, tablet_id, verson);
    }
}

std::string TabletManager::tablet_metadata_path(const std::string& group, const std::string& metadata_path) {
    if (group.back() != '/') {
        return fmt::format("{}/{}", group, metadata_path);
    } else {
        return fmt::format("{}{}", group, metadata_path);
    }
}

std::string TabletManager::tablet_metadata_cache_key(int64_t tablet_id, int64_t verson) {
    return fmt::format("tbl_{:016X}_{:016X}", tablet_id, verson);
}

// trnslog path rule $Bucket:/$ServiceID/$GroupID/txn_${TabletID}_${TxnID}
std::string TabletManager::txn_log_path(const std::string& group, int64_t tablet_id, int64_t txn_id) {
    if (group.back() != '/') {
        return fmt::format("{}/txn_{:016X}_{:016X}", group, tablet_id, txn_id);
    } else {
        return fmt::format("{}txn_{:016X}_{:016X}", group, tablet_id, txn_id);
    }
}

std::string TabletManager::txn_log_path(const std::string& group, const std::string& txnlog_path) {
    if (group.back() != '/') {
        return fmt::format("{}/{}", group, txnlog_path);
    } else {
        return fmt::format("{}{}", group, txnlog_path);
    }
}

std::string TabletManager::txn_log_cache_key(int64_t tablet_id, int64_t txn_id) {
    return fmt::format("txn_{:016X}_{:016X}", tablet_id, txn_id);
}

static void tablet_meta_data_deleter(const CacheKey& key, void* value) {
    std::string name = {key.data(), key.size()};
    if (HasPrefixString(name, "tbl_")) {
        TabletMetadataPtr* ptr = static_cast<TabletMetadataPtr*>(value);
        delete ptr;
    } else if (HasPrefixString(name, "txn_")) {
        TxnLogPtr* ptr = static_cast<TxnLogPtr*>(value);
        delete ptr;
    }
}

bool TabletManager::put_into_metacache(const std::string& key_path, void* ptr, const int cost) {
    CacheKey key(key_path);
    Cache::Handle* handle = _metacache->insert(key, ptr, cost, tablet_meta_data_deleter);
    bool res = true;
    if (handle == nullptr) {
        res = false;
    } else {
        _metacache->release(handle);
    }
    return res;
}

void* TabletManager::get_from_metacache(const std::string& key_path) {
    CacheKey key(key_path);
    Cache::Handle* handle = _metacache->lookup(key);
    if (handle == nullptr) {
        return nullptr;
    } else {
        auto ptr = _metacache->value(handle);
        _metacache->release(handle);
        return ptr;
    }
}

void TabletManager::remove_from_metacache(const std::string& key_path) {
    CacheKey key(key_path);
    _metacache->erase(key);
}

Status TabletManager::create_tablet(const TCreateTabletReq& req) {
    // generate tablet metadata pb
    TabletMetadataPB tablet_metadata_pb;
    tablet_metadata_pb.set_id(req.tablet_id);
    tablet_metadata_pb.set_version(1);
    tablet_metadata_pb.set_next_rowset_id(1);

    // schema
    uint32_t next_unique_id = 0;
    std::unordered_map<uint32_t, uint32_t> col_idx_to_unique_id;
    next_unique_id = req.tablet_schema.columns.size();
    for (uint32_t col_idx = 0; col_idx < next_unique_id; ++col_idx) {
        col_idx_to_unique_id[col_idx] = col_idx;
    }
    RETURN_IF_ERROR(starrocks::convert_t_schema_to_pb_schema(req.tablet_schema, next_unique_id, col_idx_to_unique_id,
                                                             RowsetTypePB::BETA_ROWSET,
                                                             tablet_metadata_pb.mutable_schema()));

    // get shard group
    ASSIGN_OR_RETURN(auto group_path, _group_assigner->get_group(req.tablet_id));

    // write tablet metadata
    auto metadata_path = tablet_metadata_path(group_path, tablet_metadata_pb.id(), tablet_metadata_pb.version());
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(metadata_path));
    ASSIGN_OR_RETURN(auto wf, fs->new_writable_file(metadata_path));
    RETURN_IF_ERROR(wf->append(tablet_metadata_pb.SerializeAsString()));
    RETURN_IF_ERROR(wf->close());

    // put tabletmetadata into cache after tabletmetadata object write successfully. just log if put failed.
    auto key_path = tablet_metadata_cache_key(tablet_metadata_pb.id(), tablet_metadata_pb.version());
    auto value_ptr = new std::shared_ptr<TabletMetadata>(new TabletMetadata(tablet_metadata_pb));
    bool inserted = put_into_metacache(key_path, static_cast<void*>(value_ptr), tablet_metadata_pb.SpaceUsedLong());
    if (!inserted) {
        delete value_ptr;
        LOG(WARNING) << "Failed to put tabletmetadata into cache " << metadata_path;
    }
    return Status::OK();
}

StatusOr<Tablet> TabletManager::get_tablet(int64_t tablet_id) {
    ASSIGN_OR_RETURN(auto group_path, _group_assigner->get_group(tablet_id));
    return Tablet(this, std::move(group_path), tablet_id);
}

Status TabletManager::drop_tablet(int64_t tablet_id) {
    std::vector<std::string> objects;
    const auto tablet_metadata_prefix = fmt::format("tbl_{:016X}_", tablet_id);
    const auto txnlog_prefix = fmt::format("txn_{:016X}_", tablet_id);

    // get group path
    ASSIGN_OR_RETURN(auto group_path, _group_assigner->get_group(tablet_id));
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(group_path));
    auto scan_cb = [&objects, &tablet_metadata_prefix, &txnlog_prefix](std::string_view name) {
        if (HasPrefixString(name, tablet_metadata_prefix) || HasPrefixString(name, txnlog_prefix)) {
            objects.emplace_back(name);
        }
        return true;
    };

    RETURN_IF_ERROR(fs->iterate_dir(group_path, scan_cb));
    for (const auto& obj : objects) {
        remove_from_metacache(obj);
        (void)fs->delete_file(fmt::format("{}/{}", group_path, obj));
    }

    return Status::OK();
}

Status TabletManager::put_tablet_metadata(const std::string& group, const TabletMetadata& metadata) {
    auto metadata_path = tablet_metadata_path(group, metadata.id(), metadata.version());
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(metadata_path));
    ASSIGN_OR_RETURN(auto wf, fs->new_writable_file(metadata_path));
    RETURN_IF_ERROR(wf->append(metadata.SerializeAsString()));
    RETURN_IF_ERROR(wf->close());

    // put tabletmetadata into cache
    auto key_path = tablet_metadata_cache_key(metadata.id(), metadata.version());
    auto value_ptr = new std::shared_ptr<TabletMetadata>(new TabletMetadata(metadata));
    bool inserted = put_into_metacache(key_path, static_cast<void*>(value_ptr), metadata.SpaceUsedLong());
    if (!inserted) {
        delete value_ptr;
        LOG(WARNING) << "Failed to put tabletmetadata into cache " << metadata_path;
    }
    return Status::OK();
}

Status TabletManager::put_tablet_metadata(const std::string& group, TabletMetadataPtr metadata) {
    auto metadata_path = tablet_metadata_path(group, metadata->id(), metadata->version());
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(metadata_path));
    ASSIGN_OR_RETURN(auto wf, fs->new_writable_file(metadata_path));
    RETURN_IF_ERROR(wf->append(metadata->SerializeAsString()));
    RETURN_IF_ERROR(wf->close());

    // put into metacache
    auto key_path = tablet_metadata_cache_key(metadata->id(), metadata->version());
    auto value_ptr = new std::shared_ptr<const TabletMetadata>(metadata);
    bool inserted = put_into_metacache(key_path, static_cast<void*>(value_ptr), metadata->SpaceUsedLong());
    if (!inserted) {
        delete value_ptr;
        LOG(WARNING) << "Failed to put into meta cache " << metadata_path;
    }
    return Status::OK();
}

StatusOr<TabletMetadataPtr> TabletManager::get_tablet_metadata(const string& metadata_path) {
    std::string read_buf;
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(metadata_path));
    ASSIGN_OR_RETURN(auto rf, fs->new_random_access_file(metadata_path));

    ASSIGN_OR_RETURN(auto size, rf->get_size());
    raw::stl_string_resize_uninitialized(&read_buf, size);
    RETURN_IF_ERROR(rf->read_at_fully(0, read_buf.data(), size));

    std::shared_ptr<TabletMetadata> meta = std::make_shared<TabletMetadata>();
    bool parsed = meta.get()->ParseFromArray(read_buf.data(), size);
    if (!parsed) {
        return Status::Corruption(fmt::format("failed to parse tablet meta {}", metadata_path));
    }
    return std::move(meta);
}

StatusOr<TabletMetadataPtr> TabletManager::get_tablet_metadata(const std::string& group, int64_t tablet_id,
                                                               int64_t version) {
    // search metacache
    auto key_path = tablet_metadata_cache_key(tablet_id, version);
    auto ptr = get_from_metacache(key_path);
    if (ptr != nullptr) {
        return *(static_cast<TabletMetadataPtr*>(ptr));
    }

    auto metadata_path = tablet_metadata_path(group, tablet_id, version);
    return get_tablet_metadata(metadata_path);
}

StatusOr<TabletMetadataPtr> TabletManager::get_tablet_metadata(const std::string& group, const string& path) {
    // search metacache
    auto ptr = get_from_metacache(path);
    if (ptr != nullptr) {
        return *(static_cast<TabletMetadataPtr*>(ptr));
    }

    auto metadata_path = tablet_metadata_path(group, path);
    return get_tablet_metadata(metadata_path);
}

Status TabletManager::delete_tablet_metadata(const std::string& group, int64_t tablet_id, int64_t version) {
    // drop from metacache first
    auto key_path = tablet_metadata_cache_key(tablet_id, version);
    remove_from_metacache(key_path);

    auto metadata_path = tablet_metadata_path(group, tablet_id, version);
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(metadata_path));
    return fs->delete_file(metadata_path);
}

StatusOr<TabletMetadataIter> TabletManager::list_tablet_metadata(const std::string& group) {
    return list_tablet_metadata(group, 0);
}

StatusOr<TabletMetadataIter> TabletManager::list_tablet_metadata(const std::string& group, int64_t tablet_id) {
    std::vector<std::string> objects{};
    std::string prefix;
    if (tablet_id == 0) {
        prefix = "tbl_";
    } else {
        prefix = fmt::format("tbl_{:016X}_", tablet_id);
    }
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(group));
    auto scan_cb = [&objects, &prefix](std::string_view name) {
        if (HasPrefixString(name, prefix)) {
            objects.emplace_back(name);
        }
        return true;
    };

    RETURN_IF_ERROR(fs->iterate_dir(group, scan_cb));
    return TabletMetadataIter{this, std::move(group), std::move(objects)};
}

StatusOr<TxnLogPtr> TabletManager::get_txn_log(const std::string& txnlog_path) {
    std::string read_buf;
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(txnlog_path));
    ASSIGN_OR_RETURN(auto rf, fs->new_random_access_file(txnlog_path));

    ASSIGN_OR_RETURN(auto size, rf->get_size());
    raw::stl_string_resize_uninitialized(&read_buf, size);
    RETURN_IF_ERROR(rf->read_at_fully(0, read_buf.data(), size));

    std::shared_ptr<TxnLog> meta = std::make_shared<TxnLog>();
    bool parsed = meta.get()->ParseFromArray(read_buf.data(), size);
    if (!parsed) {
        return Status::Corruption(fmt::format("failed to parse txn log {}", txnlog_path));
    }
    return std::move(meta);
}

StatusOr<TxnLogPtr> TabletManager::get_txn_log(const string& group, const std::string& path) {
    // search metacache
    auto ptr = get_from_metacache(path);
    if (ptr != nullptr) {
        return *(static_cast<TxnLogPtr*>(ptr));
    }

    auto txnlog_path = txn_log_path(group, path);
    return get_txn_log(txnlog_path);
}

StatusOr<TxnLogPtr> TabletManager::get_txn_log(const std::string& group, int64_t tablet_id, int64_t txn_id) {
    // search metacache
    auto key_path = txn_log_cache_key(tablet_id, txn_id);
    auto ptr = get_from_metacache(key_path);
    if (ptr != nullptr) {
        return *(static_cast<TxnLogPtr*>(ptr));
    }

    auto txnlog_path = txn_log_path(group, tablet_id, txn_id);
    return get_txn_log(txnlog_path);
}

Status TabletManager::put_txn_log(const std::string& group, const TxnLog& log) {
    auto txnlog_path = txn_log_path(group, log.tablet_id(), log.txn_id());
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(txnlog_path));
    ASSIGN_OR_RETURN(auto wf, fs->new_writable_file(txnlog_path));
    RETURN_IF_ERROR(wf->append(log.SerializeAsString()));
    RETURN_IF_ERROR(wf->close());

    // put txnlog into cache after txnlog object write successfully. just log if put failed.
    auto key_path = txn_log_cache_key(log.tablet_id(), log.txn_id());
    auto value_ptr = new std::shared_ptr<TxnLog>(new TxnLog(log));
    bool inserted = put_into_metacache(key_path, static_cast<void*>(value_ptr), log.SpaceUsedLong());
    if (!inserted) {
        delete value_ptr;
        LOG(WARNING) << "Failed to put txnlog into cache " << txnlog_path;
    }
    return Status::OK();
}

Status TabletManager::put_txn_log(const std::string& group, TxnLogPtr log) {
    auto txnlog_path = txn_log_path(group, log->tablet_id(), log->txn_id());
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(txnlog_path));
    ASSIGN_OR_RETURN(auto wf, fs->new_writable_file(txnlog_path));
    RETURN_IF_ERROR(wf->append(log->SerializeAsString()));
    RETURN_IF_ERROR(wf->close());

    // put txnlog into cache
    auto key_path = txn_log_cache_key(log->tablet_id(), log->txn_id());
    auto value_ptr = new std::shared_ptr<const TxnLog>(log);
    bool inserted = put_into_metacache(key_path, static_cast<void*>(value_ptr), log->SpaceUsedLong());
    if (!inserted) {
        delete value_ptr;
        LOG(WARNING) << "Failed to put txnlog into cache " << txnlog_path;
    }
    return Status::OK();
}

Status TabletManager::delete_txn_log(const std::string& group, int64_t tablet_id, int64_t txn_id) {
    // drop from metacache first
    auto key_path = txn_log_cache_key(tablet_id, txn_id);
    remove_from_metacache(key_path);

    auto txnlog_path = txn_log_path(group, tablet_id, txn_id);
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(txnlog_path));
    return fs->delete_file(txnlog_path);
}

StatusOr<TxnLogIter> TabletManager::list_txn_log(const std::string& group) {
    return list_txn_log(group, 0);
}

StatusOr<TxnLogIter> TabletManager::list_txn_log(const std::string& group, int64_t tablet_id) {
    std::vector<std::string> objects{};
    std::string prefix;
    if (tablet_id == 0) {
        prefix = "txn_";
    } else {
        prefix = fmt::format("txn_{:016X}_", tablet_id);
    }
    // get group path
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(group));
    auto scan_cb = [&objects, &prefix](std::string_view name) {
        if (HasPrefixString(name, prefix)) {
            objects.emplace_back(name);
        }
        return true;
    };

    RETURN_IF_ERROR(fs->iterate_dir(group, scan_cb));
    return TxnLogIter{this, group, std::move(objects)};
}

} // namespace starrocks::lake
