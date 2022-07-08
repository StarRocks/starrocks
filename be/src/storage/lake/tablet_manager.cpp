// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "storage/lake/tablet_manager.h"

#include "fmt/format.h"
#include "fs/fs.h"
#include "gen_cpp/AgentService_types.h"
#include "gen_cpp/lake_types.pb.h"
#include "gutil/strings/util.h"
#include "storage/lake/group_assigner.h"
#include "storage/lake/horizontal_compaction_task.h"
#include "storage/lake/tablet.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/lake/txn_log.h"
#include "storage/metadata_util.h"
#include "util/lru_cache.h"
#include "util/raw_container.h"

namespace starrocks::lake {

Status apply_txn_log(const TxnLog& log, TabletMetadata* metadata);
Status publish(Tablet* tablet, int64_t base_version, int64_t new_version, const int64_t* txns, int txns_size);

TabletManager::TabletManager(GroupAssigner* group_assigner, int64_t cache_capacity)
        : _group_assigner(group_assigner), _metacache(new_lru_cache(cache_capacity)) {}

// path $bucket:/$ServiceID/$GroupID/tbl_${TabletID}_${version}
std::string TabletManager::tablet_metadata_path(const std::string& group, int64_t tablet_id, int64_t verson) {
    return fmt::format("{}/tbl_{:016X}_{:016X}", group, tablet_id, verson);
}

std::string TabletManager::tablet_metadata_path(const std::string& group, const std::string& metadata_path) {
    return fmt::format("{}/{}", group, metadata_path);
}

std::string TabletManager::tablet_metadata_cache_key(int64_t tablet_id, int64_t version) {
    return fmt::format("tbl_{:016X}_{:016X}", tablet_id, version);
}

// txnslog path rule $Bucket:/$ServiceID/$GroupID/txn_${TabletID}_${TxnID}
std::string TabletManager::txn_log_path(const std::string& group, int64_t tablet_id, int64_t txn_id) {
    return fmt::format("{}/txn_{:016X}_{:016X}", group, tablet_id, txn_id);
}

std::string TabletManager::txn_log_path(const std::string& group, const std::string& txnlog_path) {
    return fmt::format("{}/{}", group, txnlog_path);
}

std::string TabletManager::txn_log_cache_key(int64_t tablet_id, int64_t txn_id) {
    return fmt::format("txn_{:016X}_{:016X}", tablet_id, txn_id);
}

std::string TabletManager::tablet_schema_cache_key(int64_t tablet_id) {
    return fmt::format("schema_{:016X}", tablet_id);
}

std::string TabletManager::path_assemble(const std::string& path, int64_t tablet_id) {
    return _group_assigner->path_assemble(path, tablet_id);
}

static void tablet_metadata_deleter(const CacheKey& key, void* value) {
    std::string_view name(key.data(), key.size());
    if (HasPrefixString(name, "tbl_")) {
        auto* ptr = static_cast<TabletMetadataPtr*>(value);
        delete ptr;
    } else if (HasPrefixString(name, "txn_")) {
        auto* ptr = static_cast<TxnLogPtr*>(value);
        delete ptr;
    } else if (HasPrefixString(name, "schema_")) {
        auto* ptr = static_cast<TabletSchemaPtr*>(value);
        delete ptr;
    }
}

bool TabletManager::fill_metacache(const std::string& key, void* ptr, int size) {
    Cache::Handle* handle = _metacache->insert(CacheKey(key), ptr, size, tablet_metadata_deleter);
    bool res = true;
    if (handle == nullptr) {
        res = false;
    } else {
        _metacache->release(handle);
    }
    return res;
}

TabletMetadataPtr TabletManager::lookup_tablet_metadata(const std::string& key) {
    auto handle = _metacache->lookup(CacheKey(key));
    if (handle == nullptr) {
        return nullptr;
    }
    auto ptr = *(static_cast<TabletMetadataPtr*>(_metacache->value(handle)));
    _metacache->release(handle);
    return ptr;
}

TabletSchemaPtr TabletManager::lookup_tablet_schema(const std::string& key) {
    auto handle = _metacache->lookup(CacheKey(key));
    if (handle == nullptr) {
        return nullptr;
    }
    auto ptr = *(static_cast<TabletSchemaPtr*>(_metacache->value(handle)));
    _metacache->release(handle);
    return ptr;
}

TxnLogPtr TabletManager::lookup_txn_log(const std::string& key) {
    auto handle = _metacache->lookup(CacheKey(key));
    if (handle == nullptr) {
        return nullptr;
    }
    auto ptr = *(static_cast<TxnLogPtr*>(_metacache->value(handle)));
    _metacache->release(handle);
    return ptr;
}

void TabletManager::erase_metacache(const std::string& key) {
    _metacache->erase(CacheKey(key));
}

void TabletManager::prune_metacache() {
    _metacache->prune();
}

Status TabletManager::create_tablet(const TCreateTabletReq& req) {
    // generate tablet metadata pb
    TabletMetadataPB tablet_metadata_pb;
    tablet_metadata_pb.set_id(req.tablet_id);
    tablet_metadata_pb.set_version(1);
    tablet_metadata_pb.set_next_rowset_id(1);

    // schema
    std::unordered_map<uint32_t, uint32_t> col_idx_to_unique_id;
    uint32_t next_unique_id = req.tablet_schema.columns.size();
    for (uint32_t col_idx = 0; col_idx < next_unique_id; ++col_idx) {
        col_idx_to_unique_id[col_idx] = col_idx;
    }
    RETURN_IF_ERROR(starrocks::convert_t_schema_to_pb_schema(req.tablet_schema, next_unique_id, col_idx_to_unique_id,
                                                             tablet_metadata_pb.mutable_schema()));

    // get shard group
    ASSIGN_OR_RETURN(auto group_path, _group_assigner->get_group(req.tablet_id));

    // write tablet metadata
    return put_tablet_metadata(group_path, tablet_metadata_pb);
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
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(_group_assigner->get_fs_prefix()));
    auto scan_cb = [&objects, &tablet_metadata_prefix, &txnlog_prefix](std::string_view name) {
        if (HasPrefixString(name, tablet_metadata_prefix) || HasPrefixString(name, txnlog_prefix)) {
            objects.emplace_back(name);
        }
        return true;
    };

    //drop tablet schema from metacache;
    erase_metacache(tablet_schema_cache_key(tablet_id));

    RETURN_IF_ERROR(fs->iterate_dir(path_assemble(group_path, tablet_id), scan_cb));
    for (const auto& obj : objects) {
        erase_metacache(obj);
        (void)fs->delete_file(path_assemble(fmt::format("{}/{}", group_path, obj), tablet_id));
    }

    return Status::OK();
}

Status TabletManager::put_tablet_metadata(const std::string& group, TabletMetadataPtr metadata) {
    auto options = WritableFileOptions{.sync_on_close = true, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
    auto metadata_path = tablet_metadata_path(group, metadata->id(), metadata->version());
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(_group_assigner->get_fs_prefix()));
    ASSIGN_OR_RETURN(auto wf, fs->new_writable_file(options, path_assemble(metadata_path, metadata->id())));
    RETURN_IF_ERROR(wf->append(metadata->SerializeAsString()));
    RETURN_IF_ERROR(wf->close());

    // put into metacache
    auto cache_key = tablet_metadata_cache_key(metadata->id(), metadata->version());
    auto value_ptr = new std::shared_ptr<const TabletMetadata>(metadata);
    bool inserted = fill_metacache(cache_key, value_ptr, static_cast<int>(metadata->SpaceUsedLong()));
    if (!inserted) {
        delete value_ptr;
        LOG(WARNING) << "Failed to put into meta cache " << metadata_path;
    }
    return Status::OK();
}

Status TabletManager::put_tablet_metadata(const std::string& group, const TabletMetadata& metadata) {
    auto metadata_ptr = std::make_shared<TabletMetadata>(metadata);
    return put_tablet_metadata(group, std::move(metadata_ptr));
}

StatusOr<TabletMetadataPtr> TabletManager::load_tablet_metadata(const string& metadata_path, int64_t tablet_id) {
    std::string read_buf;
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(_group_assigner->get_fs_prefix()));
    ASSIGN_OR_RETURN(auto rf, fs->new_random_access_file(path_assemble(metadata_path, tablet_id)));

    ASSIGN_OR_RETURN(auto size, rf->get_size());
    if (UNLIKELY(size > std::numeric_limits<int>::max())) {
        return Status::Corruption("file size exceeded the int range");
    }
    raw::stl_string_resize_uninitialized(&read_buf, size);
    RETURN_IF_ERROR(rf->read_at_fully(0, read_buf.data(), size));

    std::shared_ptr<TabletMetadata> meta = std::make_shared<TabletMetadata>();
    bool parsed = meta->ParseFromArray(read_buf.data(), static_cast<int>(size));
    if (!parsed) {
        return Status::Corruption(fmt::format("failed to parse tablet meta {}", metadata_path));
    }
    return std::move(meta);
}

StatusOr<TabletMetadataPtr> TabletManager::get_tablet_metadata(const std::string& group, int64_t tablet_id,
                                                               int64_t version) {
    // search metacache
    auto cache_key = tablet_metadata_cache_key(tablet_id, version);
    auto ptr = lookup_tablet_metadata(cache_key);
    RETURN_IF(ptr != nullptr, ptr);

    auto metadata_path = tablet_metadata_path(group, tablet_id, version);
    ASSIGN_OR_RETURN(ptr, load_tablet_metadata(metadata_path, tablet_id));

    auto value_ptr = new std::shared_ptr<const TabletMetadata>(ptr);
    bool inserted = fill_metacache(cache_key, value_ptr, static_cast<int>(ptr->SpaceUsedLong()));
    if (!inserted) {
        delete value_ptr;
        LOG(WARNING) << "Failed to put tabletmetadata into cache " << cache_key;
    }
    return ptr;
}

StatusOr<TabletMetadataPtr> TabletManager::get_tablet_metadata(const std::string& group, const string& path) {
    int64_t tablet_id;
    int64_t version;
    auto tablet_pos = path.find('_');
    auto version_pos = path.find('_', tablet_pos + 1);
    if (tablet_pos == std::string::npos || version_pos == std::string::npos) {
        return Status::InvalidArgument(fmt::format("invalid path format {}", path));
    }
    tablet_id = std::stol(path.substr(tablet_pos + 1, version_pos - tablet_pos - 1), nullptr, 16);
    version = std::stol(path.substr(version_pos + 1), nullptr, 16);
    return get_tablet_metadata(group, tablet_id, version);
}

Status TabletManager::delete_tablet_metadata(const std::string& group, int64_t tablet_id, int64_t version) {
    // drop from metacache first
    auto cache_key = tablet_metadata_cache_key(tablet_id, version);
    erase_metacache(cache_key);

    auto metadata_path = tablet_metadata_path(group, tablet_id, version);
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(_group_assigner->get_fs_prefix()));
    return fs->delete_file(path_assemble(metadata_path, tablet_id));
}

StatusOr<TabletMetadataIter> TabletManager::list_tablet_metadata(int64_t tablet_id, bool filter_tablet) {
    std::vector<std::string> objects{};
    std::string prefix;
    if (filter_tablet) {
        prefix = fmt::format("tbl_{:016X}_", tablet_id);
    } else {
        prefix = "tbl_";
    }

    ASSIGN_OR_RETURN(auto group, _group_assigner->get_group(tablet_id));
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(_group_assigner->get_fs_prefix()));
    auto scan_cb = [&objects, &prefix](std::string_view name) {
        if (HasPrefixString(name, prefix)) {
            objects.emplace_back(name);
        }
        return true;
    };

    RETURN_IF_ERROR(fs->iterate_dir(path_assemble(group, tablet_id), scan_cb));
    return TabletMetadataIter{this, std::move(group), std::move(objects)};
}

StatusOr<TxnLogPtr> TabletManager::load_txn_log(const std::string& txnlog_path, int64_t tablet_id) {
    std::string read_buf;
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(_group_assigner->get_fs_prefix()));
    ASSIGN_OR_RETURN(auto rf, fs->new_random_access_file(path_assemble(txnlog_path, tablet_id)));

    ASSIGN_OR_RETURN(auto size, rf->get_size());
    if (UNLIKELY(size > std::numeric_limits<int>::max())) {
        return Status::Corruption("file size exceeded the int range");
    }
    raw::stl_string_resize_uninitialized(&read_buf, size);
    RETURN_IF_ERROR(rf->read_at_fully(0, read_buf.data(), size));

    std::shared_ptr<TxnLog> meta = std::make_shared<TxnLog>();
    bool parsed = meta->ParseFromArray(read_buf.data(), static_cast<int>(size));
    if (!parsed) {
        return Status::Corruption(fmt::format("failed to parse txn log {}", txnlog_path));
    }
    return std::move(meta);
}

StatusOr<TxnLogPtr> TabletManager::get_txn_log(const string& group, const std::string& path) {
    int64_t tablet_id;
    int64_t txn_id;
    auto tablet_pos = path.find('_');
    auto txn_pos = path.find('_', tablet_pos + 1);
    if (tablet_pos == std::string::npos || txn_pos == std::string::npos) {
        return Status::InvalidArgument(fmt::format("invalid path format {}", path));
    }
    tablet_id = std::stol(path.substr(tablet_pos + 1, txn_pos - tablet_pos - 1), nullptr, 16);
    txn_id = std::stol(path.substr(txn_pos + 1), nullptr, 16);
    return get_txn_log(group, tablet_id, txn_id);
}

StatusOr<TxnLogPtr> TabletManager::get_txn_log(const std::string& group, int64_t tablet_id, int64_t txn_id) {
    // search metacache
    auto cache_key = txn_log_cache_key(tablet_id, txn_id);
    auto ptr = lookup_txn_log(cache_key);
    RETURN_IF(ptr != nullptr, ptr);

    auto txnlog_path = txn_log_path(group, tablet_id, txn_id);
    ASSIGN_OR_RETURN(ptr, load_txn_log(txnlog_path, tablet_id));

    auto value_ptr = new std::shared_ptr<const TxnLog>(ptr);
    bool inserted = fill_metacache(cache_key, value_ptr, static_cast<int>(ptr->SpaceUsedLong()));
    if (!inserted) {
        delete value_ptr;
        LOG(WARNING) << "Failed to put tabletmetadata into cache " << cache_key;
    }
    return ptr;
}

Status TabletManager::put_txn_log(const std::string& group, TxnLogPtr log) {
    auto options = WritableFileOptions{.sync_on_close = true, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
    auto txnlog_path = txn_log_path(group, log->tablet_id(), log->txn_id());
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(_group_assigner->get_fs_prefix()));
    ASSIGN_OR_RETURN(auto wf, fs->new_writable_file(options, path_assemble(txnlog_path, log->tablet_id())));
    RETURN_IF_ERROR(wf->append(log->SerializeAsString()));
    RETURN_IF_ERROR(wf->close());

    // put txnlog into cache
    auto cache_key = txn_log_cache_key(log->tablet_id(), log->txn_id());
    auto value_ptr = new std::shared_ptr<const TxnLog>(log);
    bool inserted = fill_metacache(cache_key, value_ptr, static_cast<int>(log->SpaceUsedLong()));
    if (!inserted) {
        delete value_ptr;
        LOG(WARNING) << "Failed to put txnlog into cache " << txnlog_path;
    }
    return Status::OK();
}

Status TabletManager::put_txn_log(const std::string& group, const TxnLog& log) {
    auto txnlog_ptr = std::make_shared<TxnLog>(log);
    return put_txn_log(group, std::move(txnlog_ptr));
}

Status TabletManager::delete_txn_log(const std::string& group, int64_t tablet_id, int64_t txn_id) {
    // drop from metacache first
    auto cache_key = txn_log_cache_key(tablet_id, txn_id);
    erase_metacache(cache_key);

    auto txnlog_path = txn_log_path(group, tablet_id, txn_id);
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(_group_assigner->get_fs_prefix()));
    return fs->delete_file(path_assemble(txnlog_path, tablet_id));
}

StatusOr<TxnLogIter> TabletManager::list_txn_log(int64_t tablet_id, bool filter_tablet) {
    std::vector<std::string> objects{};
    std::string prefix;
    if (filter_tablet) {
        prefix = fmt::format("txn_{:016X}_", tablet_id);
    } else {
        prefix = "txn_";
    }

    ASSIGN_OR_RETURN(auto group, _group_assigner->get_group(tablet_id));
    // get group path
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(_group_assigner->get_fs_prefix()));
    auto scan_cb = [&objects, &prefix](std::string_view name) {
        if (HasPrefixString(name, prefix)) {
            objects.emplace_back(name);
        }
        return true;
    };

    RETURN_IF_ERROR(fs->iterate_dir(path_assemble(group, tablet_id), scan_cb));
    return TxnLogIter{this, group, std::move(objects)};
}

Status TabletManager::publish_version(int64_t tablet_id, int64_t base_version, int64_t new_version, const int64_t* txns,
                                      int txns_size) {
    ASSIGN_OR_RETURN(auto tablet, get_tablet(tablet_id));
    return publish(&tablet, base_version, new_version, txns, txns_size);
}

static Status apply_write_log(const TxnLogPB_OpWrite& op_write, TabletMetadata* metadata) {
    if (op_write.has_rowset() && op_write.rowset().num_rows() > 0) {
        auto rowset = metadata->add_rowsets();
        rowset->CopyFrom(op_write.rowset());
        rowset->set_id(metadata->next_rowset_id());
        metadata->set_next_rowset_id(metadata->next_rowset_id() + rowset->segments_size());
    }
    return Status::OK();
}

static Status apply_compaction_log(const TxnLogPB_OpCompaction& op_compaction, TabletMetadata* metadata) {
    // It's ok to have a compaction log without input rowset and output rowset.
    if (op_compaction.input_rowsets().empty()) {
        DCHECK(!op_compaction.has_output_rowset() || op_compaction.output_rowset().num_rows() == 0);
        return Status::OK();
    }
    if (UNLIKELY(!op_compaction.has_output_rowset())) {
        return Status::InternalError("compaction log contains input rowset but no output rowset");
    }
    if (UNLIKELY(op_compaction.output_rowset().num_rows() == 0)) {
        return Status::InternalError("compaction output rowset is empty");
    }

    struct Finder {
        int64_t id;
        bool operator()(const RowsetMetadata& r) const { return r.id() == id; }
    };

    auto input_id = op_compaction.input_rowsets(0);
    auto first_input_pos = std::find_if(metadata->rowsets().begin(), metadata->rowsets().end(), Finder{input_id});
    if (UNLIKELY(first_input_pos == metadata->rowsets().end())) {
        return Status::InternalError(fmt::format("input rowset {} not found", input_id));
    }

    // Safety check:
    // 1. All input rowsets must exist in |metadata->rowsets()|
    // 2. Position of the input rowsets must be adjacent.
    auto pre_input_pos = first_input_pos;
    for (int i = 1, sz = metadata->rowsets_size(); i < sz; i++) {
        input_id = op_compaction.input_rowsets(i);
        auto it = std::find_if(pre_input_pos + 1, metadata->rowsets().end(), Finder{input_id});
        if (it == metadata->rowsets().end()) {
            return Status::InternalError(fmt::format("input rowset {} not exist", input_id));
        } else if (it != pre_input_pos + 1) {
            return Status::InternalError(fmt::format("input rowset position not adjacent"));
        } else {
            pre_input_pos = it;
        }
    }

    // Replace the first input rowset with output rowset
    auto first_idx = static_cast<int>(first_input_pos - metadata->rowsets().begin());
    auto output_rowset = metadata->mutable_rowsets(first_idx);
    output_rowset->CopyFrom(op_compaction.output_rowset());
    output_rowset->set_id(metadata->next_rowset_id());
    metadata->set_next_rowset_id(metadata->next_rowset_id() + output_rowset->segments_size());

    // Erase other input rowsets from metadata
    auto end_input_pos = pre_input_pos + 1;
    DCHECK_EQ(op_compaction.input_rowsets_size(), end_input_pos - first_input_pos);
    metadata->mutable_rowsets()->erase(first_input_pos + 1, end_input_pos);
    return Status::OK();
}

Status apply_txn_log(const TxnLog& log, TabletMetadata* metadata) {
    if (log.has_op_write()) {
        RETURN_IF_ERROR(apply_write_log(log.op_write(), metadata));
    }

    if (log.has_op_compaction()) {
        RETURN_IF_ERROR(apply_compaction_log(log.op_compaction(), metadata));
    }

    if (log.has_op_schema_change()) {
        return Status::NotSupported("does not support apply schema change log yet");
    }
    return Status::OK();
}

Status publish(Tablet* tablet, int64_t base_version, int64_t new_version, const int64_t* txns, int txns_size) {
    // Read base version metadata
    auto res = tablet->get_metadata(base_version);
    if (!res.ok()) {
        // Check if the new version metadata exist.
        if (res.status().is_not_found() && tablet->get_metadata(new_version).ok()) {
            // ^^^^^^^^^^^^^^^^^^^^^^^^^^^^ optimization, there is no need to invoke `get_metadata` in all
            // circumstances, e.g, network and permission problems.
            return Status::OK();
        }
        LOG(WARNING) << "Fail to get " << tablet->metadata_path(base_version) << ": " << res.status();
        return res.status();
    }
    const TabletMetadataPtr& base_metadata = res.value();

    // make a copy of metadata
    auto new_metadata = std::make_shared<TabletMetadata>(*base_metadata);
    new_metadata->set_version(new_version);

    // Apply txn logs
    for (int i = 0; i < txns_size; i++) {
        auto txnid = txns[i];
        auto txnlog = tablet->get_txn_log(txnid);
        if (txnlog.status().is_not_found() && tablet->get_metadata(new_version).ok()) {
            // txn log does not exist but the new version metadata has been generated, maybe
            // this is a duplicated publish version request.
            return Status::OK();
        } else if (!txnlog.ok()) {
            LOG(WARNING) << "Fail to get " << tablet->txn_log_path(txnid) << ": " << txnlog.status();
            return txnlog.status();
        }

        auto st = apply_txn_log(**txnlog, new_metadata.get());
        if (!st.ok()) {
            LOG(WARNING) << "Fail to apply " << tablet->txn_log_path(txnid) << ": " << st;
            return st;
        }
    }

    // Save new metadata
    if (auto st = tablet->put_metadata(new_metadata); !st.ok()) {
        LOG(WARNING) << "Fail to put " << tablet->metadata_path(new_version) << ": " << st;
        return st;
    }

    // Delete txn logs
    for (int i = 0; i < txns_size; i++) {
        auto txn_id = txns[i];
        auto st = tablet->delete_txn_log(txn_id);
        LOG_IF(WARNING, !st.ok()) << "Fail to delete " << tablet->txn_log_path(txn_id) << ": " << st;
    }
    return Status::OK();
}

// TODO: better input rowsets select policy.
StatusOr<CompactionTaskPtr> TabletManager::compact(int64_t tablet_id, int64_t version, int64_t txn_id) {
    ASSIGN_OR_RETURN(auto tablet, get_tablet(tablet_id));
    ASSIGN_OR_RETURN(auto metadata, tablet.get_metadata(version));
    auto tablet_ptr = std::make_shared<Tablet>(tablet);
    std::vector<RowsetPtr> input_rowsets;
    input_rowsets.reserve(metadata->rowsets_size());
    for (const auto& rowset : metadata->rowsets()) {
        auto metadata_ptr = std::make_shared<RowsetMetadata>(rowset);
        input_rowsets.emplace_back(std::make_shared<Rowset>(tablet_ptr.get(), std::move(metadata_ptr)));
    }
    return std::make_shared<HorizontalCompactionTask>(txn_id, version, std::move(tablet_ptr), std::move(input_rowsets));
}

} // namespace starrocks::lake
