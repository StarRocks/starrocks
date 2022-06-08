// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "storage/lake/tablet_manager.h"

#include "fmt/format.h"
#include "fs/fs.h"
#include "gen_cpp/AgentService_types.h"
#include "gen_cpp/starlake.pb.h"
#include "gutil/strings/util.h"
#include "storage/lake/group_assigner.h"
#include "storage/lake/metadata_iterator.h"
#include "storage/lake/tablet.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/lake/txn_log.h"
#include "util/lru_cache.h"
#include "util/raw_container.h"

namespace starrocks::lake {

TabletManager::TabletManager(GroupAssigner* group_assigner, int64_t cache_capacity)
        : _group_assigner(group_assigner), _metacache(new_lru_cache(cache_capacity)) {}

// path $bucket:/$ServiceID/$GroupID/tbl_${TabletID}_${version}
std::string TabletManager::tablet_meta_path(const std::string& group, int64_t tablet_id, int64_t verson) {
    if (group.back() != '/') {
        return fmt::format("{}/tbl_{:016X}_{:016X}", group, tablet_id, verson);
    } else {
        return fmt::format("{}tbl_{:016X}_{:016X}", group, tablet_id, verson);
    }
}

// trnslog path rule $Bucket:/$ServiceID/$GroupID/txn_${TabletID}_${TxnID}
std::string TabletManager::txn_log_path(const std::string& group, int64_t tablet_id, int64_t txn_id) {
    if (group.back() != '/') {
        return fmt::format("{}/txn_{:016X}_{:016X}", group, tablet_id, txn_id);
    } else {
        return fmt::format("{}txn_{:016X}_{:016X}", group, tablet_id, txn_id);
    }
}

Status TabletManager::create_tablet(const TCreateTabletReq& req) {
    // generate metapb
    TabletMetadataPB tablet_metadata_pb;
    tablet_metadata_pb.set_id(req.tablet_id);
    tablet_metadata_pb.set_version(1);

    // get shard group
    ASSIGN_OR_RETURN(auto group_path, _group_assigner->get_group(req.tablet_id));

    // write tablet meta
    auto meta_path = tablet_meta_path(group_path, tablet_metadata_pb.id(), tablet_metadata_pb.version());
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(meta_path));
    ASSIGN_OR_RETURN(auto wf, fs->new_writable_file(meta_path));
    RETURN_IF_ERROR(wf->append(tablet_metadata_pb.SerializeAsString()));
    return wf->close();
}

StatusOr<Tablet> TabletManager::get_tablet(int64_t tablet_id) {
    ASSIGN_OR_RETURN(auto group_path, _group_assigner->get_group(tablet_id));
    return Tablet(this, std::move(group_path), tablet_id);
}

Status TabletManager::drop_tablet(int64_t tablet_id) {
    vector<std::string> objects;
    const auto tablet_meta_prefix = fmt::format("tbl_{:016X}_", tablet_id);
    const auto txnlog_prefix = fmt::format("txn_{:016X}_", tablet_id);

    // get group path
    ASSIGN_OR_RETURN(auto group_path, _group_assigner->get_group(tablet_id));
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(group_path));
    auto scan_cb = [&objects, &tablet_meta_prefix, &txnlog_prefix](std::string_view name) {
        if (!HasPrefixString(name, tablet_meta_prefix) && !HasPrefixString(name, txnlog_prefix)) {
            // skip object for other tablet
            return true;
        }
        objects.emplace_back(name);
        return true;
    };
    RETURN_IF_ERROR(fs->iterate_dir(group_path, scan_cb));
    for (const auto& obj : objects) {
        (void)fs->delete_file(fmt::format("{}/{}", group_path, obj));
    }

    return Status::OK();
}

Status TabletManager::put_tablet_metadata(const std::string& group, const TabletMetadata& metadata) {
    auto meta_path = tablet_meta_path(group, metadata.id(), metadata.version());
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(meta_path));
    ASSIGN_OR_RETURN(auto wf, fs->new_writable_file(meta_path));
    RETURN_IF_ERROR(wf->append(metadata.SerializeAsString()));
    RETURN_IF_ERROR(wf->close());
    return Status::OK();
}

Status TabletManager::put_tablet_metadata(const std::string& group, TabletMetadataPtr metadata) {
    auto meta_path = tablet_meta_path(group, metadata->id(), metadata->version());
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(meta_path));
    ASSIGN_OR_RETURN(auto wf, fs->new_writable_file(meta_path));
    RETURN_IF_ERROR(wf->append(metadata->SerializeAsString()));
    RETURN_IF_ERROR(wf->close());
    return Status::OK();
}

StatusOr<TabletMetadataPtr> TabletManager::get_tablet_metadata(const std::string& group, int64_t tablet_id,
                                                               int64_t version) {
    std::string read_buf;
    auto meta_path = tablet_meta_path(group, tablet_id, version);
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(meta_path));
    ASSIGN_OR_RETURN(auto rf, fs->new_random_access_file(meta_path));

    ASSIGN_OR_RETURN(auto size, rf->get_size());
    raw::stl_string_resize_uninitialized(&read_buf, size);
    RETURN_IF_ERROR(rf->read_at_fully(0, read_buf.data(), size));

    std::shared_ptr<TabletMetadata> meta = std::make_shared<TabletMetadata>();
    bool parsed = meta.get()->ParseFromArray(read_buf.data(), size);
    if (!parsed) {
        return Status::Corruption(fmt::format("failed to parse tablet meta {}/tbl_{}_{}", group, tablet_id, version));
    }
    return std::move(meta);
}

Status TabletManager::delete_tablet_metadata(const std::string& group, int64_t tablet_id, int64_t version) {
    auto meta_path = tablet_meta_path(group, tablet_id, version);
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(meta_path));
    RETURN_IF_ERROR(fs->delete_file(meta_path));
    return Status::OK();
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
    std::string read_buf;
    auto txnlog_path = txn_log_path(group, tablet_id, txn_id);
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

Status TabletManager::put_txn_log(const std::string& group, const TxnLog& log) {
    auto txnlog_path = txn_log_path(group, log.tablet_id(), log.txn_id());
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(txnlog_path));
    ASSIGN_OR_RETURN(auto wf, fs->new_writable_file(txnlog_path));
    RETURN_IF_ERROR(wf->append(log.SerializeAsString()));
    RETURN_IF_ERROR(wf->close());
    return Status::OK();
}

Status TabletManager::delete_txn_log(const std::string& group, int64_t tablet_id, int64_t txn_id) {
    auto txnlog_path = txn_log_path(group, tablet_id, txn_id);
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(txnlog_path));
    RETURN_IF_ERROR(fs->delete_file(txnlog_path));
    return Status::OK();
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
