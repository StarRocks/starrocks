// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "storage/lake/tablet_manager.h"

#include "fs/fs.h"
#include "gen_cpp/AgentService_types.h"
#include "gen_cpp/starlake.pb.h"
#include "gutil/strings/util.h"
#include "util/raw_container.h"
#ifdef USE_STAROS
#include "service/staros_worker.h"
#include "starlet.h"
#endif
#include "fmt/format.h"
#include "storage/lake/group_assigner.h"
#include "storage/lake/metadata_iterator.h"
#include "storage/lake/tablet.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/lake/txn_log.h"
#include "util/lru_cache.h"
#include "util/raw_container.h"

namespace starrocks::lake {

static const char* const kStarletPrefix = "staros_";

TabletManager::TabletManager(Starlet* starlet, int64_t cache_capacity)
        : _starlet(starlet), _metacache(new_lru_cache(cache_capacity)) {}
TabletManager::TabletManager(GroupAssigner* group_assigner, int64_t cache_capacity)
        : _group_assigner(group_assigner), _metacache(new_lru_cache(cache_capacity)) {}

// path $bucket:/$ServiceID/$GroupID/tbl_${TabletID}_${version}
std::string TabletManager::tablet_meta_path(const std::string& group, int64_t tablet_id, int64_t verson) {
    return fmt::format("{}{}/tbl_{}_{}", kStarletPrefix, group, tablet_id, verson);
}

// trnslog path rule $Bucket:/$ServiceID/$GroupID/txn_${TabletID}_${TxnID}
std::string TabletManager::txn_log_path(const std::string& group, int64_t tablet_id, int64_t txn_id) {
    return fmt::format("{}{}/txn_{}_{}", kStarletPrefix, group, tablet_id, txn_id);
}

Status TabletManager::create_tablet(const TCreateTabletReq& req) {
#ifdef USE_STAROS
    // generate metapb
    TabletMetadataPB tablet_metadata_pb;

    // get shard info
    ASSIGN_OR_RETURN(staros::starlet::ShardInfo shard_info, get_shard_info(req.tablet_id));

    // write tablet meta
    auto meta_path =
            tablet_meta_path(shard_info.obj_store_info.uri, tablet_metadata_pb.id(), tablet_metadata_pb.version());
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(meta_path));
    ASSIGN_OR_RETURN(auto wf, fs->new_writable_file(meta_path));
    RETURN_IF_ERROR(wf->append(tablet_metadata_pb.SerializeAsString()));
    RETURN_IF_ERROR(wf->close());
    return Status::OK();
#else
    (void)req;
    return Status::NotSupported("TabletManager::create_tablet");
#endif
}

StatusOr<Tablet> TabletManager::get_tablet(int64_t tablet_id) {
#ifdef USE_STAROS
    ASSIGN_OR_RETURN(staros::starlet::ShardInfo shard_info, get_shard_info(tablet_id));
    return Tablet(this, shard_info.obj_store_info.uri, tablet_id);
#else
    (void)tablet_id;
    return Status::NotSupported("TabletManager::get_tablet");
#endif
}

// called when user drop the entire table,remove all related tablet_meta,and trn_log;
// tablet meta rule $Bucket:/$ServiceID/$GroupID/tbl_${TabletID}_${version}
// trnslog path rule $Bucket:/$ServiceID/$GroupID/txn_${TabletID}_${TxnID}
// data files  $Bucket:/${ServiceID}/${GroupID}/UUID.dat not deleted
Status TabletManager::drop_tablet(int64_t tablet_id) {
#ifdef USE_STAROS
    ASSIGN_OR_RETURN(staros::starlet::ShardInfo shard_info, get_shard_info(tablet_id));
    auto tablet_meta_prefix = fmt::format("{}/tbl_{}_", shard_info.obj_store_info.uri, tablet_id);
    auto trns_log_prefix = fmt::format("{}/txn_{}_", shard_info.obj_store_info.uri, tablet_id);
    auto object_store = _starlet->get_store(tablet_meta_prefix);
    if (object_store == nullptr) {
        return Status::InternalError(fmt::format("failed to get object store from starlet {}", tablet_meta_prefix));
    }
    auto st = object_store->delete_objects(tablet_meta_prefix);
    if (!st.ok() && st.code() != absl::StatusCode::kNotFound) {
        return Status::InternalError(fmt::format("Failed to delete tablet meta {}", st.message()));
    }
    st = object_store->delete_objects(trns_log_prefix);
    if (!st.ok() && st.code() != absl::StatusCode::kNotFound) {
        return Status::InternalError(fmt::format("Failed to delete tablet meta {}", st.message()));
    }
    return Status::OK();
#else
    (void)tablet_id;
    return Status::NotSupported("TabletManager::drop_tablet");
#endif
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
    stl_string_resize_uninitialized(&read_buf, size);
    RETURN_IF_ERROR(rf->read_at_fully(0, read_buf.data(), size);

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
    stl_string_resize_uninitialized(&read_buf, size);
    RETURN_IF_ERROR(rf->read_at_fully(0, read_buf.data(), size);

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
