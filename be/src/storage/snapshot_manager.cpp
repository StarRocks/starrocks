// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/snapshot_manager.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "storage/snapshot_manager.h"

#include <algorithm>
#include <cstdio>
#include <iterator>
#include <map>
#include <set>

#include "env/env.h"
#include "env/output_stream_wrapper.h"
#include "gen_cpp/Types_constants.h"
#include "gutil/strings/join.h"
#include "runtime/current_thread.h"
#include "runtime/exec_env.h"
#include "storage/del_vector.h"
#include "storage/rowset/beta_rowset.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_id_generator.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/storage_engine.h"
#include "storage/tablet_updates.h"
#include "util/defer_op.h"
#include "util/raw_container.h"

using std::map;
using std::nothrow;
using std::set;
using std::string;
using std::stringstream;
using std::vector;
using std::list;

namespace starrocks {

SnapshotManager* SnapshotManager::_s_instance = nullptr;
std::mutex SnapshotManager::_mlock;

SnapshotManager* SnapshotManager::instance() {
    if (_s_instance == nullptr) {
        std::lock_guard<std::mutex> lock(_mlock);
        if (_s_instance == nullptr) {
            _s_instance = new SnapshotManager(ExecEnv::GetInstance()->clone_mem_tracker());
        }
    }
    return _s_instance;
}

Status SnapshotManager::make_snapshot(const TSnapshotRequest& request, string* snapshot_path) {
    std::unique_ptr<MemTracker> mem_tracker = std::make_unique<MemTracker>(-1, "snapshot", _mem_tracker);
    MemTracker* prev_tracker = tls_thread_status.set_mem_tracker(mem_tracker.get());
    DeferOp op([&] { tls_thread_status.set_mem_tracker(prev_tracker); });

    if (config::storage_format_version == 1) {
        // If you upgrade from storage_format_version=1
        // to storage_format_version=2, clone will be rejected.
        // After upgrade, set storage_format_version=2.
        LOG(WARNING) << "storage_format_version v2 support "
                     << "DATE_V2, TIMESTAMP, DECIMAL_V2. "
                     << "Before set storage_format_version=2, "
                     << "reject the clone logic";
        return Status::NotSupported("disable make_snapshot when storage_format_version=1");
    }

    if (UNLIKELY(snapshot_path == nullptr)) {
        return Status::InvalidArgument("snapshot_path is null");
    }
    if (request.preferred_snapshot_format != g_Types_constants.TSNAPSHOT_REQ_VERSION2) {
        LOG(WARNING) << "Invalid snapshot format. version=" << request.preferred_snapshot_format;
        return Status::InvalidArgument("invalid snapshot_format");
    }
    auto tablet = StorageEngine::instance()->tablet_manager()->get_tablet(request.tablet_id);
    if (tablet == nullptr) {
        LOG(WARNING) << "make_snapshot fail to get tablet. tablet:" << request.tablet_id;
        return Status::RuntimeError("tablet not found");
    }
    int64_t timeout_s = request.__isset.timeout ? request.timeout : config::snapshot_expire_time_sec;

    StatusOr<std::string> res;
    int64_t cur_tablet_version = tablet->max_version().second;
    if (request.__isset.missing_version) {
        LOG(INFO) << "make incremental snapshot tablet:" << request.tablet_id << " cur_version:" << cur_tablet_version
                  << " req_version:" << JoinInts(request.missing_version, ",") << " timeout:" << timeout_s;
        res = snapshot_incremental(tablet, request.missing_version, timeout_s);
    } else if (request.__isset.version) {
        LOG(INFO) << "make full snapshot tablet:" << request.tablet_id << " cur_version:" << cur_tablet_version
                  << " req_version:" << request.version << " timeout:" << timeout_s;
        res = snapshot_full(tablet, request.version, timeout_s);
    } else {
        LOG(INFO) << "make full snapshot tablet:" << request.tablet_id << " cur_version:" << cur_tablet_version
                  << " req_version:" << 0 << " timeout:" << timeout_s;
        res = snapshot_full(tablet, 0, timeout_s);
    }
    if (!res.ok()) {
        return res.status();
    } else {
        return FileUtils::canonicalize(*res, snapshot_path);
    }
}

Status SnapshotManager::release_snapshot(const string& snapshot_path) {
    std::unique_ptr<MemTracker> mem_tracker = std::make_unique<MemTracker>(-1, "snapshot", _mem_tracker);
    MemTracker* prev_tracker = tls_thread_status.set_mem_tracker(mem_tracker.get());
    DeferOp op([&] { tls_thread_status.set_mem_tracker(prev_tracker); });

    // If the requested snapshot_path is located under the root/snapshot folder,
    // it is considered legitimate and can be deleted.
    // Otherwise, it is considered an illegal request and returns an error result
    auto stores = StorageEngine::instance()->get_stores();
    for (auto store : stores) {
        std::string abs_path;
        RETURN_IF_ERROR(FileUtils::canonicalize(store->path(), &abs_path));
        if (snapshot_path.compare(0, abs_path.size(), abs_path) == 0 &&
            snapshot_path.compare(abs_path.size(), SNAPSHOT_PREFIX.size(), SNAPSHOT_PREFIX) == 0) {
            (void)FileUtils::remove_all(snapshot_path);
            LOG(INFO) << "success to release snapshot path. [path='" << snapshot_path << "']";
            return Status::OK();
        }
    }

    LOG(WARNING) << "Illegal snapshot_path: " << snapshot_path;
    return Status::InvalidArgument(fmt::format("Illegal snapshot_path: {}", snapshot_path));
}

Status SnapshotManager::convert_rowset_ids(const string& clone_dir, int64_t tablet_id, int32_t schema_hash) {
    // load original tablet meta
    std::string cloned_header_file = clone_dir + "/" + std::to_string(tablet_id) + ".hdr";
    std::string cloned_meta_file = clone_dir + "/meta";

    bool has_header_file = FileUtils::check_exist(cloned_header_file);
    bool has_meta_file = FileUtils::check_exist(cloned_meta_file);
    if (has_header_file && has_meta_file) {
        return Status::InternalError("found both header and meta file");
    }
    if (!has_header_file && !has_meta_file) {
        return Status::InternalError("fail to find header or meta file");
    }
    TabletMetaPB cloned_tablet_meta_pb;
    if (has_meta_file) {
        return Status::OK();
    } else {
        TabletMeta cloned_tablet_meta;
        if (Status st = cloned_tablet_meta.create_from_file(cloned_header_file); !st.ok()) {
            LOG(WARNING) << "Fail to create rowset meta from " << cloned_header_file << ": " << st;
            return Status::RuntimeError("fail to load cloned header file");
        }
        cloned_tablet_meta.to_meta_pb(&cloned_tablet_meta_pb);
    }
    LOG(INFO) << "Assigning new rowset id for cloned rowsets";

    TabletMetaPB new_tablet_meta_pb;
    new_tablet_meta_pb = cloned_tablet_meta_pb;
    new_tablet_meta_pb.clear_rs_metas();
    new_tablet_meta_pb.clear_inc_rs_metas();
    // should modify tablet id and schema hash because in restore process the tablet id is not
    // equal to tablet id in meta
    new_tablet_meta_pb.set_tablet_id(tablet_id);
    new_tablet_meta_pb.set_schema_hash(schema_hash);
    TabletSchema tablet_schema(new_tablet_meta_pb.schema());

    std::unordered_map<Version, RowsetMetaPB*, HashOfVersion> rs_version_map;
    for (const auto& visible_rowset : cloned_tablet_meta_pb.rs_metas()) {
        if (visible_rowset.rowset_type() == ALPHA_ROWSET) {
            LOG(WARNING) << "must change V1 format to V2 format. "
                         << "tablet_id: " << visible_rowset.tablet_id()
                         << ", tablet_uid: " << visible_rowset.tablet_uid()
                         << ", schema_hash: " << visible_rowset.tablet_schema_hash()
                         << ", rowset_id:" << visible_rowset.rowset_id();
            return Status::RuntimeError("unknown rowset type");
        }
        RowsetMetaPB* rowset_meta = new_tablet_meta_pb.add_rs_metas();
        RowsetId rowset_id = StorageEngine::instance()->next_rowset_id();
        RETURN_IF_ERROR(_rename_rowset_id(visible_rowset, clone_dir, tablet_schema, rowset_id, rowset_meta));
        rowset_meta->set_tablet_id(tablet_id);
        rowset_meta->set_tablet_schema_hash(schema_hash);
        Version rowset_version = {visible_rowset.start_version(), visible_rowset.end_version()};
        rs_version_map[rowset_version] = rowset_meta;
    }

    for (const auto& inc_rowset : cloned_tablet_meta_pb.inc_rs_metas()) {
        if (inc_rowset.rowset_type() == ALPHA_ROWSET) {
            LOG(WARNING) << "must change V1 format to V2 format. "
                         << "tablet_id: " << inc_rowset.tablet_id() << ", tablet_uid: " << inc_rowset.tablet_uid()
                         << ", schema_hash: " << inc_rowset.tablet_schema_hash()
                         << ", rowset_id:" << inc_rowset.rowset_id();
            return Status::RuntimeError("unknown rowset type");
        }
        Version rowset_version = {inc_rowset.start_version(), inc_rowset.end_version()};
        auto exist_rs = rs_version_map.find(rowset_version);
        if (exist_rs != rs_version_map.end()) {
            RowsetMetaPB* rowset_meta = new_tablet_meta_pb.add_inc_rs_metas();
            *rowset_meta = *(exist_rs->second);
            continue;
        }
        RowsetMetaPB* rowset_meta = new_tablet_meta_pb.add_inc_rs_metas();
        RowsetId rowset_id = StorageEngine::instance()->next_rowset_id();
        RETURN_IF_ERROR(_rename_rowset_id(inc_rowset, clone_dir, tablet_schema, rowset_id, rowset_meta));
        rowset_meta->set_tablet_id(tablet_id);
        rowset_meta->set_tablet_schema_hash(schema_hash);
    }

    return TabletMeta::save(cloned_header_file, new_tablet_meta_pb);
}

Status SnapshotManager::_rename_rowset_id(const RowsetMetaPB& rs_meta_pb, const string& new_path,
                                          TabletSchema& tablet_schema, const RowsetId& rowset_id,
                                          RowsetMetaPB* new_rs_meta_pb) {
    // TODO use factory to obtain RowsetMeta when SnapshotManager::convert_rowset_ids supports beta rowset
    RowsetMetaSharedPtr rowset_meta(new RowsetMeta());
    rowset_meta->init_from_pb(rs_meta_pb);
    RowsetSharedPtr org_rowset;
    if (!RowsetFactory::create_rowset(&tablet_schema, new_path, rowset_meta, &org_rowset).ok()) {
        return Status::RuntimeError("fail to create rowset");
    }
    // do not use cache to load index
    // because the index file may conflict
    // and the cached fd may be invalid
    RETURN_IF_ERROR(org_rowset->load());
    RowsetMetaSharedPtr org_rowset_meta = org_rowset->rowset_meta();
    RowsetWriterContext context(kDataFormatUnknown, config::storage_format_version);
    context.rowset_id = rowset_id;
    context.tablet_id = org_rowset_meta->tablet_id();
    context.partition_id = org_rowset_meta->partition_id();
    context.tablet_schema_hash = org_rowset_meta->tablet_schema_hash();
    context.rowset_type = org_rowset_meta->rowset_type();
    context.rowset_path_prefix = new_path;
    context.tablet_schema = &tablet_schema;
    context.rowset_state = org_rowset_meta->rowset_state();
    context.version = org_rowset_meta->version();
    // keep segments_overlap same as origin rowset
    context.segments_overlap = rowset_meta->segments_overlap();

    std::unique_ptr<RowsetWriter> rs_writer;
    if (!RowsetFactory::create_rowset_writer(context, &rs_writer).ok()) {
        return Status::RuntimeError("fail to create rowset writer");
    }

    if (auto st = rs_writer->add_rowset(org_rowset); !st.ok()) {
        LOG(WARNING) << "Fail to add rowset " << org_rowset->rowset_id() << " to rowset " << rowset_id << ": " << st;
        return st;
    }
    auto new_rowset = rs_writer->build();
    if (!new_rowset.ok()) return new_rowset.status();
    if (auto st = (*new_rowset)->load(); !st.ok()) {
        LOG(WARNING) << "Fail to load new rowset: " << st;
        return st;
    }
    (*new_rowset)->rowset_meta()->to_rowset_pb(new_rs_meta_pb);
    org_rowset->remove();
    return Status::OK();
}

// get snapshot path: curtime.seq.timeout
// eg: 20190819221234.3.86400
std::string SnapshotManager::_calc_snapshot_id_path(const TabletSharedPtr& tablet, int64_t timeout_s) {
    // get current timestamp string
    string time_str;
    if (!gen_timestamp_string(&time_str).ok()) {
        LOG(WARNING) << "Fail to gen_timestamp_string";
        return "";
    }

    std::stringstream snapshot_id_path_stream;
    std::lock_guard l(_snapshot_mutex);
    snapshot_id_path_stream << tablet->data_dir()->path() << SNAPSHOT_PREFIX << "/" << time_str << "."
                            << _snapshot_base_id++ << "." << timeout_s;
    return snapshot_id_path_stream.str();
}

std::string SnapshotManager::get_schema_hash_full_path(const TabletSharedPtr& tablet,
                                                       const std::string& location) const {
    std::stringstream schema_full_path_stream;
    schema_full_path_stream << location << "/" << tablet->tablet_id() << "/" << tablet->schema_hash();
    return schema_full_path_stream.str();
}

std::string SnapshotManager::_get_header_full_path(const TabletSharedPtr& tablet,
                                                   const std::string& schema_hash_path) const {
    std::stringstream header_name_stream;
    header_name_stream << schema_hash_path << "/" << tablet->tablet_id() << ".hdr";
    return header_name_stream.str();
}

StatusOr<std::string> SnapshotManager::snapshot_incremental(const TabletSharedPtr& tablet,
                                                            const std::vector<int64_t>& delta_versions,
                                                            int64_t timeout_s) {
    TabletMetaSharedPtr snapshot_tablet_meta = std::make_shared<TabletMeta>();
    std::vector<RowsetSharedPtr> snapshot_rowsets;
    std::vector<RowsetMetaSharedPtr> snapshot_rowset_metas;

    // 1. Check whether the specified versions exist.
    std::shared_lock rdlock(tablet->get_header_lock());
    for (int64_t v : delta_versions) {
        auto rowset = tablet->get_inc_rowset_by_version(Version{v, v});
        if (rowset == nullptr && tablet->max_continuous_version_from_beginning().second >= v) {
            return Status::VersionAlreadyMerged(strings::Substitute("version $0 has been merged", v));
        } else if (rowset == nullptr) {
            return Status::RuntimeError(strings::Substitute("no incremental rowset $0", v));
        }
        snapshot_rowsets.emplace_back(std::move(rowset));
    }
    tablet->generate_tablet_meta_copy_unlocked(snapshot_tablet_meta);
    snapshot_tablet_meta->delete_alter_task();
    rdlock.unlock();

    // 2. Create snapshot directory.
    std::string snapshot_id_path = _calc_snapshot_id_path(tablet, timeout_s);
    if (UNLIKELY(snapshot_id_path.empty())) {
        return Status::RuntimeError("empty snapshot_id_path");
    }
    std::string snapshot_dir = get_schema_hash_full_path(tablet, snapshot_id_path);
    (void)FileUtils::remove_all(snapshot_dir);
    RETURN_IF_ERROR(FileUtils::create_dir(snapshot_dir));

    // If tablet is PrimaryKey tablet, we should dump snapshot meta file first and then link files
    // to snapshot directory
    // The reason is tablet clone assumes rowset file is immutable, but during rowset apply for partial update,
    // rowset file may be changed.
    // When doing partial update, if dump snapshot meta file first, there are four conditions as below
    //  1. rowset status is committed in meta, rowset file is partial rowset
    //  2. rowset status is committed in meta, rowset file is `partial rowset` when we link files, and rowset apply
    //     success after link files.
    //  3. rowset status is committed in meta, rowset file is full rowset
    //  4. rowset status is applied in meta, rowset file is full rowset
    // case1 and case4 is normal case, we don't need do additional process.
    // case2 is almost the same as case1. In normal case, partial rowset files will be delete after rowset apply. But
    // we do a hard link of partial rowset files, so the partial rowset files will not be delete until snapshot dir is
    // deleted. So the src BE will download the partial rowset files.
    // case3 is a bit trick. If the rowset status is committed in meta but the rowset file is full rowset. The src be
    // will download the full rowset file and apply it again. But we handle this contingency in partial rowset apply,
    // because if BE crash before update meta, we also need apply this rowset again after BE restart.

    // 3. Build snapshot header/meta file.
    snapshot_rowset_metas.reserve(snapshot_rowsets.size());
    for (const auto& rowset : snapshot_rowsets) {
        snapshot_rowset_metas.emplace_back(rowset->rowset_meta());
    }

    if (tablet->updates() == nullptr) {
        snapshot_tablet_meta->revise_inc_rs_metas(std::move(snapshot_rowset_metas));
        snapshot_tablet_meta->revise_rs_metas(std::vector<RowsetMetaSharedPtr>());
        std::string header_path = _get_header_full_path(tablet, snapshot_dir);
        if (Status st = snapshot_tablet_meta->save(header_path); !st.ok()) {
            LOG(WARNING) << "Fail to save tablet meta to " << header_path;
            (void)FileUtils::remove_all(snapshot_id_path);
            return Status::RuntimeError("Fail to save tablet meta to header file");
        }
    } else {
        auto st =
                make_snapshot_on_tablet_meta(SNAPSHOT_TYPE_INCREMENTAL, snapshot_dir, tablet, snapshot_rowset_metas,
                                             0 /*snapshot_version, unused*/, g_Types_constants.TSNAPSHOT_REQ_VERSION2);
        if (!st.ok()) {
            (void)FileUtils::remove_all(snapshot_id_path);
            return st;
        }
    }

    // 4. Link files to snapshot directory.
    for (const auto& rowset : snapshot_rowsets) {
        auto st = rowset->link_files_to(snapshot_dir, rowset->rowset_id());
        if (!st.ok()) {
            LOG(WARNING) << "Fail to link rowset file:" << st;
            (void)FileUtils::remove_all(snapshot_id_path);
            return st;
        }
    }

    return snapshot_id_path;
}

StatusOr<std::string> SnapshotManager::snapshot_full(const TabletSharedPtr& tablet, int64_t snapshot_version,
                                                     int64_t timeout_s) {
    TabletMetaSharedPtr snapshot_tablet_meta = std::make_shared<TabletMeta>();
    std::vector<RowsetSharedPtr> snapshot_rowsets;
    std::vector<RowsetMetaSharedPtr> snapshot_rowset_metas;

    // 1. Check whether the snapshot version exist.
    std::shared_lock rdlock(tablet->get_header_lock());
    if (snapshot_version == 0) {
        snapshot_version = tablet->max_version().second;
    }
    RETURN_IF_ERROR(tablet->capture_consistent_rowsets(Version(0, snapshot_version), &snapshot_rowsets));
    tablet->generate_tablet_meta_copy_unlocked(snapshot_tablet_meta);
    snapshot_tablet_meta->delete_alter_task();
    rdlock.unlock();

    // 2. Create snapshot directory.
    std::string snapshot_id_path = _calc_snapshot_id_path(tablet, timeout_s);
    if (UNLIKELY(snapshot_id_path.empty())) {
        return Status::RuntimeError("empty snapshot_id_path");
    }
    std::string snapshot_dir = get_schema_hash_full_path(tablet, snapshot_id_path);
    (void)FileUtils::remove_all(snapshot_dir);
    RETURN_IF_ERROR(FileUtils::create_dir(snapshot_dir));

    // 3. Link files to snapshot directory.
    snapshot_rowset_metas.reserve(snapshot_rowsets.size());
    for (const auto& snapshot_rowset : snapshot_rowsets) {
        auto st = snapshot_rowset->link_files_to(snapshot_dir, snapshot_rowset->rowset_id());
        if (!st.ok()) {
            LOG(WARNING) << "Fail to link rowset file:" << st;
            (void)FileUtils::remove_all(snapshot_id_path);
            return st;
        }
        snapshot_rowset_metas.emplace_back(snapshot_rowset->rowset_meta());
    }

    // 4. Build snapshot header/meta file.
    if (tablet->updates() == nullptr) {
        snapshot_tablet_meta->revise_inc_rs_metas(vector<RowsetMetaSharedPtr>());
        snapshot_tablet_meta->revise_rs_metas(std::move(snapshot_rowset_metas));
        std::string header_path = _get_header_full_path(tablet, snapshot_dir);
        if (Status st = snapshot_tablet_meta->save(header_path); !st.ok()) {
            LOG(WARNING) << "Fail to save tablet meta to " << header_path;
            (void)FileUtils::remove_all(snapshot_id_path);
            return Status::RuntimeError("Fail to save tablet meta to header file");
        }
        return snapshot_id_path;
    } else {
        auto st = make_snapshot_on_tablet_meta(SNAPSHOT_TYPE_FULL, snapshot_dir, tablet, snapshot_rowset_metas,
                                               snapshot_version, g_Types_constants.TSNAPSHOT_REQ_VERSION2);
        if (!st.ok()) {
            (void)FileUtils::remove_all(snapshot_id_path);
            return st;
        }
        return snapshot_id_path;
    }
}

Status SnapshotManager::make_snapshot_on_tablet_meta(const TabletSharedPtr& tablet) {
    std::vector<RowsetSharedPtr> snapshot_rowsets;
    std::shared_lock rdlock(tablet->get_header_lock());
    int64_t snapshot_version = tablet->max_version().second;
    RETURN_IF_ERROR(tablet->capture_consistent_rowsets(Version(0, snapshot_version), &snapshot_rowsets));
    rdlock.unlock();
    std::vector<RowsetMetaSharedPtr> snapshot_rowset_metas;
    snapshot_rowset_metas.reserve(snapshot_rowsets.size());
    for (const auto& snapshot_rowset : snapshot_rowsets) {
        snapshot_rowset_metas.emplace_back(snapshot_rowset->rowset_meta());
    }
    std::string meta_path = tablet->schema_hash_path();
    (void)FileUtils::remove_all(meta_path);
    RETURN_IF_ERROR(FileUtils::create_dir(meta_path));
    auto st = make_snapshot_on_tablet_meta(SNAPSHOT_TYPE_FULL, meta_path, tablet, snapshot_rowset_metas,
                                           snapshot_version, g_Types_constants.TSNAPSHOT_REQ_VERSION2);
    if (!st.ok()) {
        (void)FileUtils::remove(meta_path);
        return st;
    }
    return Status::OK();
}

Status SnapshotManager::make_snapshot_on_tablet_meta(SnapshotTypePB snapshot_type, const std::string& snapshot_dir,
                                                     const TabletSharedPtr& tablet,
                                                     const std::vector<RowsetMetaSharedPtr>& rowset_metas,
                                                     int64_t snapshot_version, int32_t snapshot_format) {
    if (snapshot_format != g_Types_constants.TSNAPSHOT_REQ_VERSION2) {
        return Status::NotSupported("unsupported snapshot format");
    }
    if (tablet->updates() == nullptr) {
        return Status::InternalError("make_snapshot_on_tablet_meta only support updatable tablet");
    }

    SnapshotMeta snapshot_meta;
    snapshot_meta.set_snapshot_format(snapshot_format);
    snapshot_meta.set_snapshot_type(snapshot_type);
    snapshot_meta.set_snapshot_version(snapshot_version);
    tablet->updates()->to_rowset_meta_pb(rowset_metas, snapshot_meta.rowset_metas());
    if (snapshot_type == SNAPSHOT_TYPE_FULL) {
        auto meta_store = tablet->data_dir()->get_meta();
        uint32_t new_rsid = 0;
        for (auto& rowset_meta_pb : snapshot_meta.rowset_metas()) {
            const uint32_t old_rsid = rowset_meta_pb.rowset_seg_id();
            for (int i = 0; i < rowset_meta_pb.num_segments(); i++) {
                int64_t dummy;
                const uint32_t old_segment_id = old_rsid + i;
                const uint32_t new_segment_id = new_rsid + i;
                CHECK(snapshot_meta.delete_vectors().count(new_segment_id) == 0);
                DelVector* delvec = &snapshot_meta.delete_vectors()[new_segment_id];
                RETURN_IF_ERROR(TabletMetaManager::get_del_vector(meta_store, tablet->tablet_id(), old_segment_id,
                                                                  snapshot_version, delvec, &dummy /*latest_version*/));
            }
            rowset_meta_pb.set_rowset_seg_id(new_rsid);
            new_rsid += std::max<uint32_t>(rowset_meta_pb.num_segments(), 1);
        }
    }

    TabletMetaPB& meta_pb = snapshot_meta.tablet_meta();
    tablet->tablet_meta()->to_meta_pb(&meta_pb);
    if (snapshot_type == SNAPSHOT_TYPE_FULL) {
        // Construct a new UpdatesPB
        meta_pb.mutable_updates()->Clear();
        auto version = meta_pb.mutable_updates()->add_versions();

        uint32_t next_segment_id = 0;
        version->mutable_version()->set_major(snapshot_version);
        version->mutable_version()->set_minor(0);
        version->set_creation_time(time(nullptr));
        for (const auto& rowset_meta_pb : snapshot_meta.rowset_metas()) {
            auto rsid = rowset_meta_pb.rowset_seg_id();
            next_segment_id = std::max<uint32_t>(next_segment_id, rsid + std::max(1L, rowset_meta_pb.num_segments()));
            version->add_rowsets(rsid);
        }
        meta_pb.mutable_updates()->set_next_rowset_id(next_segment_id);
        meta_pb.mutable_updates()->set_next_log_id(0);
        meta_pb.mutable_updates()->mutable_apply_version()->set_major(snapshot_version);
        meta_pb.mutable_updates()->mutable_apply_version()->set_minor(0);
    }

    std::unique_ptr<WritableFile> f;
    ASSIGN_OR_RETURN(f, Env::Default()->new_writable_file(snapshot_dir + "/meta"));
    RETURN_IF_ERROR(snapshot_meta.serialize_to_file(f.get()));
    RETURN_IF_ERROR(f->sync());
    RETURN_IF_ERROR(f->close());
    return Status::OK();
}

// See `SnapshotManager::make_snapshot_on_tablet_meta` for the file format.
StatusOr<SnapshotMeta> SnapshotManager::parse_snapshot_meta(const std::string& filename) {
    SnapshotMeta snapshot_meta;
    ASSIGN_OR_RETURN(auto file, Env::Default()->new_random_access_file(filename));
    RETURN_IF_ERROR(snapshot_meta.parse_from_file(file.get()));
    return std::move(snapshot_meta);
}

Status SnapshotManager::assign_new_rowset_id(SnapshotMeta* snapshot_meta, const std::string& clone_dir) {
    for (auto& rowset_meta_pb : snapshot_meta->rowset_metas()) {
        RowsetId old_rowset_id;
        RowsetId new_rowset_id = StorageEngine::instance()->next_rowset_id();
        old_rowset_id.init(rowset_meta_pb.rowset_id_v2());

        LOG(INFO) << "Replacing rowset id " << rowset_meta_pb.rowset_id_v2() << " with " << new_rowset_id;

        for (int seg_id = 0; seg_id < rowset_meta_pb.num_segments(); seg_id++) {
            auto old_path = BetaRowset::segment_file_path(clone_dir, old_rowset_id, seg_id);
            auto new_path = BetaRowset::segment_file_path(clone_dir, new_rowset_id, seg_id);
            RETURN_IF_ERROR(Env::Default()->link_file(old_path, new_path));
        }
        for (int del_id = 0; del_id < rowset_meta_pb.num_delete_files(); del_id++) {
            auto old_path = BetaRowset::segment_del_file_path(clone_dir, old_rowset_id, del_id);
            auto new_path = BetaRowset::segment_del_file_path(clone_dir, new_rowset_id, del_id);
            RETURN_IF_ERROR(Env::Default()->link_file(old_path, new_path));
        }
        rowset_meta_pb.set_rowset_id_v2(new_rowset_id.to_string());
    }
    return Status::OK();
}

} // namespace starrocks
