// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/tablet.cpp

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

#include "storage/tablet.h"

#include <pthread.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/stringbuffer.h>

#include <algorithm>
#include <cstdio>
#include <map>
#include <memory>
#include <utility>

#include "runtime/current_thread.h"
#include "runtime/exec_env.h"
#include "storage/olap_common.h"
#include "storage/olap_define.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_meta_manager.h"
#include "storage/storage_engine.h"
#include "storage/tablet_meta_manager.h"
#include "storage/tablet_updates.h"
#include "storage/update_manager.h"
#include "util/defer_op.h"
#include "util/path_util.h"
#include "util/time.h"

namespace starrocks {

TabletSharedPtr Tablet::create_tablet_from_meta(MemTracker* mem_tracker, const TabletMetaSharedPtr& tablet_meta,
                                                DataDir* data_dir) {
    auto tablet =
            std::shared_ptr<Tablet>(new Tablet(tablet_meta, data_dir), DeleterWithMemTracker<Tablet>(mem_tracker));
    mem_tracker->consume(tablet->mem_usage());
    return tablet;
}

Tablet::Tablet(TabletMetaSharedPtr tablet_meta, DataDir* data_dir)
        : BaseTablet(tablet_meta, data_dir),
          _last_cumu_compaction_failure_millis(0),
          _last_base_compaction_failure_millis(0),
          _last_cumu_compaction_success_millis(0),
          _last_base_compaction_success_millis(0),
          _cumulative_point(kInvalidCumulativePoint) {
    // change _rs_graph to _timestamped_version_tracker
    _timestamped_version_tracker.construct_versioned_tracker(_tablet_meta->all_rs_metas());
}

Tablet::~Tablet() {}

Status Tablet::_init_once_action() {
    SCOPED_THREAD_LOCAL_CHECK_MEM_LIMIT_SETTER(false);
    VLOG(3) << "begin to load tablet. tablet=" << full_name() << ", version_size=" << _tablet_meta->version_count();
    if (keys_type() == PRIMARY_KEYS) {
        _updates = std::make_unique<TabletUpdates>(*this);
        Status st = _updates->init();
        LOG_IF(WARNING, !st.ok()) << "Fail to init updates: " << st;
        return st;
    }
    for (const auto& rs_meta : _tablet_meta->all_rs_metas()) {
        Version version = rs_meta->version();
        RowsetSharedPtr rowset;
        auto st = RowsetFactory::create_rowset(&_tablet_meta->tablet_schema(), _tablet_path, rs_meta, &rowset);
        if (!st.ok()) {
            LOG(WARNING) << "fail to init rowset. tablet_id=" << tablet_id() << ", schema_hash=" << schema_hash()
                         << ", version=" << version << ", res=" << st;
            return st;
        }
        _rs_version_map[version] = std::move(rowset);
    }

    // init incremental rowset
    for (auto& inc_rs_meta : _tablet_meta->all_inc_rs_metas()) {
        Version version = inc_rs_meta->version();
        RowsetSharedPtr rowset = get_rowset_by_version(version);
        if (rowset == nullptr) {
            auto st = RowsetFactory::create_rowset(&_tablet_meta->tablet_schema(), _tablet_path, inc_rs_meta, &rowset);
            if (!st.ok()) {
                LOG(WARNING) << "fail to init incremental rowset. tablet_id:" << tablet_id()
                             << ", schema_hash:" << schema_hash() << ", version=" << version << ", res=" << st;
                return st;
            }
        }
        _inc_rs_version_map[version] = std::move(rowset);
    }

    return Status::OK();
}

Status Tablet::init() {
    return _init_once.call([this] { return _init_once_action(); });
}

// should save tablet meta to remote meta store
// if it's a primary replica
void Tablet::save_meta() {
    auto st = _tablet_meta->save_meta(_data_dir);
    CHECK(st.ok()) << "fail to save tablet_meta: " << st;
}

Status Tablet::revise_tablet_meta(MemTracker* mem_tracker, const std::vector<RowsetMetaSharedPtr>& rowsets_to_clone,
                                  const std::vector<Version>& versions_to_delete) {
    LOG(INFO) << "begin to clone data to tablet. tablet=" << full_name()
              << ", rowsets_to_clone=" << rowsets_to_clone.size()
              << ", versions_to_delete_size=" << versions_to_delete.size();
    if (_updates != nullptr) {
        LOG(WARNING) << "updatable does not support revise_tablet_meta";
        return Status::NotSupported("updatable does not support revise_tablet_meta");
    }

    Status st;
    do {
        // load new local tablet_meta to operate on
        auto new_tablet_meta = TabletMeta::create(mem_tracker);

        generate_tablet_meta_copy_unlocked(new_tablet_meta);
        // Segment store the pointer of TabletSchema, so don't release the TabletSchema of old TabletMeta
        // Shared the pointer of TabletSchema to the new TabletMeta
        new_tablet_meta->set_tablet_schema(_tablet_meta->tablet_schema_ptr());

        // delete versions from new local tablet_meta
        for (const Version& version : versions_to_delete) {
            new_tablet_meta->delete_rs_meta_by_version(version, nullptr);
            if (new_tablet_meta->version_for_delete_predicate(version)) {
                new_tablet_meta->remove_delete_predicate_by_version(version);
            }
            LOG(INFO) << "delete version from new local tablet_meta when clone. [table=" << full_name()
                      << ", version=" << version << "]";
        }

        for (const auto& rs_meta : rowsets_to_clone) {
            new_tablet_meta->add_rs_meta(rs_meta);
        }
        VLOG(3) << "load rowsets successfully when clone. tablet=" << full_name()
                << ", added rowset size=" << rowsets_to_clone.size();
        // save and reload tablet_meta
        st = new_tablet_meta->save_meta(_data_dir);
        if (!st.ok()) {
            LOG(WARNING) << "failed to save new local tablet_meta when clone: " << st;
            break;
        }
        _tablet_meta = new_tablet_meta;
    } while (false);

    for (auto& version : versions_to_delete) {
        auto it = _rs_version_map.find(version);
        DCHECK(it != _rs_version_map.end());
        StorageEngine::instance()->add_unused_rowset(it->second);
        _rs_version_map.erase(it);
    }
    for (auto& [v, rowset] : _inc_rs_version_map) {
        StorageEngine::instance()->add_unused_rowset(rowset);
    }
    for (auto& [v, rowset] : _stale_rs_version_map) {
        StorageEngine::instance()->add_unused_rowset(rowset);
    }
    _inc_rs_version_map.clear();
    _stale_rs_version_map.clear();

    for (auto& rs_meta : rowsets_to_clone) {
        Version version = {rs_meta->start_version(), rs_meta->end_version()};
        RowsetSharedPtr rowset;
        st = RowsetFactory::create_rowset(&_tablet_meta->tablet_schema(), _tablet_path, rs_meta, &rowset);
        if (!st.ok()) {
            LOG(WARNING) << "fail to init rowset. version=" << version;
            return st;
        }
        _rs_version_map[version] = std::move(rowset);
    }

    // reconstruct from tablet meta
    _timestamped_version_tracker.construct_versioned_tracker(_tablet_meta->all_rs_metas());

    LOG(INFO) << "finish to clone data to tablet. status=" << st << ", "
              << "table=" << full_name() << ", "
              << "rowsets_to_clone=" << rowsets_to_clone.size();
    return st;
}

Status Tablet::add_rowset(const RowsetSharedPtr& rowset, bool need_persist) {
    CHECK(!_updates) << "updatable tablet should not call add_rowset";
    DCHECK(rowset != nullptr);
    std::unique_lock wrlock(_meta_lock);
    // If the rowset already exist, just return directly. The rowset_id is an unique-id,
    // we can use it to check this situation.
    if (_contains_rowset(rowset->rowset_id())) {
        return Status::OK();
    }
    // Otherwise, the version shoud be not contained in any existing rowset.
    RETURN_IF_ERROR(_contains_version(rowset->version()));

    RETURN_IF_ERROR(_tablet_meta->add_rs_meta(rowset->rowset_meta()));
    _rs_version_map[rowset->version()] = rowset;
    _timestamped_version_tracker.add_version(rowset->version());

    std::vector<RowsetSharedPtr> rowsets_to_delete;
    // yiguolei: temp code, should remove the rowset contains by this rowset
    // but it should be removed in multi path version
    for (auto& it : _rs_version_map) {
        if (rowset->version().contains(it.first) && rowset->version() != it.first) {
            CHECK(it.second != nullptr) << "there exist a version=" << it.first
                                        << " contains the input rs with version=" << rowset->version()
                                        << ", but the related rs is null";
            rowsets_to_delete.push_back(it.second);
        }
    }
    modify_rowsets(std::vector<RowsetSharedPtr>(), rowsets_to_delete);

    if (need_persist) {
        Status res = RowsetMetaManager::save(data_dir()->get_meta(), tablet_uid(), rowset->rowset_id(),
                                             rowset->rowset_meta()->get_meta_pb());
        LOG_IF(FATAL, !res.ok()) << "failed to save rowset " << rowset->rowset_id() << " to local meta store: " << res;
    }
    ++_newly_created_rowset_num;
    return Status::OK();
}

void Tablet::modify_rowsets(const std::vector<RowsetSharedPtr>& to_add, const std::vector<RowsetSharedPtr>& to_delete) {
    CHECK(!_updates) << "updatable tablet should not call modify_rowsets";
    // the compaction process allow to compact the single version, eg: version[4-4].
    // this kind of "single version compaction" has same "input version" and "output version".
    // which means "to_add->version()" equals to "to_delete->version()".
    // So we should delete the "to_delete" before adding the "to_add",
    // otherwise, the "to_add" will be deleted from _rs_version_map, eventually.
    std::vector<RowsetMetaSharedPtr> rs_metas_to_delete;
    for (auto& rs : to_delete) {
        rs_metas_to_delete.push_back(rs->rowset_meta());
        _rs_version_map.erase(rs->version());

        // put compaction rowsets in _stale_rs_version_map.
        _stale_rs_version_map[rs->version()] = rs;
    }

    std::vector<RowsetMetaSharedPtr> rs_metas_to_add;
    for (auto& rs : to_add) {
        rs_metas_to_add.push_back(rs->rowset_meta());
        _rs_version_map[rs->version()] = rs;

        _timestamped_version_tracker.add_version(rs->version());
        ++_newly_created_rowset_num;
    }

    _tablet_meta->modify_rs_metas(rs_metas_to_add, rs_metas_to_delete);

    // add rs_metas_to_delete to tracker
    _timestamped_version_tracker.add_stale_path_version(rs_metas_to_delete);
}

// snapshot manager may call this api to check if version exists, so that
// the version maybe not exist
const RowsetSharedPtr Tablet::get_rowset_by_version(const Version& version) const {
    auto iter = _rs_version_map.find(version);
    if (iter == _rs_version_map.end()) {
        VLOG(3) << "no rowset for version:" << version << ", tablet: " << full_name();
        return nullptr;
    }
    return iter->second;
}

// This function only be called by SnapshotManager to perform incremental clone.
// It will be called under protected of _meta_lock(SnapshotManager will fetch it manually),
// so it is no need to lock here.
const RowsetSharedPtr Tablet::get_inc_rowset_by_version(const Version& version) const {
    if (_updates != nullptr) {
        DCHECK_EQ(version.first, version.second);
        return _updates->get_delta_rowset(version.second);
    }
    auto iter = _inc_rs_version_map.find(version);
    if (iter == _inc_rs_version_map.end()) {
        VLOG(3) << "no rowset for version:" << version << ", tablet: " << full_name();
        return nullptr;
    }
    return iter->second;
}

// Already under _meta_lock
const RowsetSharedPtr Tablet::rowset_with_max_version() const {
    Version max_version = _tablet_meta->max_version();
    if (max_version.first == -1) {
        return nullptr;
    }
    if (_updates != nullptr) {
        LOG(WARNING) << "Updatable tablet does not support rowset_with_max_version";
        return nullptr;
    }
    auto iter = _rs_version_map.find(max_version);
    DCHECK(iter != _rs_version_map.end()) << "invalid version:" << max_version;
    return iter->second;
}

RowsetSharedPtr Tablet::_rowset_with_largest_size() {
    RowsetSharedPtr largest_rowset = nullptr;
    for (auto& it : _rs_version_map) {
        if (it.second->empty() || it.second->zero_num_rows()) {
            continue;
        }
        if (largest_rowset == nullptr ||
            it.second->rowset_meta()->index_disk_size() > largest_rowset->rowset_meta()->index_disk_size()) {
            largest_rowset = it.second;
        }
    }

    return largest_rowset;
}

// add inc rowset should not persist tablet meta, because it will be persisted when publish txn.
Status Tablet::add_inc_rowset(const RowsetSharedPtr& rowset) {
    CHECK(!_updates) << "updatable tablet should not call add_inc_rowset";
    DCHECK(rowset != nullptr);
    std::unique_lock wrlock(_meta_lock);
    if (_contains_rowset(rowset->rowset_id())) {
        return Status::OK();
    }
    RETURN_IF_ERROR(_contains_version(rowset->version()));
    RETURN_IF_ERROR(_tablet_meta->add_rs_meta(rowset->rowset_meta()));
    _rs_version_map[rowset->version()] = rowset;
    _inc_rs_version_map[rowset->version()] = rowset;

    _timestamped_version_tracker.add_version(rowset->version());

    RETURN_IF_ERROR(_tablet_meta->add_inc_rs_meta(rowset->rowset_meta()));

    // warm-up this rowset
    auto st = rowset->load();
    // ignore this error, only log load failure
    LOG_IF(WARNING, !st.ok()) << "ignore load rowset error tablet:" << tablet_id() << " rowset:" << rowset->rowset_id()
                              << " " << st;
    ++_newly_created_rowset_num;
    return Status::OK();
}

void Tablet::_delete_inc_rowset_by_version(const Version& version) {
    // delete incremental rowset from map
    _inc_rs_version_map.erase(version);

    RowsetMetaSharedPtr rowset_meta = _tablet_meta->acquire_inc_rs_meta_by_version(version);
    if (rowset_meta == nullptr) {
        return;
    }
    _tablet_meta->delete_inc_rs_meta_by_version(version);
    VLOG(3) << "delete incremental rowset. tablet=" << full_name() << ", version=" << version;
}

void Tablet::_delete_stale_rowset_by_version(const Version& version) {
    RowsetMetaSharedPtr rowset_meta = _tablet_meta->acquire_stale_rs_meta_by_version(version);
    if (rowset_meta == nullptr) {
        return;
    }
    _tablet_meta->delete_stale_rs_meta_by_version(version);
    VLOG(3) << "delete stale rowset. tablet=" << full_name() << ", version=" << version;
}

void Tablet::delete_expired_inc_rowsets() {
    int64_t now = UnixSeconds();
    std::vector<Version> expired_versions;
    std::unique_lock wrlock(_meta_lock);
    for (auto& rs_meta : _tablet_meta->all_inc_rs_metas()) {
        int64_t diff = now - rs_meta->creation_time();
        if (diff >= config::inc_rowset_expired_sec) {
            Version version(rs_meta->version());
            expired_versions.emplace_back(version);
            VLOG(3) << "find expire incremental rowset. tablet=" << full_name() << ", version=" << version
                    << ", exist_sec=" << diff;
        }
    }

    if (expired_versions.empty()) {
        return;
    }

    for (auto& version : expired_versions) {
        _delete_inc_rowset_by_version(version);
        VLOG(3) << "delete expire incremental data. tablet=" << full_name() << ", version=" << version.first;
    }

    save_meta();
}

void Tablet::delete_expired_stale_rowset() {
    int64_t now = UnixSeconds();
    // Compute the end time to delete rowsets, when a expired rowset createtime less then this time, it will be deleted.
    int64_t expired_stale_sweep_endtime = now - config::tablet_rowset_stale_sweep_time_sec;

    if (_updates) {
        _updates->remove_expired_versions(expired_stale_sweep_endtime);
        return;
    }
    std::unique_lock wrlock(_meta_lock);

    std::vector<int64_t> path_id_vec;
    // capture the path version to delete
    _timestamped_version_tracker.capture_expired_paths(static_cast<int64_t>(expired_stale_sweep_endtime), &path_id_vec);

    if (path_id_vec.empty()) {
        return;
    }

    const RowsetSharedPtr lastest_delta = rowset_with_max_version();
    if (lastest_delta == nullptr) {
        LOG(WARNING) << "lastest_delta is null " << tablet_id();
        return;
    }

    std::vector<PathVersionListSharedPtr> stale_version_paths;
    stale_version_paths.reserve(path_id_vec.size());
    for (int64_t path_id : path_id_vec) {
        PathVersionListSharedPtr version_path = _timestamped_version_tracker.fetch_and_delete_path_by_id(path_id);
        stale_version_paths.emplace_back(std::move(version_path));
    }

    auto old_size = _stale_rs_version_map.size();
    auto old_meta_size = _tablet_meta->all_stale_rs_metas().size();

    // do delete operation
    for (const auto& version_path : stale_version_paths) {
        for (const auto& timestamped_version : version_path->timestamped_versions()) {
            auto it = _stale_rs_version_map.find(timestamped_version->version());
            if (it != _stale_rs_version_map.end()) {
                // delete rowset
                StorageEngine::instance()->add_unused_rowset(it->second);
                _stale_rs_version_map.erase(it);
                LOG(INFO) << "delete stale rowset tablet=" << full_name() << " version["
                          << timestamped_version->version().first << "," << timestamped_version->version().second
                          << "] move to unused_rowset success " << expired_stale_sweep_endtime;
            } else {
                LOG(WARNING) << "delete stale rowset tablet=" << full_name() << " version["
                             << timestamped_version->version().first << "," << timestamped_version->version().second
                             << "] not find in stale rs version map";
            }
            _delete_stale_rowset_by_version(timestamped_version->version());
        }
    }
    LOG(INFO) << "delete stale rowset _stale_rs_version_map tablet=" << full_name()
              << " current_size=" << _stale_rs_version_map.size() << " old_size=" << old_size
              << " current_meta_size=" << _tablet_meta->all_stale_rs_metas().size()
              << " old_meta_size=" << old_meta_size << " sweep endtime " << expired_stale_sweep_endtime;

#ifndef BE_TEST
    save_meta();
#endif
}

Status Tablet::capture_consistent_versions(const Version& spec_version, std::vector<Version>* version_path) const {
    if (_updates != nullptr) {
        LOG(ERROR) << "should not call capture_consistent_versions on updatable tablet";
        return Status::NotSupported("updatable tablet does not support capture_consistent_versions");
    }
    if (!_timestamped_version_tracker.capture_consistent_versions(spec_version, version_path).ok()) {
        std::vector<Version> missed_versions;
        calc_missed_versions_unlocked(spec_version.second, &missed_versions);
        if (missed_versions.empty()) {
            auto msg = fmt::format("version already been compacted. tablet_id: {}, version: {}",
                                   _tablet_meta->tablet_id(), spec_version.second);
            LOG(WARNING) << msg;
            return Status::VersionAlreadyMerged(msg);
        } else {
            auto msg = fmt::format("version not found. tablet_id: {}, version: {}", _tablet_meta->tablet_id(),
                                   spec_version.second);
            LOG(WARNING) << msg;
            _print_missed_versions(missed_versions);
            return Status::NotFound(msg);
        }
    }
    return Status::OK();
}

Status Tablet::check_version_integrity(const Version& version) {
    std::shared_lock rdlock(_meta_lock);
    return capture_consistent_versions(version, nullptr);
}

// If any rowset contains the specific version, it means the version already exist
bool Tablet::check_version_exist(const Version& version) const {
    for (auto& it : _rs_version_map) {
        if (it.first.contains(version)) {
            return true;
        }
    }
    return false;
}

void Tablet::list_versions(vector<Version>* versions) const {
    DCHECK(versions != nullptr && versions->empty());

    versions->reserve(_rs_version_map.size());
    // versions vector is not sorted.
    for (const auto& it : _rs_version_map) {
        versions->push_back(it.first);
    }
}

Status Tablet::capture_consistent_rowsets(const Version& spec_version, std::vector<RowsetSharedPtr>* rowsets) const {
    if (_updates != nullptr && spec_version.first == 0 && spec_version.second >= spec_version.first) {
        return _updates->get_applied_rowsets(spec_version.second, rowsets);
    } else if (_updates != nullptr) {
        return Status::InvalidArgument("invalid version");
    }
    std::vector<Version> version_path;
    RETURN_IF_ERROR(capture_consistent_versions(spec_version, &version_path));
    RETURN_IF_ERROR(_capture_consistent_rowsets_unlocked(version_path, rowsets));
    return Status::OK();
}

Status Tablet::_capture_consistent_rowsets_unlocked(const std::vector<Version>& version_path,
                                                    std::vector<RowsetSharedPtr>* rowsets) const {
    DCHECK(rowsets != nullptr && rowsets->empty());
    rowsets->reserve(version_path.size());
    for (auto& version : version_path) {
        bool is_find = false;
        do {
            auto it = _rs_version_map.find(version);
            if (it != _rs_version_map.end()) {
                is_find = true;
                rowsets->push_back(it->second);
                break;
            }

            auto it_expired = _stale_rs_version_map.find(version);
            if (it_expired != _stale_rs_version_map.end()) {
                is_find = true;
                rowsets->push_back(it_expired->second);
                break;
            }
        } while (false);

        if (!is_find) {
            LOG(WARNING) << "fail to find Rowset for version. tablet=" << full_name() << ", version='" << version;
            return Status::NotFound("no rowset of specific version");
        }
    }
    return Status::OK();
}

void Tablet::add_delete_predicate(const DeletePredicatePB& delete_predicate, int64_t version) {
    CHECK(!_updates) << "updatable tablet should not call add_delete_predicate";
    _tablet_meta->add_delete_predicate(delete_predicate, version);
}

// TODO(lingbin): what is the difference between version_for_delete_predicate() and
// version_for_load_deletion()? should at least leave a comment
bool Tablet::version_for_delete_predicate(const Version& version) {
    return _tablet_meta->version_for_delete_predicate(version);
}

AlterTabletTaskSharedPtr Tablet::alter_task() {
    return _tablet_meta->alter_task();
}

void Tablet::add_alter_task(int64_t related_tablet_id, int32_t related_schema_hash,
                            const std::vector<Version>& versions_to_alter, const AlterTabletType alter_type) {
    AlterTabletTask alter_task;
    alter_task.set_alter_state(ALTER_RUNNING);
    alter_task.set_related_tablet_id(related_tablet_id);
    alter_task.set_related_schema_hash(related_schema_hash);
    alter_task.set_alter_type(alter_type);
    _tablet_meta->add_alter_task(alter_task);
    LOG(INFO) << "successfully add alter task for tablet_id:" << this->tablet_id()
              << ", schema_hash:" << this->schema_hash() << ", related_tablet_id " << related_tablet_id
              << ", related_schema_hash " << related_schema_hash << ", alter_type " << alter_type;
}

void Tablet::delete_alter_task() {
    LOG(INFO) << "delete alter task from table. tablet=" << full_name();
    _tablet_meta->delete_alter_task();
}

Status Tablet::set_alter_state(AlterTabletState state) {
    return _tablet_meta->set_alter_state(state);
}

bool Tablet::check_migrate(const TabletSharedPtr& tablet) {
    if (tablet->is_migrating()) {
        LOG(WARNING) << "tablet is migrating. tablet_id=" << tablet->tablet_id();
        return true;
    } else {
        if (tablet != StorageEngine::instance()->tablet_manager()->get_tablet(tablet->tablet_id())) {
            LOG(WARNING) << "tablet has been migrated. tablet_id=" << tablet->tablet_id();
            return true;
        }
    }
    return false;
}

bool Tablet::can_do_compaction() {
    std::shared_lock rdlock(_meta_lock);
    const RowsetSharedPtr lastest_delta = rowset_with_max_version();
    if (lastest_delta == nullptr) {
        return false;
    }

    Version test_version = Version(0, lastest_delta->end_version());
    return capture_consistent_versions(test_version, nullptr).ok();
}

const uint32_t Tablet::calc_cumulative_compaction_score() const {
    uint32_t score = 0;
    bool base_rowset_exist = false;
    const int64_t point = cumulative_layer_point();
    for (auto& rs_meta : _tablet_meta->all_rs_metas()) {
        if (rs_meta->start_version() == 0) {
            base_rowset_exist = true;
        }
        if (rs_meta->start_version() < point) {
            // all_rs_metas() is not sorted, so we use _continue_ other than _break_ here.
            continue;
        }

        score += rs_meta->get_compaction_score();
    }

    // If base doesn't exist, tablet may be altering, skip it, set score to 0
    return base_rowset_exist ? score : 0;
}

const uint32_t Tablet::calc_base_compaction_score() const {
    uint32_t score = 0;
    const int64_t point = cumulative_layer_point();
    bool base_rowset_exist = false;
    for (auto& rs_meta : _tablet_meta->all_rs_metas()) {
        if (rs_meta->start_version() == 0) {
            base_rowset_exist = true;
        }
        if (rs_meta->start_version() >= point) {
            // all_rs_metas() is not sorted, so we use _continue_ other than _break_ here.
            continue;
        }

        score += rs_meta->get_compaction_score();
    }

    return base_rowset_exist ? score : 0;
}

void Tablet::calc_missed_versions(int64_t spec_version, std::vector<Version>* missed_versions) {
    std::shared_lock rdlock(_meta_lock);
    if (_updates != nullptr) {
        for (int64_t v = _updates->max_version() + 1; v <= spec_version; v++) {
            missed_versions->emplace_back(Version(v, v));
        }
    } else {
        calc_missed_versions_unlocked(spec_version, missed_versions);
    }
}

// TODO(lingbin): there may be a bug here, should check it.
// for example:
//     [0-4][5-5][8-8][9-9]
// if spec_version = 6, we still return {6, 7} other than {7}
void Tablet::calc_missed_versions_unlocked(int64_t spec_version, std::vector<Version>* missed_versions) const {
    DCHECK(spec_version > 0) << "invalid spec_version: " << spec_version;
    std::list<Version> existing_versions;
    for (auto& rs : _tablet_meta->all_rs_metas()) {
        existing_versions.emplace_back(rs->version());
    }

    // sort the existing versions in ascending order
    existing_versions.sort([](const Version& a, const Version& b) {
        // simple because 2 versions are certainly not overlapping
        return a.first < b.first;
    });

    // From the first version(=0),  find the missing version until spec_version
    int64_t last_version = -1;
    for (const Version& version : existing_versions) {
        if (version.first > last_version + 1) {
            for (int64_t i = last_version + 1; i < version.first; ++i) {
                missed_versions->emplace_back(Version(i, i));
            }
        }
        last_version = version.second;
        if (last_version >= spec_version) {
            break;
        }
    }
    for (int64_t i = last_version + 1; i <= spec_version; ++i) {
        missed_versions->emplace_back(Version(i, i));
    }
}

Version Tablet::max_continuous_version_from_beginning() const {
    std::shared_lock rdlock(_meta_lock);
    return _max_continuous_version_from_beginning_unlocked();
}

Version Tablet::_max_continuous_version_from_beginning_unlocked() const {
    if (_updates != nullptr) {
        return Version{0, _updates->max_version()};
    }
    std::vector<Version> existing_versions;
    for (auto& rs : _tablet_meta->all_rs_metas()) {
        existing_versions.emplace_back(rs->version());
    }

    // sort the existing versions in ascending order
    std::sort(existing_versions.begin(), existing_versions.end(), [](const Version& left, const Version& right) {
        // simple because 2 versions are certainly not overlapping
        return left.first < right.first;
    });
    Version max_continuous_version = {-1, 0};
    for (auto& existing_version : existing_versions) {
        if (existing_version.first > max_continuous_version.second + 1) {
            break;
        }
        max_continuous_version = existing_version;
    }
    return max_continuous_version;
}

void Tablet::calculate_cumulative_point() {
    std::unique_lock wrlock(_meta_lock);
    if (_cumulative_point != kInvalidCumulativePoint) {
        // only calculate the point once.
        // after that, cumulative point will be updated along with compaction process.
        return;
    }

    std::list<RowsetMetaSharedPtr> existing_rss;
    for (auto& rs : _tablet_meta->all_rs_metas()) {
        existing_rss.emplace_back(rs);
    }

    // sort the existing rowsets by version in ascending order
    existing_rss.sort([](const RowsetMetaSharedPtr& a, const RowsetMetaSharedPtr& b) {
        // simple because 2 versions are certainly not overlapping
        return a->version().first < b->version().first;
    });

    int64_t prev_version = -1;
    for (const RowsetMetaSharedPtr& rs : existing_rss) {
        if (rs->version().first > prev_version + 1) {
            // There is a hole, do not continue
            break;
        }

        bool is_delete = version_for_delete_predicate(rs->version());
        // break the loop if segments in this rowset is overlapping, or is a singleton and not delete rowset.
        if (rs->is_segments_overlapping() || (rs->is_singleton_delta() && !is_delete)) {
            _cumulative_point = rs->version().first;
            break;
        }

        prev_version = rs->version().second;
        _cumulative_point = prev_version + 1;
    }
}

// NOTE: only used when create_table, so it is sure that there is no concurrent reader and writer.
void Tablet::delete_all_files() {
    // Release resources like memory and disk space.
    // we have to call list_versions first, or else error occurs when
    // removing hash_map item and iterating hash_map concurrently.
    std::shared_lock rdlock(_meta_lock);
    for (const auto& it : _rs_version_map) {
        it.second->remove();
    }
    _rs_version_map.clear();
    for (const auto& it : _inc_rs_version_map) {
        it.second->remove();
    }
    _inc_rs_version_map.clear();
    _stale_rs_version_map.clear();
}

// check rowset id in tablet-meta and in rowset-meta atomicly
// for example, during publish version stage, it will first add rowset meta to tablet meta and then
// remove it from rowset meta manager. If we check tablet meta first and then check rowset meta using 2 step unlocked
// the sequence maybe: 1. check in tablet meta [return false]  2. add to tablet meta  3. remove from rowset meta manager
// 4. check in rowset meta manager return false. so that the rowset maybe checked return false it means it is useless and
// will be treated as a garbage.
bool Tablet::check_rowset_id(const RowsetId& rowset_id) {
    std::shared_lock rdlock(_meta_lock);
    if (StorageEngine::instance()->rowset_id_in_use(rowset_id)) {
        return true;
    }
    if (_updates) {
        if (_updates->check_rowset_id(rowset_id)) {
            return true;
        }
    } else {
        for (auto& version_rowset : _rs_version_map) {
            if (version_rowset.second->rowset_id() == rowset_id) {
                return true;
            }
        }
        for (auto& inc_version_rowset : _inc_rs_version_map) {
            if (inc_version_rowset.second->rowset_id() == rowset_id) {
                return true;
            }
        }
    }
    if (RowsetMetaManager::check_rowset_meta(_data_dir->get_meta(), tablet_uid(), rowset_id)) {
        return true;
    }
    return false;
}

void Tablet::_print_missed_versions(const std::vector<Version>& missed_versions) const {
    std::stringstream ss;
    ss << full_name() << " has " << missed_versions.size() << " missed version:";
    // print at most 10 version
    for (int i = 0; i < 10 && i < missed_versions.size(); ++i) {
        ss << missed_versions[i] << ",";
    }
    LOG(WARNING) << ss.str();
}

Status Tablet::_contains_version(const Version& version) {
    // check if there exist a rowset contains the added rowset
    for (auto& it : _rs_version_map) {
        if (it.first.contains(version)) {
            // TODO(lingbin): Is this check unnecessary?
            // because the value type is std::shared_ptr, when will it be nullptr?
            // In addition, in this class, there are many places that do not make this judgment
            // when access _rs_version_map's value.
            CHECK(it.second != nullptr) << "there exist a version=" << it.first
                                        << " contains the input rs with version=" << version
                                        << ", but the related rs is null";
            return Status::AlreadyExist("push version already exist");
        }
    }
    return Status::OK();
}

Status Tablet::set_partition_id(int64_t partition_id) {
    return _tablet_meta->set_partition_id(partition_id);
}

TabletInfo Tablet::get_tablet_info() const {
    return TabletInfo(tablet_id(), schema_hash(), tablet_uid());
}

void Tablet::pick_candicate_rowsets_to_cumulative_compaction(int64_t skip_window_sec,
                                                             std::vector<RowsetSharedPtr>* candidate_rowsets) {
    int64_t now = UnixSeconds();
    std::shared_lock rdlock(_meta_lock);
    for (auto& it : _rs_version_map) {
        if (it.first.first >= _cumulative_point && (it.second->creation_time() + skip_window_sec < now)) {
            candidate_rowsets->push_back(it.second);
        }
    }
}

void Tablet::pick_candicate_rowsets_to_base_compaction(vector<RowsetSharedPtr>* candidate_rowsets) {
    std::shared_lock rdlock(_meta_lock);
    for (auto& it : _rs_version_map) {
        if (it.first.first < _cumulative_point) {
            candidate_rowsets->push_back(it.second);
        }
    }
}

// For http compaction action
void Tablet::get_compaction_status(std::string* json_result) {
    if (keys_type() == PRIMARY_KEYS) {
        return _updates->get_compaction_status(json_result);
    }

    rapidjson::Document root;
    root.SetObject();

    rapidjson::Document path_arr;
    path_arr.SetArray();

    std::vector<RowsetSharedPtr> rowsets;
    std::vector<bool> delete_flags;
    {
        std::shared_lock rdlock(_meta_lock);
        rowsets.reserve(_rs_version_map.size());
        for (auto& it : _rs_version_map) {
            rowsets.push_back(it.second);
        }
        std::sort(rowsets.begin(), rowsets.end(), Rowset::comparator);

        delete_flags.reserve(rowsets.size());
        for (auto& rs : rowsets) {
            delete_flags.push_back(version_for_delete_predicate(rs->version()));
        }
        // get snapshot version path json_doc
        _timestamped_version_tracker.get_stale_version_path_json_doc(path_arr);
    }

    root.AddMember("cumulative point", _cumulative_point.load(), root.GetAllocator());
    rapidjson::Value cumu_value;
    std::string format_str = ToStringFromUnixMillis(_last_cumu_compaction_failure_millis.load());
    cumu_value.SetString(format_str.c_str(), format_str.length(), root.GetAllocator());
    root.AddMember("last cumulative failure time", cumu_value, root.GetAllocator());
    rapidjson::Value base_value;
    format_str = ToStringFromUnixMillis(_last_base_compaction_failure_millis.load());
    base_value.SetString(format_str.c_str(), format_str.length(), root.GetAllocator());
    root.AddMember("last base failure time", base_value, root.GetAllocator());
    rapidjson::Value cumu_success_value;
    format_str = ToStringFromUnixMillis(_last_cumu_compaction_success_millis.load());
    cumu_success_value.SetString(format_str.c_str(), format_str.length(), root.GetAllocator());
    root.AddMember("last cumulative success time", cumu_success_value, root.GetAllocator());
    rapidjson::Value base_success_value;
    format_str = ToStringFromUnixMillis(_last_base_compaction_success_millis.load());
    base_success_value.SetString(format_str.c_str(), format_str.length(), root.GetAllocator());
    root.AddMember("last base success time", base_success_value, root.GetAllocator());

    // print all rowsets' version as an array
    rapidjson::Document versions_arr;
    versions_arr.SetArray();
    for (int i = 0; i < rowsets.size(); ++i) {
        const Version& ver = rowsets[i]->version();
        rapidjson::Value value;
        std::string version_str =
                strings::Substitute("[$0-$1] $2 $3 $4", ver.first, ver.second, rowsets[i]->num_segments(),
                                    (delete_flags[i] ? "DELETE" : "DATA"),
                                    SegmentsOverlapPB_Name(rowsets[i]->rowset_meta()->segments_overlap()));
        value.SetString(version_str.c_str(), version_str.length(), versions_arr.GetAllocator());
        versions_arr.PushBack(value, versions_arr.GetAllocator());
    }
    root.AddMember("rowsets", versions_arr, root.GetAllocator());

    // add stale version rowsets
    root.AddMember("stale version path", path_arr, root.GetAllocator());

    // to json string
    rapidjson::StringBuffer strbuf;
    rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(strbuf);
    root.Accept(writer);
    *json_result = std::string(strbuf.GetString());
}

void Tablet::do_tablet_meta_checkpoint() {
    std::unique_lock store_lock(_meta_store_lock);
    if (_newly_created_rowset_num == 0) {
        return;
    }
    if (UnixMillis() - _last_checkpoint_time < config::tablet_meta_checkpoint_min_interval_secs * 1000 &&
        _newly_created_rowset_num < config::tablet_meta_checkpoint_min_new_rowsets_num) {
        return;
    }

    // hold read-lock other than write-lock, because it will not modify meta structure
    std::shared_lock rdlock(_meta_lock);
    if (tablet_state() != TABLET_RUNNING) {
        LOG(INFO) << "tablet is under state=" << tablet_state() << ", not running, skip do checkpoint"
                  << ", tablet=" << full_name();
        return;
    }
    LOG(INFO) << "start to do tablet meta checkpoint, tablet=" << full_name();
    save_meta();
    // if save meta successfully, then should remove the rowset meta existing in tablet
    // meta from rowset meta store
    for (auto& rs_meta : _tablet_meta->all_rs_metas()) {
        // If we delete it from rowset manager's meta explicitly in previous checkpoint, just skip.
        if (rs_meta->is_remove_from_rowset_meta()) {
            continue;
        }
        if (RowsetMetaManager::check_rowset_meta(_data_dir->get_meta(), tablet_uid(), rs_meta->rowset_id())) {
            (void)RowsetMetaManager::remove(_data_dir->get_meta(), tablet_uid(), rs_meta->rowset_id());
            LOG(INFO) << "remove rowset id from meta store because it is already persistent with "
                         "tablet meta"
                      << ", rowset_id=" << rs_meta->rowset_id();
        }
        rs_meta->set_remove_from_rowset_meta();
    }

    // check _stale_rs_version_map to remove meta from rowset meta store
    for (auto& rs_meta : _tablet_meta->all_stale_rs_metas()) {
        // If we delete it from rowset manager's meta explicitly in previous checkpoint, just skip.
        if (rs_meta->is_remove_from_rowset_meta()) {
            continue;
        }
        if (RowsetMetaManager::check_rowset_meta(_data_dir->get_meta(), tablet_uid(), rs_meta->rowset_id())) {
            (void)RowsetMetaManager::remove(_data_dir->get_meta(), tablet_uid(), rs_meta->rowset_id());
            LOG(INFO) << "remove rowset id from meta store because it is already persistent with "
                         "tablet meta"
                      << ", rowset_id=" << rs_meta->rowset_id();
        }
        rs_meta->set_remove_from_rowset_meta();
    }

    _newly_created_rowset_num = 0;
    _last_checkpoint_time = UnixMillis();
}

bool Tablet::rowset_meta_is_useful(const RowsetMetaSharedPtr& rowset_meta) {
    std::shared_lock rdlock(_meta_lock);
    if (_updates) {
        return _updates->check_rowset_id(rowset_meta->rowset_id());
    }
    bool find_rowset_id = false;
    bool find_version = false;
    for (auto& version_rowset : _rs_version_map) {
        if (version_rowset.second->rowset_id() == rowset_meta->rowset_id()) {
            find_rowset_id = true;
        }
        if (version_rowset.second->contains_version(rowset_meta->version())) {
            find_version = true;
        }
    }
    for (auto& inc_version_rowset : _inc_rs_version_map) {
        if (inc_version_rowset.second->rowset_id() == rowset_meta->rowset_id()) {
            find_rowset_id = true;
        }
        if (inc_version_rowset.second->contains_version(rowset_meta->version())) {
            find_version = true;
        }
    }
    return find_rowset_id || !find_version;
}

bool Tablet::_contains_rowset(const RowsetId rowset_id) {
    CHECK(!_updates);
    for (auto& version_rowset : _rs_version_map) {
        if (version_rowset.second->rowset_id() == rowset_id) {
            return true;
        }
    }
    for (auto& inc_version_rowset : _inc_rs_version_map) {
        if (inc_version_rowset.second->rowset_id() == rowset_id) {
            return true;
        }
    }
    return false;
}

void Tablet::build_tablet_report_info(TTabletInfo* tablet_info) {
    std::shared_lock rdlock(_meta_lock);
    tablet_info->__set_tablet_id(_tablet_meta->tablet_id());
    tablet_info->__set_schema_hash(_tablet_meta->schema_hash());
    tablet_info->__set_partition_id(_tablet_meta->partition_id());
    tablet_info->__set_storage_medium(_data_dir->storage_medium());
    tablet_info->__set_path_hash(_data_dir->path_hash());
    tablet_info->__set_is_in_memory(_tablet_meta->tablet_schema().is_in_memory());
    if (_updates) {
        _updates->get_tablet_info_extra(tablet_info);
    } else {
        Version version = _max_continuous_version_from_beginning_unlocked();
        auto max_rowset = rowset_with_max_version();
        if (max_rowset != nullptr) {
            if (max_rowset->version() != version) {
                tablet_info->__set_version_miss(true);
            }
        } else {
            // If the tablet is in running state, it must not be doing schema-change. so if we can not
            // access its rowsets, it means that the tablet is bad and needs to be reported to the FE
            // for subsequent repairs (through the cloning task)
            if (tablet_state() == TABLET_RUNNING) {
                tablet_info->__set_used(false);
            }
            // For other states, FE knows that the tablet is in a certain change process, so here
            // still sets the state to normal when reporting. Note that every task has an timeout,
            // so if the task corresponding to this change hangs, when the task timeout, FE will know
            // and perform state modification operations.
        }
        tablet_info->__set_version(version.second);
        tablet_info->__set_version_count(_tablet_meta->version_count());
        tablet_info->__set_row_count(_tablet_meta->num_rows());
        tablet_info->__set_data_size(_tablet_meta->tablet_footprint());
    }
}

// should use this method to get a copy of current tablet meta
// there are some rowset meta in local meta store and in in-memory tablet meta
// but not in tablet meta in local meta store
void Tablet::generate_tablet_meta_copy(const TabletMetaSharedPtr& new_tablet_meta) const {
    TabletMetaPB tablet_meta_pb;
    {
        std::shared_lock rdlock(_meta_lock);
        // FIXME: TabletUpdatesPB is lost
        _tablet_meta->to_meta_pb(&tablet_meta_pb);
    }
    new_tablet_meta->init_from_pb(&tablet_meta_pb);
}

// this is a unlocked version of generate_tablet_meta_copy()
// some method already hold the _meta_lock before calling this,
// such as EngineCloneTask::_finish_clone -> tablet->revise_tablet_meta
void Tablet::generate_tablet_meta_copy_unlocked(const TabletMetaSharedPtr& new_tablet_meta) const {
    TabletMetaPB tablet_meta_pb;
    // FIXME: TabletUpdatesPB is lost
    _tablet_meta->to_meta_pb(&tablet_meta_pb);
    new_tablet_meta->init_from_pb(&tablet_meta_pb);
}

Status Tablet::rowset_commit(int64_t version, const RowsetSharedPtr& rowset) {
    CHECK(_updates) << "updates should exists";
    return _updates->rowset_commit(version, rowset);
}

StatusOr<Tablet::IteratorList> Tablet::capture_segment_iterators(const Version& spec_version,
                                                                 const vectorized::Schema& schema,
                                                                 const vectorized::RowsetReadOptions& options) const {
    if (_updates) {
        if (spec_version.first != 0) {
            LOG(WARNING) << "cannot capture with version.first:" << spec_version.first;
            return Status::InvalidArgument("cannot capture with version.first != 0");
        }
        return _updates->read(spec_version.second, schema, options);
    }
    std::shared_lock rdlock(_meta_lock);
    std::vector<Version> version_path;
    std::vector<RowsetSharedPtr> rowsets;
    RETURN_IF_ERROR(capture_consistent_versions(spec_version, &version_path));
    RETURN_IF_ERROR(_capture_consistent_rowsets_unlocked(version_path, &rowsets));
    // Release lock before acquiring segment iterators.
    rdlock.unlock();

    IteratorList iterators;
    for (auto& rowset : rowsets) {
        RETURN_IF_ERROR(rowset->get_segment_iterators(schema, options, &iterators));
    }
    return std::move(iterators);
}

void Tablet::on_shutdown() {
    if (_updates) {
        _updates->_stop_and_wait_apply_done();
    }
}

size_t Tablet::tablet_footprint() {
    if (_updates) {
        return _updates->data_size();
    } else {
        // TODO(lingbin): Why other methods that need to get information from _tablet_meta
        // are not locked, here needs a comment to explain.
        std::shared_lock rdlock(_meta_lock);
        return _tablet_meta->tablet_footprint();
    }
}

size_t Tablet::num_rows() {
    if (_updates) {
        return _updates->num_rows();
    } else {
        // TODO(lingbin): Why other methods which need to get information from _tablet_meta
        // are not locked, here needs a comment to explain.
        std::shared_lock rdlock(_meta_lock);
        return _tablet_meta->num_rows();
    }
}

int Tablet::version_count() const {
    if (_updates) {
        return _updates->version_count();
    } else {
        return _tablet_meta->version_count();
    }
}

Version Tablet::max_version() const {
    if (_updates) {
        return Version(0, _updates->max_version());
    } else {
        return _tablet_meta->max_version();
    }
}

} // namespace starrocks
