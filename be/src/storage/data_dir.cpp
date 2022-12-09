// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/data_dir.cpp

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

#include "storage/data_dir.h"

#include <filesystem>
#include <set>
#include <sstream>
#include <utility>

#include "common/config.h"
#include "fs/fs.h"
#include "fs/fs_util.h"
#include "gutil/strings/substitute.h"
#include "runtime/exec_env.h"
#include "service/backend_options.h"
#include "storage/olap_define.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_meta.h"
#include "storage/rowset/rowset_meta_manager.h"
#include "storage/storage_engine.h"
#include "storage/tablet_manager.h"
#include "storage/tablet_meta_manager.h"
#include "storage/txn_manager.h"
#include "storage/utils.h" // for check_dir_existed
#include "util/defer_op.h"
#include "util/errno.h"
#include "util/monotime.h"
#include "util/string_util.h"

using strings::Substitute;

namespace starrocks {

static const char* const kTestFilePath = "/.testfile";

DataDir::DataDir(const std::string& path, TStorageMedium::type storage_medium, TabletManager* tablet_manager,
                 TxnManager* txn_manager)
        : _path(path),
          _available_bytes(0),
          _disk_capacity_bytes(0),
          _storage_medium(storage_medium),
          _is_used(false),
          _tablet_manager(tablet_manager),
          _txn_manager(txn_manager),
          _cluster_id_mgr(std::make_shared<ClusterIdMgr>(path)),
          _current_shard(0) {}

DataDir::~DataDir() {
    delete _id_generator;
    delete _kv_store;
}

Status DataDir::init(bool read_only) {
    ASSIGN_OR_RETURN(_fs, FileSystem::CreateSharedFromString(_path));
    RETURN_IF_ERROR(_fs->path_exists(_path));
    std::string align_tag_path = _path + ALIGN_TAG_PREFIX;
    if (access(align_tag_path.c_str(), F_OK) == 0) {
        RETURN_IF_ERROR_WITH_WARN(Status::NotFound(Substitute("align tag $0 was found", align_tag_path)),
                                  "access file failed");
    }

    RETURN_IF_ERROR_WITH_WARN(update_capacity(), "update_capacity failed");
    RETURN_IF_ERROR_WITH_WARN(_cluster_id_mgr->init(), "_cluster_id_mgr init failed");
    RETURN_IF_ERROR_WITH_WARN(_init_data_dir(), "_init_data_dir failed");
    RETURN_IF_ERROR_WITH_WARN(_init_tmp_dir(), "_init_tmp_dir failed");
    RETURN_IF_ERROR_WITH_WARN(_init_meta(read_only), "_init_meta failed");

    _is_used = true;
    return Status::OK();
}

void DataDir::stop_bg_worker() {
    _stop_bg_worker = true;
    _cv.notify_one();
}

Status DataDir::_init_data_dir() {
    std::string data_path = _path + DATA_PREFIX;
    auto st = _fs->create_dir_recursive(data_path);
    LOG_IF(ERROR, !st.ok()) << "failed to create data directory " << data_path;
    return st;
}

Status DataDir::_init_tmp_dir() {
    std::string tmp_path = _path + TMP_PREFIX;
    auto st = _fs->create_dir_recursive(tmp_path);
    LOG_IF(ERROR, !st.ok()) << "failed to create temp directory " << tmp_path;
    return st;
}

Status DataDir::_init_meta(bool read_only) {
    // init path hash
    _path_hash = hash_of_path(BackendOptions::get_localhost(), _path);
    LOG(INFO) << "path: " << _path << ", hash: " << _path_hash;

    // init meta
    _kv_store = new (std::nothrow) KVStore(_path);
    if (_kv_store == nullptr) {
        RETURN_IF_ERROR_WITH_WARN(Status::MemoryAllocFailed("allocate memory for KVStore failed"),
                                  "new KVStore failed");
    }
    Status res = _kv_store->init(read_only);
    LOG_IF(WARNING, !res.ok()) << "Fail to init meta store: " << res;
    return res;
}

Status DataDir::set_cluster_id(int32_t cluster_id) {
    return _cluster_id_mgr->set_cluster_id(cluster_id);
}

void DataDir::health_check() {
    // check disk
    if (_is_used) {
        Status res = _read_and_write_test_file();
        if (!res.ok()) {
            LOG(WARNING) << "store read/write test file occur IO Error. path=" << _path;
            if (is_io_error(res)) {
                _is_used = false;
            }
        }
    }
}

Status DataDir::_read_and_write_test_file() {
    std::string test_file = _path + kTestFilePath;
    return read_write_test_file(test_file);
}

Status DataDir::get_shard(uint64_t* shard) {
    std::stringstream shard_path_stream;
    uint32_t next_shard = 0;
    {
        std::lock_guard<std::mutex> l(_mutex);
        next_shard = _current_shard;
        _current_shard = (_current_shard + 1) % MAX_SHARD_NUM;
    }
    shard_path_stream << _path << DATA_PREFIX << "/" << next_shard;
    std::string shard_path = shard_path_stream.str();
    // First check whether the shard path exists. If it does not exist, sync the data directory.
    bool sync_data_path = false;
    if (!fs::path_exist(shard_path)) {
        sync_data_path = true;
    }
    RETURN_IF_ERROR(_fs->create_dir_recursive(shard_path));
    if (sync_data_path) {
        std::string data_path = _path + DATA_PREFIX;
        if (config::sync_tablet_meta) {
            Status st = fs::sync_dir(data_path);
            if (!st.ok()) {
                LOG(WARNING) << "Fail to sync " << data_path << ": " << st.to_string();
                return st;
            }
        }
    }
    *shard = next_shard;
    return Status::OK();
}

void DataDir::register_tablet(Tablet* tablet) {
    TabletInfo tablet_info(tablet->tablet_id(), tablet->schema_hash(), tablet->tablet_uid());

    std::lock_guard<std::mutex> l(_mutex);
    _tablet_set.emplace(tablet_info);
}

void DataDir::deregister_tablet(Tablet* tablet) {
    TabletInfo tablet_info(tablet->tablet_id(), tablet->schema_hash(), tablet->tablet_uid());

    std::lock_guard<std::mutex> l(_mutex);
    _tablet_set.erase(tablet_info);
}

void DataDir::clear_tablets(std::vector<TabletInfo>* tablet_infos) {
    std::lock_guard<std::mutex> l(_mutex);

    tablet_infos->insert(tablet_infos->end(), _tablet_set.begin(), _tablet_set.end());
    _tablet_set.clear();
}

std::string DataDir::get_absolute_shard_path(int64_t shard_id) {
    return strings::Substitute("$0$1/$2", _path, DATA_PREFIX, shard_id);
}

std::string DataDir::get_absolute_tablet_path(int64_t shard_id, int64_t tablet_id, int32_t schema_hash) {
    return strings::Substitute("$0/$1/$2", get_absolute_shard_path(shard_id), tablet_id, schema_hash);
}

void DataDir::find_tablet_in_trash(int64_t tablet_id, std::vector<std::string>* paths) {
    // path: /root_path/trash/time_label/tablet_id/schema_hash
    std::string trash_path = _path + TRASH_PREFIX;
    std::vector<std::string> sub_dirs;
    (void)_fs->get_children(trash_path, &sub_dirs);
    for (auto& sub_dir : sub_dirs) {
        // sub dir is time_label
        std::string sub_path = trash_path + "/" + sub_dir;
        auto is_dir = _fs->is_directory(sub_path);
        if (!is_dir.ok() || !is_dir.value()) {
            continue;
        }
        std::string tablet_path = sub_path + "/" + std::to_string(tablet_id);
        if (_fs->path_exists(tablet_path).ok()) {
            paths->emplace_back(std::move(tablet_path));
        }
    }
}

std::string DataDir::get_root_path_from_schema_hash_path_in_trash(const std::string& schema_hash_dir_in_trash) {
    return std::filesystem::path(schema_hash_dir_in_trash)
            .parent_path()
            .parent_path()
            .parent_path()
            .parent_path()
            .string();
}

// TODO(ygl): deal with rowsets and tablets when load failed
Status DataDir::load() {
    LOG(INFO) << "start to load tablets from " << _path;
    // load rowset meta from meta env and create rowset
    // COMMITTED: add to txn manager
    // VISIBLE: add to tablet
    // if one rowset load failed, then the total data dir will not be loaded
    std::vector<RowsetMetaSharedPtr> dir_rowset_metas;
    LOG(INFO) << "begin loading rowset from meta";
    auto load_rowset_func = [&dir_rowset_metas](const TabletUid& tablet_uid, RowsetId rowset_id,
                                                std::string_view meta_str) -> bool {
        bool parsed = false;
        auto rowset_meta = std::make_shared<RowsetMeta>(meta_str, &parsed);
        if (!parsed) {
            LOG(WARNING) << "parse rowset meta string failed for rowset_id:" << rowset_id;
            // return false will break meta iterator, return true to skip this error
            return true;
        }
        dir_rowset_metas.push_back(rowset_meta);
        return true;
    };
    Status load_rowset_status = RowsetMetaManager::traverse_rowset_metas(_kv_store, load_rowset_func);

    if (!load_rowset_status.ok()) {
        LOG(WARNING) << "errors when load rowset meta from meta env, skip this data dir:" << _path;
    } else {
        LOG(INFO) << "load rowset from meta finished, data dir: " << _path;
    }

    // load tablet
    // create tablet from tablet meta and add it to tablet mgr
    LOG(INFO) << "begin loading tablet from meta";
    std::set<int64_t> tablet_ids;
    std::set<int64_t> failed_tablet_ids;
    auto load_tablet_func = [this, &tablet_ids, &failed_tablet_ids](int64_t tablet_id, int32_t schema_hash,
                                                                    std::string_view value) -> bool {
        Status st =
                _tablet_manager->load_tablet_from_meta(this, tablet_id, schema_hash, value, false, false, false, false);
        if (!st.ok() && !st.is_not_found()) {
            // load_tablet_from_meta() may return NotFound which means the tablet status is DELETED
            // This may happen when the tablet was just deleted before the BE restarted,
            // but it has not been cleared from rocksdb. At this time, restarting the BE
            // will read the tablet in the DELETE state from rocksdb. These tablets have been
            // added to the garbage collection queue and will be automatically deleted afterwards.
            // Therefore, we believe that this situation is not a failure.
            LOG(WARNING) << "load tablet from header failed. status:" << st.to_string() << ", tablet=" << tablet_id
                         << "." << schema_hash;
            failed_tablet_ids.insert(tablet_id);
        } else {
            tablet_ids.insert(tablet_id);
        }
        return true;
    };
    Status load_tablet_status = TabletMetaManager::walk(_kv_store, load_tablet_func);
    if (failed_tablet_ids.size() != 0) {
        LOG(ERROR) << "load tablets from header failed"
                   << ", loaded tablet: " << tablet_ids.size() << ", error tablet: " << failed_tablet_ids.size()
                   << ", path: " << _path;
        if (!config::ignore_load_tablet_failure) {
            LOG(FATAL) << "load tablets encounter failure. stop BE process. path: " << _path;
        }
    }
    if (!load_tablet_status.ok()) {
        LOG(FATAL) << "there is failure when loading tablet headers, quit process"
                   << ". loaded tablet: " << tablet_ids.size() << " error tablet: " << failed_tablet_ids.size()
                   << ", path: " << _path;
    } else {
        LOG(INFO) << "load tablet from meta finished"
                  << ", loaded tablet: " << tablet_ids.size() << ", error tablet: " << failed_tablet_ids.size()
                  << ", path: " << _path;
    }

    // traverse rowset
    // 1. add committed rowset to txn map
    // 2. add visible rowset to tablet
    // ignore any errors when load tablet or rowset, because fe will repair them after report
    for (const auto& rowset_meta : dir_rowset_metas) {
        TabletSharedPtr tablet = _tablet_manager->get_tablet(rowset_meta->tablet_id(), false);
        // tablet maybe dropped, but not drop related rowset meta
        if (tablet == nullptr) {
            // LOG(WARNING) << "could not find tablet id: " << rowset_meta->tablet_id()
            //              << ", schema hash: " << rowset_meta->tablet_schema_hash()
            //              << ", for rowset: " << rowset_meta->rowset_id() << ", skip this rowset";
            continue;
        }
        RowsetSharedPtr rowset;
        Status create_status = RowsetFactory::create_rowset(&tablet->tablet_schema(), tablet->schema_hash_path(),
                                                            rowset_meta, &rowset);
        if (!create_status.ok()) {
            LOG(WARNING) << "Fail to create rowset from rowsetmeta,"
                         << " rowset=" << rowset_meta->rowset_id() << " state=" << rowset_meta->rowset_state();
            continue;
        }
        if (rowset_meta->rowset_state() == RowsetStatePB::COMMITTED &&
            rowset_meta->tablet_uid() == tablet->tablet_uid()) {
            Status commit_txn_status = _txn_manager->commit_txn(
                    _kv_store, rowset_meta->partition_id(), rowset_meta->txn_id(), rowset_meta->tablet_id(),
                    rowset_meta->tablet_schema_hash(), rowset_meta->tablet_uid(), rowset_meta->load_id(), rowset, true);
            if (!commit_txn_status.ok() && !commit_txn_status.is_already_exist()) {
                LOG(WARNING) << "Fail to add committed rowset=" << rowset_meta->rowset_id()
                             << " tablet=" << rowset_meta->tablet_id() << " txn_id: " << rowset_meta->txn_id();
            } else {
                LOG(INFO) << "Added committed rowset=" << rowset_meta->rowset_id()
                          << " tablet=" << rowset_meta->tablet_id()
                          << " schema hash=" << rowset_meta->tablet_schema_hash()
                          << " txn_id: " << rowset_meta->txn_id();
            }
        } else if (rowset_meta->rowset_state() == RowsetStatePB::VISIBLE &&
                   rowset_meta->tablet_uid() == tablet->tablet_uid()) {
            Status publish_status = tablet->add_rowset(rowset, false);
            if (!publish_status.ok() && !publish_status.is_already_exist()) {
                LOG(WARNING) << "Fail to add visible rowset=" << rowset->rowset_id()
                             << " to tablet=" << rowset_meta->tablet_id() << " txn id=" << rowset_meta->txn_id()
                             << " start version=" << rowset_meta->version().first
                             << " end version=" << rowset_meta->version().second;
            }
        } else {
            LOG(WARNING) << "Found invalid rowset=" << rowset_meta->rowset_id()
                         << " tablet id=" << rowset_meta->tablet_id() << " tablet uid=" << rowset_meta->tablet_uid()
                         << " schema hash=" << rowset_meta->tablet_schema_hash() << " txn_id: " << rowset_meta->txn_id()
                         << " current valid tablet uid=" << tablet->tablet_uid();
        }
    }
    return Status::OK();
}

// gc unused tablet schemahash dir
void DataDir::perform_path_gc_by_tablet() {
    std::unique_lock<std::mutex> lck(_check_path_mutex);
    _cv.wait(lck, [this] { return _stop_bg_worker || !_all_tablet_schemahash_paths.empty(); });
    if (_stop_bg_worker) {
        return;
    }
    LOG(INFO) << "start to path gc by tablet schema hash.";
    int counter = 0;
    for (auto& path : _all_tablet_schemahash_paths) {
        ++counter;
        if (config::path_gc_check_step > 0 && counter % config::path_gc_check_step == 0) {
            SleepFor(MonoDelta::FromMilliseconds(config::path_gc_check_step_interval_ms));
        }
        TTabletId tablet_id = -1;
        TSchemaHash schema_hash = -1;
        bool is_valid = _tablet_manager->get_tablet_id_and_schema_hash_from_path(path, &tablet_id, &schema_hash);
        if (!is_valid) {
            LOG(WARNING) << "unknown path:" << path;
            continue;
        }
        // should not happen, because already check it is a valid tablet schema hash path in previous step
        // so that log fatal here
        if (tablet_id < 1 || schema_hash < 1) {
            LOG(WARNING) << "invalid tablet id " << tablet_id << " or schema hash " << schema_hash << ", path=" << path;
            continue;
        }
        TabletSharedPtr tablet = _tablet_manager->get_tablet(tablet_id, true);
        if (tablet != nullptr) {
            // could find the tablet, then skip check it
            continue;
        }
        std::filesystem::path schema_hash_path(path);
        std::filesystem::path tablet_id_path = schema_hash_path.parent_path();
        std::filesystem::path data_dir_path = tablet_id_path.parent_path().parent_path().parent_path();
        std::string data_dir_string = data_dir_path.string();
        DataDir* data_dir = StorageEngine::instance()->get_store(data_dir_string);
        if (data_dir == nullptr) {
            LOG(WARNING) << "could not find data dir for tablet path " << path;
            continue;
        }
        _tablet_manager->try_delete_unused_tablet_path(data_dir, tablet_id, schema_hash, tablet_id_path.string());
    }
    _all_tablet_schemahash_paths.clear();
    LOG(INFO) << "finished one time path gc by tablet.";
}

void DataDir::perform_path_gc_by_rowsetid() {
    // init the set of valid path
    // validate the path in data dir
    std::unique_lock<std::mutex> lck(_check_path_mutex);
    _cv.wait(lck, [this] { return _stop_bg_worker || !_all_check_paths.empty(); });
    if (_stop_bg_worker) {
        return;
    }
    LOG(INFO) << "start to path gc by rowsetid.";
    int counter = 0;
    for (auto& path : _all_check_paths) {
        ++counter;
        if (config::path_gc_check_step > 0 && counter % config::path_gc_check_step == 0) {
            SleepFor(MonoDelta::FromMilliseconds(config::path_gc_check_step_interval_ms));
        }
        TTabletId tablet_id = -1;
        TSchemaHash schema_hash = -1;
        bool is_valid = _tablet_manager->get_tablet_id_and_schema_hash_from_path(path, &tablet_id, &schema_hash);
        if (!is_valid) {
            LOG(WARNING) << "unknown path:" << path;
            continue;
        }
        if (tablet_id > 0 && schema_hash > 0) {
            // tablet schema hash path or rowset file path
            // gc thread should get tablet include deleted tablet
            // or it will delete rowset file before tablet is garbage collected
            RowsetId rowset_id;
            bool is_rowset_file = TabletManager::get_rowset_id_from_path(path, &rowset_id);
            if (is_rowset_file) {
                TabletSharedPtr tablet = _tablet_manager->get_tablet(tablet_id, false);
                if (tablet != nullptr) {
                    if (!tablet->check_rowset_id(rowset_id) &&
                        !StorageEngine::instance()->check_rowset_id_in_unused_rowsets(rowset_id)) {
                        _process_garbage_path(path);
                    }
                }
            }
        }
    }
    _all_check_paths.clear();
    LOG(INFO) << "finished one time path gc by rowsetid.";
}

// path producer
void DataDir::perform_path_scan() {
    {
        std::unique_lock<std::mutex> lck(_check_path_mutex);
        if (!_all_check_paths.empty()) {
            LOG(INFO) << "_all_check_paths is not empty when path scan.";
            return;
        }
        LOG(INFO) << "start to scan data dir path:" << _path;
        std::set<std::string> shards;
        std::string data_path = _path + DATA_PREFIX;

        Status ret = fs::list_dirs_files(_fs.get(), data_path, &shards, nullptr);
        if (!ret.ok()) {
            LOG(WARNING) << "fail to walk dir. path=[" + data_path << "] error[" << ret.to_string() << "]";
            return;
        }

        for (const auto& shard : shards) {
            std::string shard_path = data_path + "/" + shard;
            std::set<std::string> tablet_ids;
            ret = fs::list_dirs_files(_fs.get(), shard_path, &tablet_ids, nullptr);
            if (!ret.ok()) {
                LOG(WARNING) << "fail to walk dir. [path=" << shard_path << "] error[" << ret.to_string() << "]";
                continue;
            }
            for (const auto& tablet_id : tablet_ids) {
                std::string tablet_id_path = shard_path + "/" + tablet_id;
                std::set<std::string> schema_hashes;
                ret = fs::list_dirs_files(_fs.get(), tablet_id_path, &schema_hashes, nullptr);
                if (!ret.ok()) {
                    LOG(WARNING) << "fail to walk dir. [path=" << tablet_id_path << "]"
                                 << " error[" << ret.to_string() << "]";
                    continue;
                }
                for (const auto& schema_hash : schema_hashes) {
                    std::string tablet_schema_hash_path = tablet_id_path + "/" + schema_hash;
                    _all_tablet_schemahash_paths.insert(tablet_schema_hash_path);
                    std::set<std::string> rowset_files;

                    ret = fs::list_dirs_files(_fs.get(), tablet_schema_hash_path, nullptr, &rowset_files);
                    if (!ret.ok()) {
                        LOG(WARNING) << "fail to walk dir. [path=" << tablet_schema_hash_path << "] error["
                                     << ret.to_string() << "]";
                        continue;
                    }
                    for (const auto& rowset_file : rowset_files) {
                        std::string rowset_file_path = tablet_schema_hash_path + "/" + rowset_file;
                        _all_check_paths.insert(rowset_file_path);
                    }
                }
            }
        }
        LOG(INFO) << "scan data dir path:" << _path << " finished. path size:" << _all_check_paths.size();
    }
    _cv.notify_one();
}

void DataDir::_process_garbage_path(const std::string& path) {
    if (_fs->path_exists(path).ok()) {
        LOG(INFO) << "collect garbage dir path: " << path;
        auto st = _fs->delete_dir_recursive(path);
        LOG_IF(WARNING, !st.ok()) << "failed to remove garbage dir " << path << ": " << st;
    }
}

Status DataDir::update_capacity() {
    ASSIGN_OR_RETURN(auto space_info, FileSystem::Default()->space(_path));
    _available_bytes = space_info.available;
    _disk_capacity_bytes = space_info.capacity;
    return Status::OK();
}

bool DataDir::capacity_limit_reached(int64_t incoming_data_size) {
    double used_pct = (_disk_capacity_bytes - _available_bytes + incoming_data_size) / (double)_disk_capacity_bytes;
    int64_t left_bytes = _disk_capacity_bytes - _available_bytes - incoming_data_size;

    if (used_pct >= config::storage_flood_stage_usage_percent / 100.0 &&
        left_bytes <= config::storage_flood_stage_left_capacity_bytes) {
        LOG(WARNING) << "reach capacity limit. used pct: " << used_pct << ", left bytes: " << left_bytes
                     << ", path: " << _path;
        return true;
    }
    return false;
}
} // namespace starrocks
