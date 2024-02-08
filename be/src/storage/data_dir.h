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
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/data_dir.h

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

#pragma once

#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <set>
#include <shared_mutex>
#include <string>

#include "common/status.h"
#include "fs/fs.h"
#include "gen_cpp/Types_types.h"
#include "gen_cpp/olap_file.pb.h"
#include "storage/cluster_id_mgr.h"
#include "storage/kv_store.h"
#include "storage/olap_common.h"
#include "storage/rowset/rowset_id_generator.h"

namespace starrocks {

class Tablet;
class TabletManager;
class TxnManager;

enum DiskState {
    ONLINE,
    OFFLINE,       // detected by health_check, tablets on OFFLINE disk will be dropped.
    DISABLED,      // set by user, tablets on DISABLED disk will be dropped.
    DECOMMISSIONED // set by user, tablets on DECOMMISSIONED disk will be migrated to other disks.
};

// A DataDir used to manage data in same path.
// Now, After DataDir was created, it will never be deleted for easy implementation.
class DataDir {
public:
    explicit DataDir(const std::string& path, TStorageMedium::type storage_medium = TStorageMedium::HDD,
                     TabletManager* tablet_manager = nullptr, TxnManager* txn_manager = nullptr);
    ~DataDir();

    DataDir(const DataDir&) = delete;
    void operator=(const DataDir&) = delete;

    Status init(bool read_only = false);
    void stop_bg_worker();

    const std::string& path() const { return _path; }
    int64_t path_hash() const { return _path_hash; }
    bool is_used() const { return _state == DiskState::ONLINE || _state == DiskState::DECOMMISSIONED; }
    DiskState get_state() const { return _state; }
    void set_state(DiskState state) { _state = state; }
    int32_t cluster_id() const { return _cluster_id_mgr->cluster_id(); }

    DataDirInfo get_dir_info() {
        DataDirInfo info;
        info.path = _path;
        info.path_hash = _path_hash;
        info.disk_capacity = _disk_capacity_bytes;
        info.available = _available_bytes;
        info.is_used = is_used();
        info.storage_medium = _storage_medium;
        return info;
    }

    int64_t available_bytes() const { return _available_bytes; }
    int64_t disk_capacity_bytes() const { return _disk_capacity_bytes; }
    double disk_usage(int64_t incoming_data_size) const {
        return (double)(_disk_capacity_bytes - _available_bytes + incoming_data_size) / (double)_disk_capacity_bytes;
    }

    // save a cluster_id file under data path to prevent
    // invalid be config for example two be use the same
    // data path
    Status set_cluster_id(int32_t cluster_id);
    void health_check();

    Status get_shard(uint64_t* shard);

    KVStore* get_meta() { return _kv_store; }

    bool is_ssd_disk() const { return _storage_medium == TStorageMedium::SSD; }

    TStorageMedium::type storage_medium() const { return _storage_medium; }

    void register_tablet(Tablet* tablet);
    void deregister_tablet(Tablet* tablet);
    void clear_tablets(std::vector<TabletInfo>* tablet_infos);

    std::string get_absolute_shard_path(int64_t shard_id);
    std::string get_absolute_tablet_path(int64_t shard_id, int64_t tablet_id, int32_t schema_hash);

    Status create_dir_if_path_not_exists(const std::string& path);
    void find_tablet_in_trash(int64_t tablet_id, std::vector<std::string>* paths);

    static std::string get_root_path_from_schema_hash_path_in_trash(const std::string& schema_hash_dir_in_trash);

    // load data from meta and data files
    Status load();

    // this function scans the paths in data dir to collect the paths to check
    // this is a producer function. After scan, it will notify the perform_path_gc function to gc
    void perform_path_scan();

    void perform_path_gc_by_rowsetid();

    void perform_path_gc_by_tablet();

    void perform_delta_column_files_gc();

    // check if the capacity reach the limit after adding the incoming data
    // return true if limit reached, otherwise, return false.
    // TODO(cmy): for now we can not precisely calculate the capacity StarRocks used,
    // so in order to avoid running out of disk capacity, we currently use the actual
    // disk available capacity and total capacity to do the calculation.
    // So that the capacity StarRocks actually used may exceed the user specified capacity.
    bool capacity_limit_reached(int64_t incoming_data_size);

    Status update_capacity();

    std::string get_persistent_index_path() { return _path + PERSISTENT_INDEX_PREFIX; }
    Status init_persistent_index_dir();

    std::string get_replication_path() { return _path + REPLICATION_PREFIX; }

    std::string get_tmp_path() { return _path + TMP_PREFIX; }

    // for test
    size_t get_all_check_dcg_files_cnt() const { return _all_check_dcg_files.size(); }

private:
    Status _init_data_dir();
    Status _init_tmp_dir();
    Status _init_meta(bool read_only = false);

    Status _read_and_write_test_file();

    void _process_garbage_path(const std::string& path);

    bool _need_gc_delta_column_files(const std::string& path, int64_t tablet_id,
                                     std::unordered_map<int64_t, std::unordered_set<std::string>>& delta_column_files);

    bool _stop_bg_worker = false;

    std::shared_ptr<FileSystem> _fs;
    std::string _path;
    int64_t _path_hash;
    // the actual available capacity of the disk of this data dir
    int64_t _available_bytes;
    // the actual capacity of the disk of this data dir
    int64_t _disk_capacity_bytes;
    TStorageMedium::type _storage_medium;
    DiskState _state;

    TabletManager* _tablet_manager;
    TxnManager* _txn_manager;
    std::shared_ptr<ClusterIdMgr> _cluster_id_mgr;

    // used to protect _current_shard and _tablet_set
    std::mutex _mutex;
    uint64_t _current_shard;
    std::set<TabletInfo> _tablet_set;

    static const uint32_t MAX_SHARD_NUM = 1024;

    KVStore* _kv_store = nullptr;
    RowsetIdGenerator* _id_generator = nullptr;

    std::mutex _check_path_mutex;
    std::condition_variable _cv;
    std::set<std::string> _all_check_paths;
    std::set<std::string> _all_tablet_schemahash_paths;
    std::set<std::string> _all_check_dcg_files;
};

} // namespace starrocks
