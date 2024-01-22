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
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/snapshot_manager.h

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
#include <ctime>
#include <list>
#include <map>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "common/status.h"
#include "fs/fs_util.h"
#include "storage/data_dir.h"
#include "storage/olap_common.h"
#include "storage/olap_define.h"
#include "storage/snapshot_meta.h"
#include "storage/tablet.h"
#include "storage/tablet_meta_manager.h"
#include "util/starrocks_metrics.h"

namespace starrocks {

class SnapshotManager {
public:
    ~SnapshotManager() = default;

    Status make_snapshot(const TSnapshotRequest& request, std::string* snapshot_path);

    std::string get_schema_hash_full_path(const TabletSharedPtr& ref_tablet, const std::string& location) const;

    Status release_snapshot(const std::string& snapshot_path);

    static SnapshotManager* instance();

    Status convert_rowset_ids(const string& clone_dir, int64_t tablet_id, int32_t schema_hash);

    // Create a file named `meta` under the directory |snapshot_dir|. See the
    // comment in snapshot_manager.cpp for the details of the file format.
    // Any existing file with the name `meta` will be deleted, and a new file
    // will be created.
    //
    // REQUIRES:
    //  - |tablet| is a updatable tablet, i.e, tablet->keys_type() is PRIMARY_KEYS
    //  - |snapshot_dir| is an existing directory with write permission.
    //  - |snapshot_version| will NOT be removed until end of this method.
    Status make_snapshot_on_tablet_meta(SnapshotTypePB snapshot_type, const std::string& snapshot_dir,
                                        const TabletSharedPtr& tablet,
                                        const std::vector<RowsetMetaSharedPtr>& rowset_metas, int64_t snapshot_version,
                                        int32_t snapshot_format);

    StatusOr<SnapshotMeta> parse_snapshot_meta(const std::string& filename);

    // On success, return the absolute path of the root directory of snapshot.
    StatusOr<std::string> snapshot_incremental(const TabletSharedPtr& tablet,
                                               const std::vector<int64_t>& delta_versions, int64_t timeout_s);

    // On success, return the absolute path of the root directory of snapshot.
    StatusOr<std::string> snapshot_full(const TabletSharedPtr& tablet, int64_t snapshot_version, int64_t timeout_s,
                                        bool ignore = false);

    // On success, return the absolute path of the root directory of snapshot.
    StatusOr<std::string> snapshot_primary(const TabletSharedPtr& tablet,
                                           const std::vector<int64_t>& missing_version_ranges, int64_t timeout_s);

    Status make_snapshot_on_tablet_meta(const TabletSharedPtr& tablet);

    Status assign_new_rowset_id(SnapshotMeta* snapshot_meta, const std::string& clone_dir,
                                const TabletSchemaCSPtr& tablet_schema);

    // this function is only used for ut
    std::string calc_snapshot_id_path(const TabletSharedPtr& tablet, int64_t timeout_s) {
        return _calc_snapshot_id_path(tablet, timeout_s);
    }

private:
    SnapshotManager(MemTracker* mem_tracker) : _mem_tracker(mem_tracker) {}

    std::string _calc_snapshot_id_path(const TabletSharedPtr& tablet, int64_t timeout_s);

    std::string _get_header_full_path(const TabletSharedPtr& ref_tablet, const std::string& schema_hash_path) const;

    Status _rename_rowset_id(const RowsetMetaPB& rs_meta_pb, const string& new_path, TabletSchemaCSPtr& tablet_schema,
                             const RowsetId& next_id, RowsetMetaPB* new_rs_meta_pb);

    static SnapshotManager* _s_instance;
    static std::mutex _mlock;

    MemTracker* _mem_tracker = nullptr;

    // snapshot
    std::mutex _snapshot_mutex;
    uint64_t _snapshot_base_id{0};
}; // SnapshotManager

} // namespace starrocks
