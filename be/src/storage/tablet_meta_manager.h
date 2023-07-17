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
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/tablet_meta_manager.h

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

#include <rapidjson/document.h>

#include <string>

#include "common/compiler_util.h"
#include "gen_cpp/persistent_index.pb.h"
#include "storage/data_dir.h"
#include "storage/delta_column_group.h"
#include "storage/kv_store.h"
#include "storage/olap_define.h"
#include "storage/tablet_meta.h"

namespace starrocks {

class DelVector;
using DelVectorPtr = std::shared_ptr<DelVector>;
struct EditVersion;
class EditVersionMetaPB;
class RowsetMetaPB;
class TabletMetaPB;

struct TabletMetaStats {
    TTabletId tablet_id = 0;
    TTableId table_id = 0;
    size_t tablet_meta_bytes = 0;
    // update tablet related
    size_t log_count = 0;
    size_t log_meta_bytes = 0;
    size_t rowset_count = 0;
    size_t rowset_meta_bytes = 0;
    size_t pending_rowset_count = 0;
    size_t pending_rowset_meta_bytes = 0;
    size_t delvec_count = 0;
    size_t delvec_meta_bytes = 0;
};

struct MetaStoreStats {
    size_t tablet_count = 0;
    size_t tablet_meta_bytes = 0;
    size_t rowset_count = 0;
    size_t rowset_meta_bytes = 0;
    // update tablet related
    size_t update_tablet_count = 0;
    size_t update_tablet_meta_bytes = 0;
    size_t log_count = 0;
    size_t log_meta_bytes = 0;
    size_t delvec_count = 0;
    size_t delvec_meta_bytes = 0;
    size_t update_rowset_count = 0;
    size_t update_rowset_meta_bytes = 0;
    size_t pending_rowset_count = 0;
    size_t pending_rowset_meta_bytes = 0;
    size_t total_count = 0;
    size_t total_meta_bytes = 0;
    size_t error_count = 0;
    std::map<TTabletId, TabletMetaStats> tablets;
};

// Helper Class for managing tablet headers of one root path.
class TabletMetaManager {
public:
    static Status get_primary_meta(KVStore* meta, TTabletId tablet_id, TabletMetaPB& tablet_meta_pb, string* json_meta);

    static Status get_tablet_meta(DataDir* store, TTabletId tablet_id, TSchemaHash schema_hash,
                                  TabletMeta* tablet_meta);

    static Status get_persistent_index_meta(DataDir* store, TTabletId tablet_id, PersistentIndexMetaPB* index_meta);

    static Status get_json_meta(DataDir* store, TTabletId tablet_id, TSchemaHash schema_hash, std::string* json_meta);

    static Status build_primary_meta(DataDir* store, rapidjson::Document& doc, rocksdb::ColumnFamilyHandle* cf,
                                     rocksdb::WriteBatch& batch);

    static Status save(DataDir* store, const TabletMetaPB& meta_pb);

    static Status remove(DataDir* store, TTabletId tablet_id, TSchemaHash schema_hash);

    static Status walk(KVStore* meta, std::function<bool(long, long, std::string_view)> const& func);

    static Status walk_until_timeout(KVStore* meta, std::function<bool(long, long, std::string_view)> const& func,
                                     int64_t limit_time);

    static Status load_json_meta(DataDir* store, const std::string& meta_path);

    static Status get_json_meta(DataDir* store, TTabletId tablet_id, std::string* json_meta);

    static Status get_stats(DataDir* store, MetaStoreStats* stats, bool detail = false);

    static Status remove(DataDir* store, TTabletId tablet_id);

    //
    // Updatable tablet meta operations
    //

    // commit a rowset into tablet
    static Status rowset_commit(DataDir* store, TTabletId tablet_id, int64_t logid, EditVersionMetaPB* edit,
                                const RowsetMetaPB& rowset, const string& rowset_meta_key);

    static Status write_persistent_index_meta(DataDir* store, TTabletId tablet_id, const PersistentIndexMetaPB& meta);

    using RowsetIterateFunc = std::function<bool(RowsetMetaSharedPtr rowset_meta)>;
    static Status rowset_iterate(DataDir* store, TTabletId tablet_id, const RowsetIterateFunc& func);

    // methods for operating pending commits
    static Status pending_rowset_commit(DataDir* store, TTabletId tablet_id, int64_t version,
                                        const RowsetMetaPB& rowset, const string& rowset_meta_key);

    using PendingRowsetIterateFunc = std::function<bool(int64_t version, std::string_view rowset_meta_data)>;
    static Status pending_rowset_iterate(DataDir* store, TTabletId tablet_id, const PendingRowsetIterateFunc& func);

    // On success, store a pointer to `RowsetMeta` in |*meta| and return OK status.
    // On failure, store a nullptr in |*meta| and return a non-OK status.
    // Return NotFound if the rowset does not exist.
    static Status rowset_get(DataDir* store, TTabletId tablet_id, uint32_t rowset_id, RowsetMetaSharedPtr* meta);

    // For updatable tablet's rowset only.
    // Remove rowset meta from |store|, leave tablet meta unchanged.
    // |rowset_id| is the value returned from `RowsetMeta::get_rowset_seg_id`.
    // |segments| is the number of segments in the rowset, i.e, `Rowset::num_segments`.
    // All delete vectors that associated with this rowset will be deleted too.
    static Status rowset_delete(DataDir* store, TTabletId tablet_id, uint32_t rowset_id, uint32_t segments);

    // update meta after state of a rowset commit is applied
    static Status apply_rowset_commit(DataDir* store, TTabletId tablet_id, int64_t logid, const EditVersion& version,
                                      std::vector<std::pair<uint32_t, DelVectorPtr>>& delvecs,
                                      const PersistentIndexMetaPB& index_meta, bool enable_persistent_index,
                                      const starrocks::RowsetMetaPB* rowset_meta);

    // used in column mode partial update
    static Status apply_rowset_commit(DataDir* store, TTabletId tablet_id, int64_t logid, const EditVersion& version,
                                      const std::map<uint32_t, DeltaColumnGroupPtr>& delta_column_groups,
                                      const starrocks::RowsetMetaPB* rowset_meta);

    // traverse all the op logs for a tablet
    static Status traverse_meta_logs(DataDir* store, TTabletId tablet_id,
                                     const std::function<bool(uint64_t, const TabletMetaLogPB&)>& func);

    // TODO: rename parameter |segment_id|, it's different from `Segment::id()`
    static Status set_del_vector(KVStore* meta, TTabletId tablet_id, uint32_t segment_id, const DelVector& delvec);

    // TODO: rename parameter |segment_id|, it's different from `Segment::id()`
    static Status get_del_vector(KVStore* meta, TTabletId tablet_id, uint32_t segment_id, int64_t version,
                                 DelVector* delvec, int64_t* latest_version);

    static Status del_vector_iterate(KVStore* meta, TTabletId tablet_id, uint32_t lower, uint32_t upper,
                                     const std::function<bool(uint32_t, int64_t, std::string_view)>& func);
    // The first element of pair is segment id, the second element of pair is version.
    using DeleteVectorList = std::vector<std::pair<uint32_t, int64_t>>;

    static StatusOr<DeleteVectorList> list_del_vector(KVStore* meta, TTabletId tablet_id, int64_t max_version);

    static Status get_delta_column_group(KVStore* meta, TTabletId tablet_id, uint32_t segment_id, int64_t version,
                                         DeltaColumnGroupList* dcgs);

    static Status get_delta_column_group(KVStore* meta, TTabletId tablet_id, RowsetId rowsetid, uint32_t segment_id,
                                         int64_t version, DeltaColumnGroupList* dcgs);

    static Status scan_delta_column_group(KVStore* meta, TTabletId tablet_id, uint32_t segment_id,
                                          int64_t begin_version, int64_t end_version, DeltaColumnGroupList* dcgs);

    static Status scan_delta_column_group(KVStore* meta, TTabletId tablet_id, RowsetId rowsetid, uint32_t segment_id,
                                          int64_t begin_version, int64_t end_version, DeltaColumnGroupList* dcgs);

    static Status delete_delta_column_group(KVStore* meta, TTabletId tablet_id, uint32_t rowset_id, uint32_t segments);
    static Status delete_delta_column_group(KVStore* meta, WriteBatch* batch, const TabletSegmentId& tsid,
                                            int64_t version);
    static Status delete_delta_column_group(KVStore* meta, TTabletId tablet_id, RowsetId rowsetid, uint32_t segments);

    // delete all delete vectors of a tablet not useful anymore for query version < `version`, for example
    // suppose we have delete vectors of version 1, 3, 5, 6, 7, 12, 16
    // min queryable version is 10, which require delvector of version 7
    // delvector of versin < 7 can be deleted, that is [1,3,5,6]
    // return num of del vector deleted
    static StatusOr<size_t> delete_del_vector_before_version(KVStore* meta, TTabletId tablet_id, int64_t version);

    static Status delete_del_vector_range(KVStore* meta, TTabletId tablet_id, uint32_t segment_id,
                                          int64_t start_version, int64_t end_version);

    static Status put_rowset_meta(DataDir* store, WriteBatch* batch, TTabletId tablet_id,
                                  const RowsetMetaPB& rowset_meta);

    static Status put_del_vector(DataDir* store, WriteBatch* batch, TTabletId tablet_id, uint32_t segment_id,
                                 const DelVector& delvec);

    static Status put_delta_column_group(DataDir* store, WriteBatch* batch, TTabletId tablet_id, uint32_t segment_id,
                                         const DeltaColumnGroupList& dcgs);

    static Status put_delta_column_group(DataDir* store, WriteBatch* batch, TTabletId tablet_id, RowsetId rowsetid,
                                         uint32_t segment_id, const DeltaColumnGroupList& dcgs);

    static Status put_delta_column_group(DataDir* store, WriteBatch* batch, TTabletId tablet_id,
                                         const std::string& rowsetid, uint32_t segment_id,
                                         const DeltaColumnGroupList& dcgs);

    static Status put_tablet_meta(DataDir* store, WriteBatch* batch, const TabletMetaPB& tablet_meta);

    static Status delete_pending_rowset(DataDir* store, WriteBatch* batch, TTabletId tablet_id, int64_t version);

    static Status delete_pending_rowset(DataDir* store, TTabletId tablet_id, int64_t version);

    // Unlike `rowset_delete`, this method will NOT clear delete vectors.
    static Status clear_rowset(DataDir* store, WriteBatch* batch, TTabletId tablet_id);

    static Status clear_pending_rowset(DataDir* store, WriteBatch* batch, TTabletId tablet_id);

    static Status clear_log(DataDir* store, WriteBatch* batch, TTabletId tablet_id);

    static Status clear_del_vector(DataDir* store, WriteBatch* batch, TTabletId tablet_id);

    static Status clear_delta_column_group(DataDir* store, WriteBatch* batch, TTabletId tablet_id);

    static Status clear_persistent_index(DataDir* store, WriteBatch* batch, TTabletId tablet_id);

    static Status remove_tablet_meta(DataDir* store, WriteBatch* batch, TTabletId tablet_id, TSchemaHash schema_hash);

    static Status remove_primary_key_meta(DataDir* store, WriteBatch* batch, TTabletId tablet_id);

    static Status remove_table_meta(DataDir* store, TTableId table_id);

    static Status remove_table_persistent_index_meta(DataDir* store, TTableId table_id);
};

} // namespace starrocks
