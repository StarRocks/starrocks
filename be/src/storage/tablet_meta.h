// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/tablet_meta.h

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

#include <mutex>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <vector>

#include "common/logging.h"
#include "common/status.h"
#include "gen_cpp/olap_file.pb.h"
#include "storage/delete_handler.h"
#include "storage/olap_common.h"
#include "storage/olap_define.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/rowset_meta.h"
#include "storage/tablet_schema.h"
#include "util/uid_util.h"

namespace starrocks {

// Lifecycle states that a Tablet can be in. Legal state transitions for a
// Tablet object:
//
//   NOTREADY -> RUNNING -> TOMBSTONED -> STOPPED -> SHUTDOWN
//      |           |            |          ^^^
//      |           |            +----------++|
//      |           +------------------------+|
//      +-------------------------------------+

enum TabletState {
    // Tablet is under alter table, rollup, clone
    TABLET_NOTREADY,

    TABLET_RUNNING,

    // Tablet integrity has been violated, such as missing versions.
    // In this state, tablet will not accept any incoming request.
    // Report this state to FE, scheduling BE to drop tablet.
    TABLET_TOMBSTONED,

    // Tablet is shutting down, files in disk still remained.
    TABLET_STOPPED,

    // Files have been removed, tablet has been shutdown completely.
    TABLET_SHUTDOWN
};

class RowsetMeta;
class Rowset;
class DataDir;
class TabletMeta;
using TabletMetaSharedPtr = std::shared_ptr<TabletMeta>;
class TabletUpdates;

// Class encapsulates meta of tablet.
// The concurrency control is handled in Tablet Class, not in this class.
class TabletMeta {
public:
    static Status create(const TCreateTabletReq& request, const TabletUid& tablet_uid, uint64_t shard_id,
                         uint32_t next_unique_id,
                         const std::unordered_map<uint32_t, uint32_t>& col_ordinal_to_unique_id,
                         TabletMetaSharedPtr* tablet_meta);

    static TabletMetaSharedPtr create();

    explicit TabletMeta();
    TabletMeta(int64_t table_id, int64_t partition_id, int64_t tablet_id, int32_t schema_hash, uint64_t shard_id,
               const TTabletSchema& tablet_schema, uint32_t next_unique_id, bool enable_persistent_index,
               const std::unordered_map<uint32_t, uint32_t>& col_ordinal_to_unique_id, const TabletUid& tablet_uid,
               TTabletType::type tabletType, TCompressionType::type compression_type);

    virtual ~TabletMeta();

    // Function create_from_file is used to be compatible with previous tablet_meta.
    // Previous tablet_meta is a physical file in tablet dir, which is not stored in rocksdb.
    Status create_from_file(const std::string& file_path);
    Status save(const std::string& file_path);
    static Status save(const std::string& file_path, const TabletMetaPB& tablet_meta_pb);
    static Status reset_tablet_uid(const std::string& file_path);
    static std::string construct_header_file_path(const std::string& schema_hash_path, int64_t tablet_id);
    Status save_meta(DataDir* data_dir);

    Status serialize(std::string* meta_binary);
    Status deserialize(std::string_view data);
    void init_from_pb(TabletMetaPB* ptablet_meta_pb);

    void to_meta_pb(TabletMetaPB* tablet_meta_pb);
    void to_json(std::string* json_string, json2pb::Pb2JsonOptions& options);

    TabletTypePB tablet_type() const { return _tablet_type; }
    TabletUid tablet_uid() const;
    int64_t table_id() const;
    int64_t partition_id() const;
    int64_t tablet_id() const;
    void set_tablet_id(int64_t tablet_id) { _tablet_id = tablet_id; }
    int32_t schema_hash() const;
    int32_t shard_id() const;
    void set_shard_id(int32_t shard_id);
    int64_t creation_time() const;
    void set_creation_time(int64_t creation_time);
    int64_t cumulative_layer_point() const;
    void set_cumulative_layer_point(int64_t new_point);

    size_t num_rows() const;
    // disk space occupied by tablet
    size_t tablet_footprint() const;
    size_t version_count() const;
    size_t segment_count() const;
    Version max_version() const;

    TabletState tablet_state() const;
    // NOTE: Normally you should NOT call this method directly but call Tablet::set_tablet_state().
    // This is a dangerous method, it may change the state from SHUTDOWN to RUNNING again, which should not happen
    // in normal cases
    void set_tablet_state(TabletState state);

    bool in_restore_mode() const;

    const TabletSchema& tablet_schema() const;

    void set_tablet_schema(const std::shared_ptr<const TabletSchema>& tablet_schema) { _schema = tablet_schema; }

    std::shared_ptr<const TabletSchema>& tablet_schema_ptr() { return _schema; }

    const std::vector<RowsetMetaSharedPtr>& all_rs_metas() const;
    Status add_rs_meta(const RowsetMetaSharedPtr& rs_meta);
    void delete_rs_meta_by_version(const Version& version, std::vector<RowsetMetaSharedPtr>* deleted_rs_metas);
    void modify_rs_metas(const std::vector<RowsetMetaSharedPtr>& to_add,
                         const std::vector<RowsetMetaSharedPtr>& to_delete);
    void revise_rs_metas(std::vector<RowsetMetaSharedPtr> rs_metas);

    void revise_inc_rs_metas(std::vector<RowsetMetaSharedPtr> rs_metas);
    const std::vector<RowsetMetaSharedPtr>& all_inc_rs_metas() const;
    const std::vector<RowsetMetaSharedPtr>& all_stale_rs_metas() const;
    Status add_inc_rs_meta(const RowsetMetaSharedPtr& rs_meta);
    void delete_inc_rs_meta_by_version(const Version& version);
    RowsetMetaSharedPtr acquire_inc_rs_meta_by_version(const Version& version) const;
    void delete_stale_rs_meta_by_version(const Version& version);

    void reset_tablet_schema_for_restore(const TabletSchemaPB& schema_pb);

    void add_delete_predicate(const DeletePredicatePB& delete_predicate, int64_t version);
    void remove_delete_predicate_by_version(const Version& version);
    const DelPredicateArray& delete_predicates() const;
    bool version_for_delete_predicate(const Version& version);
    std::string full_name() const;

    Status set_partition_id(int64_t partition_id);

    // used when create new tablet
    void create_inital_updates_meta();
    // _updates will become empty after release
    TabletUpdatesPB* release_updates(TabletUpdates* updates) {
        _updates = updates;
        return _updatesPB.release();
    }

    bool get_enable_persistent_index() const { return _enable_persistent_index; }

    void set_enable_persistent_index(bool enable_persistent_index) {
        _enable_persistent_index = enable_persistent_index;
    }

private:
    int64_t _mem_usage() const { return sizeof(TabletMeta); }

    Status _save_meta(DataDir* data_dir);

    // _del_pred_array is ignored to compare.
    friend bool operator==(const TabletMeta& a, const TabletMeta& b);
    friend bool operator!=(const TabletMeta& a, const TabletMeta& b);

    int64_t _table_id = 0;
    int64_t _partition_id = 0;
    int64_t _tablet_id = 0;
    int32_t _schema_hash = 0;
    int32_t _shard_id = 0;
    int64_t _creation_time = 0;
    int64_t _cumulative_layer_point = 0;
    bool _enable_persistent_index = false;
    TabletUid _tablet_uid;
    TabletTypePB _tablet_type = TabletTypePB::TABLET_TYPE_DISK;

    TabletState _tablet_state = TABLET_NOTREADY;
    // Note: Segment store the pointer of TabletSchema,
    // so this point should never change
    std::shared_ptr<const TabletSchema> _schema = nullptr;

    std::vector<RowsetMetaSharedPtr> _rs_metas;
    std::vector<RowsetMetaSharedPtr> _inc_rs_metas;
    // This variable _stale_rs_metas is used to record these rowsets' meta which are be compacted.
    // These stale rowsets meta are been removed when rowsets' pathVersion is expired,
    // this policy is judged and computed by TimestampedVersionTracker.
    std::vector<RowsetMetaSharedPtr> _stale_rs_metas;

    DelPredicateArray _del_pred_array;
    bool _in_restore_mode = false;

    // This meta is used for TabletUpdates' load process,
    // to save memory it will be passed to TabletUpdates for usage
    // and then deleted.
    std::unique_ptr<TabletUpdatesPB> _updatesPB;
    // A reference to TabletUpdates, so update related meta
    // can be serialized with tablet meta automatically
    TabletUpdates* _updates = nullptr;

    std::shared_mutex _meta_lock;
};

inline TabletUid TabletMeta::tablet_uid() const {
    return _tablet_uid;
}

inline int64_t TabletMeta::table_id() const {
    return _table_id;
}

inline int64_t TabletMeta::partition_id() const {
    return _partition_id;
}

inline int64_t TabletMeta::tablet_id() const {
    return _tablet_id;
}

inline int32_t TabletMeta::schema_hash() const {
    return _schema_hash;
}

inline int32_t TabletMeta::shard_id() const {
    return _shard_id;
}

inline void TabletMeta::set_shard_id(int32_t shard_id) {
    _shard_id = shard_id;
}

inline int64_t TabletMeta::creation_time() const {
    return _creation_time;
}

inline void TabletMeta::set_creation_time(int64_t creation_time) {
    _creation_time = creation_time;
}

inline int64_t TabletMeta::cumulative_layer_point() const {
    return _cumulative_layer_point;
}

inline void TabletMeta::set_cumulative_layer_point(int64_t new_point) {
    _cumulative_layer_point = new_point;
}

inline size_t TabletMeta::num_rows() const {
    size_t num_rows = 0;
    for (auto& rs : _rs_metas) {
        num_rows += rs->num_rows();
    }
    return num_rows;
}

inline size_t TabletMeta::tablet_footprint() const {
    size_t total_size = 0;
    for (auto& rs : _rs_metas) {
        total_size += rs->data_disk_size();
    }
    return total_size;
}

inline size_t TabletMeta::version_count() const {
    return _rs_metas.size();
}

inline size_t TabletMeta::segment_count() const {
    size_t num_segments = 0;
    for (auto rowset_meta : _rs_metas) {
        num_segments += rowset_meta->num_segments();
    }
    return num_segments;
}

inline TabletState TabletMeta::tablet_state() const {
    return _tablet_state;
}

inline void TabletMeta::set_tablet_state(TabletState state) {
    _tablet_state = state;
}

inline bool TabletMeta::in_restore_mode() const {
    return _in_restore_mode;
}

inline const TabletSchema& TabletMeta::tablet_schema() const {
    return *_schema;
}

inline const std::vector<RowsetMetaSharedPtr>& TabletMeta::all_rs_metas() const {
    return _rs_metas;
}

inline const std::vector<RowsetMetaSharedPtr>& TabletMeta::all_inc_rs_metas() const {
    return _inc_rs_metas;
}

inline const std::vector<RowsetMetaSharedPtr>& TabletMeta::all_stale_rs_metas() const {
    return _stale_rs_metas;
}

// Only for unit test now.
bool operator==(const TabletMeta& a, const TabletMeta& b);
bool operator!=(const TabletMeta& a, const TabletMeta& b);

} // namespace starrocks
