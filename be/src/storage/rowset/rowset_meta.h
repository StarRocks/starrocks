// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/rowset/rowset_meta.h

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

#ifndef STARROCKS_BE_SRC_OLAP_ROWSET_ROWSET_META_H
#define STARROCKS_BE_SRC_OLAP_ROWSET_ROWSET_META_H

#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "common/logging.h"
#include "gen_cpp/olap_file.pb.h"
#include "google/protobuf/util/message_differencer.h"
#include "json2pb/json_to_pb.h"
#include "json2pb/pb_to_json.h"
#include "storage/olap_common.h"

namespace starrocks {

class RowsetMeta;
using RowsetMetaSharedPtr = std::shared_ptr<RowsetMeta>;

class RowsetMeta {
public:
    ~RowsetMeta() = default;

    bool init(const std::string_view& pb_rowset_meta) {
        bool ret = _deserialize_from_pb(pb_rowset_meta);
        if (!ret) {
            return false;
        }
        _init();
        return true;
    }

    bool init_from_pb(const RowsetMetaPB& rowset_meta_pb) {
        _rowset_meta_pb = rowset_meta_pb;
        _init();
        return true;
    }

    bool init_from_json(const std::string& json_rowset_meta) {
        bool ret = json2pb::JsonToProtoMessage(json_rowset_meta, &_rowset_meta_pb);
        if (!ret) {
            return false;
        }
        _init();
        return true;
    }

    bool serialize(std::string* value) { return _serialize_to_pb(value); }

    bool json_rowset_meta(std::string* json_rowset_meta) {
        json2pb::Pb2JsonOptions json_options;
        json_options.pretty_json = true;
        bool ret = json2pb::ProtoMessageToJson(_rowset_meta_pb, json_rowset_meta, json_options);
        return ret;
    }

    RowsetId rowset_id() const { return _rowset_id; }

    void set_rowset_id(const RowsetId& rowset_id) {
        // rowset id is a required field, just set it to 0
        _rowset_meta_pb.set_rowset_id(0);
        _rowset_id = rowset_id;
        _rowset_meta_pb.set_rowset_id_v2(rowset_id.to_string());
    }

    int64_t tablet_id() const { return _rowset_meta_pb.tablet_id(); }

    void set_tablet_id(int64_t tablet_id) { _rowset_meta_pb.set_tablet_id(tablet_id); }

    TabletUid tablet_uid() const { return _rowset_meta_pb.tablet_uid(); }

    void set_tablet_uid(const TabletUid& tablet_uid) {
        *(_rowset_meta_pb.mutable_tablet_uid()) = tablet_uid.to_proto();
    }

    int64_t txn_id() const { return _rowset_meta_pb.txn_id(); }

    void set_txn_id(int64_t txn_id) { _rowset_meta_pb.set_txn_id(txn_id); }

    int32_t tablet_schema_hash() const { return _rowset_meta_pb.tablet_schema_hash(); }

    void set_tablet_schema_hash(int64_t tablet_schema_hash) {
        _rowset_meta_pb.set_tablet_schema_hash(tablet_schema_hash);
    }

    RowsetTypePB rowset_type() const { return _rowset_meta_pb.rowset_type(); }

    void set_rowset_type(RowsetTypePB rowset_type) { _rowset_meta_pb.set_rowset_type(rowset_type); }

    RowsetStatePB rowset_state() const { return _rowset_meta_pb.rowset_state(); }

    void set_rowset_state(RowsetStatePB rowset_state) { _rowset_meta_pb.set_rowset_state(rowset_state); }

    Version version() const { return {_rowset_meta_pb.start_version(), _rowset_meta_pb.end_version()}; }

    void set_version(Version version) {
        _rowset_meta_pb.set_start_version(version.first);
        _rowset_meta_pb.set_end_version(version.second);
    }

    bool has_version() const { return _rowset_meta_pb.has_start_version() && _rowset_meta_pb.has_end_version(); }

    int64_t start_version() const { return _rowset_meta_pb.start_version(); }

    void set_start_version(int64_t start_version) { _rowset_meta_pb.set_start_version(start_version); }

    int64_t end_version() const { return _rowset_meta_pb.end_version(); }

    void set_end_version(int64_t end_version) { _rowset_meta_pb.set_end_version(end_version); }

    VersionHash version_hash() const { return _rowset_meta_pb.version_hash(); }

    void set_version_hash(VersionHash version_hash) { _rowset_meta_pb.set_version_hash(version_hash); }

    int64_t num_rows() const { return _rowset_meta_pb.num_rows(); }

    void set_num_rows(int64_t num_rows) { _rowset_meta_pb.set_num_rows(num_rows); }

    int64_t total_row_size() { return _rowset_meta_pb.total_row_size(); }

    void set_total_row_size(int64_t total_row_size) { _rowset_meta_pb.set_total_row_size(total_row_size); }

    size_t total_disk_size() const { return _rowset_meta_pb.total_disk_size(); }

    void set_total_disk_size(size_t total_disk_size) { _rowset_meta_pb.set_total_disk_size(total_disk_size); }

    size_t data_disk_size() const { return _rowset_meta_pb.data_disk_size(); }

    void set_data_disk_size(size_t data_disk_size) { _rowset_meta_pb.set_data_disk_size(data_disk_size); }

    size_t index_disk_size() const { return _rowset_meta_pb.index_disk_size(); }

    void set_index_disk_size(size_t index_disk_size) { _rowset_meta_pb.set_index_disk_size(index_disk_size); }

    void zone_maps(std::vector<ZoneMap>* zone_maps) {
        for (const ZoneMap& zone_map : _rowset_meta_pb.zone_maps()) {
            zone_maps->push_back(zone_map);
        }
    }

    bool has_delete_predicate() const { return _rowset_meta_pb.has_delete_predicate(); }

    const DeletePredicatePB& delete_predicate() const { return _rowset_meta_pb.delete_predicate(); }

    DeletePredicatePB* mutable_delete_predicate() { return _rowset_meta_pb.mutable_delete_predicate(); }

    void set_delete_predicate(const DeletePredicatePB& delete_predicate) {
        DeletePredicatePB* new_delete_condition = _rowset_meta_pb.mutable_delete_predicate();
        *new_delete_condition = delete_predicate;
    }

    bool empty() const { return _rowset_meta_pb.empty(); }

    void set_empty(bool empty) { _rowset_meta_pb.set_empty(empty); }

    PUniqueId load_id() const { return _rowset_meta_pb.load_id(); }

    void set_load_id(const PUniqueId& load_id) {
        PUniqueId* new_load_id = _rowset_meta_pb.mutable_load_id();
        new_load_id->set_hi(load_id.hi());
        new_load_id->set_lo(load_id.lo());
    }

    bool delete_flag() const { return _rowset_meta_pb.delete_flag(); }

    int64_t creation_time() const { return _rowset_meta_pb.creation_time(); }

    void set_creation_time(int64_t creation_time) { return _rowset_meta_pb.set_creation_time(creation_time); }

    int64_t partition_id() const { return _rowset_meta_pb.partition_id(); }

    void set_partition_id(int64_t partition_id) { return _rowset_meta_pb.set_partition_id(partition_id); }

    int64_t num_segments() const { return _rowset_meta_pb.num_segments(); }

    void set_num_segments(int64_t num_segments) { _rowset_meta_pb.set_num_segments(num_segments); }

    void to_rowset_pb(RowsetMetaPB* rs_meta_pb) const { *rs_meta_pb = _rowset_meta_pb; }

    RowsetMetaPB to_rowset_pb() {
        RowsetMetaPB meta_pb;
        to_rowset_pb(&meta_pb);
        return meta_pb;
    }

    bool is_singleton_delta() const {
        return has_version() && _rowset_meta_pb.start_version() == _rowset_meta_pb.end_version();
    }

    // Some time, we may check if this rowset is in rowset meta manager's meta by using RowsetMetaManager::check_rowset_meta.
    // But, this check behavior may cost a lot of time when it is frequent.
    // If we explicitly remove this rowset from rowset meta manager's meta, we can set _is_removed_from_rowset_meta to true,
    // And next time when we want to check if this rowset is in rowset mata manager's meta, we can
    // check is_remove_from_rowset_meta() first.
    void set_remove_from_rowset_meta() { _is_removed_from_rowset_meta = true; }

    bool is_remove_from_rowset_meta() const { return _is_removed_from_rowset_meta; }

    SegmentsOverlapPB segments_overlap() const { return _rowset_meta_pb.segments_overlap_pb(); }

    void set_segments_overlap(SegmentsOverlapPB segments_overlap) {
        _rowset_meta_pb.set_segments_overlap_pb(segments_overlap);
    }

    // return true if segments in this rowset has overlapping data.
    // this is not same as `segments_overlap()` method.
    // `segments_overlap()` only return the value of "segments_overlap" field in rowset meta,
    // but "segments_overlap" may be UNKNOWN.
    //
    // Returns true iff all of the following conditions are met
    // 1. the rowset contains more than one segment
    // 2. the rowset's start version == end version (non-singleton rowset was generated by compaction process
    //    which always produces non-overlapped segments)
    // 3. segments_overlap() flag is not NONOVERLAPPING (OVERLAP_UNKNOWN and OVERLAPPING are OK)
    bool is_segments_overlapping() const {
        return num_segments() > 1 && is_singleton_delta() && segments_overlap() != NONOVERLAPPING;
    }

    // get the compaction score of this rowset.
    // if segments are overlapping, the score equals to the number of segments,
    // otherwise, score is 1.
    uint32_t get_compaction_score() const {
        uint32_t score = 0;
        if (!is_segments_overlapping()) {
            score = 1;
        } else {
            score = num_segments();
            CHECK(score > 0);
        }
        return score;
    }

    int64_t mem_usage() const { return sizeof(RowsetMeta) + _rowset_meta_pb.SpaceUsedLong() - sizeof(_rowset_meta_pb); }

    uint32_t get_rowset_seg_id() const { return _rowset_meta_pb.rowset_seg_id(); }

    void set_rowset_seg_id(uint32_t id) { _rowset_meta_pb.set_rowset_seg_id(id); }

    uint32_t get_num_delete_files() const { return _rowset_meta_pb.num_delete_files(); }

    void set_num_delete_files(uint32_t num_delete_files) { _rowset_meta_pb.set_num_delete_files(num_delete_files); }

    const RowsetMetaPB& get_meta_pb() const { return _rowset_meta_pb; }

private:
    bool _deserialize_from_pb(const std::string_view& value) {
        return _rowset_meta_pb.ParseFromArray(value.data(), value.size());
    }

    bool _serialize_to_pb(std::string* value) {
        if (value == nullptr) {
            return false;
        }
        return _rowset_meta_pb.SerializeToString(value);
    }

    void _init() {
        if (_rowset_meta_pb.rowset_id() > 0) {
            _rowset_id.init(_rowset_meta_pb.rowset_id());
        } else {
            _rowset_id.init(_rowset_meta_pb.rowset_id_v2());
        }
    }

    friend bool operator==(const RowsetMeta& a, const RowsetMeta& b) {
        if (a._rowset_id != b._rowset_id) return false;
        if (a._is_removed_from_rowset_meta != b._is_removed_from_rowset_meta) return false;
        return google::protobuf::util::MessageDifferencer::Equals(a._rowset_meta_pb, b._rowset_meta_pb);
    }

    friend bool operator!=(const RowsetMeta& a, const RowsetMeta& b) { return !(a == b); }

    RowsetMetaPB _rowset_meta_pb;
    RowsetId _rowset_id;
    bool _is_removed_from_rowset_meta = false;
};

} // namespace starrocks

#endif // STARROCKS_BE_SRC_OLAP_ROWSET_ROWSET_META_H
