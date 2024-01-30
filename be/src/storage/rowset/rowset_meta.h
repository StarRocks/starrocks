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

#pragma once

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
#include "storage/tablet_schema.h"
#include "storage/tablet_schema_map.h"

namespace starrocks {

class RowsetMeta;
using RowsetMetaSharedPtr = std::shared_ptr<RowsetMeta>;

class RowsetMeta {
public:
    RowsetMeta() = delete;

    explicit RowsetMeta(const RowsetMetaPB& rowset_meta_pb);
    explicit RowsetMeta(std::unique_ptr<RowsetMetaPB>& rowset_meta_pb);
    RowsetMeta(std::string_view pb_rowset_meta, bool* parse_ok);

    ~RowsetMeta();

    RowsetId rowset_id() const { return _rowset_id; }

    int64_t tablet_id() const { return _rowset_meta_pb->tablet_id(); }

    TabletUid tablet_uid() const { return _rowset_meta_pb->tablet_uid(); }

    int64_t txn_id() const { return _rowset_meta_pb->txn_id(); }

    int32_t tablet_schema_hash() const { return _rowset_meta_pb->tablet_schema_hash(); }

    RowsetStatePB rowset_state() const { return _rowset_meta_pb->rowset_state(); }

    void set_rowset_state(RowsetStatePB rowset_state) { _rowset_meta_pb->set_rowset_state(rowset_state); }

    Version version() const { return {_rowset_meta_pb->start_version(), _rowset_meta_pb->end_version()}; }

    void set_version(Version version) {
        _rowset_meta_pb->set_start_version(version.first);
        _rowset_meta_pb->set_end_version(version.second);
    }

    bool has_version() const { return _rowset_meta_pb->has_start_version() && _rowset_meta_pb->has_end_version(); }

    int64_t start_version() const { return _rowset_meta_pb->start_version(); }

    int64_t end_version() const { return _rowset_meta_pb->end_version(); }

    int64_t num_rows() const { return _rowset_meta_pb->num_rows(); }

    void set_num_rows(int64_t num_rows) { _rowset_meta_pb->set_num_rows(num_rows); }

    int64_t total_row_size() { return _rowset_meta_pb->total_row_size(); }

    void set_total_row_size(int64_t total_size) { _rowset_meta_pb->set_total_row_size(total_size); }

    int64_t total_update_row_size() { return _rowset_meta_pb->total_update_row_size(); }

    size_t total_disk_size() const { return _rowset_meta_pb->total_disk_size(); }

    void set_total_disk_size(size_t disk_size) { _rowset_meta_pb->set_total_disk_size(disk_size); }

    size_t data_disk_size() const { return _rowset_meta_pb->data_disk_size(); }

    void set_data_disk_size(size_t data_size) { _rowset_meta_pb->set_data_disk_size(data_size); }

    size_t index_disk_size() const { return _rowset_meta_pb->index_disk_size(); }

    void set_index_disk_size(int64_t index_disk_size) { _rowset_meta_pb->set_index_disk_size(index_disk_size); }

    bool has_delete_predicate() const { return _rowset_meta_pb->has_delete_predicate(); }

    const DeletePredicatePB& delete_predicate() const { return _rowset_meta_pb->delete_predicate(); }

    DeletePredicatePB* mutable_delete_predicate() { return _rowset_meta_pb->mutable_delete_predicate(); }

    void set_delete_predicate(const DeletePredicatePB& delete_predicate) {
        *_rowset_meta_pb->mutable_delete_predicate() = delete_predicate;
    }

    // return semgent_footer position and size if rowset is partial_rowset
    const FooterPointerPB* partial_rowset_footer(uint32_t segment_id) const {
        if (!_rowset_meta_pb->has_txn_meta() || _rowset_meta_pb->txn_meta().has_merge_condition() ||
            _rowset_meta_pb->txn_meta().has_auto_increment_partial_update_column_id() ||
            _rowset_meta_pb->num_update_files() > 0) {
            return nullptr;
        }
        return &_rowset_meta_pb->txn_meta().partial_rowset_footers(segment_id);
    }

    // for determining whether the rowset is in column partial update is whether it contains the .upt files
    bool is_column_mode_partial_update() const { return _rowset_meta_pb->num_update_files() > 0; }

    bool has_txn_meta() const { return _rowset_meta_pb->has_txn_meta(); }

    const RowsetTxnMetaPB& txn_meta() const { return _rowset_meta_pb->txn_meta(); }

    void clear_txn_meta() { _rowset_meta_pb->clear_txn_meta(); }

    bool empty() const { return _rowset_meta_pb->empty(); }

    PUniqueId load_id() const { return _rowset_meta_pb->load_id(); }

    int64_t creation_time() const { return _rowset_meta_pb->creation_time(); }

    void set_creation_time(int64_t creation_time) { return _rowset_meta_pb->set_creation_time(creation_time); }

    int64_t partition_id() const { return _rowset_meta_pb->partition_id(); }

    int64_t num_segments() const { return _rowset_meta_pb->num_segments(); }

    void set_num_segments(int64_t num_segments) { _rowset_meta_pb->set_num_segments(num_segments); }

    void set_empty(bool empty) { _rowset_meta_pb->set_empty(empty); }

    bool is_singleton_delta() const {
        return has_version() && _rowset_meta_pb->start_version() == _rowset_meta_pb->end_version();
    }

    // Some time, we may check if this rowset is in rowset meta manager's meta by using RowsetMetaManager::check_rowset_meta.
    // But, this check behavior may cost a lot of time when it is frequent.
    // If we explicitly remove this rowset from rowset meta manager's meta, we can set _is_removed_from_rowset_meta to true,
    // And next time when we want to check if this rowset is in rowset mata manager's meta, we can
    // check is_remove_from_rowset_meta() first.
    void set_remove_from_rowset_meta() { _is_removed_from_rowset_meta = true; }

    bool is_remove_from_rowset_meta() const { return _is_removed_from_rowset_meta; }

    SegmentsOverlapPB segments_overlap() const { return _rowset_meta_pb->segments_overlap_pb(); }

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

    int64_t mem_usage() const { return _mem_usage; }

    uint32_t get_rowset_seg_id() const { return _rowset_meta_pb->rowset_seg_id(); }

    void set_segments_overlap_pb(SegmentsOverlapPB overlap) {
        return _rowset_meta_pb->set_segments_overlap_pb(overlap);
    }

    void set_max_compact_input_rowset_id(uint32_t max_compact_input_rowset_id) {
        _rowset_meta_pb->set_max_compact_input_rowset_id(max_compact_input_rowset_id);
    }

    uint32_t max_compact_input_rowset_id() const { return _rowset_meta_pb->max_compact_input_rowset_id(); }
    bool has_max_compact_input_rowset_id() const { return _rowset_meta_pb->has_max_compact_input_rowset_id(); }

    void set_rowset_seg_id(uint32_t id) { _rowset_meta_pb->set_rowset_seg_id(id); }

    uint32_t get_num_delete_files() const { return _rowset_meta_pb->num_delete_files(); }

    uint32_t get_num_update_files() const { return _rowset_meta_pb->num_update_files(); }

    // rowset_meta_pb keep `tablet_schema_pb` right now and it will use more memory.
    // But it is not necessary always hold tablet schema pb in memory. The access frequency of
    // tablet_schema_pb is very low and it could be generated from `_schema` temporarily.
    // So we will remove `tablet_scheam_pb` from rowset_meta_pb in memory. But when we persistent
    // rowset_meta_pb to disk, we will generated a new full rowset_meta_pb.
    const RowsetMetaPB& get_meta_pb_without_schema() const { return *_rowset_meta_pb; }

    // This function will copy a new rowset_meta_pb with tablet_schema_pb.
    //
    // Most of the time when this function is called, it's during the persistence of `rowset_meta` or create a
    // new rowset.
    // Before calling it, please confirm if you need a complete `rowset_meta` that includes `tablet_schema_pb`.
    // If not, perhaps `get_meta_pb_without_schema()` is enough.
    void get_full_meta_pb(RowsetMetaPB* rs_meta_pb) const {
        *rs_meta_pb = *_rowset_meta_pb;
        rs_meta_pb->clear_tablet_schema();
        TabletSchemaPB* ts_pb = rs_meta_pb->mutable_tablet_schema();
        DCHECK(_schema != nullptr);
        _schema->to_schema_pb(ts_pb);
    }

    void get_tablet_schema_pb(TabletSchemaPB* tablet_schema_pb) {
        DCHECK(_schema != nullptr);
        _schema->to_schema_pb(tablet_schema_pb);
    }

    void set_tablet_schema(const TabletSchemaCSPtr& tablet_schema_ptr) {
        _rowset_meta_pb->clear_tablet_schema();
        TabletSchemaPB ts_pb;
        tablet_schema_ptr->to_schema_pb(&ts_pb);
        if (ts_pb.has_id() && ts_pb.id() != TabletSchema::invalid_id()) {
            _schema = GlobalTabletSchemaMap::Instance()->emplace(ts_pb).first;
        } else {
            _schema = TabletSchemaCSPtr(TabletSchema::copy(tablet_schema_ptr));
        }
        _has_tablet_schema_pb = true;
    }

    const TabletSchemaCSPtr tablet_schema() { return _schema; }

    void set_partial_schema_change(bool partial_schema_change) {
        _rowset_meta_pb->set_partial_schema_change(partial_schema_change);
    }

    bool partial_schema_change() { return _rowset_meta_pb->partial_schema_change(); }

    bool has_tablet_schema_pb() { return _has_tablet_schema_pb; }

private:
    bool _deserialize_from_pb(std::string_view value) {
        return _rowset_meta_pb->ParseFromArray(value.data(), value.size());
    }

    void _init() {
        if (_rowset_meta_pb->deprecated_rowset_id() > 0) {
            _rowset_id.init(_rowset_meta_pb->deprecated_rowset_id());
        } else {
            _rowset_id.init(_rowset_meta_pb->rowset_id());
        }

        if (_rowset_meta_pb->has_tablet_schema()) {
            if (_rowset_meta_pb->tablet_schema().has_id() &&
                _rowset_meta_pb->tablet_schema().id() != TabletSchema::invalid_id()) {
                _schema = GlobalTabletSchemaMap::Instance()->emplace(_rowset_meta_pb->tablet_schema()).first;
            } else {
                _schema = TabletSchema::create(_rowset_meta_pb->tablet_schema());
            }
        }
        _has_tablet_schema_pb = _rowset_meta_pb->has_tablet_schema();

        // clear does not release memory but only set it to default value, so we need to copy a new _rowset_meta_pb
        _rowset_meta_pb->clear_tablet_schema();
        std::unique_ptr<RowsetMetaPB> ptr = std::make_unique<RowsetMetaPB>(*_rowset_meta_pb);
        _rowset_meta_pb = std::move(ptr);
    }

    int64_t _calc_mem_usage() const {
        int64_t size = sizeof(RowsetMeta);
        if (_rowset_meta_pb != nullptr) {
            size += static_cast<int64_t>(_rowset_meta_pb->SpaceUsedLong());
        }
        return size;
    }

    friend bool operator==(const RowsetMeta& a, const RowsetMeta& b) {
        if (a._rowset_id != b._rowset_id) return false;
        if (a._is_removed_from_rowset_meta != b._is_removed_from_rowset_meta) return false;
        return google::protobuf::util::MessageDifferencer::Equals(*a._rowset_meta_pb, *b._rowset_meta_pb);
    }

    friend bool operator!=(const RowsetMeta& a, const RowsetMeta& b) { return !(a == b); }

    // RowsetMeta may be modifyed after create,
    // so it may be not inconsistent at construct and destruct using `_rowset_meta_pb->SpaceUsedLong`,
    // So we add one item to record the mem usage. This method will have a certain deviation,
    // but it can ensure that the statistical error will not accumulate.
    int64_t _mem_usage = 0;

    std::unique_ptr<RowsetMetaPB> _rowset_meta_pb;
    RowsetId _rowset_id;
    bool _is_removed_from_rowset_meta = false;
    TabletSchemaCSPtr _schema = nullptr;
    bool _has_tablet_schema_pb = false;
};

} // namespace starrocks
