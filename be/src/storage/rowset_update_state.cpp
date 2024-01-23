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

#include "rowset_update_state.h"

#include "common/tracer.h"
#include "fs/fs_util.h"
#include "gutil/strings/substitute.h"
#include "serde/column_array_serde.h"
#include "storage/chunk_helper.h"
#include "storage/primary_key_encoder.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/rowset_options.h"
#include "storage/rowset/segment_rewriter.h"
#include "storage/tablet.h"
#include "storage/tablet_meta_manager.h"
#include "util/defer_op.h"
#include "util/phmap/phmap.h"
#include "util/stack_util.h"
#include "util/time.h"
#include "util/trace.h"

namespace starrocks {

RowsetUpdateState::RowsetUpdateState() = default;

RowsetUpdateState::~RowsetUpdateState() {
    if (!_status.ok()) {
        LOG(WARNING) << "bad RowsetUpdateState released tablet:" << _tablet_id;
    }
}

Status RowsetUpdateState::load(Tablet* tablet, Rowset* rowset) {
    if (UNLIKELY(!_status.ok())) {
        return _status;
    }
    std::call_once(_load_once_flag, [&] {
        _tablet_id = tablet->tablet_id();
        _status = _do_load(tablet, rowset);
        if (!_status.ok()) {
            LOG(WARNING) << "load RowsetUpdateState error: " << _status << " tablet:" << _tablet_id << " stack:\n"
                         << get_stack_trace();
            if (_status.is_mem_limit_exceeded()) {
                LOG(WARNING) << CurrentThread::mem_tracker()->debug_string();
            }
        }
    });
    return _status;
}

Status RowsetUpdateState::_load_deletes(Rowset* rowset, uint32_t idx, Column* pk_column) {
    DCHECK(_deletes.size() >= idx);
    // always one file for now.
    if (_deletes.size() == 0) {
        _deletes.resize(rowset->num_delete_files());
    }
    if (_deletes.size() == 0 || _deletes[idx] != nullptr) {
        return Status::OK();
    }

    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(rowset->rowset_path()));
    auto path = Rowset::segment_del_file_path(rowset->rowset_path(), rowset->rowset_id(), idx);
    ASSIGN_OR_RETURN(auto read_file, fs->new_random_access_file(path));
    ASSIGN_OR_RETURN(auto file_size, read_file->get_size());
    std::vector<uint8_t> read_buffer(file_size);
    RETURN_IF_ERROR(read_file->read_at_fully(0, read_buffer.data(), read_buffer.size()));
    auto col = pk_column->clone();
    if (serde::ColumnArraySerde::deserialize(read_buffer.data(), col.get()) == nullptr) {
        return Status::InternalError("column deserialization failed");
    }
    col->raw_data();
    _memory_usage += col != nullptr ? col->memory_usage() : 0;
    _deletes[idx] = std::move(col);
    return Status::OK();
}

Status RowsetUpdateState::_load_upserts(Rowset* rowset, uint32_t idx, Column* pk_column) {
    RowsetReleaseGuard guard(rowset->shared_from_this());
    DCHECK(_upserts.size() >= idx);
    if (_upserts.size() == 0) {
        _upserts.resize(rowset->num_segments());
    }
    if (_upserts.size() == 0 || _upserts[idx] != nullptr) {
        return Status::OK();
    }

    OlapReaderStatistics stats;
    auto& schema = rowset->schema();
    vector<uint32_t> pk_columns;
    for (size_t i = 0; i < schema.num_key_columns(); i++) {
        pk_columns.push_back((uint32_t)i);
    }
    Schema pkey_schema = ChunkHelper::convert_schema(schema, pk_columns);
    auto res = rowset->get_segment_iterators2(pkey_schema, nullptr, 0, &stats);
    if (!res.ok()) {
        return res.status();
    }
    auto& itrs = res.value();
    CHECK(itrs.size() == rowset->num_segments()) << "itrs.size != num_segments";

    // only hold pkey, so can use larger chunk size
    auto chunk_shared_ptr = ChunkHelper::new_chunk(pkey_schema, 4096);
    auto chunk = chunk_shared_ptr.get();
    auto& dest = _upserts[idx];
    auto col = pk_column->clone();
    auto itr = itrs[idx].get();
    if (itr != nullptr) {
        auto num_rows = rowset->segments()[idx]->num_rows();
        col->reserve(num_rows);
        while (true) {
            chunk->reset();
            auto st = itr->get_next(chunk);
            if (st.is_end_of_file()) {
                break;
            } else if (!st.ok()) {
                return st;
            } else {
                PrimaryKeyEncoder::encode(pkey_schema, *chunk, 0, chunk->num_rows(), col.get());
            }
        }
        CHECK(col->size() == num_rows) << "read segment: iter rows != num rows";
    }
    for (const auto& itr : itrs) {
        itr->close();
    }
    dest = std::move(col);
    // This is a little bit trick. If pk column is a binary column, we will call function `raw_data()` in the following
    // And the function `raw_data()` will build slice of pk column which will increase the memory usage of pk column
    // So we try build slice in advance in here to make sure the correctness of memory statistics
    dest->raw_data();
    _memory_usage += dest != nullptr ? dest->memory_usage() : 0;

    return Status::OK();
}

Status RowsetUpdateState::_do_load(Tablet* tablet, Rowset* rowset) {
    TRACE_COUNTER_SCOPE_LATENCY_US("rowset_update_state_load");
    auto span = Tracer::Instance().start_trace_txn_tablet("rowset_update_state_load", rowset->txn_id(),
                                                          tablet->tablet_id());
    _tablet_id = tablet->tablet_id();
    auto& schema = rowset->schema();
    vector<uint32_t> pk_columns;
    for (size_t i = 0; i < schema.num_key_columns(); i++) {
        pk_columns.push_back((uint32_t)i);
    }
    Schema pkey_schema = ChunkHelper::convert_schema(schema, pk_columns);
    std::unique_ptr<Column> pk_column;
    if (!PrimaryKeyEncoder::create_column(pkey_schema, &pk_column).ok()) {
        CHECK(false) << "create column for primary key encoder failed";
    }
    // if rowset is partial rowset, we need to load rowset totally because we don't support load multiple load
    // for partial update so far
    bool ignore_mem_limit = rowset->rowset_meta()->get_meta_pb().has_txn_meta() && rowset->num_segments() != 0;

    if (ignore_mem_limit) {
        for (size_t i = 0; i < rowset->num_delete_files(); i++) {
            RETURN_IF_ERROR(_load_deletes(rowset, i, pk_column.get()));
        }
        for (size_t i = 0; i < rowset->num_segments(); i++) {
            RETURN_IF_ERROR(_load_upserts(rowset, i, pk_column.get()));
        }
    } else {
        RETURN_IF_ERROR(_load_deletes(rowset, 0, pk_column.get()));
        RETURN_IF_ERROR(_load_upserts(rowset, 0, pk_column.get()));
    }

    if (!_check_partial_update(rowset)) {
        return Status::OK();
    }

    return _prepare_partial_update_states(tablet, rowset, 0, true);
}

Status RowsetUpdateState::load_deletes(Rowset* rowset, uint32_t idx) {
    auto& schema = rowset->schema();
    vector<uint32_t> pk_columns;
    for (size_t i = 0; i < schema.num_key_columns(); i++) {
        pk_columns.push_back((uint32_t)i);
    }
    Schema pkey_schema = ChunkHelper::convert_schema(schema, pk_columns);
    std::unique_ptr<Column> pk_column;
    if (!PrimaryKeyEncoder::create_column(pkey_schema, &pk_column).ok()) {
        CHECK(false) << "create column for primary key encoder failed";
    }
    return _load_deletes(rowset, idx, pk_column.get());
}

Status RowsetUpdateState::load_upserts(Rowset* rowset, uint32_t upsert_id) {
    auto& schema = rowset->schema();
    vector<uint32_t> pk_columns;
    for (size_t i = 0; i < schema.num_key_columns(); i++) {
        pk_columns.push_back((uint32_t)i);
    }
    Schema pkey_schema = ChunkHelper::convert_schema(schema, pk_columns);
    std::unique_ptr<Column> pk_column;
    if (!PrimaryKeyEncoder::create_column(pkey_schema, &pk_column, true).ok()) {
        CHECK(false) << "create column for primary key encoder failed";
    }
    return _load_upserts(rowset, upsert_id, pk_column.get());
}

void RowsetUpdateState::release_upserts(uint32_t idx) {
    if (idx >= _upserts.size()) {
        return;
    }
    if (_upserts[idx] != nullptr) {
        _memory_usage -= _upserts[idx]->memory_usage();
        _upserts[idx].reset();
    }
}

void RowsetUpdateState::release_deletes(uint32_t idx) {
    if (idx >= _deletes.size()) {
        return;
    }
    if (_deletes[idx] != nullptr) {
        _memory_usage -= _deletes[idx]->memory_usage();
        _deletes[idx].reset();
    }
}

struct RowidSortEntry {
    uint32_t rowid;
    uint32_t idx;
    RowidSortEntry(uint32_t rowid, uint32_t idx) : rowid(rowid), idx(idx) {}
    bool operator<(const RowidSortEntry& rhs) const { return rowid < rhs.rowid; }
};

// group rowids by rssid, and for each group sort by rowid, return as `rowids_by_rssid`
// num_default: return the number of rows that need to fill in default values
// idxes: reverse indexes to restore values from (rssid,rowid) order to rowid order
// i.e.
// input rowids: [
//    (0, 3),
//    (-1,-1),
//    (1, 3),
//    (0, 1),
//    (-1,-1),
//    (1, 2),
// ]
// output:
//   num_default: 2
//   rowids_by_rssid: {
//     0: [
//        1,
//        3
//     ],
//     1: [
//        2,
//        3
//	   ]
//   }
//   the read column values will be in this order: [default_value, (0,1), (0,3), (1,2), (1,3)]
//   the indexes used to convert read columns values to write order will be: [2, 0, 4, 1, 0, 3]
void RowsetUpdateState::plan_read_by_rssid(const vector<uint64_t>& rowids, size_t* num_default,
                                           std::map<uint32_t, std::vector<uint32_t>>* rowids_by_rssid,
                                           vector<uint32_t>* idxes) {
    uint32_t n = rowids.size();
    phmap::node_hash_map<uint32_t, vector<RowidSortEntry>> sort_entry_by_rssid;
    std::vector<uint32_t> defaults;
    for (uint32_t i = 0; i < n; i++) {
        uint64_t v = rowids[i];
        uint32_t rssid = v >> 32;
        if (rssid == (uint32_t)-1) {
            defaults.push_back(i);
        } else {
            uint32_t rowid = v & ROWID_MASK;
            sort_entry_by_rssid[rssid].emplace_back(rowid, i);
        }
    }
    *num_default = defaults.size();
    idxes->resize(rowids.size());
    size_t ridx = 0;
    if (defaults.size() > 0) {
        // set defaults idxes to 0
        for (uint32_t e : defaults) {
            (*idxes)[e] = ridx;
        }
        ridx++;
    }
    // construct rowids_by_rssid
    for (auto& e : sort_entry_by_rssid) {
        std::sort(e.second.begin(), e.second.end());
        rowids_by_rssid->emplace(e.first, vector<uint32_t>(e.second.size()));
    }
    // iterate rowids_by_rssid by rssid order
    for (auto& e : *rowids_by_rssid) {
        auto& sort_entries = sort_entry_by_rssid[e.first];
        for (uint32_t i = 0; i < sort_entries.size(); i++) {
            e.second[i] = sort_entries[i].rowid;
            (*idxes)[sort_entries[i].idx] = ridx;
            ridx++;
        }
    }
}

// Assume segment idx has been loaded and _upserts[idx] is not null
// The caller should make sure `load_upserts` has been called success before call this function
Status RowsetUpdateState::_prepare_partial_update_states(Tablet* tablet, Rowset* rowset, uint32_t idx, bool need_lock) {
    if (_partial_update_states.size() == 0) {
        _partial_update_states.resize(rowset->num_segments());
    }

    if (_partial_update_states[idx].inited == true) {
        return Status::OK();
    }

    int64_t t_start = MonotonicMillis();
    const auto& txn_meta = rowset->rowset_meta()->get_meta_pb().txn_meta();
    const auto& tablet_schema = tablet->tablet_schema();

    std::vector<uint32_t> update_column_ids(txn_meta.partial_update_column_ids().begin(),
                                            txn_meta.partial_update_column_ids().end());
    std::set<uint32_t> update_columns_set(update_column_ids.begin(), update_column_ids.end());

    std::vector<uint32_t> read_column_ids;
    for (uint32_t i = 0; i < tablet_schema.num_columns(); i++) {
        if (update_columns_set.find(i) == update_columns_set.end()) {
            read_column_ids.push_back(i);
        }
    }

    DCHECK(_upserts[idx] != nullptr);
    auto read_column_schema = ChunkHelper::convert_schema(tablet_schema, read_column_ids);
    std::vector<std::unique_ptr<Column>> read_columns(read_column_ids.size());

    _partial_update_states[idx].write_columns.resize(read_columns.size());
    _partial_update_states[idx].src_rss_rowids.resize(_upserts[idx]->size());
    for (uint32_t i = 0; i < read_columns.size(); ++i) {
        auto column = ChunkHelper::column_from_field(*read_column_schema.field(i).get());
        read_columns[i] = column->clone_empty();
        _partial_update_states[idx].write_columns[i] = column->clone_empty();
    }

    int64_t t_read_index = MonotonicMillis();
    if (need_lock) {
        RETURN_IF_ERROR(tablet->updates()->get_rss_rowids_by_pk(tablet, *_upserts[idx],
                                                                &(_partial_update_states[idx].read_version),
                                                                &(_partial_update_states[idx].src_rss_rowids)));
    } else {
        RETURN_IF_ERROR(tablet->updates()->get_rss_rowids_by_pk_unlock(tablet, *_upserts[idx],
                                                                       &(_partial_update_states[idx].read_version),
                                                                       &(_partial_update_states[idx].src_rss_rowids)));
    }

    int64_t t_read_values = MonotonicMillis();
    size_t total_rows = 0;
    // rows actually needed to be read, excluding rows with default values
    size_t num_default = 0;
    std::map<uint32_t, std::vector<uint32_t>> rowids_by_rssid;
    vector<uint32_t> idxes;
    plan_read_by_rssid(_partial_update_states[idx].src_rss_rowids, &num_default, &rowids_by_rssid, &idxes);
    total_rows += _partial_update_states[idx].src_rss_rowids.size();
    RETURN_IF_ERROR(tablet->updates()->get_column_values(read_column_ids,
                                                         _partial_update_states[idx].read_version.major(),
                                                         num_default > 0, rowids_by_rssid, &read_columns, nullptr));
    for (size_t col_idx = 0; col_idx < read_column_ids.size(); col_idx++) {
        _partial_update_states[idx].write_columns[col_idx]->append_selective(*read_columns[col_idx], idxes.data(), 0,
                                                                             idxes.size());
        _memory_usage += _partial_update_states[idx].write_columns[col_idx]->memory_usage();
    }
    int64_t t_end = MonotonicMillis();
    _partial_update_states[idx].update_byte_size();
    _partial_update_states[idx].inited = true;

    LOG(INFO) << strings::Substitute(
            "prepare PartialUpdateState tablet:$0 segment:$1 #row:$2(#non-default:$3) #column:$4 "
            "time:$5ms(index:$6/value:$7)",
            _tablet_id, idx, total_rows, total_rows - num_default, read_columns.size(), t_end - t_start,
            t_read_values - t_read_index, t_end - t_read_values);
    return Status::OK();
}

Status RowsetUpdateState::_prepare_auto_increment_partial_update_states(Tablet* tablet, Rowset* rowset, uint32_t idx,
                                                                        EditVersion latest_applied_version,
                                                                        const std::vector<uint32_t>& column_id) {
    if (_auto_increment_partial_update_states.size() == 0) {
        _auto_increment_partial_update_states.resize(rowset->num_segments());
    }
    DCHECK_EQ(column_id.size(), 1);
    const auto& rowset_meta_pb = rowset->rowset_meta()->get_meta_pb();
    auto read_column_schema = ChunkHelper::convert_schema(tablet->tablet_schema(), column_id);
    std::vector<std::unique_ptr<Column>> read_column;
    read_column.resize(1);

    std::shared_ptr<TabletSchema> schema = nullptr;
    if (!rowset_meta_pb.txn_meta().partial_update_column_ids().empty()) {
        std::vector<int32_t> update_column_ids(rowset_meta_pb.txn_meta().partial_update_column_ids().begin(),
                                               rowset_meta_pb.txn_meta().partial_update_column_ids().end());
        schema = TabletSchema::create(tablet->tablet_schema(), update_column_ids);
    }

    _auto_increment_partial_update_states[idx].init(
            rowset, schema != nullptr ? schema.get() : const_cast<TabletSchema*>(&tablet->tablet_schema()),
            rowset_meta_pb.txn_meta().auto_increment_partial_update_column_id(), idx);
    _auto_increment_partial_update_states[idx].src_rss_rowids.resize(_upserts[idx]->size());

    auto column = ChunkHelper::column_from_field(*read_column_schema.field(0).get());
    read_column[0] = column->clone_empty();
    _auto_increment_partial_update_states[idx].write_column = column->clone_empty();

    tablet->updates()->get_rss_rowids_by_pk_unlock(tablet, *_upserts[idx], nullptr,
                                                   &_auto_increment_partial_update_states[idx].src_rss_rowids);

    std::vector<uint32_t> rowids;
    uint32_t n = _auto_increment_partial_update_states[idx].src_rss_rowids.size();
    for (uint32_t i = 0; i < n; i++) {
        uint64_t v = _auto_increment_partial_update_states[idx].src_rss_rowids[i];
        uint32_t rssid = v >> 32;
        if (rssid == (uint32_t)-1) {
            rowids.emplace_back(i);
        }
    }
    std::swap(_auto_increment_partial_update_states[idx].rowids, rowids);

    size_t new_rows = 0;
    std::vector<uint32_t> idxes;
    std::map<uint32_t, std::vector<uint32_t>> rowids_by_rssid;
    plan_read_by_rssid(_auto_increment_partial_update_states[idx].src_rss_rowids, &new_rows, &rowids_by_rssid, &idxes);

    if (new_rows == n) {
        _auto_increment_partial_update_states[idx].skip_rewrite = true;
    }

    if (new_rows > 0) {
        uint32_t last = idxes.size() - new_rows;
        for (int i = 0; i < idxes.size(); ++i) {
            if (idxes[i] != 0) {
                --idxes[i];
            } else {
                idxes[i] = last;
                ++last;
            }
        }
    }

    RETURN_IF_ERROR(tablet->updates()->get_column_values(column_id, latest_applied_version.major(), new_rows > 0,
                                                         rowids_by_rssid, &read_column,
                                                         &_auto_increment_partial_update_states[idx]));

    _auto_increment_partial_update_states[idx].write_column->append_selective(*read_column[0], idxes.data(), 0,
                                                                              idxes.size());

    /*
     * Suppose we have auto increment ids for the rows which are not exist in the previous version.
     * The ids are allocated by system for partial update in this case. It is impossible that the ids
     * contain 0 in the normal case. But if the delete-partial update conflict happen with the previous transaction,
     * it is possible that the ids contain 0 in current transaction. So if we detect the 0, we should handle
     * this conflict case with deleting the row directly. This mechanism will cause some potential side effects as follow:
     *
     * 1. If the delete-partial update conflict happen, partial update operation maybe lost.
     * 2. If it is the streamload combine with the delete and partial update ops and manipulate on a row which has existed
     *    in the previous version, all the partial update ops after delete ops maybe lost for this row if they contained in
     *    different segment file.
    */
    _auto_increment_partial_update_states[idx].delete_pks = _upserts[idx]->clone_empty();
    std::vector<uint32_t> delete_idxes;
    const int64* data =
            reinterpret_cast<const int64*>(_auto_increment_partial_update_states[idx].write_column->raw_data());

    // just check the rows which are not exist in the previous version
    // because the rows exist in the previous version may contain 0 which are specified by the user
    for (int i = 0; i < _auto_increment_partial_update_states[idx].rowids.size(); ++i) {
        uint32_t row_idx = _auto_increment_partial_update_states[idx].rowids[i];
        if (data[row_idx] == 0) {
            delete_idxes.emplace_back(row_idx);
        }
    }

    if (delete_idxes.size() != 0) {
        _auto_increment_partial_update_states[idx].delete_pks->append_selective(*_upserts[idx], delete_idxes.data(), 0,
                                                                                delete_idxes.size());
    }

    return Status::OK();
}

bool RowsetUpdateState::_check_partial_update(Rowset* rowset) {
    if (!rowset->rowset_meta()->get_meta_pb().has_txn_meta() || rowset->num_segments() == 0) {
        return false;
    }
    // Merge condition and auto-increment-column-only partial update will also set txn_meta
    // but will not set partial_update_column_ids
    const auto& txn_meta = rowset->rowset_meta()->get_meta_pb().txn_meta();
    return !txn_meta.partial_update_column_ids().empty();
}

Status RowsetUpdateState::_check_and_resolve_conflict(Tablet* tablet, Rowset* rowset, uint32_t rowset_id,
                                                      uint32_t segment_id, EditVersion latest_applied_version,
                                                      std::vector<uint32_t>& read_column_ids,
                                                      const PrimaryIndex& index) {
    if (_partial_update_states.size() <= segment_id || !_partial_update_states[segment_id].inited) {
        std::string msg = strings::Substitute(
                "_check_and_reslove_conflict tablet:$0 rowset:$1 segment:$2 failed, partial_update_states size:$3",
                tablet->tablet_id(), rowset_id, segment_id, _partial_update_states.size());
        LOG(WARNING) << msg;
        return Status::InternalError(msg);
    }

    // _read_version is equal to latest_applied_version which means there is no other rowset is applied
    // the data of write_columns can be write to segment file directly
    VLOG(2) << "latest_applied_version is " << latest_applied_version.to_string() << " read version is "
            << _partial_update_states[segment_id].read_version.to_string();
    if (latest_applied_version == _partial_update_states[segment_id].read_version) {
        return Status::OK();
    }

    // check if there are delta column files generated from read_version to now.
    // If yes, then need to force resolve conflict.
    const bool need_resolve_conflict = tablet->updates()->check_delta_column_generate_from_version(
            _partial_update_states[segment_id].read_version);

    // get rss_rowids to identify conflict exist or not
    int64_t t_start = MonotonicMillis();
    std::vector<uint64_t> new_rss_rowids(_upserts[segment_id]->size());
    index.get(*_upserts[segment_id], &new_rss_rowids);
    int64_t t_read_index = MonotonicMillis();

    size_t total_conflicts = 0;
    uint32_t num_rows = new_rss_rowids.size();
    std::vector<uint32_t> conflict_idxes;
    std::vector<uint64_t> conflict_rowids;
    DCHECK_EQ(num_rows, _partial_update_states[segment_id].src_rss_rowids.size());
    for (size_t i = 0; i < new_rss_rowids.size(); ++i) {
        uint64_t new_rss_rowid = new_rss_rowids[i];
        uint32_t new_rssid = new_rss_rowid >> 32;
        uint64_t rss_rowid = _partial_update_states[segment_id].src_rss_rowids[i];
        uint32_t rssid = rss_rowid >> 32;

        if (rssid != new_rssid || need_resolve_conflict) {
            conflict_idxes.emplace_back(i);
            conflict_rowids.emplace_back(new_rss_rowid);
        }
    }
    if (!conflict_idxes.empty()) {
        total_conflicts += conflict_idxes.size();
        std::vector<std::unique_ptr<Column>> read_columns;
        read_columns.resize(_partial_update_states[segment_id].write_columns.size());
        for (uint32_t i = 0; i < read_columns.size(); ++i) {
            read_columns[i] = _partial_update_states[segment_id].write_columns[i]->clone_empty();
        }
        size_t num_default = 0;
        std::map<uint32_t, std::vector<uint32_t>> rowids_by_rssid;
        std::vector<uint32_t> read_idxes;
        plan_read_by_rssid(conflict_rowids, &num_default, &rowids_by_rssid, &read_idxes);
        DCHECK_EQ(conflict_idxes.size(), read_idxes.size());
        RETURN_IF_ERROR(tablet->updates()->get_column_values(read_column_ids, latest_applied_version.major(),
                                                             num_default > 0, rowids_by_rssid, &read_columns, nullptr));

        for (size_t col_idx = 0; col_idx < read_column_ids.size(); col_idx++) {
            std::unique_ptr<Column> new_write_column =
                    _partial_update_states[segment_id].write_columns[col_idx]->clone_empty();
            new_write_column->append_selective(*read_columns[col_idx], read_idxes.data(), 0, read_idxes.size());
            RETURN_IF_ERROR(_partial_update_states[segment_id].write_columns[col_idx]->update_rows(
                    *new_write_column, conflict_idxes.data()));
        }
    }
    int64_t t_end = MonotonicMillis();
    LOG(INFO) << strings::Substitute(
            "_check_and_resolve_conflict tablet:$0 rowset:$1 segmet:$2 version:($3 $4) #conflict-row:$5 #column:$6 "
            "time:$7ms(index:$8/value:$9)",
            tablet->tablet_id(), rowset_id, segment_id, _partial_update_states[segment_id].read_version.to_string(),
            latest_applied_version.to_string(), total_conflicts, read_column_ids.size(), t_end - t_start,
            t_read_index - t_start, t_end - t_read_index);

    return Status::OK();
}

Status RowsetUpdateState::apply(Tablet* tablet, Rowset* rowset, uint32_t rowset_id, uint32_t segment_id,
                                EditVersion latest_applied_version, const PrimaryIndex& index,
                                std::unique_ptr<Column>& delete_pks, int64_t* append_column_size) {
    const auto& rowset_meta_pb = rowset->rowset_meta()->get_meta_pb();
    if (!rowset_meta_pb.has_txn_meta() || rowset->num_segments() == 0) {
        return Status::OK();
    }
    const auto& txn_meta = rowset_meta_pb.txn_meta();
    // columns needs to be read from tablet's data
    std::vector<uint32_t> read_column_ids;
    // currently assume it's a partial update (explict for normal, implict for auto increment)
    if (!txn_meta.partial_update_column_ids().empty()) {
        const auto& tschema = tablet->tablet_schema();
        // columns supplied in rowset
        std::vector<uint32_t> update_colum_ids(txn_meta.partial_update_column_ids().begin(),
                                               txn_meta.partial_update_column_ids().end());
        std::set<uint32_t> update_columns_set(update_colum_ids.begin(), update_colum_ids.end());
        for (uint32_t i = 0; i < tschema.num_columns(); i++) {
            if (update_columns_set.find(i) == update_columns_set.end()) {
                read_column_ids.push_back(i);
            }
        }

        DCHECK(_upserts[segment_id] != nullptr);
        if (_partial_update_states.size() == 0 || !_partial_update_states[segment_id].inited) {
            RETURN_IF_ERROR(_prepare_partial_update_states(tablet, rowset, segment_id, false));
        } else {
            // reslove conflict of segment
            RETURN_IF_ERROR(_check_and_resolve_conflict(tablet, rowset, rowset_id, segment_id, latest_applied_version,
                                                        read_column_ids, index));
        }
    }

    if (txn_meta.has_auto_increment_partial_update_column_id()) {
        uint32_t id = 0;
        for (int i = 0; i < tablet->tablet_schema().num_columns(); ++i) {
            if (tablet->tablet_schema().column(i).is_auto_increment()) {
                id = i;
                break;
            }
        }
        std::vector<uint32_t> column_id(1, id);
        RETURN_IF_ERROR(_prepare_auto_increment_partial_update_states(tablet, rowset, segment_id,
                                                                      latest_applied_version, column_id));
    }

    auto src_path = Rowset::segment_file_path(tablet->schema_hash_path(), rowset->rowset_id(), segment_id);
    auto dest_path = Rowset::segment_temp_file_path(tablet->schema_hash_path(), rowset->rowset_id(), segment_id);
    DeferOp clean_temp_files([&] { FileSystem::Default()->delete_file(dest_path); });
    int64_t t_rewrite_start = MonotonicMillis();
    if (txn_meta.has_auto_increment_partial_update_column_id() &&
        !_auto_increment_partial_update_states[segment_id].skip_rewrite) {
        RETURN_IF_ERROR(SegmentRewriter::rewrite(
                src_path, dest_path, tablet->tablet_schema(), _auto_increment_partial_update_states[segment_id],
                read_column_ids,
                _partial_update_states.size() != 0 ? &_partial_update_states[segment_id].write_columns : nullptr));
    } else if (_partial_update_states.size() != 0) {
        FooterPointerPB partial_rowset_footer = txn_meta.partial_rowset_footers(segment_id);
        FileInfo file_info{.path = dest_path};
        RETURN_IF_ERROR(SegmentRewriter::rewrite(src_path, &file_info, tablet->tablet_schema(), read_column_ids,
                                                 _partial_update_states[segment_id].write_columns, segment_id,
                                                 partial_rowset_footer));
    }
    int64_t t_rewrite_end = MonotonicMillis();
    LOG(INFO) << strings::Substitute("apply partial segment tablet:$0 rowset:$1 seg:$2 #column:$3 #rewrite:$4ms",
                                     tablet->tablet_id(), rowset_id, segment_id, read_column_ids.size(),
                                     t_rewrite_end - t_rewrite_start);

    // we should reload segment after rewrite segment file because we may read data from the segment during
    // the subsequent apply process. And the segment will be treated as a full segment, so we must reload
    // segment[segment_id] of partial rowset
    FileSystem::Default()->rename_file(dest_path, src_path);
    RETURN_IF_ERROR(rowset->reload_segment(segment_id));

    if (!txn_meta.partial_update_column_ids().empty()) {
        for (size_t col_idx = 0; col_idx < _partial_update_states[segment_id].write_columns.size(); col_idx++) {
            if (_partial_update_states[segment_id].write_columns[col_idx] != nullptr) {
                _memory_usage -= _partial_update_states[segment_id].write_columns[col_idx]->memory_usage();
            }
        }
        *append_column_size += _partial_update_states[segment_id].byte_size;
        _partial_update_states[segment_id].release();
    }
    if (txn_meta.has_auto_increment_partial_update_column_id()) {
        if (_auto_increment_partial_update_states[segment_id].delete_pks->size() != 0) {
            delete_pks.swap(_auto_increment_partial_update_states[segment_id].delete_pks);
        }
        _auto_increment_partial_update_states[segment_id].release();
    }
    return Status::OK();
}

std::string RowsetUpdateState::to_string() const {
    return strings::Substitute("RowsetUpdateState tablet:$0", _tablet_id);
}

} // namespace starrocks
