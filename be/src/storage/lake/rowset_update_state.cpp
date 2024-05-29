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
#include "gutil/strings/substitute.h"
#include "serde/column_array_serde.h"
#include "storage/chunk_helper.h"
#include "storage/lake/location_provider.h"
#include "storage/lake/meta_file.h"
#include "storage/lake/rowset.h"
#include "storage/lake/update_manager.h"
#include "storage/primary_key_encoder.h"
#include "storage/rowset/segment_rewriter.h"
#include "storage/tablet_schema.h"
#include "util/defer_op.h"
#include "util/phmap/phmap.h"
#include "util/stack_util.h"
#include "util/time.h"
#include "util/trace.h"

namespace starrocks::lake {

RowsetUpdateState::RowsetUpdateState() = default;

RowsetUpdateState::~RowsetUpdateState() = default;

// Restore state to what it was before the data was loaded
void RowsetUpdateState::_reset() {
    _upserts.clear();
    _deletes.clear();
    _partial_update_states.clear();
    _auto_increment_partial_update_states.clear();
    _auto_increment_delete_pks.clear();
    _memory_usage = 0;
    _base_versions.clear();
    _schema_version = 0;
    _segment_iters.clear();
    _rowset_ptr.reset();
}

void RowsetUpdateState::init(const RowsetUpdateStateParams& params) {
    DCHECK_GT(params.metadata.version(), 0);
    DCHECK_EQ(params.tablet->id(), params.metadata.id());
    if (!_base_versions.empty() && _schema_version < params.metadata.schema().schema_version()) {
        LOG(INFO) << "schema version has changed from " << _schema_version << " to "
                  << params.metadata.schema().schema_version() << ", need to reload the update state."
                  << " tablet_id: " << params.tablet->id() << " old base version: " << _base_versions[0]
                  << " new base version: " << params.metadata.version();
        // The data has been loaded, but the schema has changed and needs to be reloaded according to the new schema
        _reset();
    }
    _tablet_id = params.metadata.id();
    _schema_version = params.metadata.schema().schema_version();
}

static bool has_partial_update_state(const RowsetUpdateStateParams& params) {
    return !params.op_write.txn_meta().partial_update_column_unique_ids().empty();
}

static bool has_auto_increment_partial_update_state(const RowsetUpdateStateParams& params) {
    return params.op_write.txn_meta().has_auto_increment_partial_update_column_id();
}

Status RowsetUpdateState::load_segment(uint32_t segment_id, const RowsetUpdateStateParams& params, int64_t base_version,
                                       bool need_resolve_conflict, bool need_lock) {
    TRACE_COUNTER_SCOPE_LATENCY_US("load_segment_us");
    if (_rowset_ptr == nullptr) {
        _rowset_ptr = std::make_unique<Rowset>(params.tablet->tablet_mgr(), params.tablet->id(),
                                               &params.op_write.rowset(), -1 /*unused*/, params.tablet_schema);
    }
    _upserts.resize(_rowset_ptr->num_segments());
    _base_versions.resize(_rowset_ptr->num_segments());
    _partial_update_states.resize(_rowset_ptr->num_segments());
    _auto_increment_partial_update_states.resize(_rowset_ptr->num_segments());
    _auto_increment_delete_pks.resize(_rowset_ptr->num_segments());

    if (_upserts.size() == 0) {
        // Empty rowset
        return Status::OK();
    }
    if (_upserts[segment_id] == nullptr) {
        _base_versions[segment_id] = base_version;
        RETURN_IF_ERROR(_do_load_upserts(segment_id, params));
    }

    if (!params.op_write.has_txn_meta() || params.op_write.txn_meta().has_merge_condition()) {
        return Status::OK();
    }
    if (has_partial_update_state(params)) {
        if (_partial_update_states[segment_id].src_rss_rowids.empty()) {
            RETURN_IF_ERROR(_prepare_partial_update_states(segment_id, params, need_lock));
        }
    }
    if (has_auto_increment_partial_update_state(params)) {
        if (_auto_increment_partial_update_states[segment_id].src_rss_rowids.empty()) {
            RETURN_IF_ERROR(_prepare_auto_increment_partial_update_states(segment_id, params, need_lock));
        }
    }
    if (need_resolve_conflict) {
        RETURN_IF_ERROR(_resolve_conflict(segment_id, params, base_version));
    }
    return Status::OK();
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
void RowsetUpdateState::plan_read_by_rssid(const std::vector<uint64_t>& rowids, size_t* num_default,
                                           std::map<uint32_t, std::vector<uint32_t>>* rowids_by_rssid,
                                           std::vector<uint32_t>* idxes) {
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

Status RowsetUpdateState::_do_load_upserts(uint32_t segment_id, const RowsetUpdateStateParams& params) {
    vector<uint32_t> pk_columns;
    for (size_t i = 0; i < params.tablet_schema->num_key_columns(); i++) {
        pk_columns.push_back((uint32_t)i);
    }
    Schema pkey_schema = ChunkHelper::convert_schema(params.tablet_schema, pk_columns);
    std::unique_ptr<Column> pk_column;
    RETURN_IF_ERROR(PrimaryKeyEncoder::create_column(pkey_schema, &pk_column));

    if (_segment_iters.empty()) {
        ASSIGN_OR_RETURN(_segment_iters, _rowset_ptr->get_each_segment_iterator(pkey_schema, &_stats));
    }
    RETURN_ERROR_IF_FALSE(_segment_iters.size() == _rowset_ptr->num_segments());
    // only hold pkey, so can use larger chunk size
    auto chunk_shared_ptr = ChunkHelper::new_chunk(pkey_schema, 4096);
    auto chunk = chunk_shared_ptr.get();

    auto itr = _segment_iters[segment_id].get();
    auto& dest = _upserts[segment_id];
    auto col = pk_column->clone();
    if (itr != nullptr) {
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
        itr->close();
    }
    dest = std::move(col);
    dest->raw_data();
    _memory_usage += dest->memory_usage();

    return Status::OK();
}

// Return the id, i.e, position index, of unmodified columns in the |tablet_schema|
static std::vector<ColumnId> get_read_columns_ids(const TxnLogPB_OpWrite& op_write,
                                                  const TabletSchemaCSPtr& tablet_schema) {
    const auto& txn_meta = op_write.txn_meta();

    std::set<ColumnUID> modified_column_unique_ids(txn_meta.partial_update_column_unique_ids().begin(),
                                                   txn_meta.partial_update_column_unique_ids().end());

    std::vector<ColumnId> unmodified_column_ids;
    for (uint32_t i = 0, num_cols = tablet_schema->num_columns(); i < num_cols; i++) {
        auto uid = tablet_schema->column(i).unique_id();
        if (modified_column_unique_ids.count(uid) == 0) {
            unmodified_column_ids.push_back(i);
        }
    }

    return unmodified_column_ids;
}

Status RowsetUpdateState::_prepare_auto_increment_partial_update_states(uint32_t segment_id,
                                                                        const RowsetUpdateStateParams& params,
                                                                        bool need_lock) {
    const auto& txn_meta = params.op_write.txn_meta();

    uint32_t auto_increment_column_id = 0;
    for (int i = 0; i < params.tablet_schema->num_columns(); ++i) {
        if (params.tablet_schema->column(i).is_auto_increment()) {
            auto_increment_column_id = i;
            break;
        }
    }
    std::vector<uint32_t> column_id{auto_increment_column_id};
    auto auto_inc_column_schema = ChunkHelper::convert_schema(params.tablet_schema, column_id);
    auto column = ChunkHelper::column_from_field(*auto_inc_column_schema.field(0).get());
    std::vector<std::unique_ptr<Column>> read_column;

    std::shared_ptr<TabletSchema> modified_columns_schema = nullptr;
    if (has_partial_update_state(params)) {
        std::vector<ColumnUID> update_column_ids(txn_meta.partial_update_column_unique_ids().begin(),
                                                 txn_meta.partial_update_column_unique_ids().end());
        modified_columns_schema = TabletSchema::create_with_uid(params.tablet_schema, update_column_ids);
    } else {
        std::vector<int32_t> all_column_ids;
        all_column_ids.resize(params.tablet_schema->num_columns());
        std::iota(all_column_ids.begin(), all_column_ids.end(), 0);
        modified_columns_schema = TabletSchema::create(params.tablet_schema, all_column_ids);
    }

    _auto_increment_partial_update_states[segment_id].init(
            modified_columns_schema, txn_meta.auto_increment_partial_update_column_id(), segment_id);
    _auto_increment_partial_update_states[segment_id].src_rss_rowids.resize(_upserts[segment_id]->size());
    read_column.resize(1);
    read_column[0] = column->clone_empty();
    _auto_increment_partial_update_states[segment_id].write_column = column->clone_empty();

    // use upserts to get rowids in this segment
    RETURN_IF_ERROR(params.tablet->update_mgr()->get_rowids_from_pkindex(
            params.tablet->id(), _base_versions[segment_id], _upserts[segment_id],
            &(_auto_increment_partial_update_states[segment_id].src_rss_rowids), need_lock));

    std::vector<uint32_t> rowids;
    uint32_t n = _auto_increment_partial_update_states[segment_id].src_rss_rowids.size();
    for (uint32_t j = 0; j < n; j++) {
        uint64_t v = _auto_increment_partial_update_states[segment_id].src_rss_rowids[j];
        uint32_t rssid = v >> 32;
        if (rssid == (uint32_t)-1) {
            rowids.emplace_back(j);
        }
    }
    std::swap(_auto_increment_partial_update_states[segment_id].rowids, rowids);

    size_t new_rows = 0;
    std::vector<uint32_t> idxes;
    std::map<uint32_t, std::vector<uint32_t>> rowids_by_rssid;
    plan_read_by_rssid(_auto_increment_partial_update_states[segment_id].src_rss_rowids, &new_rows, &rowids_by_rssid,
                       &idxes);

    if (new_rows == n) {
        _auto_increment_partial_update_states[segment_id].skip_rewrite = true;
    }

    if (new_rows > 0) {
        uint32_t last = idxes.size() - new_rows;
        for (unsigned int& idx : idxes) {
            if (idx != 0) {
                --idx;
            } else {
                idx = last;
                ++last;
            }
        }
    }

    RETURN_IF_ERROR(params.tablet->update_mgr()->get_column_values(params, column_id, new_rows > 0, rowids_by_rssid,
                                                                   &read_column,
                                                                   &_auto_increment_partial_update_states[segment_id]));

    _auto_increment_partial_update_states[segment_id].write_column->append_selective(*read_column[0], idxes.data(), 0,
                                                                                     idxes.size());
    _memory_usage += _auto_increment_partial_update_states[segment_id].write_column->memory_usage();
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
    _auto_increment_delete_pks[segment_id].reset();
    _auto_increment_delete_pks[segment_id] = _upserts[segment_id]->clone_empty();
    std::vector<uint32_t> delete_idxes;
    const int64* data =
            reinterpret_cast<const int64*>(_auto_increment_partial_update_states[segment_id].write_column->raw_data());

    // just check the rows which are not exist in the previous version
    // because the rows exist in the previous version may contain 0 which are specified by the user
    for (unsigned int row_idx : _auto_increment_partial_update_states[segment_id].rowids) {
        if (data[row_idx] == 0) {
            delete_idxes.emplace_back(row_idx);
        }
    }

    if (delete_idxes.size() != 0) {
        _auto_increment_delete_pks[segment_id]->append_selective(*_upserts[segment_id], delete_idxes.data(), 0,
                                                                 delete_idxes.size());
        _memory_usage += _auto_increment_delete_pks[segment_id]->memory_usage();
    }
    return Status::OK();
}

Status RowsetUpdateState::_prepare_partial_update_states(uint32_t segment_id, const RowsetUpdateStateParams& params,
                                                         bool need_lock) {
    std::vector<ColumnId> read_column_ids = get_read_columns_ids(params.op_write, params.tablet_schema);

    auto read_column_schema = ChunkHelper::convert_schema(params.tablet_schema, read_column_ids);
    // column list that need to read from source segment
    std::vector<std::unique_ptr<Column>> read_columns;
    read_columns.resize(read_column_ids.size());
    _partial_update_states[segment_id].write_columns.resize(read_columns.size());
    _partial_update_states[segment_id].src_rss_rowids.resize(_upserts[segment_id]->size());
    for (uint32_t j = 0; j < read_columns.size(); ++j) {
        auto column = ChunkHelper::column_from_field(*read_column_schema.field(j).get());
        read_columns[j] = column->clone_empty();
        _partial_update_states[segment_id].write_columns[j] = column->clone_empty();
    }

    // use upsert to get rowids for this segment
    RETURN_IF_ERROR(params.tablet->update_mgr()->get_rowids_from_pkindex(
            params.tablet->id(), _base_versions[segment_id], _upserts[segment_id],
            &(_partial_update_states[segment_id].src_rss_rowids), need_lock));

    size_t num_default = 0;
    std::map<uint32_t, std::vector<uint32_t>> rowids_by_rssid;
    vector<uint32_t> idxes;
    plan_read_by_rssid(_partial_update_states[segment_id].src_rss_rowids, &num_default, &rowids_by_rssid, &idxes);
    size_t total_rows = _partial_update_states[segment_id].src_rss_rowids.size();
    // get column values by rowid, also get default values if needed
    RETURN_IF_ERROR(params.tablet->update_mgr()->get_column_values(params, read_column_ids, num_default > 0,
                                                                   rowids_by_rssid, &read_columns));
    for (size_t col_idx = 0; col_idx < read_column_ids.size(); col_idx++) {
        _partial_update_states[segment_id].write_columns[col_idx]->append_selective(*read_columns[col_idx],
                                                                                    idxes.data(), 0, idxes.size());
        _memory_usage += _partial_update_states[segment_id].write_columns[col_idx]->memory_usage();
    }
    TRACE_COUNTER_INCREMENT("partial_upt_total_rows", total_rows);
    TRACE_COUNTER_INCREMENT("partial_upt_default_rows", num_default);

    return Status::OK();
}

StatusOr<bool> RowsetUpdateState::file_exist(const std::string& full_path) {
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(full_path));
    auto st = fs->path_exists(full_path);
    if (st.ok()) {
        return true;
    } else if (st.is_not_found()) {
        return false;
    } else {
        return st;
    }
}

Status RowsetUpdateState::rewrite_segment(uint32_t segment_id, const RowsetUpdateStateParams& params,
                                          std::map<int, FileInfo>* replace_segments,
                                          std::vector<std::string>* orphan_files) {
    TRACE_COUNTER_SCOPE_LATENCY_US("rewrite_segment_latency_us");
    const RowsetMetadata& rowset_meta = params.op_write.rowset();
    auto root_path = params.tablet->metadata_root_location();
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(root_path));
    std::shared_ptr<TabletSchema> tablet_schema = std::make_shared<TabletSchema>(params.metadata.schema());
    // get rowset schema
    if (!params.op_write.has_txn_meta() || params.op_write.rewrite_segments_size() == 0 ||
        rowset_meta.num_rows() == 0 || params.op_write.txn_meta().has_merge_condition()) {
        return Status::OK();
    }
    RETURN_ERROR_IF_FALSE(params.op_write.rewrite_segments_size() == rowset_meta.segments_size());
    // currently assume it's a partial update
    const auto& txn_meta = params.op_write.txn_meta();
    std::vector<ColumnId> unmodified_column_ids = get_read_columns_ids(params.op_write, params.tablet_schema);

    bool has_auto_increment_col = false;
    for (uint32_t i = 0; i < params.tablet_schema->num_columns(); i++) {
        if (params.tablet_schema->column(i).is_auto_increment()) {
            has_auto_increment_col = true;
        }
    }
    // Correct the unmodified_column_ids if there is a auto increment column in the schema
    // This is a very special scene which we only partial update the auto increment column itself
    // but not other columns.
    // In this case, txn_meta.partial_update_column_unique_ids() will be always empty and we
    // will get unmodified_column_ids is full schema which is wrong.
    // To solve it, we can simply clear the unmodified_column_ids.
    if (has_auto_increment_col && unmodified_column_ids.size() == params.tablet_schema->num_columns() &&
        !has_partial_update_state(params)) {
        unmodified_column_ids.clear();
    }

    bool need_rename = true;
    const auto& src_path = rowset_meta.segments(segment_id);
    const auto& dest_path = params.op_write.rewrite_segments(segment_id);
    DCHECK(src_path != dest_path);

    bool skip_because_file_exist = false;
    int64_t t_rewrite_start = MonotonicMillis();
    if (has_auto_increment_partial_update_state(params) &&
        !_auto_increment_partial_update_states[segment_id].skip_rewrite) {
        FileInfo file_info{.path = params.tablet->segment_location(dest_path)};
        ASSIGN_OR_RETURN(bool skip_rewrite, file_exist(file_info.path));
        if (!skip_rewrite) {
            RETURN_IF_ERROR(SegmentRewriter::rewrite(
                    &file_info, params.tablet_schema, _auto_increment_partial_update_states[segment_id],
                    unmodified_column_ids,
                    has_partial_update_state(params) ? &_partial_update_states[segment_id].write_columns : nullptr,
                    params.op_write, params.tablet));
        } else {
            skip_because_file_exist = true;
        }
        file_info.path = dest_path;
        (*replace_segments)[segment_id] = file_info;
    } else if (has_partial_update_state(params)) {
        const FooterPointerPB& partial_rowset_footer = txn_meta.partial_rowset_footers(segment_id);
        FileInfo file_info{.path = params.tablet->segment_location(dest_path)};
        // if rewrite fail, let segment gc to clean dest segment file
        ASSIGN_OR_RETURN(bool skip_rewrite, file_exist(file_info.path));
        if (!skip_rewrite) {
            RETURN_IF_ERROR(SegmentRewriter::rewrite(
                    params.tablet->segment_location(src_path), &file_info, params.tablet_schema, unmodified_column_ids,
                    _partial_update_states[segment_id].write_columns, segment_id, partial_rowset_footer));
        } else {
            skip_because_file_exist = true;
        }
        file_info.path = dest_path;
        (*replace_segments)[segment_id] = file_info;
    } else {
        need_rename = false;
    }
    int64_t t_rewrite_end = MonotonicMillis();
    LOG(INFO) << strings::Substitute(
            "lake apply partial segment tablet:$0 rowset:$1 seg:$2 #column:$3 #rewrite:$4ms [$5 -> $6] "
            "skip_because_file_exist:$7",
            params.tablet->id(), rowset_meta.id(), segment_id, unmodified_column_ids.size(),
            t_rewrite_end - t_rewrite_start, src_path, dest_path, skip_because_file_exist);

    // rename segment file
    if (need_rename) {
        // after rename, add old segment to orphan files, for gc later.
        orphan_files->push_back(rowset_meta.segments(segment_id));
    }
    TRACE("end rewrite segment");
    return Status::OK();
}

Status RowsetUpdateState::_resolve_conflict(uint32_t segment_id, const RowsetUpdateStateParams& params,
                                            int64_t base_version) {
    // There are two cases that we must resolve conflict here:
    // 1. Current transaction's base version isn't equal latest base version, which means that conflict happens.
    // 2. We use batch publish here. This transaction may conflict with a transaction in the same batch.
    if (base_version == _base_versions[segment_id] && base_version + 1 == params.metadata.version()) {
        return Status::OK();
    }
    _base_versions[segment_id] = base_version;
    TRACE_COUNTER_SCOPE_LATENCY_US("resolve_conflict_latency_us");
    // skip resolve conflict when not partial update happen.
    if (!params.op_write.has_txn_meta() || params.op_write.rowset().segments_size() == 0 ||
        params.op_write.txn_meta().has_merge_condition()) {
        return Status::OK();
    }

    // use upserts to get rowids in this segment
    std::vector<uint64_t> new_rss_rowids(_upserts[segment_id]->size());
    RETURN_IF_ERROR(params.tablet->update_mgr()->get_rowids_from_pkindex(
            params.tablet->id(), _base_versions[segment_id], _upserts[segment_id], &new_rss_rowids, false));

    size_t total_conflicts = 0;
    std::shared_ptr<TabletSchema> tablet_schema = std::make_shared<TabletSchema>(params.metadata.schema());
    std::vector<ColumnId> read_column_ids = get_read_columns_ids(params.op_write, params.tablet_schema);
    // get rss_rowids to identify conflict exist or not
    int64_t t_start = MonotonicMillis();

    // reslove normal partial update
    if (has_partial_update_state(params)) {
        RETURN_IF_ERROR(
                _resolve_conflict_partial_update(params, new_rss_rowids, read_column_ids, segment_id, total_conflicts));
    }

    // reslove auto increment
    if (has_auto_increment_partial_update_state(params)) {
        RETURN_IF_ERROR(_resolve_conflict_auto_increment(params, new_rss_rowids, segment_id, total_conflicts));
    }
    int64_t t_end = MonotonicMillis();
    LOG(INFO) << strings::Substitute(
            "lake resolve_conflict tablet:$0 base_version:$1 #conflict-row:$2 "
            "#column:$3 time:$4ms",
            params.tablet->id(), _base_versions[segment_id], total_conflicts, read_column_ids.size(), t_end - t_start);

    return Status::OK();
}

Status RowsetUpdateState::_resolve_conflict_partial_update(const RowsetUpdateStateParams& params,
                                                           const std::vector<uint64_t>& new_rss_rowids,
                                                           std::vector<uint32_t>& read_column_ids, uint32_t segment_id,
                                                           size_t& total_conflicts) {
    uint32_t num_rows = new_rss_rowids.size();
    std::vector<uint32_t> conflict_idxes;
    std::vector<uint64_t> conflict_rowids;
    DCHECK_EQ(num_rows, _partial_update_states[segment_id].src_rss_rowids.size());
    for (size_t i = 0; i < new_rss_rowids.size(); ++i) {
        uint64_t new_rss_rowid = new_rss_rowids[i];
        uint32_t new_rssid = new_rss_rowid >> 32;
        uint64_t rss_rowid = _partial_update_states[segment_id].src_rss_rowids[i];
        uint32_t rssid = rss_rowid >> 32;

        if (rssid != new_rssid) {
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
        RETURN_IF_ERROR(params.tablet->update_mgr()->get_column_values(params, read_column_ids, num_default > 0,
                                                                       rowids_by_rssid, &read_columns));

        for (size_t col_idx = 0; col_idx < read_column_ids.size(); col_idx++) {
            std::unique_ptr<Column> new_write_column =
                    _partial_update_states[segment_id].write_columns[col_idx]->clone_empty();
            new_write_column->append_selective(*read_columns[col_idx], read_idxes.data(), 0, read_idxes.size());
            RETURN_IF_EXCEPTION(_partial_update_states[segment_id].write_columns[col_idx]->update_rows(
                    *new_write_column, conflict_idxes.data()));
        }
    }

    return Status::OK();
}

Status RowsetUpdateState::_resolve_conflict_auto_increment(const RowsetUpdateStateParams& params,
                                                           const std::vector<uint64_t>& new_rss_rowids,
                                                           uint32_t segment_id, size_t& total_conflicts) {
    uint32_t num_rows = new_rss_rowids.size();
    std::vector<uint32_t> conflict_idxes;
    std::vector<uint64_t> conflict_rowids;
    DCHECK_EQ(num_rows, _auto_increment_partial_update_states[segment_id].src_rss_rowids.size());
    for (size_t i = 0; i < new_rss_rowids.size(); ++i) {
        uint64_t new_rss_rowid = new_rss_rowids[i];
        uint32_t new_rssid = new_rss_rowid >> 32;
        uint64_t rss_rowid = _auto_increment_partial_update_states[segment_id].src_rss_rowids[i];
        uint32_t rssid = rss_rowid >> 32;

        if (rssid != new_rssid) {
            conflict_idxes.emplace_back(i);
            conflict_rowids.emplace_back(new_rss_rowid);
        }
    }
    if (!conflict_idxes.empty()) {
        total_conflicts += conflict_idxes.size();
        // in conflict case, rewrite segment must be needed
        _auto_increment_partial_update_states[segment_id].skip_rewrite = false;
        _auto_increment_partial_update_states[segment_id].src_rss_rowids = new_rss_rowids;

        std::vector<uint32_t> rowids;
        uint32_t n = _auto_increment_partial_update_states[segment_id].src_rss_rowids.size();
        for (uint32_t j = 0; j < n; j++) {
            uint64_t v = _auto_increment_partial_update_states[segment_id].src_rss_rowids[j];
            uint32_t rssid = v >> 32;
            if (rssid == (uint32_t)-1) {
                rowids.emplace_back(j);
            }
        }
        std::swap(_auto_increment_partial_update_states[segment_id].rowids, rowids);

        size_t new_rows = 0;
        std::vector<uint32_t> idxes;
        std::map<uint32_t, std::vector<uint32_t>> rowids_by_rssid;
        plan_read_by_rssid(conflict_rowids, &new_rows, &rowids_by_rssid, &idxes);

        if (new_rows > 0) {
            uint32_t last = idxes.size() - new_rows;
            for (unsigned int& idx : idxes) {
                if (idx != 0) {
                    --idx;
                } else {
                    idx = last;
                    ++last;
                }
            }
        }

        uint32_t auto_increment_column_id = 0;
        for (int i = 0; i < params.tablet_schema->num_columns(); ++i) {
            if (params.tablet_schema->column(i).is_auto_increment()) {
                auto_increment_column_id = i;
                break;
            }
        }
        std::vector<uint32_t> column_id{auto_increment_column_id};
        std::vector<std::unique_ptr<Column>> auto_increment_read_column;
        auto_increment_read_column.resize(1);
        auto_increment_read_column[0] = _auto_increment_partial_update_states[segment_id].write_column->clone_empty();
        RETURN_IF_ERROR(params.tablet->update_mgr()->get_column_values(
                params, column_id, new_rows > 0, rowids_by_rssid, &auto_increment_read_column,
                &_auto_increment_partial_update_states[segment_id]));

        std::unique_ptr<Column> new_write_column =
                _auto_increment_partial_update_states[segment_id].write_column->clone_empty();
        new_write_column->append_selective(*auto_increment_read_column[0], idxes.data(), 0, idxes.size());
        RETURN_IF_EXCEPTION(_auto_increment_partial_update_states[segment_id].write_column->update_rows(
                *new_write_column, conflict_idxes.data()));

        // reslove delete-partial update conflict base on latest column values
        _auto_increment_delete_pks[segment_id].reset();
        _auto_increment_delete_pks[segment_id] = _upserts[segment_id]->clone_empty();
        std::vector<uint32_t> delete_idxes;
        const int64* data = reinterpret_cast<const int64*>(
                _auto_increment_partial_update_states[segment_id].write_column->raw_data());

        // just check the rows which are not exist in the previous version
        // because the rows exist in the previous version may contain 0 which are specified by the user
        for (unsigned int row_idx : _auto_increment_partial_update_states[segment_id].rowids) {
            if (data[row_idx] == 0) {
                delete_idxes.emplace_back(row_idx);
            }
        }

        if (delete_idxes.size() != 0) {
            _auto_increment_delete_pks[segment_id]->append_selective(*_upserts[segment_id], delete_idxes.data(), 0,
                                                                     delete_idxes.size());
        }
    }
    return Status::OK();
}

void RowsetUpdateState::release_segment(uint32_t segment_id) {
    _memory_usage -= _upserts[segment_id] ? _upserts[segment_id]->memory_usage() : 0;
    _upserts[segment_id].reset();
    _memory_usage -= _partial_update_states[segment_id].memory_usage();
    _partial_update_states[segment_id].reset();
    _memory_usage -= _auto_increment_partial_update_states[segment_id].memory_usage();
    _auto_increment_partial_update_states[segment_id].reset();
    _memory_usage -=
            _auto_increment_delete_pks[segment_id] ? _auto_increment_delete_pks[segment_id]->memory_usage() : 0;
    _auto_increment_delete_pks[segment_id].reset();
}

Status RowsetUpdateState::load_delete(uint32_t del_id, const RowsetUpdateStateParams& params) {
    // always one file for now.
    TRACE_COUNTER_SCOPE_LATENCY_US("load_delete_us");
    _deletes.resize(params.op_write.dels_size());
    if (_deletes[del_id] != nullptr) {
        // Already load.
        return Status::OK();
    }
    vector<uint32_t> pk_columns;
    for (size_t i = 0; i < params.tablet_schema->num_key_columns(); i++) {
        pk_columns.push_back((uint32_t)i);
    }
    Schema pkey_schema = ChunkHelper::convert_schema(params.tablet_schema, pk_columns);
    std::unique_ptr<Column> pk_column;
    RETURN_IF_ERROR(PrimaryKeyEncoder::create_column(pkey_schema, &pk_column));

    auto root_path = params.tablet->metadata_root_location();
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(root_path));
    const std::string& path = params.op_write.dels(del_id);
    ASSIGN_OR_RETURN(auto read_file, fs->new_random_access_file(params.tablet->del_location(path)));
    ASSIGN_OR_RETURN(auto file_size, read_file->get_size());
    std::vector<uint8_t> read_buffer(file_size);
    RETURN_IF_ERROR(read_file->read_at_fully(0, read_buffer.data(), read_buffer.size()));
    auto col = pk_column->clone();
    if (serde::ColumnArraySerde::deserialize(read_buffer.data(), col.get()) == nullptr) {
        return Status::InternalError("column deserialization failed");
    }
    col->raw_data();
    _memory_usage += col->memory_usage();
    _deletes[del_id] = std::move(col);
    TRACE("end read $0-th deletes files", del_id);
    return Status::OK();
}

void RowsetUpdateState::release_delete(uint32_t del_id) {
    _memory_usage -= _deletes[del_id] ? _deletes[del_id]->memory_usage() : 0;
    _deletes[del_id].reset();
}

const std::unique_ptr<Column>& RowsetUpdateState::auto_increment_deletes(uint32_t segment_id) const {
    return _auto_increment_delete_pks[segment_id];
}

std::string RowsetUpdateState::to_string() const {
    return strings::Substitute("RowsetUpdateState tablet:$0", _tablet_id);
}

} // namespace starrocks::lake
