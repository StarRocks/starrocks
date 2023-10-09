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

RowsetUpdateState::~RowsetUpdateState() {
    if (!_status.ok()) {
        LOG(WARNING) << "bad RowsetUpdateState released tablet:" << _tablet_id;
    }
}

Status RowsetUpdateState::load(const TxnLogPB_OpWrite& op_write, const TabletMetadata& metadata, int64_t base_version,
                               Tablet* tablet, const MetaFileBuilder* builder, bool need_resolve_conflict) {
    if (UNLIKELY(!_status.ok())) {
        return _status;
    }
    std::call_once(_load_once_flag, [&] {
        _base_version = base_version;
        _builder = builder;
        _tablet_id = metadata.id();
        _status = _do_load(op_write, metadata, tablet);
        if (!_status.ok()) {
            if (!_status.is_uninitialized()) {
                LOG(WARNING) << "load RowsetUpdateState error: " << _status << " tablet:" << _tablet_id << " stack:\n"
                             << get_stack_trace();
            }
            if (_status.is_mem_limit_exceeded()) {
                LOG(WARNING) << CurrentThread::mem_tracker()->debug_string();
            }
        }
    });
    if (need_resolve_conflict && _status.ok()) {
        RETURN_IF_ERROR(_resolve_conflict(op_write, metadata, base_version, tablet, builder));
    }
    return _status;
}

Status RowsetUpdateState::_do_load(const TxnLogPB_OpWrite& op_write, const TabletMetadata& metadata, Tablet* tablet) {
    std::shared_ptr<TabletSchema> tablet_schema = std::make_shared<TabletSchema>(metadata.schema());
    std::unique_ptr<Rowset> rowset_ptr =
            std::make_unique<Rowset>(*tablet, std::make_shared<RowsetMetadataPB>(op_write.rowset()));

    RETURN_IF_ERROR(_do_load_upserts_deletes(op_write, tablet_schema, tablet, rowset_ptr.get()));

    if (!op_write.has_txn_meta() || rowset_ptr->num_segments() == 0 || op_write.txn_meta().has_merge_condition()) {
        return Status::OK();
    }
    if (!op_write.txn_meta().partial_update_column_ids().empty()) {
        RETURN_IF_ERROR(_prepare_partial_update_states(op_write, metadata, tablet, tablet_schema));
    }
    if (op_write.txn_meta().has_auto_increment_partial_update_column_id()) {
        RETURN_IF_ERROR(_prepare_auto_increment_partial_update_states(op_write, metadata, tablet, tablet_schema));
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

Status RowsetUpdateState::_do_load_upserts_deletes(const TxnLogPB_OpWrite& op_write,
                                                   const TabletSchemaCSPtr& tablet_schema, Tablet* tablet,
                                                   Rowset* rowset_ptr) {
    vector<uint32_t> pk_columns;
    for (size_t i = 0; i < tablet_schema->num_key_columns(); i++) {
        pk_columns.push_back((uint32_t)i);
    }
    Schema pkey_schema = ChunkHelper::convert_schema(tablet_schema, pk_columns);
    std::unique_ptr<Column> pk_column;
    if (!PrimaryKeyEncoder::create_column(pkey_schema, &pk_column).ok()) {
        CHECK(false) << "create column for primary key encoder failed";
    }

    auto root_path = tablet->metadata_root_location();
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(root_path));
    // always one file for now.
    for (const std::string& path : op_write.dels()) {
        ASSIGN_OR_RETURN(auto read_file, fs->new_random_access_file(tablet->del_location(path)));
        ASSIGN_OR_RETURN(auto file_size, read_file->get_size());
        std::vector<uint8_t> read_buffer(file_size);
        RETURN_IF_ERROR(read_file->read_at_fully(0, read_buffer.data(), read_buffer.size()));
        auto col = pk_column->clone();
        if (serde::ColumnArraySerde::deserialize(read_buffer.data(), col.get()) == nullptr) {
            return Status::InternalError("column deserialization failed");
        }
        _deletes.emplace_back(std::move(col));
    }
    if (op_write.dels_size() > 0) {
        TRACE("end read $0 deletes files", op_write.dels_size());
    }

    OlapReaderStatistics stats;
    auto res = rowset_ptr->get_each_segment_iterator(pkey_schema, &stats);
    if (!res.ok()) {
        return res.status();
    }
    auto& itrs = res.value();
    CHECK(itrs.size() == rowset_ptr->num_segments())
            << "itrs.size != num_segments " << itrs.size() << ", " << rowset_ptr->num_segments();
    _upserts.resize(rowset_ptr->num_segments());
    // only hold pkey, so can use larger chunk size
    auto chunk_shared_ptr = ChunkHelper::new_chunk(pkey_schema, 4096);
    auto chunk = chunk_shared_ptr.get();
    for (size_t i = 0; i < itrs.size(); i++) {
        auto& dest = _upserts[i];
        auto col = pk_column->clone();
        auto itr = itrs[i].get();
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
    }
    if (itrs.size() > 0) {
        TRACE("end read $0 upserts files", itrs.size());
    }

    for (const auto& upsert : _upserts) {
        upsert->raw_data();
        _memory_usage += upsert != nullptr ? upsert->memory_usage() : 0;
    }
    for (const auto& one_delete : _deletes) {
        one_delete->raw_data();
        _memory_usage += one_delete != nullptr ? one_delete->memory_usage() : 0;
    }

    return Status::OK();
}

static std::vector<uint32_t> get_read_columns_ids(const TxnLogPB_OpWrite& op_write,
                                                  const TabletSchemaCSPtr& tablet_schema) {
    const auto& txn_meta = op_write.txn_meta();

    std::vector<uint32_t> update_column_ids(txn_meta.partial_update_column_ids().begin(),
                                            txn_meta.partial_update_column_ids().end());
    std::set<uint32_t> update_columns_set(update_column_ids.begin(), update_column_ids.end());

    std::vector<uint32_t> read_column_ids;
    for (uint32_t i = 0; i < tablet_schema->num_columns(); i++) {
        if (update_columns_set.find(i) == update_columns_set.end()) {
            read_column_ids.push_back(i);
        }
    }

    return read_column_ids;
}

Status RowsetUpdateState::_prepare_auto_increment_partial_update_states(const TxnLogPB_OpWrite& op_write,
                                                                        const TabletMetadata& metadata, Tablet* tablet,
                                                                        const TabletSchemaCSPtr& tablet_schema) {
    const auto& txn_meta = op_write.txn_meta();
    size_t num_segments = op_write.rowset().segments_size();
    _auto_increment_partial_update_states.resize(num_segments);
    _auto_increment_delete_pks.resize(num_segments);

    uint32_t auto_increment_column_id = 0;
    for (int i = 0; i < tablet_schema->num_columns(); ++i) {
        if (tablet_schema->column(i).is_auto_increment()) {
            auto_increment_column_id = i;
            break;
        }
    }
    std::vector<uint32_t> column_id{auto_increment_column_id};
    auto read_column_schema = ChunkHelper::convert_schema(tablet_schema, column_id);
    auto column = ChunkHelper::column_from_field(*read_column_schema.field(0).get());
    std::vector<std::vector<std::unique_ptr<Column>>> read_column(num_segments);

    std::shared_ptr<TabletSchema> schema = nullptr;
    if (!txn_meta.partial_update_column_ids().empty()) {
        std::vector<int32_t> update_column_ids(txn_meta.partial_update_column_ids().begin(),
                                               txn_meta.partial_update_column_ids().end());
        schema = TabletSchema::create(tablet_schema, update_column_ids);
    } else {
        std::vector<int32_t> all_column_ids;
        all_column_ids.resize(tablet_schema->num_columns());
        std::iota(all_column_ids.begin(), all_column_ids.end(), 0);
        schema = TabletSchema::create(tablet_schema, all_column_ids);
    }

    for (size_t i = 0; i < num_segments; i++) {
        _auto_increment_partial_update_states[i].init(schema, txn_meta.auto_increment_partial_update_column_id(), i);
        _auto_increment_partial_update_states[i].src_rss_rowids.resize(_upserts[i]->size());
        read_column[i].resize(1);
        read_column[i][0] = column->clone_empty();
        _auto_increment_partial_update_states[i].write_column = column->clone_empty();
    }

    // segment id -> [rowids list]
    std::vector<std::vector<uint64_t>*> rss_rowids;
    rss_rowids.resize(num_segments);
    for (size_t i = 0; i < num_segments; ++i) {
        rss_rowids[i] = &(_auto_increment_partial_update_states[i].src_rss_rowids);
    }
    DCHECK_EQ(_upserts.size(), num_segments);
    // use upserts to get rowids in each segment
    RETURN_IF_ERROR(tablet->update_mgr()->get_rowids_from_pkindex(tablet, _base_version, _upserts, &rss_rowids));

    for (size_t i = 0; i < num_segments; i++) {
        std::vector<uint32_t> rowids;
        uint32_t n = _auto_increment_partial_update_states[i].src_rss_rowids.size();
        for (uint32_t j = 0; j < n; j++) {
            uint64_t v = _auto_increment_partial_update_states[i].src_rss_rowids[j];
            uint32_t rssid = v >> 32;
            if (rssid == (uint32_t)-1) {
                rowids.emplace_back(j);
            }
        }
        std::swap(_auto_increment_partial_update_states[i].rowids, rowids);

        size_t new_rows = 0;
        std::vector<uint32_t> idxes;
        std::map<uint32_t, std::vector<uint32_t>> rowids_by_rssid;
        plan_read_by_rssid(_auto_increment_partial_update_states[i].src_rss_rowids, &new_rows, &rowids_by_rssid,
                           &idxes);

        if (new_rows == n) {
            _auto_increment_partial_update_states[i].skip_rewrite = true;
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

        RETURN_IF_ERROR(tablet->update_mgr()->get_column_values(tablet, metadata, op_write, tablet_schema, column_id,
                                                                new_rows > 0, rowids_by_rssid, &read_column[i],
                                                                &_auto_increment_partial_update_states[i]));

        _auto_increment_partial_update_states[i].write_column->append_selective(*read_column[i][0], idxes.data(), 0,
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
        _auto_increment_delete_pks[i].reset();
        _auto_increment_delete_pks[i] = _upserts[i]->clone_empty();
        std::vector<uint32_t> delete_idxes;
        const int64* data =
                reinterpret_cast<const int64*>(_auto_increment_partial_update_states[i].write_column->raw_data());

        // just check the rows which are not exist in the previous version
        // because the rows exist in the previous version may contain 0 which are specified by the user
        for (unsigned int row_idx : _auto_increment_partial_update_states[i].rowids) {
            if (data[row_idx] == 0) {
                delete_idxes.emplace_back(row_idx);
            }
        }

        if (delete_idxes.size() != 0) {
            _auto_increment_delete_pks[i]->append_selective(*_upserts[i], delete_idxes.data(), 0, delete_idxes.size());
        }
    }
    return Status::OK();
}

Status RowsetUpdateState::_prepare_partial_update_states(const TxnLogPB_OpWrite& op_write,
                                                         const TabletMetadata& metadata, Tablet* tablet,
                                                         const TabletSchemaCSPtr& tablet_schema) {
    int64_t t_start = MonotonicMillis();
    std::vector<uint32_t> read_column_ids = get_read_columns_ids(op_write, tablet_schema);

    auto read_column_schema = ChunkHelper::convert_schema(tablet_schema, read_column_ids);
    size_t num_segments = op_write.rowset().segments_size();
    // segment id -> column list
    std::vector<std::vector<std::unique_ptr<Column>>> read_columns;
    read_columns.resize(num_segments);
    _partial_update_states.resize(num_segments);
    for (size_t i = 0; i < num_segments; i++) {
        read_columns[i].resize(read_column_ids.size());
        _partial_update_states[i].write_columns.resize(read_columns[i].size());
        _partial_update_states[i].src_rss_rowids.resize(_upserts[i]->size());
        for (uint32_t j = 0; j < read_columns[i].size(); ++j) {
            auto column = ChunkHelper::column_from_field(*read_column_schema.field(j).get());
            read_columns[i][j] = column->clone_empty();
            _partial_update_states[i].write_columns[j] = column->clone_empty();
        }
    }

    int64_t t_read_index = MonotonicMillis();
    // segment id -> [rowids list]
    std::vector<std::vector<uint64_t>*> rss_rowids;
    rss_rowids.resize(num_segments);
    for (size_t i = 0; i < num_segments; ++i) {
        rss_rowids[i] = &(_partial_update_states[i].src_rss_rowids);
    }
    DCHECK_EQ(_upserts.size(), num_segments);
    // use upserts to get rowids in each segment
    RETURN_IF_ERROR(tablet->update_mgr()->get_rowids_from_pkindex(tablet, _base_version, _upserts, &rss_rowids));

    int64_t t_read_values = MonotonicMillis();
    size_t total_rows = 0;
    // rows actually needed to be read, excluding rows with default values
    size_t total_nondefault_rows = 0;
    for (size_t i = 0; i < num_segments; i++) {
        size_t num_default = 0;
        std::map<uint32_t, std::vector<uint32_t>> rowids_by_rssid;
        vector<uint32_t> idxes;
        plan_read_by_rssid(_partial_update_states[i].src_rss_rowids, &num_default, &rowids_by_rssid, &idxes);
        total_rows += _partial_update_states[i].src_rss_rowids.size();
        total_nondefault_rows += _partial_update_states[i].src_rss_rowids.size() - num_default;
        // get column values by rowid, also get default values if needed
        RETURN_IF_ERROR(tablet->update_mgr()->get_column_values(tablet, metadata, op_write, tablet_schema,
                                                                read_column_ids, num_default > 0, rowids_by_rssid,
                                                                &read_columns[i]));
        for (size_t col_idx = 0; col_idx < read_column_ids.size(); col_idx++) {
            _partial_update_states[i].write_columns[col_idx]->append_selective(*read_columns[i][col_idx], idxes.data(),
                                                                               0, idxes.size());
        }
        // release read column memory
        read_columns[i].clear();
    }
    int64_t t_end = MonotonicMillis();

    LOG(INFO) << strings::Substitute(
            "prepare lake PartialUpdateState tablet:$0 #segment:$1 #row:$2(#non-default:$3) #column:$4 "
            "time:$5ms(index:$6/value:$7)",
            _tablet_id, num_segments, total_rows, total_nondefault_rows, read_columns.size(), t_end - t_start,
            t_read_values - t_read_index, t_end - t_read_values);
    return Status::OK();
}

Status RowsetUpdateState::rewrite_segment(const TxnLogPB_OpWrite& op_write, const TabletMetadata& metadata,
                                          Tablet* tablet, std::map<int, std::string>* replace_segments,
                                          std::vector<std::string>* orphan_files) {
    const RowsetMetadata& rowset_meta = op_write.rowset();
    auto root_path = tablet->metadata_root_location();
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(root_path));
    std::shared_ptr<TabletSchema> tablet_schema = std::make_shared<TabletSchema>(metadata.schema());
    // get rowset schema
    if (!op_write.has_txn_meta() || op_write.rewrite_segments_size() == 0 || rowset_meta.num_rows() == 0 ||
        op_write.txn_meta().has_merge_condition()) {
        return Status::OK();
    }
    CHECK(op_write.rewrite_segments_size() == rowset_meta.segments_size());
    // currently assume it's a partial update
    const auto& txn_meta = op_write.txn_meta();
    // columns supplied in rowset
    std::vector<uint32_t> update_colum_ids(txn_meta.partial_update_column_ids().begin(),
                                           txn_meta.partial_update_column_ids().end());
    std::set<uint32_t> update_columns_set(update_colum_ids.begin(), update_colum_ids.end());
    // columns needs to be read from tablet's data
    std::vector<uint32_t> read_column_ids;
    // may be the implict partial update for auto increment only
    if (!txn_meta.partial_update_column_ids().empty()) {
        for (uint32_t i = 0; i < tablet_schema->num_columns(); i++) {
            if (update_columns_set.find(i) == update_columns_set.end()) {
                read_column_ids.push_back(i);
            }
        }
    }

    std::vector<bool> need_rename(rowset_meta.segments_size(), true);
    for (int i = 0; i < rowset_meta.segments_size(); i++) {
        const auto& src_path = rowset_meta.segments(i);
        const auto& dest_path = op_write.rewrite_segments(i);
        DCHECK(src_path != dest_path);

        int64_t t_rewrite_start = MonotonicMillis();
        if (op_write.txn_meta().has_auto_increment_partial_update_column_id() &&
            !_auto_increment_partial_update_states[i].skip_rewrite) {
            RETURN_IF_ERROR(SegmentRewriter::rewrite(
                    tablet->segment_location(src_path), tablet->segment_location(dest_path), tablet_schema,
                    _auto_increment_partial_update_states[i], read_column_ids,
                    _partial_update_states.size() != 0 ? &_partial_update_states[i].write_columns : nullptr, op_write,
                    tablet));
        } else if (_partial_update_states.size() != 0) {
            const FooterPointerPB& partial_rowset_footer = txn_meta.partial_rowset_footers(i);
            // if rewrite fail, let segment gc to clean dest segment file
            RETURN_IF_ERROR(SegmentRewriter::rewrite(
                    tablet->segment_location(src_path), tablet->segment_location(dest_path), tablet_schema,
                    read_column_ids, _partial_update_states[i].write_columns, i, partial_rowset_footer));
        } else {
            need_rename[i] = false;
        }
        int64_t t_rewrite_end = MonotonicMillis();
        LOG(INFO) << strings::Substitute(
                "lake apply partial segment tablet:$0 rowset:$1 seg:$2 #column:$3 #rewrite:$4ms [$5 -> $6]",
                tablet->id(), rowset_meta.id(), i, read_column_ids.size(), t_rewrite_end - t_rewrite_start, src_path,
                dest_path);
    }

    // rename segment file
    for (int i = 0; i < rowset_meta.segments_size(); i++) {
        if (need_rename[i]) {
            // after rename, add old segment to orphan files, for gc later.
            orphan_files->push_back(rowset_meta.segments(i));
            (*replace_segments)[i] = op_write.rewrite_segments(i);
        }
    }
    TRACE("end rewrite segment");
    return Status::OK();
}

Status RowsetUpdateState::_resolve_conflict(const TxnLogPB_OpWrite& op_write, const TabletMetadata& metadata,
                                            int64_t base_version, Tablet* tablet, const MetaFileBuilder* builder) {
    _builder = builder;
    // check if base version is match and pk index has not been updated yet, and we can skip resolve conflict
    if (base_version == _base_version && !_builder->has_update_index()) {
        return Status::OK();
    }
    _base_version = base_version;
    // skip resolve conflict when not partial update happen.
    if (!op_write.has_txn_meta() || op_write.rowset().segments_size() == 0 ||
        op_write.txn_meta().has_merge_condition()) {
        return Status::OK();
    }

    // we must get segment number from op_write but not _partial_update_states,
    // because _resolve_conflict maybe done for auto increment col partial update only
    const int num_segments = op_write.rowset().segments_size();
    // use upserts to get rowids in each segment
    // segment id -> [rowids list]
    std::vector<std::vector<uint64_t>> new_rss_rowids_vec;
    new_rss_rowids_vec.resize(num_segments);
    for (uint32_t segment_id = 0; segment_id < num_segments; segment_id++) {
        new_rss_rowids_vec[segment_id].resize(_upserts[segment_id]->size());
    }
    RETURN_IF_ERROR(
            tablet->update_mgr()->get_rowids_from_pkindex(tablet, _base_version, _upserts, &new_rss_rowids_vec));

    size_t total_conflicts = 0;
    std::shared_ptr<TabletSchema> tablet_schema = std::make_shared<TabletSchema>(metadata.schema());
    std::vector<uint32_t> read_column_ids = get_read_columns_ids(op_write, tablet_schema);
    // get rss_rowids to identify conflict exist or not
    int64_t t_start = MonotonicMillis();
    for (uint32_t segment_id = 0; segment_id < num_segments; segment_id++) {
        std::vector<uint64_t>& new_rss_rowids = new_rss_rowids_vec[segment_id];

        // reslove normal partial update
        if (!op_write.txn_meta().partial_update_column_ids().empty()) {
            RETURN_IF_ERROR(_resolve_conflict_partial_update(op_write, metadata, tablet, new_rss_rowids,
                                                             read_column_ids, segment_id, total_conflicts,
                                                             tablet_schema));
        }

        // reslove auto increment
        if (op_write.txn_meta().has_auto_increment_partial_update_column_id()) {
            RETURN_IF_ERROR(_resolve_conflict_auto_increment(op_write, metadata, tablet, new_rss_rowids, segment_id,
                                                             total_conflicts, tablet_schema));
        }
    }
    int64_t t_end = MonotonicMillis();
    LOG(INFO) << strings::Substitute(
            "lake resolve_conflict tablet:$0 base_version:$1 #conflict-row:$2 "
            "#column:$3 time:$4ms",
            tablet->id(), _base_version, total_conflicts, read_column_ids.size(), t_end - t_start);

    return Status::OK();
}

Status RowsetUpdateState::_resolve_conflict_partial_update(const TxnLogPB_OpWrite& op_write,
                                                           const TabletMetadata& metadata, Tablet* tablet,
                                                           const std::vector<uint64_t>& new_rss_rowids,
                                                           std::vector<uint32_t>& read_column_ids, uint32_t segment_id,
                                                           size_t& total_conflicts,
                                                           const TabletSchemaCSPtr& tablet_schema) {
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
        RETURN_IF_ERROR(tablet->update_mgr()->get_column_values(tablet, metadata, op_write, tablet_schema,
                                                                read_column_ids, num_default > 0, rowids_by_rssid,
                                                                &read_columns));

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

Status RowsetUpdateState::_resolve_conflict_auto_increment(const TxnLogPB_OpWrite& op_write,
                                                           const TabletMetadata& metadata, Tablet* tablet,
                                                           const std::vector<uint64_t>& new_rss_rowids,
                                                           uint32_t segment_id, size_t& total_conflicts,
                                                           const TabletSchemaCSPtr& tablet_schema) {
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
        for (int i = 0; i < tablet_schema->num_columns(); ++i) {
            if (tablet_schema->column(i).is_auto_increment()) {
                auto_increment_column_id = i;
                break;
            }
        }
        std::vector<uint32_t> column_id{auto_increment_column_id};
        std::vector<std::unique_ptr<Column>> auto_increment_read_column;
        auto_increment_read_column.resize(1);
        auto_increment_read_column[0] = _auto_increment_partial_update_states[segment_id].write_column->clone_empty();
        RETURN_IF_ERROR(tablet->update_mgr()->get_column_values(
                tablet, metadata, op_write, tablet_schema, column_id, new_rows > 0, rowids_by_rssid,
                &auto_increment_read_column, &_auto_increment_partial_update_states[segment_id]));

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

const std::vector<std::unique_ptr<Column>>& RowsetUpdateState::auto_increment_deletes() const {
    return _auto_increment_delete_pks;
}

std::string RowsetUpdateState::to_string() const {
    return strings::Substitute("RowsetUpdateState tablet:$0", _tablet_id);
}

} // namespace starrocks::lake
