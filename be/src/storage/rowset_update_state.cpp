// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "rowset_update_state.h"

#include "gutil/strings/substitute.h"
#include "serde/column_array_serde.h"
#include "storage/chunk_helper.h"
#include "storage/primary_key_encoder.h"
#include "storage/rowset/beta_rowset.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/rowset_options.h"
#include "storage/rowset/segment_rewriter.h"
#include "storage/tablet.h"
#include "storage/tablet_meta_manager.h"
#include "util/defer_op.h"
#include "util/phmap/phmap.h"
#include "util/stack_util.h"
#include "util/time.h"

namespace starrocks {

using vectorized::ChunkHelper;

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

Status RowsetUpdateState::_do_load(Tablet* tablet, Rowset* rowset) {
<<<<<<< HEAD
=======
    auto span = Tracer::Instance().start_trace_txn_tablet("rowset_update_state_load", rowset->txn_id(),
                                                          tablet->tablet_id());
>>>>>>> d2a883052 ([Enhancement] Remove txn lock and add tracing for run_publish_version_task (#7187))
    _tablet_id = tablet->tablet_id();
    auto& schema = rowset->schema();
    vector<uint32_t> pk_columns;
    for (size_t i = 0; i < schema.num_key_columns(); i++) {
        pk_columns.push_back((uint32_t)i);
    }
    vectorized::Schema pkey_schema = vectorized::ChunkHelper::convert_schema_to_format_v2(schema, pk_columns);
    std::unique_ptr<vectorized::Column> pk_column;
    if (!PrimaryKeyEncoder::create_column(pkey_schema, &pk_column).ok()) {
        CHECK(false) << "create column for primary key encoder failed";
    }

    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(rowset->rowset_path()));
    // always one file for now.
    for (auto i = 0; i < rowset->num_delete_files(); i++) {
        auto path = BetaRowset::segment_del_file_path(rowset->rowset_path(), rowset->rowset_id(), i);
        ASSIGN_OR_RETURN(auto read_file, fs->new_random_access_file(path));
        ASSIGN_OR_RETURN(auto file_size, read_file->get_size());
        std::vector<uint8_t> read_buffer(file_size);
        RETURN_IF_ERROR(read_file->read_at_fully(0, read_buffer.data(), read_buffer.size()));
        auto col = pk_column->clone();
        if (serde::ColumnArraySerde::deserialize(read_buffer.data(), col.get()) == nullptr) {
            return Status::InternalError("column deserialization failed");
        }
        _deletes.emplace_back(std::move(col));
    }

    RowsetReleaseGuard guard(rowset->shared_from_this());
    OlapReaderStatistics stats;
    auto beta_rowset = down_cast<BetaRowset*>(rowset);
    auto res = beta_rowset->get_segment_iterators2(pkey_schema, nullptr, 0, &stats);
    if (!res.ok()) {
        return res.status();
    }
    // TODO(cbl): auto close iterators on failure
    auto& itrs = res.value();
    CHECK(itrs.size() == rowset->num_segments()) << "itrs.size != num_segments";
    _upserts.resize(rowset->num_segments());
    // only hold pkey, so can use larger chunk size
    auto chunk_shared_ptr = ChunkHelper::new_chunk(pkey_schema, 4096);
    auto chunk = chunk_shared_ptr.get();
    for (size_t i = 0; i < itrs.size(); i++) {
        auto& dest = _upserts[i];
        auto col = pk_column->clone();
        auto itr = itrs[i].get();
        if (itr != nullptr) {
            auto num_rows = beta_rowset->segments()[i]->num_rows();
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
            itr->close();
            CHECK(col->size() == num_rows) << "read segment: iter rows != num rows";
        }
        dest = std::move(col);
    }
    for (const auto& upsert : _upserts) {
        _memory_usage += upsert != nullptr ? upsert->memory_usage() : 0;
    }
    for (const auto& one_delete : _deletes) {
        _memory_usage += one_delete != nullptr ? one_delete->memory_usage() : 0;
    }
    const auto& rowset_meta_pb = rowset->rowset_meta()->get_meta_pb();
    if (!rowset_meta_pb.has_txn_meta() || rowset->num_segments() == 0) {
        return Status::OK();
    }

    return _prepare_partial_update_states(tablet, rowset);
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
static void plan_read_by_rssid(const vector<uint64_t>& rowids, size_t* num_default,
                               std::map<uint32_t, std::vector<uint32_t>>* rowids_by_rssid, vector<uint32_t>* idxes) {
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

Status RowsetUpdateState::_prepare_partial_update_states(Tablet* tablet, Rowset* rowset) {
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

    auto read_column_schema = ChunkHelper::convert_schema_to_format_v2(tablet_schema, read_column_ids);
    std::vector<std::unique_ptr<vectorized::Column>> read_columns(read_column_ids.size());
    size_t num_segments = rowset->num_segments();
    _partial_update_states.resize(num_segments);
    for (size_t i = 0; i < num_segments; i++) {
        _partial_update_states[i].write_columns.resize(read_columns.size());
        _partial_update_states[i].src_rss_rowids.resize(_upserts[i]->size());
        for (uint32_t j = 0; j < read_columns.size(); ++j) {
            auto column = ChunkHelper::column_from_field(*read_column_schema.field(j).get());
            read_columns[j] = column->clone_empty();
            _partial_update_states[i].write_columns[j] = column->clone_empty();
        }
    }

    int64_t t_read_index = MonotonicMillis();
    std::vector<std::vector<uint64_t>*> rss_rowids;
    rss_rowids.resize(num_segments);
    for (size_t i = 0; i < num_segments; ++i) {
        rss_rowids[i] = &(_partial_update_states[i].src_rss_rowids);
    }
    DCHECK_EQ(_upserts.size(), num_segments);
    RETURN_IF_ERROR(tablet->updates()->prepare_partial_update_states(tablet, _upserts, &_read_version, &_next_rowset_id,
                                                                     &rss_rowids));

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
        RETURN_IF_ERROR(
                tablet->updates()->get_column_values(read_column_ids, num_default > 0, rowids_by_rssid, &read_columns));
        for (size_t col_idx = 0; col_idx < read_column_ids.size(); col_idx++) {
            _partial_update_states[i].write_columns[col_idx]->append_selective(*read_columns[col_idx], idxes.data(), 0,
                                                                               idxes.size());
        }
    }
    int64_t t_end = MonotonicMillis();

    LOG(INFO) << Substitute(
            "prepare PartialUpdateState tablet:$0 read_version:$1 #segment:$2 #row:$3(#non-default:$4) #column:$5 "
            "time:$6ms(index:$7/value:$8)",
            _tablet_id, _read_version.to_string(), num_segments, total_rows, total_nondefault_rows, read_columns.size(),
            t_end - t_start, t_read_values - t_read_index, t_end - t_read_values);
    return Status::OK();
}

Status RowsetUpdateState::_check_and_resolve_conflict(Tablet* tablet, Rowset* rowset, uint32_t rowset_id,
                                                      EditVersion latest_applied_version,
                                                      std::vector<uint32_t>& read_column_ids,
                                                      const PrimaryIndex& index) {
    // _partial_update_states is empty which means write column is empty
    if (_partial_update_states.empty()) {
        return Status::InternalError("write column is empty");
    }

    // _read_version is equal to latest_applied_version which means there is no other rowset is applied
    // the data of write_columns can be write to segment file directly
    if (latest_applied_version == _read_version) {
        return Status::OK();
    }

    // get rss_rowids to identify conflict exist or not
    int64_t t_start = MonotonicMillis();
    uint32_t num_segments = _upserts.size();
    std::vector<std::vector<uint64_t>> new_rss_rowids;
    new_rss_rowids.resize(num_segments);
    for (uint32_t i = 0; i < num_segments; ++i) {
        auto& pks = *_upserts[i];
        new_rss_rowids[i].resize(pks.size());
        index.get(pks, &new_rss_rowids[i]);
    }
    int64_t t_read_index = MonotonicMillis();

    size_t total_conflicts = 0;
    for (uint32_t i = 0; i < num_segments; ++i) {
        uint32_t num_rows = new_rss_rowids[i].size();
        std::vector<uint32_t> conflict_idxes;
        std::vector<uint64_t> conflict_rowids;
        DCHECK_EQ(num_rows, _partial_update_states[i].src_rss_rowids.size());
        for (size_t j = 0; j < new_rss_rowids[i].size(); ++j) {
            uint64_t new_rss_rowid = new_rss_rowids[i][j];
            uint32_t new_rssid = new_rss_rowid >> 32;
            uint64_t rss_rowid = _partial_update_states[i].src_rss_rowids[j];
            uint32_t rssid = rss_rowid >> 32;

            if (rssid != new_rssid) {
                conflict_idxes.emplace_back(j);
                conflict_rowids.emplace_back(new_rss_rowid);
            }
        }
        if (!conflict_idxes.empty()) {
            total_conflicts += conflict_idxes.size();
            std::vector<std::unique_ptr<vectorized::Column>> read_columns;
            read_columns.resize(_partial_update_states[i].write_columns.size());
            for (uint32_t j = 0; j < read_columns.size(); ++j) {
                read_columns[j] = _partial_update_states[i].write_columns[j]->clone_empty();
            }
            size_t num_default = 0;
            std::map<uint32_t, std::vector<uint32_t>> rowids_by_rssid;
            std::vector<uint32_t> read_idxes;
            plan_read_by_rssid(conflict_rowids, &num_default, &rowids_by_rssid, &read_idxes);
            DCHECK_EQ(conflict_idxes.size(), read_idxes.size());
            RETURN_IF_ERROR(tablet->updates()->get_column_values(read_column_ids, num_default > 0, rowids_by_rssid,
                                                                 &read_columns));

            for (size_t col_idx = 0; col_idx < read_column_ids.size(); col_idx++) {
                std::unique_ptr<vectorized::Column> new_write_column =
                        _partial_update_states[i].write_columns[col_idx]->clone_empty();
                new_write_column->append_selective(*read_columns[col_idx], read_idxes.data(), 0, read_idxes.size());
                RETURN_IF_ERROR(_partial_update_states[i].write_columns[col_idx]->update_rows(*new_write_column,
                                                                                              conflict_idxes.data()));
            }
        }
    }
    int64_t t_end = MonotonicMillis();
    LOG(INFO) << Substitute(
            "_check_and_resolve_conflict tablet:$0 rowset:$1 version:($2 $3) #conflict-row:$4 #column:$5 "
            "time:$6ms(index:$7/value:$8)",
            tablet->tablet_id(), rowset_id, _read_version.to_string(), latest_applied_version.to_string(),
            total_conflicts, read_column_ids.size(), t_end - t_start, t_read_index - t_start, t_end - t_read_index);

    return Status::OK();
}

Status RowsetUpdateState::apply(Tablet* tablet, Rowset* rowset, uint32_t rowset_id, EditVersion latest_applied_version,
                                const PrimaryIndex& index) {
    const auto& rowset_meta_pb = rowset->rowset_meta()->get_meta_pb();
    if (!rowset_meta_pb.has_txn_meta() || rowset->num_segments() == 0) {
        return Status::OK();
    }
    // currently assume it's a partial update
    const auto& txn_meta = rowset_meta_pb.txn_meta();
    const auto& tschema = tablet->tablet_schema();
    // columns supplied in rowset
    std::vector<uint32_t> update_colum_ids(txn_meta.partial_update_column_ids().begin(),
                                           txn_meta.partial_update_column_ids().end());
    std::set<uint32_t> update_columns_set(update_colum_ids.begin(), update_colum_ids.end());
    // columns needs to be read from tablet's data
    std::vector<uint32_t> read_column_ids;
    for (uint32_t i = 0; i < tschema.num_columns(); i++) {
        if (update_columns_set.find(i) == update_columns_set.end()) {
            read_column_ids.push_back(i);
        }
    }

    size_t num_segments = rowset->num_segments();
    DCHECK(num_segments == _upserts.size());
    vector<std::pair<string, string>> rewrite_files;
    DeferOp clean_temp_files([&] {
        for (auto& e : rewrite_files) {
            FileSystem::Default()->delete_file(e.second);
        }
    });
    bool is_rewrite = config::rewrite_partial_segment;
    RETURN_IF_ERROR(
            _check_and_resolve_conflict(tablet, rowset, rowset_id, latest_applied_version, read_column_ids, index));

    for (size_t i = 0; i < num_segments; i++) {
        auto src_path = BetaRowset::segment_file_path(tablet->schema_hash_path(), rowset->rowset_id(), i);
        auto dest_path = BetaRowset::segment_temp_file_path(tablet->schema_hash_path(), rowset->rowset_id(), i);
        rewrite_files.emplace_back(src_path, dest_path);

        int64_t t_rewrite_start = MonotonicMillis();
        FooterPointerPB partial_rowset_footer = txn_meta.partial_rowset_footers(i);
        // if is_rewrite is true, rewrite partial segment file into dest_path first, then append write_columns
        // if is_rewrite is false, append write_columns into src_path and rebuild segment footer
        if (is_rewrite) {
            RETURN_IF_ERROR(SegmentRewriter::rewrite(src_path, dest_path, tablet->tablet_schema(), read_column_ids,
                                                     _partial_update_states[i].write_columns, i,
                                                     partial_rowset_footer));
        } else {
            RETURN_IF_ERROR(SegmentRewriter::rewrite(src_path, tablet->tablet_schema(), read_column_ids,
                                                     _partial_update_states[i].write_columns, i,
                                                     partial_rowset_footer));
        }
        int64_t t_rewrite_end = MonotonicMillis();
        LOG(INFO) << Substitute("apply partial segment tablet:$0 rowset:$1 seg:$2 #column:$3 #rewrite:$4ms",
                                tablet->tablet_id(), rowset_id, i, read_column_ids.size(),
                                t_rewrite_end - t_rewrite_start);
    }
    if (is_rewrite) {
        for (size_t i = 0; i < num_segments; i++) {
            RETURN_IF_ERROR(FileSystem::Default()->rename_file(rewrite_files[i].second, rewrite_files[i].first));
        }
    }
    // clean this to prevent DeferOp clean files
    rewrite_files.clear();
    auto beta_rowset = down_cast<BetaRowset*>(rowset);
    RETURN_IF_ERROR(beta_rowset->reload());

    return Status::OK();
}

std::string RowsetUpdateState::to_string() const {
    return Substitute("RowsetUpdateState tablet:$0", _tablet_id);
}

} // namespace starrocks
