// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "rowset_update_state.h"

#include "gutil/strings/substitute.h"
#include "serde/column_array_serde.h"
#include "storage/primary_key_encoder.h"
#include "storage/rowset/beta_rowset.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/segment_rewriter.h"
#include "storage/rowset/vectorized/rowset_options.h"
#include "storage/tablet.h"
#include "storage/vectorized/chunk_helper.h"
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

    auto block_manager = fs::fs_util::block_manager();
    // always one file for now.
    for (auto i = 0; i < rowset->num_delete_files(); i++) {
        auto path = BetaRowset::segment_del_file_path(rowset->rowset_path(), rowset->rowset_id(), i);
        std::unique_ptr<fs::ReadableBlock> rblock;
        RETURN_IF_ERROR(block_manager->open_block(path, &rblock));
        uint64_t file_size = 0;
        rblock->size(&file_size);
        std::vector<uint8_t> read_buffer(file_size);
        Slice read_slice(read_buffer.data(), read_buffer.size());
        rblock->read(0, read_slice);
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
    for (const auto& upsert : upserts()) {
        _memory_usage += upsert != nullptr ? upsert->memory_usage() : 0;
    }
    for (const auto& one_delete : deletes()) {
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
    defaults.clear();
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
    _parital_update_states.resize(num_segments);
    for (size_t i = 0; i < num_segments; i++) {
        _parital_update_states[i].write_columns.resize(read_columns.size());
        _parital_update_states[i].src_rss_rowids.resize(_upserts[i]->size());
        for (uint32_t j = 0; j < read_columns.size(); ++j) {
            const auto column = ChunkHelper::column_from_field(*read_column_schema.field(j));
            read_columns[j] = column->clone_empty();
            _parital_update_states[i].write_columns[j] = column->clone_empty();
        }
    }

    std::vector<std::vector<uint64_t>*> rss_rowids;
    rss_rowids.resize(num_segments);
    for (size_t i = 0; i < num_segments; ++i) {
        rss_rowids[i] = &(_parital_update_states[i].src_rss_rowids);
    }
    DCHECK_EQ(_upserts.size(), num_segments);
    RETURN_IF_ERROR(tablet->updates()->prepare_partial_update_states(tablet, _upserts, &_read_version, &_next_rowset_id,
                                                                     &rss_rowids));

    for (size_t i = 0; i < num_segments; i++) {
        size_t num_default = 0;
        std::map<uint32_t, std::vector<uint32_t>> rowids_by_rssid;
        vector<uint32_t> idxes;
        plan_read_by_rssid(_parital_update_states[i].src_rss_rowids, &num_default, &rowids_by_rssid, &idxes);
        // get column values by rowid, also get default values if needed
        RETURN_IF_ERROR(
                tablet->updates()->get_column_values(read_column_ids, num_default > 0, rowids_by_rssid, &read_columns));
        for (size_t col_idx = 0; col_idx < read_column_ids.size(); col_idx++) {
            _parital_update_states[i].write_columns[col_idx]->append_selective(*read_columns[col_idx], idxes.data(), 0,
                                                                               idxes.size());
        }
    }

    return Status::OK();
}

Status RowsetUpdateState::apply(Tablet* tablet, Rowset* rowset, uint32_t rowset_id, const PrimaryIndex& index) {
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
    vector<std::unique_ptr<vectorized::Column>> read_columns(read_column_ids.size());
    vector<std::unique_ptr<vectorized::Column>> write_columns(read_column_ids.size());
    for (auto i = 0; i < read_column_ids.size(); i++) {
        const auto read_column_id = read_column_ids[i];
        auto tablet_column = tschema.column(read_column_id);
        auto column = ChunkHelper::column_from_field_type(tablet_column.type(), tablet_column.is_nullable());
        read_columns[i] = column->clone_empty();
        write_columns[i] = column->clone_empty();
    }
    size_t num_segments = rowset->num_segments();
    DCHECK(num_segments == _upserts.size());
    vector<std::pair<string, string>> rewrite_files;
    DeferOp clean_temp_files([&] {
        for (auto& e : rewrite_files) {
            Env::Default()->delete_file(e.second);
        }
    });
    for (size_t i = 0; i < num_segments; i++) {
        auto& pks = *_upserts[i];
        int64_t t_start = MonotonicMillis();
        std::vector<uint64_t> rowids(pks.size());
        int64_t t_get_rowids = MonotonicMillis();
        index.get(pks, &rowids);
        size_t num_default = 0;
        std::map<uint32_t, std::vector<uint32_t>> rowids_by_rssid;
        vector<uint32_t> idxes;
        // group rowids by rssid, and for each group sort by rowid
        plan_read_by_rssid(rowids, &num_default, &rowids_by_rssid, &idxes);
        // get column values by rowid, also get default values if needed
        RETURN_IF_ERROR(
                tablet->updates()->get_column_values(read_column_ids, num_default > 0, rowids_by_rssid, &read_columns));
        for (size_t col_idx = 0; col_idx < read_column_ids.size(); col_idx++) {
            write_columns[col_idx]->reset_column();
            write_columns[col_idx]->append_selective(*read_columns[col_idx], idxes.data(), 0, idxes.size());
        }
        int64_t t_get_column_values = MonotonicMillis();
        auto src_path = BetaRowset::segment_file_path(tablet->schema_hash_path(), rowset->rowset_id(), i);
        auto dest_path = BetaRowset::segment_temp_file_path(tablet->schema_hash_path(), rowset->rowset_id(), i);
        rewrite_files.emplace_back(src_path, dest_path);
        RETURN_IF_ERROR(SegmentRewriter::rewrite(src_path, dest_path, tablet->tablet_schema(), read_column_ids,
                                                 write_columns, i));
        int64_t t_rewrite = MonotonicMillis();
        LOG(INFO) << Substitute(
                "apply partial segment tablet:$0 rowset:$1 seg:$2 #column:$3 #default:$4 getrowid:$5ms #read:$6($7ms) "
                "write:$8ms total:$9ms",
                tablet->tablet_id(), rowset_id, i, read_column_ids.size(), num_default,
                (t_get_rowids - t_start) / 1000000, pks.size() - num_default,
                (t_get_column_values - t_get_rowids) / 1000000, (t_rewrite - t_get_column_values) / 1000000,
                (t_rewrite - t_start) / 1000000);
    }
    for (size_t i = 0; i < num_segments; i++) {
        RETURN_IF_ERROR(Env::Default()->rename_file(rewrite_files[i].second, rewrite_files[i].first));
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
