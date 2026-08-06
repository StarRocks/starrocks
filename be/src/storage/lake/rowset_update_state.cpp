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

#include "base/debug/trace.h"
#include "base/phmap/phmap.h"
#include "base/time/time.h"
#include "base/utility/defer_op.h"
#include "column/chunk_factory.h"
#include "column/raw_data_visitor.h"
#include "column/serde/column_array_serde.h"
#include "common/constexpr.h"
#include "common/flexible_partial_update.h"
#include "common/stack_util.h"
#include "common/tracer.h"
#include "fs/fs_factory.h"
#include "fs/fs_util.h"
#include "gutil/strings/substitute.h"
#include "platform/key_cache.h"
#include "runtime/current_thread.h"
#include "storage/chunk_helper.h"
#include "storage/lake/filenames.h"
#include "storage/lake/location_provider.h"
#include "storage/lake/meta_file.h"
#include "storage/lake/rowset.h"
#include "storage/lake/update_manager.h"
#include "storage/lake/vector_index_utils.h"
#include "storage/olap_common.h"
#include "storage/rowset/segment_rewriter.h"
#include "storage/rowset/segment_writer.h"
#include "storage/tablet_schema.h"
#include "storage_primitive/primary_key_encoder.h"

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
    DCHECK_GT(params.metadata->version(), 0);
    DCHECK_EQ(params.tablet->id(), params.metadata->id());
    if (!_base_versions.empty() && _schema_version < params.metadata->schema().schema_version()) {
        LOG(INFO) << "schema version has changed from " << _schema_version << " to "
                  << params.metadata->schema().schema_version() << ", need to reload the update state."
                  << " tablet_id: " << params.tablet->id() << " old base version: " << _base_versions[0]
                  << " new base version: " << params.metadata->version();
        // The data has been loaded, but the schema has changed and needs to be reloaded according to the new schema
        _reset();
    }
    _tablet_id = params.metadata->id();
    _schema_version = params.metadata->schema().schema_version();
}

static bool has_partial_update_state(const RowsetUpdateStateParams& params) {
    return !params.op_write.txn_meta().partial_update_column_unique_ids().empty();
}

static bool has_auto_increment_partial_update_state(const RowsetUpdateStateParams& params) {
    return params.op_write.txn_meta().has_auto_increment_partial_update_column_id();
}

// Helper to check if this transaction involves any type of partial update
static bool has_partial_update(const RowsetUpdateStateParams& params) {
    return has_partial_update_state(params) || has_auto_increment_partial_update_state(params);
}

// Determines whether segment lazy loading should be enabled for primary key column iteration.
// Lazy loading defers reading the full primary key column into memory until actually needed,
// reducing memory usage for large segments.
//
// Lazy load is ENABLED when:
// 1. Normal transaction (no txn_meta) - safe to defer loading as no special merge logic needed
// 2. Condition update without partial updates - both the SST-backed parallel path and the
//    non-SST chunk-parallel path iterate the PK column chunk by chunk, so neither needs the
//    full segment materialized upfront.
//
// Lazy load is DISABLED when:
// 1. Partial updates are involved - WHY: partial update handler needs immediate access to standalone
//    PK columns to merge with existing data, can't wait for lazy loading.
//
// PERFORMANCE: Enabling lazy load reduces peak memory by ~50% for large primary key columns (>100MB)
// and, for the non-SST condition-merge path, lets _do_update_with_condition produce multiple
// per-segment chunks so its compare phase actually scales on lake_partial_update_thread_pool.
static bool should_enable_lazy_load(const RowsetUpdateStateParams& params) {
    return !params.op_write.has_txn_meta() ||
           (!params.op_write.txn_meta().merge_condition().empty() && !has_partial_update(params));
}

Status RowsetUpdateState::load_segment(uint32_t segment_id, const RowsetUpdateStateParams& params, int64_t base_version,
                                       bool need_resolve_conflict, bool need_lock) {
    TRACE_COUNTER_SCOPE_LATENCY_US("load_segment_us");
    // prepare() must have been called before load_segment.
    DCHECK(_rowset_ptr != nullptr);

    if (_upserts.size() == 0) {
        // Empty rowset
        return Status::OK();
    }
    if (_upserts[segment_id] == nullptr) {
        _base_versions[segment_id] = base_version;
        RETURN_IF_ERROR(_do_load_upserts(segment_id, params));
    }

    // Early return optimization: Skip partial update state loading for condition-only updates.
    // WHY: Condition updates without partial columns don't need the complex partial update machinery.
    // This applies to both normal transactions and condition updates that only compare full rows.
    if (!params.op_write.has_txn_meta() || !has_partial_update(params)) {
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
    CHECK_MEM_LIMIT("RowsetUpdateState::_do_load_upserts");
    TRACE_COUNTER_SCOPE_LATENCY_US("do_load_upserts_us");
    // prepare() must have initialized _segment_iters and _pkey_schema already.
    DCHECK(!_segment_iters.empty());
    RETURN_ERROR_IF_FALSE(_segment_iters.size() == _rowset_ptr->num_segments());
    ASSIGN_OR_RETURN(auto pk_encoding_type, params.tablet_schema->primary_key_encoding_type_or_error());
    auto& iter = _segment_iters[segment_id];
    SegmentPKIteratorPtr result = std::make_unique<SegmentPKIterator>();
    RETURN_IF_ERROR(result->init(iter, _pkey_schema, should_enable_lazy_load(params), pk_encoding_type));
    _upserts[segment_id] = std::move(result);
    _memory_usage += _upserts[segment_id]->memory_usage();

    return Status::OK();
}

// FLEXIBLE-on-ROW signal: a flexible load whose per-row column-set dictionary was folded into
// txn_meta. Decoupled from partial_update_mode (the storage MODE is ROW_MODE for this path).
// Static free function so file-local helpers below can share it; the header exposes the same
// predicate as RowsetUpdateState::_is_flexible_partial_update for callers outside this file.
static bool flexible_partial_update_enabled(const TxnLogPB_OpWrite& op_write) {
    return op_write.has_txn_meta() && op_write.txn_meta().flexible_partial_update() &&
           op_write.txn_meta().distinct_column_sets_size() > 0;
}

// Return the id, i.e, position index, of unmodified columns in the |tablet_schema|
static std::vector<ColumnId> get_read_columns_ids(const TxnLogPB_OpWrite& op_write,
                                                  const TabletSchemaCSPtr& tablet_schema) {
    const auto& txn_meta = op_write.txn_meta();

    // FLEXIBLE-on-ROW: under a flexible load NO value column is guaranteed present for every row
    // (each row updates a different subset, and the .upt is a DENSE UNION with NULL placeholders
    // for omitted cells). So the masked full-row rewrite must have a base fallback for EVERY
    // non-key value column: a row that did not touch column c keeps its base value (or DEFAULT for
    // a brand-new key). Return the full non-key value-column complement (every non-key, non-auto-
    // increment column). Key columns are carried separately (they are identical per row). This is
    // intentionally wider than the homogeneous complement, which only reads columns NOT in the
    // union; here even union columns need a base read because the union is per-load, not per-row.
    if (flexible_partial_update_enabled(op_write)) {
        std::vector<ColumnId> base_read_column_ids;
        for (uint32_t i = 0, num_cols = tablet_schema->num_columns(); i < num_cols; i++) {
            const auto& col = tablet_schema->column(i);
            if (col.is_key()) {
                continue;
            }
            // Auto-increment + flexible is rejected up-front in rewrite_segment; keep the same
            // shape as the homogeneous path (auto-increment columns are handled separately) so
            // the complement stays a pure value-column list.
            if (col.is_auto_increment()) {
                continue;
            }
            base_read_column_ids.push_back(i);
        }
        return base_read_column_ids;
    }

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

// Drain a `.upt` segment iterator fully into |result_chunk| in physical (upsert) row order.
// Mirrors ColumnModePartialUpdateHandler::read_chunk_from_update_file (file-local there).
static Status read_chunk_from_update_file(const ChunkIteratorPtr& iter, const ChunkUniquePtr& result_chunk) {
    auto chunk = result_chunk->clone_empty(1024);
    while (true) {
        chunk->reset();
        auto st = iter->get_next(chunk.get());
        if (st.is_end_of_file()) {
            break;
        } else if (!st.ok()) {
            return st;
        } else {
            result_chunk->append(*chunk);
        }
    }
    return Status::OK();
}

bool RowsetUpdateState::_is_flexible_partial_update(const TxnLogPB_OpWrite& op_write) {
    return flexible_partial_update_enabled(op_write);
}

std::vector<std::set<ColumnUID>> RowsetUpdateState::_decode_distinct_column_sets(const RowsetTxnMetaPB& txn_meta) {
    std::vector<std::set<ColumnUID>> covered;
    covered.reserve(txn_meta.distinct_column_sets_size());
    for (const auto& set_pb : txn_meta.distinct_column_sets()) {
        std::set<ColumnUID> uids;
        for (uint32_t uid : set_pb.column_unique_ids()) {
            uids.insert(static_cast<ColumnUID>(uid));
        }
        covered.push_back(std::move(uids));
    }
    return covered;
}

StatusOr<std::vector<int32_t>> RowsetUpdateState::_read_cset_column_from_upt(uint32_t segment_id) {
    // Read the hidden "__cset__" per-row set-id column for this `.upt` segment. The `.upt` carries it
    // as a real Segment v2 column with the RESERVED unique-id kCsetReservedColumnUid (see
    // flexible_partial_update.h and the lake delta_writer's synthetic-column append). The Rowset's
    // tablet_schema() is the FULL base schema and does NOT contain "__cset__", so we synthesize a
    // single-column TabletSchema carrying "__cset__" (reserved uid + SMALLINT) and let segment
    // iteration resolve it against the `.upt` footer BY UNIQUE-ID. Ports
    // ColumnModePartialUpdateHandler::_read_cset_column_from_upt verbatim.
    DCHECK(_rowset_ptr != nullptr);
    TabletColumn cset_col(STORAGE_AGGREGATE_REPLACE, TYPE_SMALLINT, /*is_nullable=*/false,
                          static_cast<int32_t>(kCsetReservedColumnUid), sizeof(int16_t));
    cset_col.set_name(LOAD_CSET_COLUMN);
    cset_col.set_is_key(false);
    std::vector<TabletColumn> cset_cols;
    cset_cols.emplace_back(std::move(cset_col));
    auto cset_tschema = TabletSchema::copy(*_rowset_ptr->tablet_schema(), cset_cols);
    // TabletSchema::copy inherits the base schema's num_short_key_columns (e.g. 1 for the PK) but
    // leaves only the single non-key "__cset__" column (num_key_columns becomes 0). Opening a Segment
    // with that inconsistency (1 short key over 0 key columns) is unsafe, so pin it to 0 -- mirroring
    // Segment::new_sparse_dcg_segment / the column-mode handler's reserved-uid read schema.
    cset_tschema->set_num_short_key_columns(0);
    Schema cset_schema = ChunkHelper::convert_schema(cset_tschema);

    OlapReaderStatistics stats;
    // Read "__cset__" via a cache-bypassing open bound to cset_tschema: the metacache already holds a
    // base-schema Segment for this .upt path (loaded earlier in apply) which has NO reader for the
    // reserved-uid column; a plain get_each_segment_iterator would reuse it and fail.
    ASSIGN_OR_RETURN(auto segment_iters,
                     _rowset_ptr->get_each_segment_iterator_with_schema(cset_schema, cset_tschema, true, &stats));
    if (segment_id >= segment_iters.size()) {
        return Status::InternalError(
                strings::Substitute("FLEXIBLE-on-ROW: segment_id $0 out of range ($1 segments) reading `$2`",
                                    segment_id, segment_iters.size(), LOAD_CSET_COLUMN));
    }
    DeferOp close_iters([&]() {
        for (auto& it : segment_iters) {
            if (it != nullptr) {
                it->close();
            }
        }
    });

    ChunkUniquePtr cset_chunk = ChunkFactory::new_chunk(cset_schema, DEFAULT_CHUNK_SIZE);
    RETURN_IF_ERROR(read_chunk_from_update_file(segment_iters[segment_id], cset_chunk));
    const auto& col = cset_chunk->get_column_by_index(0);
    const LogicalType cset_type = cset_tschema->column(0).type();
    std::vector<int32_t> set_ids;
    set_ids.reserve(col->size());
    for (size_t i = 0; i < col->size(); ++i) {
        const auto datum = col->get(i);
        switch (cset_type) {
        case TYPE_SMALLINT:
            set_ids.push_back(static_cast<int32_t>(datum.get_int16()));
            break;
        case TYPE_INT:
            set_ids.push_back(datum.get_int32());
            break;
        case TYPE_BIGINT:
            set_ids.push_back(static_cast<int32_t>(datum.get_int64()));
            break;
        default:
            return Status::InternalError(strings::Substitute(
                    "FLEXIBLE-on-ROW: `$0` column has unexpected type $1 (expected SMALLINT/INT/BIGINT)",
                    LOAD_CSET_COLUMN, static_cast<int>(cset_type)));
        }
    }
    return set_ids;
}

// Resolve the rewrite-path vector index options for the dest segment, mirroring the normal lake
// writer (reset_segment_writer): location-provider-resolved .vi paths keyed on |dest_path| — also
// filled in async mode, where they only feed id recording — plus the schema's index_build_mode
// and deferred-build threshold.
static StatusOr<RewriteVectorIndexOptions> resolve_rewrite_vector_index_options(const RowsetUpdateStateParams& params,
                                                                                const std::string& dest_path) {
    RewriteVectorIndexOptions vi_opts;
    SegmentWriterOptions opts;
    RETURN_IF_ERROR(fill_vector_index_file_paths(params.tablet_schema, params.tablet->id(), dest_path,
                                                 params.tablet->tablet_mgr(), /*location_provider=*/nullptr,
                                                 /*fs=*/nullptr, opts));
    vi_opts.file_paths = std::move(opts.vector_index_file_paths);
    vi_opts.defer_build = has_async_vector_index(params.tablet_schema);
    vi_opts.build_threshold = get_vector_index_build_threshold(params.tablet_schema);
    return vi_opts;
}

// A partial update whose update set includes a vector-indexed column raw-copies that column's
// data into the dest segment (rewrite_partial_update creates no column writer for it), so a sync
// .vi built inline for the src partial segment is still valid for the dest — the rewrite keeps
// the row order. Carry it over to the dest segment name and record the id; the src ids in the
// op_write metadata list exactly the indexes whose .vi was built on the updated columns. Async
// mode has no src .vi to carry: the dest ids recorded by the rewrite already cover all indexes.
static Status carry_src_segment_vector_indexes(const RowsetUpdateStateParams& params,
                                               const SegmentMetadataPB& src_seg_meta, const std::string& src_path,
                                               const std::string& dest_path, SegmentFileInfo* file_info) {
    // The src .vi is named by the src segment's recorded owner; the dest is a brand-new segment
    // written by this tablet, so its .vi is named by this tablet (owner recorded by the caller
    // via stamp_rewrite_vector_index_owner).
    for (int64_t index_id : src_seg_meta.vector_index_ids()) {
        auto src_vi = params.tablet->segment_location(
                gen_vector_index_filename(src_path, resolve_segment_vector_index_uid(src_seg_meta), index_id));
        auto dest_vi =
                params.tablet->segment_location(gen_vector_index_filename(dest_path, params.tablet->id(), index_id));
        RETURN_IF_ERROR(fs::copy_file(src_vi, dest_vi).status());
        file_info->vector_index_ids.push_back(index_id);
    }
    return Status::OK();
}

// The dest segment of a rewrite is a brand-new file written by this tablet, so whatever
// vector_index_ids the rewrite recorded (built inline, scheduled async, or carried from the src)
// have their .vi named under params.tablet->id(); record it as the owner for every rewrite path.
static void stamp_rewrite_vector_index_owner(const RowsetUpdateStateParams& params, SegmentFileInfo* file_info) {
    if (!file_info->vector_index_ids.empty()) {
        file_info->segment_vector_index_uid = params.tablet->id();
    }
}

Status RowsetUpdateState::_prepare_auto_increment_partial_update_states(uint32_t segment_id,
                                                                        const RowsetUpdateStateParams& params,
                                                                        bool need_lock) {
    CHECK_MEM_LIMIT("RowsetUpdateState::_prepare_auto_increment_partial_update_states");
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
    auto column = ChunkFactory::column_from_field(*auto_inc_column_schema.field(0).get());
    MutableColumns read_column;

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
    _auto_increment_partial_update_states[segment_id].src_rss_rowids.resize(
            _upserts[segment_id]->standalone_pk_column()->size());
    read_column.resize(1);
    read_column[0] = column->clone_empty();
    _auto_increment_partial_update_states[segment_id].write_column = column->clone_empty();

    // use upserts to get rowids in this segment
    RETURN_IF_ERROR(params.tablet->update_mgr()->get_rowids_from_pkindex(
            params.tablet->id(), _base_versions[segment_id], _upserts[segment_id]->standalone_pk_column(),
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
                                                                   &read_column, &_column_to_expr_value,
                                                                   &_auto_increment_partial_update_states[segment_id]));

    TRY_CATCH_BAD_ALLOC(_auto_increment_partial_update_states[segment_id].write_column->append_selective(
            *read_column[0], idxes.data(), 0, idxes.size()));
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
    _auto_increment_delete_pks[segment_id] = _upserts[segment_id]->standalone_pk_column()->clone_empty();
    std::vector<uint32_t> delete_idxes;
    const int64* data = nullptr;
    RawDataVisitor visitor;
    RETURN_IF_ERROR(_auto_increment_partial_update_states[segment_id].write_column->accept(&visitor));
    data = reinterpret_cast<const int64*>(visitor.result());

    // just check the rows which are not exist in the previous version
    // because the rows exist in the previous version may contain 0 which are specified by the user
    for (unsigned int row_idx : _auto_increment_partial_update_states[segment_id].rowids) {
        if (data[row_idx] == 0) {
            delete_idxes.emplace_back(row_idx);
        }
    }

    if (delete_idxes.size() != 0) {
        TRY_CATCH_BAD_ALLOC(_auto_increment_delete_pks[segment_id]->append_selective(
                *(_upserts[segment_id]->standalone_pk_column()), delete_idxes.data(), 0, delete_idxes.size()));
        _memory_usage += _auto_increment_delete_pks[segment_id]->memory_usage();
    }
    return Status::OK();
}

Status RowsetUpdateState::_prepare_partial_update_states(uint32_t segment_id, const RowsetUpdateStateParams& params,
                                                         bool need_lock) {
    CHECK_MEM_LIMIT("RowsetUpdateState::_prepare_partial_update_states");
    std::vector<ColumnId> read_column_ids = get_read_columns_ids(params.op_write, params.tablet_schema);

    // _column_to_expr_value is already populated by prepare().
    auto read_column_schema = ChunkHelper::convert_schema(params.tablet_schema, read_column_ids);
    // column list that need to read from source segment
    MutableColumns read_columns;
    read_columns.resize(read_column_ids.size());
    _partial_update_states[segment_id].write_columns.resize(read_columns.size());
    _partial_update_states[segment_id].src_rss_rowids.resize(_upserts[segment_id]->standalone_pk_column()->size());
    for (uint32_t j = 0; j < read_columns.size(); ++j) {
        auto column = ChunkFactory::column_from_field(*read_column_schema.field(j).get());
        read_columns[j] = column->clone_empty();
        _partial_update_states[segment_id].write_columns[j] = column->clone_empty();
    }

    // use upsert to get rowids for this segment
    RETURN_IF_ERROR(params.tablet->update_mgr()->get_rowids_from_pkindex(
            params.tablet->id(), _base_versions[segment_id], _upserts[segment_id]->standalone_pk_column(),
            &(_partial_update_states[segment_id].src_rss_rowids), need_lock));

    size_t num_default = 0;
    std::map<uint32_t, std::vector<uint32_t>> rowids_by_rssid;
    vector<uint32_t> idxes;
    plan_read_by_rssid(_partial_update_states[segment_id].src_rss_rowids, &num_default, &rowids_by_rssid, &idxes);
    size_t total_rows = _partial_update_states[segment_id].src_rss_rowids.size();
    // get column values by rowid, also get default values if needed
    RETURN_IF_ERROR(params.tablet->update_mgr()->get_column_values(
            params, read_column_ids, num_default > 0, rowids_by_rssid, &read_columns, &_column_to_expr_value));
    for (size_t col_idx = 0; col_idx < read_column_ids.size(); col_idx++) {
        TRY_CATCH_BAD_ALLOC(_partial_update_states[segment_id].write_columns[col_idx]->append_selective(
                *read_columns[col_idx], idxes.data(), 0, idxes.size()));
        _memory_usage += _partial_update_states[segment_id].write_columns[col_idx]->memory_usage();
    }
    TRACE_COUNTER_INCREMENT("partial_upt_total_rows", total_rows);
    TRACE_COUNTER_INCREMENT("partial_upt_default_rows", num_default);

    return Status::OK();
}

StatusOr<bool> RowsetUpdateState::file_exist(const std::string& full_path) {
    ASSIGN_OR_RETURN(auto fs, FileSystemFactory::CreateSharedFromString(full_path));
    auto st = fs->path_exists(full_path);
    if (st.ok()) {
        return true;
    } else if (st.is_not_found()) {
        return false;
    } else {
        return st;
    }
}

Status RowsetUpdateState::rewrite_segment(uint32_t segment_id, int64_t txn_id, const RowsetUpdateStateParams& params,
                                          std::map<int, SegmentFileInfo>* replace_segments,
                                          std::vector<FileMetaPB>* orphan_files) {
    CHECK_MEM_LIMIT("RowsetUpdateState::rewrite_segment");
    TRACE_COUNTER_SCOPE_LATENCY_US("rewrite_segment_latency_us");
    const RowsetMetadata& rowset_meta = params.op_write.rowset();
    auto root_path = params.tablet->metadata_root_location();
    ASSIGN_OR_RETURN(auto fs, FileSystemFactory::CreateSharedFromString(root_path));
    std::shared_ptr<TabletSchema> tablet_schema = std::make_shared<TabletSchema>(params.metadata->schema());
    // get rowset schema
    if (!params.op_write.has_txn_meta() || params.op_write.rewrite_segments_meta_size() == 0 ||
        rowset_meta.num_rows() == 0) {
        return Status::OK();
    }
    RETURN_ERROR_IF_FALSE(params.op_write.rewrite_segments_meta_size() == rowset_meta.segment_metas_size());
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
    const auto& src_seg_meta = rowset_meta.segment_metas(segment_id);
    const auto& src_path = src_seg_meta.filename();
    const auto& dest_path = gen_segment_filename(txn_id);
    DCHECK(src_path != dest_path);

    FileInfo src{.path = params.tablet->segment_location(src_path)};
    if (src_seg_meta.has_size()) {
        src.size = src_seg_meta.size();
    }
    if (src_seg_meta.has_encryption_meta()) {
        src.encryption_meta = src_seg_meta.encryption_meta();
    }
    if (src_seg_meta.has_bundle_file_offset()) {
        src.bundle_file_offset = src_seg_meta.bundle_file_offset();
    }

    // The dest segment is a full rewrite, so mirror the normal lake writer's vector-index
    // handling: sync indexes are built inline at the reader-visible location-provider path;
    // async builds are deferred to the FE-scheduled VectorIndexBuildTask; and the dest segment's
    // vector_index_ids are persisted via the replace FileInfo. Without this the SegmentWriter
    // would fall back to the IndexDescriptor path (unreachable in shared-data, since reads/builds
    // key off the segment-name path) and the dest segment would carry no vector_index_ids,
    // silently dropping the index after publish.
    ASSIGN_OR_RETURN(auto vector_index_opts, resolve_rewrite_vector_index_options(params, dest_path));
    const bool defer_vector_index_build = vector_index_opts.defer_build;

    const bool flexible = _is_flexible_partial_update(params.op_write);

    int64_t t_rewrite_start = MonotonicMillis();
    if (flexible) {
        // ============================ FLEXIBLE-on-ROW masked full-row rewrite ============================
        // Each row of this load updated a DIFFERENT subset of value columns. The .upt is a DENSE UNION
        // (partial_update_column_unique_ids) with NULL placeholders for cells a row did NOT touch. Feeding
        // those placeholders verbatim into the rewrite would overwrite base with NULL => SILENT DATA LOSS.
        // So we apply a per-ROW covered-column MASK: a cell takes the .upt value ONLY if the row's set-id
        // covers that column; otherwise it keeps the base value already in write_columns (or DEFAULT for a
        // brand-new key, which get_column_values filled via the with_default path). The result is a complete
        // set of value columns, which rewrite_full_row_lake writes as a FRESH full segment.
        //
        // v1 scope: auto-increment + flexible is rejected (the auto-increment write_column machinery is not
        // wired through the masked merge). Version-gate is DEFERRED for this POC (homogeneous cluster).
        // TODO(flexible-row): add a version gate before mixing old/new CN apply of flexible+ROW rowsets.
        if (has_auto_increment_col) {
            return Status::NotSupported("FLEXIBLE-on-ROW partial update does not support auto-increment columns in v1");
        }
        RETURN_ERROR_IF_FALSE(has_partial_update_state(params),
                              "FLEXIBLE-on-ROW: expected populated partial update state (union non-empty)");

        // write_columns holds the base values of EVERY non-key value column (see get_read_columns_ids
        // flexible branch), in unmodified_column_ids order == schema order. They are in UPSERT/physical row
        // order, the SAME order as the .upt rows and the set_ids vector.
        MutableColumns& write_columns = _partial_update_states[segment_id].write_columns;
        RETURN_ERROR_IF_FALSE(write_columns.size() == unmodified_column_ids.size(),
                              "FLEXIBLE-on-ROW: write_columns/unmodified_column_ids length mismatch");
        const size_t num_upsert_rows = write_columns.empty() ? 0 : write_columns[0]->size();
        // Release-build OOB backstop: update_rows below only DCHECKs the destination index, so any
        // per-column length divergence would be a raw out-of-bounds write (the historical OOB class).
        // _prepare_partial_update_states fills every column with the same append_selective(idxes), so this
        // holds today; assert it loudly so a future divergence fails instead of corrupting memory.
        for (size_t p = 0; p < write_columns.size(); ++p) {
            RETURN_ERROR_IF_FALSE(write_columns[p] != nullptr && write_columns[p]->size() == num_upsert_rows,
                                  "FLEXIBLE-on-ROW: write_column row count != num_upsert_rows");
        }

        // (a) per-row set-id vector for this .upt segment, physical row order. ASSERT equal length: set_ids[r],
        //     write_columns[*][r] and the .upt row r all refer to the SAME upsert row.
        ASSIGN_OR_RETURN(std::vector<int32_t> set_ids, _read_cset_column_from_upt(segment_id));
        RETURN_ERROR_IF_FALSE(set_ids.size() == num_upsert_rows,
                              strings::Substitute("FLEXIBLE-on-ROW: __cset__ row count $0 != upsert row count $1",
                                                  set_ids.size(), num_upsert_rows));

        // (b) decode the per-set covered value-column uid sets.
        std::vector<std::set<ColumnUID>> covered = _decode_distinct_column_sets(txn_meta);

        // Map base value-column uid -> its position in write_columns (== position in unmodified_column_ids).
        std::unordered_map<ColumnUID, size_t> uid_to_write_pos;
        uid_to_write_pos.reserve(unmodified_column_ids.size());
        for (size_t p = 0; p < unmodified_column_ids.size(); ++p) {
            uid_to_write_pos.emplace(params.tablet_schema->column(unmodified_column_ids[p]).unique_id(), p);
        }

        // (c) read the .upt UNION value columns (partial_update_column_unique_ids, excluding the reserved
        //     __cset__ uid which is never in that list) into one chunk, in .upt physical row order. Rows
        //     align with write_columns by bare index r. NOTE: TabletSchema::create_with_uid emits columns in
        //     SCHEMA (cid) order, NOT in union_uids order, so we resolve each read column's uid from the
        //     resulting schema (union_schema_ts->column(uc).unique_id()), never by assuming union_uids order.
        // partial_update_column_unique_ids is the UNION and INCLUDES the PK/key column uids (the load
        // carries the key per row) and the per-row set-ids in distinct_column_sets also list the key.
        // The masked merge only overlays NON-KEY value columns (keys are read straight from the .upt by
        // rewrite_full_row_lake and never live in write_columns), so the key/auto-increment uids must be
        // filtered out of union_uids here -- exactly as the column-mode handler does
        // (column_mode_partial_update_handler.cpp:987). Without this, a union column at uid==key has no
        // write_columns slot and the merge errors, the publish retries forever, and no update lands.
        std::vector<ColumnUID> union_uids;
        union_uids.reserve(txn_meta.partial_update_column_unique_ids_size());
        for (uint32_t uid : txn_meta.partial_update_column_unique_ids()) {
            if (static_cast<int32_t>(uid) == kCsetReservedColumnUid) {
                continue; // defensive: writer already excludes it
            }
            int32_t cid = params.tablet_schema->field_index(static_cast<ColumnUID>(uid));
            if (cid < 0 || params.tablet_schema->column(cid).is_key() ||
                params.tablet_schema->column(cid).is_auto_increment()) {
                continue; // keys are read from the .upt by the full-row writer; not overlaid here
            }
            union_uids.push_back(static_cast<ColumnUID>(uid));
        }
        auto union_schema_ts = TabletSchema::create_with_uid(params.tablet_schema, union_uids);
        Schema union_schema = ChunkHelper::convert_schema(union_schema_ts);

        OlapReaderStatistics upt_stats;
        // Read the union value columns via the schema-BOUND (cache-bypassing) iterator, resolving each
        // column against the .upt footer BY UNIQUE-ID -- exactly like the __cset__ read above. The plain
        // get_each_segment_iterator opens the .upt with the rowset's BASE schema and resolves the read
        // columns POSITIONALLY, which mis-reads a partial .upt whose physical layout is the write schema
        // [keys..., union value cols..., __cset__]: union_schema [c1,c2,c3] would read segment positions
        // 0,1,2 = [id,c1,c2], shifting every value column by the key count.
        ASSIGN_OR_RETURN(auto upt_iters, _rowset_ptr->get_each_segment_iterator_with_schema(
                                                 union_schema, union_schema_ts, true, &upt_stats));
        RETURN_ERROR_IF_FALSE(segment_id < upt_iters.size(),
                              "FLEXIBLE-on-ROW: segment_id out of range reading .upt union columns");
        DeferOp close_upt_iters([&]() {
            for (auto& it : upt_iters) {
                if (it != nullptr) {
                    it->close();
                }
            }
        });
        ChunkUniquePtr upt_chunk = ChunkFactory::new_chunk(union_schema, DEFAULT_CHUNK_SIZE);
        RETURN_IF_ERROR(read_chunk_from_update_file(upt_iters[segment_id], upt_chunk));
        RETURN_ERROR_IF_FALSE(upt_chunk->num_rows() == num_upsert_rows,
                              strings::Substitute("FLEXIBLE-on-ROW: .upt union row count $0 != upsert row count $1",
                                                  upt_chunk->num_rows(), num_upsert_rows));
        RETURN_ERROR_IF_FALSE(upt_chunk->num_columns() == union_schema_ts->num_columns(),
                              "FLEXIBLE-on-ROW: .upt union chunk column count != union schema column count");

        // (d) MASKED MERGE: for each union value column (uid u), write_columns[p][r] takes the .upt value
        //     ONLY for rows r whose set-id covers u; every other row keeps its base value. We build the
        //     covered-row index list I and gather the corresponding .upt rows, then update_rows lands them
        //     at exactly those destination positions. uc indexes the read schema; u is read FROM that schema.
        for (size_t uc = 0; uc < union_schema_ts->num_columns(); ++uc) {
            const ColumnUID u = union_schema_ts->column(uc).unique_id();
            auto pos_it = uid_to_write_pos.find(u);
            // Every union value-column uid is a non-key value column, so it must have a write_columns slot.
            RETURN_ERROR_IF_FALSE(pos_it != uid_to_write_pos.end(),
                                  strings::Substitute("FLEXIBLE-on-ROW: union uid $0 has no base column slot", u));
            const size_t p = pos_it->second;
            const auto& upt_col = upt_chunk->get_column_by_index(uc);

            std::vector<uint32_t> covered_rows;
            covered_rows.reserve(num_upsert_rows);
            for (uint32_t r = 0; r < num_upsert_rows; ++r) {
                const int32_t set_id = set_ids[r];
                // (e) bounds-guard set_id (mirror handler.cpp:622-626). upt_rowid==r is bounded by the equal-
                //     length asserts above.
                if (set_id < 0 || static_cast<size_t>(set_id) >= covered.size()) {
                    return Status::InternalError(strings::Substitute(
                            "FLEXIBLE-on-ROW: set-id $0 out of range of distinct_column_sets (size $1)", set_id,
                            covered.size()));
                }
                if (covered[set_id].count(u) > 0) {
                    covered_rows.push_back(r);
                }
            }
            if (covered_rows.empty()) {
                continue; // no row updated this column; base values (already in write_columns) stand.
            }
            // Gather the covered rows' .upt values, then overlay them onto the base write_column.
            auto gathered = upt_col->clone_empty();
            TRY_CATCH_BAD_ALLOC(gathered->append_selective(*upt_col, covered_rows.data(), 0, covered_rows.size()));
            RETURN_IF_EXCEPTION(write_columns[p]->update_rows(*gathered, covered_rows.data()));
        }

        // (f) route to the FRESH full-row writer: key columns read from base + the merged full value
        //     columns. __cset__ is naturally DROPPED (its reserved uid is not in tablet_schema).
        SegmentFileInfo file_info;
        file_info.path = params.tablet->segment_location(dest_path);
        RETURN_IF_ERROR(SegmentRewriter::rewrite_full_row_lake(
                src, &file_info, params.tablet_schema, unmodified_column_ids, write_columns, segment_id, params.tablet,
                std::move(vector_index_opts), &file_info.vector_index_ids));
        file_info.path = dest_path;
        // The dest is a complete fresh segment written through column writers for the whole schema, so any
        // sync vector index is built inline here (its ids captured into file_info.vector_index_ids by
        // rewrite_full_row_lake), carrying nothing from src -- mirrors rewrite_auto_increment_lake.
        stamp_rewrite_vector_index_owner(params, &file_info);
        (*replace_segments)[segment_id] = file_info;
    } else if (has_auto_increment_partial_update_state(params) &&
               !_auto_increment_partial_update_states[segment_id].skip_rewrite) {
        SegmentFileInfo file_info;
        file_info.path = params.tablet->segment_location(dest_path);
        RETURN_IF_ERROR(SegmentRewriter::rewrite_auto_increment_lake(
                src, &file_info, params.tablet_schema, _auto_increment_partial_update_states[segment_id],
                unmodified_column_ids,
                has_partial_update_state(params) ? &_partial_update_states[segment_id].write_columns : nullptr,
                params.tablet, std::move(vector_index_opts), &file_info.vector_index_ids));
        file_info.path = dest_path;
        stamp_rewrite_vector_index_owner(params, &file_info);
        (*replace_segments)[segment_id] = file_info;
    } else if (has_partial_update_state(params)) {
        const FooterPointerPB& partial_rowset_footer = txn_meta.partial_rowset_footers(segment_id);
        SegmentFileInfo file_info;
        file_info.path = params.tablet->segment_location(dest_path);

        RETURN_IF_ERROR(SegmentRewriter::rewrite_partial_update(
                src, &file_info, params.tablet_schema, unmodified_column_ids,
                _partial_update_states[segment_id].write_columns, segment_id, partial_rowset_footer,
                {root_path, std::to_string(rowset_meta.id())}, std::move(vector_index_opts),
                &file_info.vector_index_ids));
        file_info.path = dest_path;

        // Sync indexes on the *updated* columns are not rebuilt by the rewrite (their data is
        // raw-copied without a column writer); carry the src partial segment's .vi over to the
        // dest segment name instead. Async mode normally needs no carry — the rewrite records the
        // scheduled ids itself — except on the copy-only fast path (no unmodified columns left,
        // reachable when a schema change lands between the partial write and its publish): there
        // the rewrite never sees a SegmentWriter, so carry the src's scheduled ids (async has no
        // .vi file to copy) lest the metadata refresh wipe them.
        if (!defer_vector_index_build) {
            RETURN_IF_ERROR(carry_src_segment_vector_indexes(params, src_seg_meta, src_path, dest_path, &file_info));
        } else if (unmodified_column_ids.empty()) {
            // Copy-only async fast path (see the block comment above).
            for (int64_t index_id : src_seg_meta.vector_index_ids()) {
                file_info.vector_index_ids.push_back(index_id);
            }
        }
        stamp_rewrite_vector_index_owner(params, &file_info);
        (*replace_segments)[segment_id] = file_info;
    } else {
        need_rename = false;
    }
    int64_t t_rewrite_end = MonotonicMillis();
    LOG(INFO) << strings::Substitute(
            "lake apply partial segment tablet:$0 rowset:$1 seg:$2 #column:$3 #rewrite:$4ms [$5 -> $6]",
            params.tablet->id(), rowset_meta.id(), segment_id, unmodified_column_ids.size(),
            t_rewrite_end - t_rewrite_start, src_path, dest_path);

    // rename segment file
    if (need_rename) {
        // after rename, add old segment to orphan files, for gc later.
        FileMetaPB file_meta;
        file_meta.set_name(src_seg_meta.filename());
        // A bundled segment is physically shared with sibling tablets, but its shared-ness is
        // encoded by bundle_file_offset rather than the `shared` flag. The orphan FileMetaPB only
        // carries `shared`, so mirror is_shared_segment() here (shared || has_bundle_file_offset).
        // Otherwise vacuum's collect_garbage_files sees file.shared()==false, routes the orphan to
        // the plain deleter, and deletes the physical bundle file even while a sibling tablet still
        // references it in a live rowset -- wedging that sibling's publish.
        file_meta.set_shared(src_seg_meta.shared() || src_seg_meta.has_bundle_file_offset());
        orphan_files->push_back(std::move(file_meta));
        // A sync .vi built for the replaced src segment is keyed on the src segment name and
        // unreachable after the replace (the dest segment has its own copy); orphan it too. In
        // async mode the src ids only marked a scheduled build — no .vi file ever existed.
        if (!defer_vector_index_build) {
            // The replaced src .vi was named by the src segment's owner tablet id. Carry the src's
            // shared flag (like the segment orphan above) so a split-shared .vi still referenced by
            // sibling tablets is routed through the shared-file deleter and delayed, not deleted.
            for (int64_t index_id : src_seg_meta.vector_index_ids()) {
                FileMetaPB vi_meta;
                vi_meta.set_name(gen_vector_index_filename_for_segment(src_seg_meta, index_id));
                if (src_seg_meta.has_shared()) {
                    vi_meta.set_shared(src_seg_meta.shared());
                }
                orphan_files->push_back(std::move(vi_meta));
            }
        }
    }
    TRACE("end rewrite segment");
    return Status::OK();
}

Status RowsetUpdateState::_resolve_conflict(uint32_t segment_id, const RowsetUpdateStateParams& params,
                                            int64_t base_version) {
    CHECK_MEM_LIMIT("RowsetUpdateState::_resolve_conflict");
    // There are two cases that we must resolve conflict here:
    // 1. Current transaction's base version isn't equal latest base version, which means that conflict happens.
    // 2. We use batch publish here. This transaction may conflict with a transaction in the same batch.
    if (base_version == _base_versions[segment_id] && base_version + 1 == params.metadata->version()) {
        return Status::OK();
    }
    _base_versions[segment_id] = base_version;
    TRACE_COUNTER_SCOPE_LATENCY_US("resolve_conflict_latency_us");
    // skip resolve conflict when not partial update happen.
    if (!params.op_write.has_txn_meta() || params.op_write.rowset().segment_metas_size() == 0) {
        return Status::OK();
    }

    // use upserts to get rowids in this segment
    std::vector<uint64_t> new_rss_rowids(_upserts[segment_id]->standalone_pk_column()->size());
    RETURN_IF_ERROR(params.tablet->update_mgr()->get_rowids_from_pkindex(
            params.tablet->id(), _base_versions[segment_id], _upserts[segment_id]->standalone_pk_column(),
            &new_rss_rowids, false));

    size_t total_conflicts = 0;
    std::shared_ptr<TabletSchema> tablet_schema = std::make_shared<TabletSchema>(params.metadata->schema());
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
        MutableColumns read_columns;
        read_columns.resize(_partial_update_states[segment_id].write_columns.size());
        for (uint32_t i = 0; i < read_columns.size(); ++i) {
            read_columns[i] = _partial_update_states[segment_id].write_columns[i]->clone_empty();
        }
        size_t num_default = 0;
        std::map<uint32_t, std::vector<uint32_t>> rowids_by_rssid;
        std::vector<uint32_t> read_idxes;
        plan_read_by_rssid(conflict_rowids, &num_default, &rowids_by_rssid, &read_idxes);
        DCHECK_EQ(conflict_idxes.size(), read_idxes.size());
        RETURN_IF_ERROR(params.tablet->update_mgr()->get_column_values(
                params, read_column_ids, num_default > 0, rowids_by_rssid, &read_columns, &_column_to_expr_value));

        for (size_t col_idx = 0; col_idx < read_column_ids.size(); col_idx++) {
            MutableColumnPtr new_write_column =
                    _partial_update_states[segment_id].write_columns[col_idx]->clone_empty();
            TRY_CATCH_BAD_ALLOC(new_write_column->append_selective(*read_columns[col_idx], read_idxes.data(), 0,
                                                                   read_idxes.size()));
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
        MutableColumns auto_increment_read_column;
        auto_increment_read_column.resize(1);
        auto_increment_read_column[0] = _auto_increment_partial_update_states[segment_id].write_column->clone_empty();
        RETURN_IF_ERROR(params.tablet->update_mgr()->get_column_values(
                params, column_id, new_rows > 0, rowids_by_rssid, &auto_increment_read_column, &_column_to_expr_value,
                &_auto_increment_partial_update_states[segment_id]));

        MutableColumnPtr new_write_column =
                _auto_increment_partial_update_states[segment_id].write_column->clone_empty();
        TRY_CATCH_BAD_ALLOC(
                new_write_column->append_selective(*auto_increment_read_column[0], idxes.data(), 0, idxes.size()));
        RETURN_IF_EXCEPTION(_auto_increment_partial_update_states[segment_id].write_column->update_rows(
                *new_write_column, conflict_idxes.data()));

        // reslove delete-partial update conflict base on latest column values
        _auto_increment_delete_pks[segment_id].reset();
        _auto_increment_delete_pks[segment_id] = _upserts[segment_id]->standalone_pk_column()->clone_empty();
        std::vector<uint32_t> delete_idxes;
        const int64* data = nullptr;
        RawDataVisitor visitor;
        RETURN_IF_ERROR(_auto_increment_partial_update_states[segment_id].write_column->accept(&visitor));
        data = reinterpret_cast<const int64*>(visitor.result());

        // just check the rows which are not exist in the previous version
        // because the rows exist in the previous version may contain 0 which are specified by the user
        for (unsigned int row_idx : _auto_increment_partial_update_states[segment_id].rowids) {
            if (data[row_idx] == 0) {
                delete_idxes.emplace_back(row_idx);
            }
        }

        if (delete_idxes.size() != 0) {
            TRY_CATCH_BAD_ALLOC(_auto_increment_delete_pks[segment_id]->append_selective(
                    *(_upserts[segment_id]->standalone_pk_column()), delete_idxes.data(), 0, delete_idxes.size()));
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

void RowsetUpdateState::release_segment_partial_state(uint32_t segment_id) {
    // Release write_columns and auto-increment partial state used only by rewrite_segment.
    // Keep _upserts and _auto_increment_delete_pks for Phase 2 (_do_update + index.erase).
    _memory_usage -= _partial_update_states[segment_id].memory_usage();
    _partial_update_states[segment_id].reset();
    _memory_usage -= _auto_increment_partial_update_states[segment_id].memory_usage();
    _auto_increment_partial_update_states[segment_id].reset();
}

Status RowsetUpdateState::prepare(const RowsetUpdateStateParams& params) {
    // Initialize shared state: rowset, per-segment vectors, segment iterators, column expr values.
    // Must be called once on the main thread before any load_segment call.
    if (_rowset_ptr == nullptr) {
        _rowset_meta_ptr = std::make_unique<const RowsetMetadata>(params.op_write.rowset());
        _rowset_ptr = std::make_unique<Rowset>(params.tablet->tablet_mgr(), params.tablet->id(), _rowset_meta_ptr.get(),
                                               -1 /*unused*/, params.tablet_schema);
    }
    if (_upserts.size() != _rowset_ptr->num_segments()) {
        TRY_CATCH_BAD_ALLOC({
            _upserts.resize(_rowset_ptr->num_segments());
            _base_versions.resize(_rowset_ptr->num_segments());
            _partial_update_states.resize(_rowset_ptr->num_segments());
            _auto_increment_partial_update_states.resize(_rowset_ptr->num_segments());
            _auto_increment_delete_pks.resize(_rowset_ptr->num_segments());
        });
    }
    if (_segment_iters.empty()) {
        vector<uint32_t> pk_columns;
        pk_columns.reserve(params.tablet_schema->num_key_columns());
        for (size_t i = 0; i < params.tablet_schema->num_key_columns(); i++) {
            pk_columns.push_back((uint32_t)i);
        }
        _pkey_schema = ChunkHelper::convert_schema(params.tablet_schema, pk_columns);
        ASSIGN_OR_RETURN(_segment_iters, _rowset_ptr->get_each_segment_iterator(_pkey_schema, false, &_stats));
    }
    if (_column_to_expr_value.empty() && params.op_write.has_txn_meta()) {
        for (auto& entry : params.op_write.txn_meta().column_to_expr_value()) {
            _column_to_expr_value.insert({entry.first, entry.second});
        }
    }
    return Status::OK();
}

Status RowsetUpdateState::load_delete(uint32_t del_id, const RowsetUpdateStateParams& params) {
    CHECK_MEM_LIMIT("RowsetUpdateState::load_delete");
    // always one file for now.
    TRACE_COUNTER_SCOPE_LATENCY_US("load_delete_us");
    _deletes.resize(params.op_write.dels_meta_size());
    if (_deletes[del_id] != nullptr) {
        // Already load.
        return Status::OK();
    }
    vector<uint32_t> pk_columns;
    pk_columns.reserve(params.tablet_schema->num_key_columns());
    for (size_t i = 0; i < params.tablet_schema->num_key_columns(); i++) {
        pk_columns.push_back((uint32_t)i);
    }
    Schema pkey_schema = ChunkHelper::convert_schema(params.tablet_schema, pk_columns);
    MutableColumnPtr pk_column;
    ASSIGN_OR_RETURN(auto pk_encoding_type, params.tablet_schema->primary_key_encoding_type_or_error());
    RETURN_IF_ERROR(PrimaryKeyEncoder::create_column(pkey_schema, &pk_column, pk_encoding_type));

    auto root_path = params.tablet->metadata_root_location();
    ASSIGN_OR_RETURN(auto fs, FileSystemFactory::CreateSharedFromString(root_path));
    const auto& del_meta = params.op_write.dels_meta(del_id);
    const std::string& path = del_meta.name();
    RandomAccessFileOptions opts;
    if (del_meta.has_encryption_meta()) {
        // When upgrade from old version, `encryption_meta` could be empty.
        auto& meta = del_meta.encryption_meta();
        if (!meta.empty()) {
            ASSIGN_OR_RETURN(auto info, KeyCache::instance().unwrap_encryption_meta(meta));
            opts.encryption_info = std::move(info);
        }
    }
    ASSIGN_OR_RETURN(auto read_file, fs->new_random_access_file(opts, params.tablet->del_location(path)));
    ASSIGN_OR_RETURN(auto read_buffer, read_file->read_all());
    auto col = pk_column->clone();
    using Serd = serde::ColumnArraySerde;
    const auto* begin = reinterpret_cast<const uint8_t*>(read_buffer.data());
    const auto* end = begin + read_buffer.size();
    RETURN_IF_ERROR(Serd::deserialize(begin, end, col.get()));
    _memory_usage += col->memory_usage();
    _deletes[del_id] = std::move(col);
    TRACE("end read $0-th deletes files", del_id);
    return Status::OK();
}

void RowsetUpdateState::release_delete(uint32_t del_id) {
    _memory_usage -= _deletes[del_id] ? _deletes[del_id]->memory_usage() : 0;
    _deletes[del_id].reset();
}

const MutableColumnPtr& RowsetUpdateState::auto_increment_deletes(uint32_t segment_id) const {
    return _auto_increment_delete_pks[segment_id];
}

std::string RowsetUpdateState::to_string() const {
    return strings::Substitute("RowsetUpdateState tablet:$0", _tablet_id);
}

} // namespace starrocks::lake
