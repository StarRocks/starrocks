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

#include "storage/lake/column_mode_partial_update_handler.h"

#include <algorithm>
#include <map>
#include <roaring/roaring.hh>
#include <set>

#include "base/debug/trace.h"
#include "base/phmap/phmap.h"
#include "base/time/time.h"
#include "base/utility/defer_op.h"
#include "column/chunk_factory.h"
#include "column/chunk_schema_helper.h"
#include "column/serde/column_array_serde.h"
#include "common/config_compaction_fwd.h"
#include "common/config_primary_key_fwd.h"
#include "common/config_rowset_fwd.h"
#include "common/flexible_partial_update.h"
#include "common/tracer.h"
#include "fs/fs_factory.h"
#include "fs/fs_util.h"
#include "gutil/strings/substitute.h"
#include "platform/key_cache.h"
#include "runtime/current_thread.h"
#include "runtime/runtime_env.h"
#include "storage/chunk_helper.h"
#include "storage/delta_column_group.h"
#include "storage/lake/column_mode_partial_update_handler.h"
#include "storage/lake/filenames.h"
#include "storage/lake/meta_file.h"
#include "storage/lake/update_manager.h"
#include "storage/options.h"
#include "storage/primary_index.h"
#include "storage/rows_mapper.h"
#include "storage/rowset/column_iterator.h"
#include "storage/rowset/default_value_column_iterator.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/rowset_options.h"
#include "storage/rowset/segment.h"
#include "storage/rowset/segment_options.h"
#include "storage/rowset/segment_rewriter.h"
#include "storage/storage_metrics.h"
#include "storage/tablet.h"
#include "storage_primitive/primary_key_encoder.h"

namespace starrocks::lake {

// Literal name of the hidden per-row set-id column (the SET-ID SPINE). FE injects it as a load slot
// immediately before "__op"; it survives into the `.upt` as a real partial-update column. The writer
// (Agent B's lake delta_writer) appends it with the reserved unique-id kCsetReservedColumnUid; readers
// resolve it against the `.upt` footer BY THAT UNIQUE-ID (segment v2 maps schema fields to physical
// columns by unique_id), not by name -- the name is kept only for diagnostics. Reconciled with Agent B:
// kSDCGCsetColumnName == flexible_partial_update.h LOAD_CSET_COLUMN ("__cset__"). Defined at namespace
// top so both the FLEX_COLUMN dense overlay and the packed-sparse path (declared later) can use it.
static constexpr const char* kSDCGCsetColumnName = "__cset__";

// : _tablet_metadata(std::move(tablet_metadata)) {}

LakeDeltaColumnGroupLoader::LakeDeltaColumnGroupLoader(TabletMetadataPtr tablet_metadata)
        : _tablet_metadata(std::move(tablet_metadata)) {}

Status LakeDeltaColumnGroupLoader::load(const TabletSegmentId& tsid, int64_t version, DeltaColumnGroupList* pdcgs) {
    auto iter = _tablet_metadata->dcg_meta().dcgs().find(tsid.segment_id);
    if (iter != _tablet_metadata->dcg_meta().dcgs().end()) {
        auto dcg_ptr = std::make_shared<DeltaColumnGroup>();
        RETURN_IF_ERROR(dcg_ptr->load(_tablet_metadata->version(), iter->second));
        pdcgs->push_back(std::move(dcg_ptr));
    }
    return Status::OK();
}

Status LakeDeltaColumnGroupLoader::load(int64_t tablet_id, RowsetId rowsetid, uint32_t segment_id, int64_t version,
                                        DeltaColumnGroupList* pdcgs) {
    return Status::NotSupported("LakeDeltaColumnGroupLoader::load not supported");
}

ColumnModePartialUpdateHandler::ColumnModePartialUpdateHandler(int64_t base_version, int64_t txn_id,
                                                               MemTracker* tracker)
        : _base_version(base_version), _txn_id(txn_id), _tracker(tracker) {}

ColumnModePartialUpdateHandler::~ColumnModePartialUpdateHandler() {
    _tracker->release(_memory_usage);
}

Status ColumnModePartialUpdateHandler::_load_update_state(const RowsetUpdateStateParams& params) {
    if (_rowset_ptr == nullptr) {
        _rowset_meta_ptr = std::make_unique<const RowsetMetadata>(params.op_write.rowset());
        _rowset_ptr = std::make_unique<Rowset>(params.tablet->tablet_mgr(), params.tablet->id(), _rowset_meta_ptr.get(),
                                               -1 /*unused*/, params.tablet_schema);
    }

    const uint32_t num_segments = _rowset_ptr->num_segments();
    if (num_segments == 0) {
        return Status::OK();
    }

    // Build PK schema
    vector<uint32_t> pk_columns;
    pk_columns.reserve(params.tablet_schema->num_key_columns());
    for (size_t i = 0; i < params.tablet_schema->num_key_columns(); i++) {
        pk_columns.push_back((uint32_t)i);
    }
    Schema pkey_schema = ChunkHelper::convert_schema(params.tablet_schema, pk_columns);
    ASSIGN_OR_RETURN(auto pk_encoding_type, params.tablet_schema->primary_key_encoding_type_or_error());

    // Create segment iterators for update files
    OlapReaderStatistics stats;
    ASSIGN_OR_RETURN(auto segment_iters, _rowset_ptr->get_each_segment_iterator(pkey_schema, true, &stats));
    RETURN_ERROR_IF_FALSE(segment_iters.size() == num_segments);

    // Create lazy-load SegmentPKIterators with deferred first load.
    // defer_data_load=true avoids loading the first chunk during init(), so that
    // all iterators can be created without a memory spike. The actual data load
    // happens on-demand when each iterator is first consumed in batch_parallel_get_rss_rowids.
    std::vector<SegmentPKIteratorPtr> pk_iters(num_segments);
    for (uint32_t i = 0; i < num_segments; i++) {
        pk_iters[i] = std::make_unique<SegmentPKIterator>();
        RETURN_IF_ERROR(pk_iters[i]->init(segment_iters[i], pkey_schema, true /*lazy_load*/, pk_encoding_type,
                                          true /*defer_data_load*/));
    }

    // Parallel query PK index: each segment's PKs are loaded chunk-by-chunk (lazy)
    // and each chunk is queried against the index in parallel via thread pool.
    std::vector<std::vector<uint64_t>> rss_rowids_per_segment;
    RETURN_IF_ERROR(params.tablet->update_mgr()->batch_get_rss_rowids_from_pkindex(
            params.tablet->id(), _base_version, pk_iters, &rss_rowids_per_segment, false /*need_lock*/));

    // Build rss_rowid_to_update_rowid mapping for each update segment. Pass each
    // segment's physical rowid base (range_start, captured by the iterator during
    // the query above) so insert_rowids are physical positions in the segment file
    // — see ColumnPartialUpdateState::build_rss_rowid_to_update_rowid.
    _partial_update_states.resize(num_segments);
    for (uint32_t i = 0; i < num_segments; i++) {
        _partial_update_states[i].src_rss_rowids = std::move(rss_rowids_per_segment[i]);
        _partial_update_states[i].build_rss_rowid_to_update_rowid(pk_iters[i]->physical_rowid_base());
        _partial_update_states[i].inited = true;
    }

    return Status::OK();
}

// this function build delta writer for delta column group's file.(end with `.col`)
StatusOr<std::unique_ptr<SegmentWriter>> ColumnModePartialUpdateHandler::_prepare_delta_column_group_writer(
        const RowsetUpdateStateParams& params, const std::shared_ptr<TabletSchema>& tschema) {
    const std::string path = params.tablet->segment_location(gen_cols_filename(_txn_id));
    WritableFileOptions opts{.sync_on_close = true, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
    SegmentWriterOptions writer_options;

    if (auto metadata = params.tablet->tablet_mgr()->get_latest_cached_tablet_metadata(params.tablet->id());
        metadata && metadata->has_flat_json_config()) {
        writer_options.flat_json_config = std::make_shared<FlatJsonConfig>();
        writer_options.flat_json_config->update(metadata->flat_json_config());
    }

    if (config::enable_transparent_data_encryption) {
        ASSIGN_OR_RETURN(auto pair, KeyCache::instance().create_encryption_meta_pair_using_current_kek());
        opts.encryption_info = pair.info;
        writer_options.encryption_meta = std::move(pair.encryption_meta);
    }
    ASSIGN_OR_RETURN(auto wfile, fs::new_writable_file(opts, path));
    auto segment_writer = std::make_unique<SegmentWriter>(std::move(wfile), 0, tschema, writer_options);
    RETURN_IF_ERROR(segment_writer->init(false));
    return std::move(segment_writer);
}

StatusOr<ChunkPtr> ColumnModePartialUpdateHandler::_read_from_source_segment(const RowsetUpdateStateParams& params,
                                                                             const Schema& schema, uint32_t rssid) {
    TRACE_COUNTER_SCOPE_LATENCY_US("pcu_read_from_source_us");
    OlapReaderStatistics stats;
    size_t footer_size_hint = 16 * 1024;
    LakeIOOptions lake_io_opts{.fill_data_cache = true};
    if (params.container.rssid_to_file().count(rssid) == 0) {
        // Should not happen
        return Status::InternalError(fmt::format("ColumnModePartialUpdateHandler read tablet {} segment {} not found",
                                                 params.tablet->id(), rssid));
    }
    // 1. get relative file path by rowset segment id.
    auto& relative_file_info = params.container.rssid_to_file().at(rssid);
    FileInfo fileinfo{.path = params.tablet->segment_location(relative_file_info.path),
                      .encryption_meta = relative_file_info.encryption_meta};
    if (relative_file_info.size.has_value()) {
        fileinfo.size = relative_file_info.size;
    }
    if (relative_file_info.bundle_file_offset.has_value()) {
        fileinfo.bundle_file_offset = relative_file_info.bundle_file_offset;
    }
    uint32_t rowset_id = params.container.rssid_to_rowid().at(rssid);
    // 2. load segment meta.
    ASSIGN_OR_RETURN(auto segment, params.tablet->tablet_mgr()->load_segment(
                                           fileinfo, rssid - rowset_id /* segment id inside rowset */,
                                           &footer_size_hint, lake_io_opts, true, params.tablet_schema));
    SegmentReadOptions seg_options;
    ASSIGN_OR_RETURN(seg_options.fs, FileSystemFactory::CreateSharedFromString(fileinfo.path));
    seg_options.stats = &stats;
    seg_options.is_primary_keys = true;
    seg_options.tablet_id = params.tablet->id();
    seg_options.rowset_id = rowset_id;
    seg_options.version = _base_version;
    seg_options.tablet_schema = params.tablet_schema;
    // not use delvec loader
    seg_options.dcg_loader = std::make_shared<LakeDeltaColumnGroupLoader>(params.metadata);
    ASSIGN_OR_RETURN(auto seg_iter, segment->new_iterator(schema, seg_options));
    auto source_chunk_ptr = ChunkFactory::new_chunk(schema, segment->num_rows());
    auto tmp_chunk_ptr = ChunkFactory::new_chunk(schema, 1024);
    while (true) {
        tmp_chunk_ptr->reset();
        auto st = seg_iter->get_next(tmp_chunk_ptr.get());
        if (st.is_end_of_file()) {
            break;
        } else if (!st.ok()) {
            return st;
        } else {
            source_chunk_ptr->append(*tmp_chunk_ptr);
        }
    }
    return source_chunk_ptr;
}

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

// read from upt files and update rows in source chunk.
// When condition_idx_in_partial_schema >= 0, pairs where the old condition value is
// strictly greater than the new one are dropped before applying the update, so the
// corresponding source rows keep their previous values. Equal values let the new row
// win, matching the existing upsert/row-mode condition-update semantics (see
// UpdateManager::_process_single_chunk_update_with_condition).
Status ColumnModePartialUpdateHandler::_update_source_chunk_by_upt(const UptidToRowidPairs& upt_id_to_rowid_pairs,
                                                                   const Schema& partial_schema, ChunkPtr* source_chunk,
                                                                   int32_t condition_idx_in_partial_schema) {
    TRACE_COUNTER_SCOPE_LATENCY_US("pcu_update_source_by_upt_us");
    // build iterators
    OlapReaderStatistics stats;
    ASSIGN_OR_RETURN(auto segment_iters, _rowset_ptr->get_each_segment_iterator(partial_schema, true, &stats));
    RETURN_ERROR_IF_FALSE(segment_iters.size() == _rowset_ptr->num_segments());
    // handle upt files one by one
    for (const auto& each : upt_id_to_rowid_pairs) {
        const uint32_t upt_id = each.first;
        // A nullptr iterator is a lost update-file segment ignored via
        // experimental_lake_ignore_lost_segment. A lost upt segment produces no rowid pairs, so this
        // upt_id should not appear here; guard anyway so read_chunk_from_update_file never dereferences
        // a null iterator (mirrors the close() guard below).
        if (segment_iters[upt_id] == nullptr) {
            LOG(WARNING) << "column-mode partial update skips a null update-file segment iterator, tablet: "
                         << _rowset_ptr->tablet_id() << ", upt_id: " << upt_id
                         << " (a lost segment via experimental_lake_ignore_lost_segment, or an unexpected empty slot)";
            continue;
        }
        // 1. get chunk from upt file
        ChunkUniquePtr upt_chunk = ChunkFactory::new_chunk(partial_schema, DEFAULT_CHUNK_SIZE);
        DeferOp iter_defer([&]() {
            if (segment_iters[upt_id] != nullptr) {
                segment_iters[upt_id]->close();
            }
        });
        RETURN_IF_ERROR(read_chunk_from_update_file(segment_iters[upt_id], upt_chunk));
        const size_t upt_chunk_size = upt_chunk->memory_usage();
        _tracker->consume(upt_chunk_size);
        DeferOp tracker_defer([&]() { _tracker->release(upt_chunk_size); });
        // 2. update source chunk
        std::vector<uint32_t> sorted_source_rowids;
        std::vector<uint32_t> unsorted_upt_rowids;
        // Sort source rowid -> upt rowid pairs by source rowid.
        split_rowid_pairs(each.second, &sorted_source_rowids, &unsorted_upt_rowids, nullptr);
        DCHECK(sorted_source_rowids.size() == unsorted_upt_rowids.size());

        // When condition update is enabled, compare the condition column value in
        // source chunk vs upt chunk and keep winners (old <= new); equal values let
        // the new row win, matching the upsert path.
        if (condition_idx_in_partial_schema >= 0) {
            const auto& source_cond = (*source_chunk)->get_column_by_index(condition_idx_in_partial_schema);
            const auto& upt_cond = upt_chunk->get_column_by_index(condition_idx_in_partial_schema);
            const size_t original_size = sorted_source_rowids.size();
            std::vector<uint32_t> filtered_source_rowids;
            std::vector<uint32_t> filtered_upt_rowids;
            filtered_source_rowids.reserve(original_size);
            filtered_upt_rowids.reserve(original_size);
            for (size_t i = 0; i < original_size; ++i) {
                const uint32_t src_rowid = sorted_source_rowids[i];
                const uint32_t upt_rowid = unsorted_upt_rowids[i];
                if (source_cond->compare_at(src_rowid, upt_rowid, *upt_cond, -1) <= 0) {
                    filtered_source_rowids.push_back(src_rowid);
                    filtered_upt_rowids.push_back(upt_rowid);
                }
            }
            sorted_source_rowids.swap(filtered_source_rowids);
            unsorted_upt_rowids.swap(filtered_upt_rowids);
            TRACE_COUNTER_INCREMENT("pcu_condition_kept_cnt", sorted_source_rowids.size());
            TRACE_COUNTER_INCREMENT("pcu_condition_dropped_cnt", original_size - sorted_source_rowids.size());
        }
        if (sorted_source_rowids.empty()) {
            continue;
        }
        auto tmp_chunk = ChunkFactory::new_chunk(partial_schema, unsorted_upt_rowids.size());
        TRY_CATCH_BAD_ALLOC(
                tmp_chunk->append_selective(*upt_chunk, unsorted_upt_rowids.data(), 0, unsorted_upt_rowids.size()));
        RETURN_IF_EXCEPTION((*source_chunk)->update_rows(*tmp_chunk, sorted_source_rowids.data()));
    }
    return Status::OK();
}

Status ColumnModePartialUpdateHandler::_update_source_chunk_by_upt_flexible(
        const UptidToRowidPairs& upt_id_to_rowid_pairs, const Schema& partial_schema,
        const std::vector<ColumnUID>& selective_unique_update_column_ids,
        const std::vector<std::vector<ColumnUID>>& distinct_column_sets, ChunkPtr* source_chunk) {
    TRACE_COUNTER_SCOPE_LATENCY_US("pcu_update_source_by_upt_flexible_us");
    const size_t num_value_cols = partial_schema.num_fields();
    RETURN_ERROR_IF_FALSE(num_value_cols == selective_unique_update_column_ids.size());

    // Decode the dictionary into per-set-id covered-column masks over this batch's value-column positions.
    // (Same row-view -> column-coverage decode as _build_packed_sparse_chunk_from_upt step 0.)
    std::unordered_map<ColumnUID, size_t> uid_to_value_pos;
    uid_to_value_pos.reserve(num_value_cols);
    for (size_t c = 0; c < num_value_cols; ++c) {
        uid_to_value_pos.emplace(selective_unique_update_column_ids[c], c);
    }
    std::vector<std::vector<bool>> set_covers(distinct_column_sets.size());
    for (size_t s = 0; s < distinct_column_sets.size(); ++s) {
        set_covers[s].assign(num_value_cols, false);
        for (ColumnUID uid : distinct_column_sets[s]) {
            auto it = uid_to_value_pos.find(uid);
            if (it != uid_to_value_pos.end()) {
                set_covers[s][it->second] = true;
            }
        }
    }

    OlapReaderStatistics stats;
    ASSIGN_OR_RETURN(auto segment_iters, _rowset_ptr->get_each_segment_iterator(partial_schema, true, &stats));
    RETURN_ERROR_IF_FALSE(segment_iters.size() == _rowset_ptr->num_segments());

    int64_t total_overlaid = 0;
    // Process upt files in ascending upt_id order so later upt_ids win per (column, source_rowid),
    // matching the homogeneous dense path's last-write-wins overwrite.
    for (const auto& each : upt_id_to_rowid_pairs) {
        const uint32_t upt_id = each.first;
        ChunkUniquePtr upt_chunk = ChunkFactory::new_chunk(partial_schema, DEFAULT_CHUNK_SIZE);
        DeferOp iter_defer([&]() {
            if (segment_iters[upt_id] != nullptr) {
                segment_iters[upt_id]->close();
            }
        });
        RETURN_IF_ERROR(read_chunk_from_update_file(segment_iters[upt_id], upt_chunk));
        const size_t upt_chunk_size = upt_chunk->memory_usage();
        _tracker->consume(upt_chunk_size);
        DeferOp tracker_defer([&]() { _tracker->release(upt_chunk_size); });

        // Per-row set-id (hidden "__cset__"); validate once per pair.
        ASSIGN_OR_RETURN(std::vector<int32_t> set_ids, _read_cset_column_from_upt(upt_id));
        struct Pair {
            uint32_t source_rowid;
            uint32_t upt_rowid;
            int32_t set_id;
        };
        std::vector<Pair> pairs;
        pairs.reserve(each.second.size());
        for (const auto& pr : each.second) {
            const uint32_t upt_rowid = pr.second;
            if (upt_rowid >= set_ids.size()) {
                return Status::InternalError(
                        fmt::format("ColumnModePartialUpdateHandler(flex): upt_rowid {} out of range of `{}` (size {})",
                                    upt_rowid, kSDCGCsetColumnName, set_ids.size()));
            }
            const int32_t set_id = set_ids[upt_rowid];
            if (set_id < 0 || static_cast<size_t>(set_id) >= set_covers.size()) {
                return Status::InternalError(
                        fmt::format("ColumnModePartialUpdateHandler(flex): set-id {} out of range (size {})", set_id,
                                    set_covers.size()));
            }
            pairs.push_back(Pair{pr.first, upt_rowid, set_id});
        }

        // For each value column, overlay ONLY at the source rows whose set covers it. Columns a row did not
        // declare keep the base/current value already in *source_chunk (read through the DCG overlay). Sort
        // the covered source rowids ascending to match the homogeneous path's update_rows contract.
        for (size_t c = 0; c < num_value_cols; ++c) {
            std::vector<std::pair<uint32_t, uint32_t>> covered; // (source_rowid, upt_rowid)
            covered.reserve(pairs.size());
            for (const auto& p : pairs) {
                if (set_covers[p.set_id][c]) covered.emplace_back(p.source_rowid, p.upt_rowid);
            }
            if (covered.empty()) continue;
            std::sort(covered.begin(), covered.end(), [](const auto& a, const auto& b) { return a.first < b.first; });
            std::vector<uint32_t> src_rowids(covered.size());
            std::vector<uint32_t> upt_rowids(covered.size());
            for (size_t i = 0; i < covered.size(); ++i) {
                src_rowids[i] = covered[i].first;
                upt_rowids[i] = covered[i].second;
            }
            const auto& upt_col = upt_chunk->get_column_by_index(c);
            auto sel_col = upt_col->clone_empty();
            TRY_CATCH_BAD_ALLOC(sel_col->append_selective(*upt_col, upt_rowids.data(), 0, upt_rowids.size()));
            RETURN_IF_EXCEPTION((*source_chunk)
                                        ->get_column_by_index(c)
                                        ->as_mutable_raw_ptr()
                                        ->update_rows(*sel_col, src_rowids.data()));
            total_overlaid += static_cast<int64_t>(covered.size());
        }
    }
    LOG(INFO) << fmt::format(
            "FLEX_COLUMN dense apply: txn_id: {} union_cols: {} cells_overlaid: {} (per-row masked, row-complete "
            ".cols)",
            _txn_id, num_value_cols, total_overlaid);
    return Status::OK();
}

StatusOr<int64_t> ColumnModePartialUpdateHandler::_resolve_source_segment_num_rows(
        const RowsetUpdateStateParams& params, uint32_t rssid) {
    // Fast path: read SegmentMetadataPB.num_rows straight from the published metadata (zero footer GET).
    // Map rssid -> (rowset, segment position) the same way RssidFileInfoContainer does, via get_rssid.
    if (params.metadata != nullptr) {
        for (const auto& rowset : params.metadata->rowsets()) {
            for (int seg_pos = 0; seg_pos < rowset.segment_metas_size(); ++seg_pos) {
                if (get_rssid(rowset, seg_pos) == rssid) {
                    const auto& seg_meta = rowset.segment_metas(seg_pos);
                    if (seg_meta.has_num_rows() && seg_meta.num_rows() > 0) {
                        return static_cast<int64_t>(seg_meta.num_rows());
                    }
                    // Legacy segment without num_rows: fall through to footer-only open below.
                    break;
                }
            }
        }
    }

    // Fallback: footer-only open via the existing source-segment loader to read num_rows().
    if (params.container.rssid_to_file().count(rssid) == 0 || params.container.rssid_to_rowid().count(rssid) == 0) {
        return Status::InternalError(
                fmt::format("ColumnModePartialUpdateHandler resolve M: tablet {} segment {} not found",
                            params.tablet->id(), rssid));
    }
    size_t footer_size_hint = 16 * 1024;
    LakeIOOptions lake_io_opts{.fill_data_cache = true};
    const auto& relative_file_info = params.container.rssid_to_file().at(rssid);
    FileInfo fileinfo{.path = params.tablet->segment_location(relative_file_info.path),
                      .encryption_meta = relative_file_info.encryption_meta};
    if (relative_file_info.size.has_value()) {
        fileinfo.size = relative_file_info.size;
    }
    if (relative_file_info.bundle_file_offset.has_value()) {
        fileinfo.bundle_file_offset = relative_file_info.bundle_file_offset;
    }
    uint32_t rowset_id = params.container.rssid_to_rowid().at(rssid);
    ASSIGN_OR_RETURN(auto segment,
                     params.tablet->tablet_mgr()->load_segment(fileinfo, rssid - rowset_id, &footer_size_hint,
                                                               lake_io_opts, true, params.tablet_schema));
    return static_cast<int64_t>(segment->num_rows());
}

std::shared_ptr<TabletSchema> ColumnModePartialUpdateHandler::build_sparse_tablet_schema(
        const TabletSchemaCSPtr& base_tablet_schema, const std::shared_ptr<TabletSchema>& value_tschema) {
    // Synthetic source_rowid column. Values are rowid_t (uint32) widened to BIGINT: the segment
    // writer's EncodingInfo has no encoding registered for unsigned integer logical types (TYPE_UNSIGNED_*
    // would hit "fail to find valid type encoding" in ScalarColumnWriter::init), and int64 holds the full
    // uint32 range. Its reserved uid (kSDCGSourceRowidUid) never appears in
    // DeltaColumnGroup::column_ids(), so the read layer never resolves a real column to it. It is column 0
    // so the file's first column is the rowid->ordinal key.
    TabletColumn source_rowid_col(STORAGE_AGGREGATE_NONE, TYPE_BIGINT, /*is_nullable=*/false,
                                  static_cast<int32_t>(kSDCGSourceRowidUid), sizeof(int64_t));
    source_rowid_col.set_name("__sdcg_source_rowid");
    source_rowid_col.set_is_key(false);

    std::vector<TabletColumn> cols;
    cols.reserve(1 + value_tschema->num_columns());
    cols.emplace_back(std::move(source_rowid_col));
    for (size_t i = 0; i < value_tschema->num_columns(); ++i) {
        cols.emplace_back(value_tschema->column(i));
    }
    // copy() rebuilds a schema from the given columns (clears the source's columns first), keeping the
    // base schema's keys_type/compression etc. The source_rowid column is non-key, like the value columns.
    return TabletSchema::copy(*base_tablet_schema, cols);
}

StatusOr<ChunkPtr> ColumnModePartialUpdateHandler::_build_sparse_chunk_from_upt(
        const UptidToRowidPairs& upt_id_to_rowid_pairs, const Schema& value_schema, const Schema& sparse_schema,
        int64_t source_segment_num_rows, int64_t* out_num_rows, int64_t* out_min_source_rowid,
        int64_t* out_max_source_rowid) {
    TRACE_COUNTER_SCOPE_LATENCY_US("pcu_build_sparse_chunk_us");
    *out_min_source_rowid = kSDCGPresenceUnknown;
    *out_max_source_rowid = kSDCGPresenceUnknown;
    // 1. Collect the union of distinct source_rowids across all upt_ids for this rssid. Sorted ascending
    //    gives both the source_rowid column values and a base_rowid -> local ordinal [0,K) map.
    std::set<uint32_t> distinct_source_rowids;
    for (const auto& each : upt_id_to_rowid_pairs) {
        for (const auto& pair : each.second) {
            distinct_source_rowids.insert(pair.first);
        }
    }
    const int64_t K = static_cast<int64_t>(distinct_source_rowids.size());
    *out_num_rows = K;
    if (K == 0) {
        return Status::InternalError("ColumnModePartialUpdateHandler: empty sparse equivalence class");
    }
    // The set is sorted ascending, so begin()/rbegin() give the presence [min, max] range directly.
    *out_min_source_rowid = static_cast<int64_t>(*distinct_source_rowids.begin());
    *out_max_source_rowid = static_cast<int64_t>(*distinct_source_rowids.rbegin());
    // Fingerprint guard: every source_rowid must be a valid ordinal of the base segment.
    if (source_segment_num_rows > 0) {
        DCHECK_LT(static_cast<int64_t>(*distinct_source_rowids.rbegin()), source_segment_num_rows);
        if (static_cast<int64_t>(*distinct_source_rowids.rbegin()) >= source_segment_num_rows) {
            return Status::InternalError(
                    fmt::format("ColumnModePartialUpdateHandler: source_rowid {} >= source segment num_rows {}",
                                *distinct_source_rowids.rbegin(), source_segment_num_rows));
        }
    }
    std::unordered_map<uint32_t, uint32_t> base_rowid_to_local;
    base_rowid_to_local.reserve(distinct_source_rowids.size());
    {
        uint32_t local = 0;
        for (uint32_t base_rowid : distinct_source_rowids) {
            base_rowid_to_local.emplace(base_rowid, local++);
        }
    }

    // 2. Build the K-row sparse chunk (source_rowid + value columns). Column 0 = sorted source_rowids.
    auto sparse_chunk = ChunkFactory::new_chunk(sparse_schema, K);
    {
        auto& rowid_column = sparse_chunk->get_column_by_index(0);
        std::vector<int64_t> sorted_rowids(distinct_source_rowids.begin(), distinct_source_rowids.end());
        TRY_CATCH_BAD_ALLOC((void)rowid_column->as_mutable_raw_ptr()->append_numbers(
                sorted_rowids.data(), sorted_rowids.size() * sizeof(int64_t)));
    }

    // 3. The value columns are a separate K-row chunk that mirrors sparse_chunk's value columns by
    //    position. Initialize it to K rows (default-filled), then overlay each upt's gathered values at
    //    the local ordinals. Iterating upt_ids in ascending map order makes the highest upt_id win per
    //    source_rowid (last-write-wins), matching the dense path's ascending-upt_id overwrite.
    auto value_chunk = ChunkFactory::new_chunk(value_schema, K);
    {
        // Default-fill K rows so update_rows has valid destination positions for every local ordinal.
        for (size_t c = 0; c < value_chunk->num_columns(); ++c) {
            value_chunk->get_column_by_index(c)->as_mutable_raw_ptr()->append_default(K);
        }
    }

    OlapReaderStatistics stats;
    ASSIGN_OR_RETURN(auto segment_iters, _rowset_ptr->get_each_segment_iterator(value_schema, true, &stats));
    RETURN_ERROR_IF_FALSE(segment_iters.size() == _rowset_ptr->num_segments());
    for (const auto& each : upt_id_to_rowid_pairs) {
        const uint32_t upt_id = each.first;
        ChunkUniquePtr upt_chunk = ChunkFactory::new_chunk(value_schema, DEFAULT_CHUNK_SIZE);
        DeferOp iter_defer([&]() {
            if (segment_iters[upt_id] != nullptr) {
                segment_iters[upt_id]->close();
            }
        });
        RETURN_IF_ERROR(read_chunk_from_update_file(segment_iters[upt_id], upt_chunk));
        const size_t upt_chunk_size = upt_chunk->memory_usage();
        _tracker->consume(upt_chunk_size);
        DeferOp tracker_defer([&]() { _tracker->release(upt_chunk_size); });

        // Sort source rowid -> upt rowid pairs by source rowid (split_rowid_pairs requires sorted input;
        // the pairs were built sorted by source_rowid upstream).
        std::vector<uint32_t> sorted_source_rowids;
        std::vector<uint32_t> unsorted_upt_rowids;
        split_rowid_pairs(each.second, &sorted_source_rowids, &unsorted_upt_rowids, nullptr);
        DCHECK(sorted_source_rowids.size() == unsorted_upt_rowids.size());
        if (sorted_source_rowids.empty()) {
            continue;
        }

        // Gather this upt's values for its rows, then translate base rowids -> local ordinals so the
        // overlay lands at the right positions in the K-row value_chunk.
        auto gathered = ChunkFactory::new_chunk(value_schema, unsorted_upt_rowids.size());
        TRY_CATCH_BAD_ALLOC(
                gathered->append_selective(*upt_chunk, unsorted_upt_rowids.data(), 0, unsorted_upt_rowids.size()));
        std::vector<uint32_t> local_ordinals;
        local_ordinals.reserve(sorted_source_rowids.size());
        for (uint32_t base_rowid : sorted_source_rowids) {
            local_ordinals.push_back(base_rowid_to_local.at(base_rowid));
        }
        RETURN_IF_EXCEPTION(value_chunk->update_rows(*gathered, local_ordinals.data()));
    }

    // 4. Splice the K-row value columns into the sparse chunk after the source_rowid column. Both have K
    //    rows in the same local-ordinal order, so source_rowid[i] <-> value[i] alignment is preserved.
    for (size_t c = 0; c < value_chunk->num_columns(); ++c) {
        sparse_chunk->get_column_by_index(c + 1)->as_mutable_raw_ptr()->swap_column(
                *value_chunk->get_column_by_index(c)->as_mutable_raw_ptr());
    }
    return sparse_chunk;
}

StatusOr<std::vector<int32_t>> ColumnModePartialUpdateHandler::_read_cset_column_from_upt(uint32_t upt_id) {
    // Read the hidden "__cset__" set-id column for this upt segment. The `.upt` carries it as a real
    // Segment v2 column with the RESERVED unique-id kCsetReservedColumnUid (see flexible_partial_update.h and
    // the lake delta_writer's synthetic-column append). The Rowset's tablet_schema() is the FULL base schema
    // and does NOT contain "__cset__", so we cannot resolve it by field_index there. Instead we synthesize a
    // single-column TabletSchema carrying a "__cset__" column with the reserved uid + SMALLINT storage and
    // let segment iteration resolve it against the `.upt` footer BY UNIQUE-ID (segment v2 maps schema fields
    // to physical columns by unique_id). This mirrors exactly what the writer appended.
    TabletColumn cset_col(STORAGE_AGGREGATE_REPLACE, TYPE_SMALLINT, /*is_nullable=*/false,
                          static_cast<int32_t>(kCsetReservedColumnUid), sizeof(int16_t));
    cset_col.set_name(kSDCGCsetColumnName);
    cset_col.set_is_key(false);
    std::vector<TabletColumn> cset_cols;
    cset_cols.emplace_back(std::move(cset_col));
    auto cset_tschema = TabletSchema::copy(*_rowset_ptr->tablet_schema(), cset_cols);
    // TabletSchema::copy inherits the base schema's num_short_key_columns (e.g. 1 for the PK) but
    // _clear_columns leaves only the single non-key "__cset__" column (num_key_columns becomes 0).
    // Opening a Segment with that inconsistency (1 short key over 0 key columns) is unsafe, so pin
    // it to 0 -- mirroring Segment::new_sparse_dcg_segment, which builds its reserved-uid read
    // schema with num_short_key_columns(0).
    cset_tschema->set_num_short_key_columns(0);
    Schema cset_schema = ChunkHelper::convert_schema(cset_tschema);

    OlapReaderStatistics stats;
    // Read "__cset__" via a cache-bypassing open bound to cset_tschema. "__cset__" is NOT in the
    // rowset's base schema, so (a) the iterator must resolve the single output column against this
    // 1-column schema (cid 0 -> __cset__ SMALLINT, uid kCsetReservedColumnUid), and (b) the segment
    // must be OPENED with cset_tschema so Segment::_create_column_readers builds a reader for the
    // reserved-uid footer column. The metacache already holds a base-schema Segment for this .upt
    // path (loaded earlier in apply), which has no reader for "__cset__"; a plain
    // get_each_segment_iterator would reuse it and fail with "nonexistent column(__cset__)".
    ASSIGN_OR_RETURN(auto segment_iters,
                     _rowset_ptr->get_each_segment_iterator_with_schema(cset_schema, cset_tschema, true, &stats));
    if (upt_id >= segment_iters.size()) {
        return Status::InternalError(
                fmt::format("ColumnModePartialUpdateHandler: upt_id {} out of range ({} segments) reading `{}`", upt_id,
                            segment_iters.size(), kSDCGCsetColumnName));
    }
    DeferOp close_iters([&]() {
        for (auto& it : segment_iters) {
            if (it != nullptr) it->close();
        }
    });

    ChunkUniquePtr cset_chunk = ChunkFactory::new_chunk(cset_schema, DEFAULT_CHUNK_SIZE);
    RETURN_IF_ERROR(read_chunk_from_update_file(segment_iters[upt_id], cset_chunk));
    const auto& col = cset_chunk->get_column_by_index(0);
    // The Datum is a variant typed by the column's storage; dispatch on the cset column's logical type so
    // we read the correct integer width (SMALLINT spine slot == int16; INT/BIGINT also accepted).
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
            return Status::InternalError(fmt::format(
                    "ColumnModePartialUpdateHandler: `{}` column has unexpected type {} (expected SMALLINT/INT/BIGINT)",
                    kSDCGCsetColumnName, static_cast<int>(cset_type)));
        }
    }
    return set_ids;
}

StatusOr<ChunkPtr> ColumnModePartialUpdateHandler::_build_packed_sparse_chunk_from_upt(
        const UptidToRowidPairs& upt_id_to_rowid_pairs, const Schema& value_schema, const Schema& sparse_schema,
        const std::vector<ColumnUID>& selective_unique_update_column_ids,
        const std::vector<std::vector<ColumnUID>>& distinct_column_sets, int64_t source_segment_num_rows,
        int64_t* out_num_rows, int64_t* out_min_source_rowid, int64_t* out_max_source_rowid,
        std::vector<PackedColumnPresence>* out_column_presences) {
    TRACE_COUNTER_SCOPE_LATENCY_US("pcu_build_packed_sparse_chunk_us");
    *out_num_rows = 0;
    *out_min_source_rowid = kSDCGPresenceUnknown;
    *out_max_source_rowid = kSDCGPresenceUnknown;
    out_column_presences->clear();

    const size_t num_value_cols = value_schema.num_fields();
    DCHECK_EQ(num_value_cols, selective_unique_update_column_ids.size());

    // 0. Decode the dictionary into per-set-id covered-uid masks restricted to THIS batch's value columns.
    //    A uid in the set that is not part of this batch is ignored (it lands in another column batch).
    std::unordered_map<ColumnUID, size_t> uid_to_value_pos;
    uid_to_value_pos.reserve(num_value_cols);
    for (size_t c = 0; c < num_value_cols; ++c) {
        uid_to_value_pos.emplace(selective_unique_update_column_ids[c], c);
    }
    // (②) Per-set-id LIST of covered value-column positions (not a full O(num_value_cols) bitmask): step 1
    // iterates ONLY the columns a row actually covers — O(rows × avg-coverage) instead of O(rows ×
    // num_value_cols), the win on wide tables with sparse per-row coverage.
    std::vector<std::vector<uint32_t>> set_cover_cols(distinct_column_sets.size());
    for (size_t s = 0; s < distinct_column_sets.size(); ++s) {
        for (ColumnUID uid : distinct_column_sets[s]) {
            auto it = uid_to_value_pos.find(uid);
            if (it != uid_to_value_pos.end()) {
                set_cover_cols[s].push_back(static_cast<uint32_t>(it->second));
            }
        }
    }

    // 1. Per-column (source_rowid, upt_id, upt_rowid) collection. A pair enters column c only when its
    //    upt row's set-id mask covers c. We also collect the union of source_rowids across all columns.
    //    Iterating upt_ids in ascending order makes later upt_ids win per (column, source_rowid)
    //    (last-write-wins), matching the dense / homogeneous path's ascending-upt_id overwrite.
    struct ColumnPair {
        uint32_t source_rowid;
        uint32_t upt_id;
        uint32_t upt_rowid;
    };
    std::vector<std::vector<ColumnPair>> per_column_pairs(num_value_cols);
    std::vector<uint32_t> union_vec; // (④) collect then sort+unique, instead of a per-insert std::set

    // (①) Read every upt segment's `__cset__` in ONE iterator-open pass (the per-upt_id helper opened all
    // N iterators on EVERY call -> O(N^2) opens of a cache-bypassing custom-schema segment). all_set_ids[seg]
    // = per-row set-ids for upt segment seg.
    std::vector<std::vector<int32_t>> all_set_ids;
    {
        TabletColumn cset_col(STORAGE_AGGREGATE_REPLACE, TYPE_SMALLINT, /*is_nullable=*/false,
                              static_cast<int32_t>(kCsetReservedColumnUid), sizeof(int16_t));
        cset_col.set_name(kSDCGCsetColumnName);
        cset_col.set_is_key(false);
        std::vector<TabletColumn> cset_cols;
        cset_cols.emplace_back(std::move(cset_col));
        auto cset_tschema = TabletSchema::copy(*_rowset_ptr->tablet_schema(), cset_cols);
        cset_tschema->set_num_short_key_columns(0);
        Schema cset_schema = ChunkHelper::convert_schema(cset_tschema);
        const LogicalType cset_type = cset_tschema->column(0).type();
        OlapReaderStatistics cset_stats;
        ASSIGN_OR_RETURN(auto cset_iters, _rowset_ptr->get_each_segment_iterator_with_schema(cset_schema, cset_tschema,
                                                                                             true, &cset_stats));
        DeferOp close_cset([&]() {
            for (auto& it : cset_iters) {
                if (it != nullptr) it->close();
            }
        });
        all_set_ids.resize(cset_iters.size());
        for (size_t seg = 0; seg < cset_iters.size(); ++seg) {
            if (cset_iters[seg] == nullptr) continue;
            ChunkUniquePtr cset_chunk = ChunkFactory::new_chunk(cset_schema, DEFAULT_CHUNK_SIZE);
            RETURN_IF_ERROR(read_chunk_from_update_file(cset_iters[seg], cset_chunk));
            const auto& ccol = cset_chunk->get_column_by_index(0);
            auto& sids = all_set_ids[seg];
            sids.reserve(ccol->size());
            for (size_t i = 0; i < ccol->size(); ++i) {
                const auto d = ccol->get(i);
                switch (cset_type) {
                case TYPE_SMALLINT:
                    sids.push_back(static_cast<int32_t>(d.get_int16()));
                    break;
                case TYPE_INT:
                    sids.push_back(d.get_int32());
                    break;
                case TYPE_BIGINT:
                    sids.push_back(static_cast<int32_t>(d.get_int64()));
                    break;
                default:
                    return Status::InternalError(
                            fmt::format("ColumnModePartialUpdateHandler: `{}` column has unexpected type {}",
                                        kSDCGCsetColumnName, static_cast<int>(cset_type)));
                }
            }
        }
    }

    for (const auto& each : upt_id_to_rowid_pairs) {
        const uint32_t upt_id = each.first;
        if (upt_id >= all_set_ids.size()) {
            return Status::InternalError(
                    fmt::format("ColumnModePartialUpdateHandler: upt_id {} out of range ({} cset segments)", upt_id,
                                all_set_ids.size()));
        }
        const std::vector<int32_t>& set_ids = all_set_ids[upt_id];
        for (const auto& pair : each.second) {
            const uint32_t source_rowid = pair.first;
            const uint32_t upt_rowid = pair.second;
            if (upt_rowid >= set_ids.size()) {
                return Status::InternalError(fmt::format(
                        "ColumnModePartialUpdateHandler: upt_rowid {} out of range of `{}` column (size {})", upt_rowid,
                        kSDCGCsetColumnName, set_ids.size()));
            }
            const int32_t set_id = set_ids[upt_rowid];
            if (set_id < 0 || static_cast<size_t>(set_id) >= set_cover_cols.size()) {
                return Status::InternalError(fmt::format(
                        "ColumnModePartialUpdateHandler: set-id {} out of range of distinct_column_sets (size {})",
                        set_id, set_cover_cols.size()));
            }
            const auto& cols = set_cover_cols[set_id]; // (②) only the columns this row actually covers
            if (!cols.empty()) {
                for (uint32_t c : cols) {
                    per_column_pairs[c].push_back(ColumnPair{source_rowid, upt_id, upt_rowid});
                }
                union_vec.push_back(source_rowid);
            }
        }
    }
    // (④) ascending unique union of source_rowids.
    std::sort(union_vec.begin(), union_vec.end());
    union_vec.erase(std::unique(union_vec.begin(), union_vec.end()), union_vec.end());

    const int64_t K_union = static_cast<int64_t>(union_vec.size());
    *out_num_rows = K_union;
    if (K_union == 0) {
        return Status::InternalError("ColumnModePartialUpdateHandler: empty packed sparse class");
    }
    *out_min_source_rowid = static_cast<int64_t>(union_vec.front());
    *out_max_source_rowid = static_cast<int64_t>(union_vec.back());
    // Fingerprint guard: every union source_rowid must be a valid ordinal of the base segment.
    if (source_segment_num_rows > 0) {
        DCHECK_LT(static_cast<int64_t>(union_vec.back()), source_segment_num_rows);
        if (static_cast<int64_t>(union_vec.back()) >= source_segment_num_rows) {
            return Status::InternalError(
                    fmt::format("ColumnModePartialUpdateHandler: packed source_rowid {} >= source segment num_rows {}",
                                union_vec.back(), source_segment_num_rows));
        }
    }

    // base_rowid -> union ordinal [0, K_union): union_vec is sorted ascending, so the ordinal is its
    // lower_bound index (④: a binary search, no per-cell hash-map build/lookup).
    auto union_ordinal_of = [&union_vec](uint32_t base_rowid) -> uint32_t {
        return static_cast<uint32_t>(std::lower_bound(union_vec.begin(), union_vec.end(), base_rowid) -
                                     union_vec.begin());
    };

    // 2. Build the K_union-row packed chunk: column 0 = ascending union source_rowids; value columns
    //    default-filled to K_union (placeholders) then overlaid only at their covered union ordinals.
    auto sparse_chunk = ChunkFactory::new_chunk(sparse_schema, K_union);
    {
        auto& rowid_column = sparse_chunk->get_column_by_index(0);
        std::vector<int64_t> sorted_rowids(union_vec.begin(), union_vec.end());
        TRY_CATCH_BAD_ALLOC((void)rowid_column->as_mutable_raw_ptr()->append_numbers(
                sorted_rowids.data(), sorted_rowids.size() * sizeof(int64_t)));
    }
    auto value_chunk = ChunkFactory::new_chunk(value_schema, K_union);
    for (size_t c = 0; c < value_chunk->num_columns(); ++c) {
        value_chunk->get_column_by_index(c)->as_mutable_raw_ptr()->append_default(K_union);
    }

    // 3. Gather each column's real values from the `.upt` and overlay at union ordinals. We read each upt
    //    segment once per upt_id (shared across columns of that upt_id), collecting the per-column slices.
    //    For correctness the per-column pairs are grouped by upt_id; within an upt_id we sort by source
    //    rowid (split_rowid_pairs precondition) and gather by upt_rowid.
    OlapReaderStatistics stats;
    ASSIGN_OR_RETURN(auto segment_iters, _rowset_ptr->get_each_segment_iterator(value_schema, true, &stats));
    RETURN_ERROR_IF_FALSE(segment_iters.size() == _rowset_ptr->num_segments());

    // Per-column accumulating roaring of covered base rowids (the authoritative apply gate). Built as we
    // overlay so it stays the EXACT set of union ordinals each column lands a real value at.
    std::vector<roaring::Roaring> column_roaring(num_value_cols);

    // Reorganize per-column pairs grouped by upt_id so we open each upt segment once.
    std::map<uint32_t, std::vector<std::pair<size_t, ColumnPair>>> by_upt_id; // upt_id -> [(value_pos, pair)]
    for (size_t c = 0; c < num_value_cols; ++c) {
        for (const auto& cp : per_column_pairs[c]) {
            by_upt_id[cp.upt_id].emplace_back(c, cp);
        }
    }

    for (const auto& [upt_id, entries] : by_upt_id) {
        if (upt_id >= segment_iters.size()) {
            return Status::InternalError(
                    fmt::format("ColumnModePartialUpdateHandler: upt_id {} out of range ({} segments)", upt_id,
                                segment_iters.size()));
        }
        ChunkUniquePtr upt_chunk = ChunkFactory::new_chunk(value_schema, DEFAULT_CHUNK_SIZE);
        DeferOp iter_defer([&]() {
            if (segment_iters[upt_id] != nullptr) {
                segment_iters[upt_id]->close();
            }
        });
        RETURN_IF_ERROR(read_chunk_from_update_file(segment_iters[upt_id], upt_chunk));
        const size_t upt_chunk_size = upt_chunk->memory_usage();
        _tracker->consume(upt_chunk_size);
        DeferOp tracker_defer([&]() { _tracker->release(upt_chunk_size); });

        // Group this upt_id's entries by value-column position.
        std::vector<std::vector<ColumnPair>> col_pairs(num_value_cols);
        for (const auto& [value_pos, cp] : entries) {
            col_pairs[value_pos].push_back(cp);
        }
        for (size_t c = 0; c < num_value_cols; ++c) {
            if (col_pairs[c].empty()) continue;
            // Sort by source_rowid so gather/overlay are deterministic; build aligned upt_rowid vector.
            std::sort(col_pairs[c].begin(), col_pairs[c].end(),
                      [](const ColumnPair& a, const ColumnPair& b) { return a.source_rowid < b.source_rowid; });
            std::vector<uint32_t> upt_rowids;
            std::vector<uint32_t> union_ordinals;
            upt_rowids.reserve(col_pairs[c].size());
            union_ordinals.reserve(col_pairs[c].size());
            std::vector<uint32_t> col_source_rowids;
            col_source_rowids.reserve(col_pairs[c].size());
            for (const auto& cp : col_pairs[c]) {
                upt_rowids.push_back(cp.upt_rowid);
                union_ordinals.push_back(union_ordinal_of(cp.source_rowid)); // (④) lower_bound, no hash map
                col_source_rowids.push_back(cp.source_rowid);
            }
            // (③) bulk-add this group's covered base rowids to the column roaring (one addMany vs per-cell add).
            column_roaring[c].addMany(col_source_rowids.size(), col_source_rowids.data());
            // Gather this column's values from the upt chunk (single-column gather), overlay at the union
            // ordinals in the K_union value_chunk. Clone the source column to gather into so the result
            // column type matches exactly, then update_rows overlays at the union ordinals.
            auto gathered_col = upt_chunk->get_column_by_index(c)->clone_empty();
            TRY_CATCH_BAD_ALLOC(gathered_col->append_selective(*upt_chunk->get_column_by_index(c), upt_rowids.data(), 0,
                                                               upt_rowids.size()));
            RETURN_IF_EXCEPTION(value_chunk->get_column_by_index(c)->as_mutable_raw_ptr()->update_rows(
                    *gathered_col, union_ordinals.data()));
        }
    }

    // 4. Splice value columns into the packed chunk after column 0.
    for (size_t c = 0; c < value_chunk->num_columns(); ++c) {
        sparse_chunk->get_column_by_index(c + 1)->as_mutable_raw_ptr()->swap_column(
                *value_chunk->get_column_by_index(c)->as_mutable_raw_ptr());
    }

    // 5. Emit per-column presence: serialize each covered roaring + min/max/count. Columns covering NO row
    //    are omitted entirely (the caller drops them from the file's column id list). The DCHECK enforces
    //    the writer invariant: every covered rowid is a member of the file's union source_rowids.
    out_column_presences->reserve(num_value_cols);
    for (size_t c = 0; c < num_value_cols; ++c) {
        if (column_roaring[c].isEmpty()) continue;
        PackedColumnPresence p;
        p.column_uid = selective_unique_update_column_ids[c];
        p.count = static_cast<int64_t>(column_roaring[c].cardinality());
        p.min_source_rowid = static_cast<int64_t>(column_roaring[c].minimum());
        p.max_source_rowid = static_cast<int64_t>(column_roaring[c].maximum());
        // Writer DCHECK: a column's covered set must be a subset of the file's union source_rowids.
        DCHECK_GE(p.min_source_rowid, *out_min_source_rowid);
        DCHECK_LE(p.max_source_rowid, *out_max_source_rowid);
        column_roaring[c].runOptimize();
        std::string buf;
        buf.resize(column_roaring[c].getSizeInBytes());
        column_roaring[c].write(buf.data());
        p.roaring = std::move(buf);
        out_column_presences->push_back(std::move(p));
    }
    return sparse_chunk;
}

StatusOr<std::unique_ptr<SegmentWriter>> ColumnModePartialUpdateHandler::_prepare_sparse_delta_column_group_writer(
        const RowsetUpdateStateParams& params, const std::shared_ptr<TabletSchema>& sparse_tschema) {
    // Mirror _prepare_delta_column_group_writer exactly, only swapping the filename to a `.spcols` name
    // and the schema to the synthetic source_rowid + value-columns schema.
    const std::string path = params.tablet->segment_location(gen_spcols_filename(_txn_id));
    WritableFileOptions opts{.sync_on_close = true, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
    SegmentWriterOptions writer_options;

    if (auto metadata = params.tablet->tablet_mgr()->get_latest_cached_tablet_metadata(params.tablet->id());
        metadata && metadata->has_flat_json_config()) {
        writer_options.flat_json_config = std::make_shared<FlatJsonConfig>();
        writer_options.flat_json_config->update(metadata->flat_json_config());
    }

    if (config::enable_transparent_data_encryption) {
        ASSIGN_OR_RETURN(auto pair, KeyCache::instance().create_encryption_meta_pair_using_current_kek());
        opts.encryption_info = pair.info;
        writer_options.encryption_meta = std::move(pair.encryption_meta);
    }
    ASSIGN_OR_RETURN(auto wfile, fs::new_writable_file(opts, path));
    auto segment_writer = std::make_unique<SegmentWriter>(std::move(wfile), 0, sparse_tschema, writer_options);
    RETURN_IF_ERROR(segment_writer->init(false));
    return std::move(segment_writer);
}

template <typename T>
static std::vector<T> append_fixed_batch(const std::vector<T>& base_array, size_t offset, size_t batch_size) {
    std::vector<T> new_array;
    for (int i = offset; i < offset + batch_size && i < base_array.size(); i++) {
        new_array.push_back(base_array[i]);
    }
    return new_array;
}

static void padding_char_columns(const Schema& schema, const TabletSchemaCSPtr& tschema, Chunk* chunk) {
    auto char_field_indexes = ChunkSchemaHelper::get_char_field_indexes(schema);
    ChunkHelper::padding_char_columns(char_field_indexes, schema, tschema, chunk);
}

StatusOr<int32_t> ColumnModePartialUpdateHandler::_resolve_condition_cid(const RowsetTxnMetaPB& txn_meta,
                                                                         const TabletSchema& tschema) {
    if (txn_meta.merge_condition().empty()) {
        return -1;
    }
    for (size_t i = 0; i < tschema.num_columns(); ++i) {
        if (tschema.column(i).name() == txn_meta.merge_condition()) {
            return static_cast<int32_t>(i);
        }
    }
    return Status::InvalidArgument(
            strings::Substitute("merge_condition column '$0' not found in tablet schema", txn_meta.merge_condition()));
}

StatusOr<int32_t> ColumnModePartialUpdateHandler::_locate_condition_idx_in_partial_schema(
        const std::vector<ColumnId>& selective_update_column_ids, int32_t condition_cid) {
    DCHECK_GE(condition_cid, 0);
    for (size_t i = 0; i < selective_update_column_ids.size(); ++i) {
        if (selective_update_column_ids[i] == static_cast<ColumnId>(condition_cid)) {
            return static_cast<int32_t>(i);
        }
    }
    // delta_writer has validated that the condition column is in the partial column set, and
    // execute() forces a single batch so all partial columns land here — missing means a logic
    // bug somewhere upstream; fail loudly rather than silently disabling condition filtering.
    return Status::InternalError(strings::Substitute(
            "merge_condition column id $0 is missing from the partial column batch", condition_cid));
}

// SDCG convergence guard. Inspect the EXISTING delta column group chain already recorded for |rssid|
// in |metadata| and measure, restricted to the columns this batch is about to update, how deep the
// sparse overlay chain is. Returns the number of existing SPARSE_PERCOL files that cover ANY of
// |batch_update_uids| (chain_len) and the sum of their stored row counts K (cum_K). DENSE entries and
// entries that touch none of the batch's columns are ignored: a fresh dense rewrite of those columns
// would supersede the sparse layers anyway, so they don't count toward the chain we must converge.
//
// This runs over the in-memory metadata the publish already loaded for reads (no extra I/O): the same
// dcg_meta() that LakeDeltaColumnGroupLoader reads. When the writer's density decision sees the chain
// growing past the configured limits it forces the dense path for this batch, which (because the dense
// rewrite reads the source THROUGH the overlay readers) materializes the whole chain and lets
// append_dcg's dense-supersede orphan it -- convergence with no background workers.
static void inspect_existing_sparse_chain(const TabletMetadataPtr& metadata, uint32_t rssid,
                                          const std::vector<ColumnUID>& batch_update_uids, int32_t* out_chain_len,
                                          int64_t* out_cum_k) {
    *out_chain_len = 0;
    *out_cum_k = 0;
    if (metadata == nullptr) return;
    const auto& dcgs = metadata->dcg_meta().dcgs();
    auto it = dcgs.find(rssid);
    if (it == dcgs.end()) return;
    const DeltaColumnGroupVerPB& dcg = it->second;
    const std::unordered_set<ColumnUID> batch_uids(batch_update_uids.begin(), batch_update_uids.end());
    for (int i = 0; i < dcg.column_files_size(); ++i) {
        // Legacy hinge: an absent file_kinds slot is DENSE; only SPARSE entries form the chain.
        const DeltaColumnFileKindPB kind = i < dcg.file_kinds_size() ? dcg.file_kinds(i) : DENSE_COLS;
        if (kind != SPARSE_PERCOL) continue;
        if (i >= dcg.unique_column_ids_size()) continue;
        bool covers_batch_col = false;
        for (auto uid : dcg.unique_column_ids(i).column_ids()) {
            if (batch_uids.count(static_cast<ColumnUID>(uid)) > 0) {
                covers_batch_col = true;
                break;
            }
        }
        if (!covers_batch_col) continue;
        ++(*out_chain_len);
        *out_cum_k += i < dcg.sparse_row_counts_size() ? dcg.sparse_row_counts(i) : 0;
    }
}

// ================= partial_update_mode=auto: adaptive write-mode cost model (SHADOW) =================
// Picks the cheapest write mode per (column_batch, rssid) from WRITE-TIME-only signals. Cost is in
// byte-equivalent work; the per-mode coefficients (FRAME, DCG_OVH) are CALIBRATION TARGETS tuned in shadow
// mode against the 4-way / cold-read perf corpus, NOT final. Read penalty is a STATIC per-table weight
// (query freq / cold-vs-warm are unknowable at write time) x (chain_depth+1) x sparse bytes -- so a deep
// chain steers the choice toward dense/row (continuous convergence, replacing the promotion hard-cap valve).
// ROW is a candidate for BOTH homogeneous and heterogeneous inputs; without it auto cannot match the
// scenarios where full-row rewrite is the empirical best.
enum class SdcgAutoMode { COLUMN_DENSE, COLUMN_SPARSE, PACKED_SPARSE, MASKED_DENSE, ROW_REWRITE };

static const char* sdcg_auto_mode_name(SdcgAutoMode m) {
    switch (m) {
    case SdcgAutoMode::COLUMN_DENSE:
        return "column_dense";
    case SdcgAutoMode::COLUMN_SPARSE:
        return "column_sparse";
    case SdcgAutoMode::PACKED_SPARSE:
        return "packed_sparse";
    case SdcgAutoMode::MASKED_DENSE:
        return "masked_dense";
    case SdcgAutoMode::ROW_REWRITE:
        return "row_rewrite";
    }
    return "?";
}

struct SdcgAutoSignals {
    int64_t M = 0;              // base segment rows
    int64_t K = 0;              // distinct updated rows in this (batch, rssid)
    int64_t K_union = 0;        // union rows for packed (== K when homogeneous)
    double W_upd = 8;           // updated-value bytes/row (measured from upt chunk; schema proxy in shadow)
    double W_row = 8;           // full row bytes
    int chain_depth = 0;        // existing sparse chain depth D for these columns on this segment
    bool heterogeneous = false; // distinct_column_sets > 1
    double read_weight = 0.0;   // lambda: 0 = write-optimized; larger = read-strict (from per-table budget)
    int sticky = -1;            // last chosen SdcgAutoMode for this segment (-1 = none) -> hysteresis
};

// Column-mode paths (dense/sparse/packed/masked) carry per-updated-row bookkeeping (source_rowid indexing +
// apply-time DCG merge) that the ROW rewrite avoids; the DCG_OVH term is why row can win on throughput even
// when it writes more bytes (empirical finding: sparse never beats the row baseline on write). All constants
// are calibration targets, not final.
// allow_row=false restricts the choice to the apply-time column-mode formats (dense/sparse/packed/
// masked-dense). The column-vs-ROW decision is a per-load (level-1) choice made at the writer, since ROW
// (masked full-row rewrite) is a separate apply path the handler cannot emit; allow_row=true is used only
// for the shadow log's full-picture recommendation.
static SdcgAutoMode select_write_mode(const SdcgAutoSignals& s, bool allow_row) {
    const double W_upd = std::max(1.0, s.W_upd);
    const double W_row = std::max(W_upd, s.W_row);
    const double FRAME = 4096.0; // per-file framing (footer/index) -> favors row for micro-batches
    const double DCG_OVH = 24.0; // per-updated-row column-mode bookkeeping (source_rowid + apply merge)
    // read-strict (read_weight>0): HARD-exclude the append-overlay formats (sparse/packed). Their read cost
    // grows with chain depth (measured 2.9x at depth 64) and a read-sensitive table cannot risk it; the
    // read-flat supersede formats (dense/masked-dense) are the only column candidates (ROW stays a level-1
    // choice). This is exclusion, not a soft penalty: on a large segment a 1x read penalty is too weak to
    // beat sparse's cheap write, so sparse would still win -- wrong for a read-strict table.
    const bool read_strict = s.read_weight > 0.0;
    // sustained-load WRITE penalty: sparse/packed write throughput decays as the chain deepens (chains
    // outrun compaction; measured c1 8662->5915 over 3 sweeps), so scale their write cost by
    // (1 + sdcg_auto_sparse_depth_penalty * depth). This migrates the choice sparse->dense over sustained
    // load instead of staying on sparse until the hard depth valve trips.
    const double depth_mult = 1.0 + config::sdcg_auto_sparse_depth_penalty * static_cast<double>(s.chain_depth);
    auto read_penalty = [&](double sparse_bytes) {
        return s.read_weight * static_cast<double>(s.chain_depth + 1) * sparse_bytes;
    };
    struct Cand {
        SdcgAutoMode mode;
        double cost;
    };
    std::vector<Cand> cands;
    if (allow_row) {
        // ROW: rewrite K full rows (+ base-read proxy); no DCG bookkeeping, read-flat.
        cands.push_back({SdcgAutoMode::ROW_REWRITE, static_cast<double>(s.K) * (W_row + W_row) + FRAME});
    }
    if (!s.heterogeneous) {
        if (!read_strict) {
            const double sp_bytes = static_cast<double>(s.K) * (W_upd + 8);
            cands.push_back({SdcgAutoMode::COLUMN_SPARSE, (sp_bytes + static_cast<double>(s.K) * DCG_OVH) * depth_mult +
                                                                  FRAME + read_penalty(sp_bytes)});
        }
        cands.push_back({SdcgAutoMode::COLUMN_DENSE,
                         static_cast<double>(s.M) * W_upd + static_cast<double>(s.K) * DCG_OVH + FRAME});
    } else {
        if (!read_strict) {
            const double pk_bytes = static_cast<double>(s.K_union) * (W_upd + 8);
            cands.push_back(
                    {SdcgAutoMode::PACKED_SPARSE, (pk_bytes + static_cast<double>(s.K_union) * DCG_OVH) * depth_mult +
                                                          FRAME + read_penalty(pk_bytes)});
        }
        // masked-dense writes the union value columns row-complete (M rows) -> worst wide writer.
        cands.push_back({SdcgAutoMode::MASKED_DENSE,
                         static_cast<double>(s.M) * W_row + static_cast<double>(s.K) * DCG_OVH + FRAME});
    }
    auto best =
            std::min_element(cands.begin(), cands.end(), [](const Cand& a, const Cand& b) { return a.cost < b.cost; });
    // Hysteresis: keep the sticky (last-chosen) mode if it is still a candidate within a deadband of the best,
    // so a segment converges to and STAYS on one mode instead of oscillating into a mixed (read-worse) chain.
    if (s.sticky >= 0) {
        for (const auto& c : cands) {
            if (static_cast<int>(c.mode) == s.sticky && c.cost <= best->cost * 1.15) {
                return c.mode;
            }
        }
    }
    return best->mode;
}

Status ColumnModePartialUpdateHandler::execute(const RowsetUpdateStateParams& params, MetaFileBuilder* builder,
                                               std::vector<std::vector<uint32_t>>* insert_rowids_by_segment) {
    TRACE_COUNTER_SCOPE_LATENCY_US("pcu_execute_us");
    // 1. load update state first
    RETURN_IF_ERROR(_load_update_state(params));

    const auto& txn_meta = params.op_write.txn_meta();

    // cid may shift across schema versions; recompute it from uid against the current
    // tablet schema. partial_update_column_ids in txn_meta is kept for compatibility only.
    DCHECK_EQ(txn_meta.partial_update_column_ids_size(), txn_meta.partial_update_column_unique_ids_size());
    std::vector<ColumnId> update_column_ids;
    std::vector<ColumnUID> unique_update_column_ids;
    for (int i = 0; i < txn_meta.partial_update_column_unique_ids_size(); ++i) {
        const uint32_t uid = txn_meta.partial_update_column_unique_ids(i);
        const auto cid = params.tablet_schema->field_index(uid);
        if (cid == -1) {
            std::string msg = strings::Substitute("column with unique id:$0 does not exist. tablet:$1", uid,
                                                  params.tablet->tablet_id());
            LOG(ERROR) << msg;
            return Status::InternalError(msg);
        }
        const auto& col = params.tablet_schema->column(cid);
        if (col.is_key() || col.is_auto_increment()) {
            continue;
        }
        update_column_ids.push_back(cid);
        unique_update_column_ids.push_back(uid);

        if (cid != static_cast<ColumnId>(txn_meta.partial_update_column_ids(i))) {
            LOG(INFO) << "lake pcu schema drift detected: tablet=" << params.tablet->tablet_id() << " uid=" << uid
                      << " frozen_cid=" << txn_meta.partial_update_column_ids(i) << " current_cid=" << cid
                      << " schema_id=" << params.tablet_schema->id();
        }
    }

    // When condition update is enabled together with column-mode PCU, delta_writer has validated
    // that the condition column is part of the partial column set; we force a single column batch
    // so the condition column and the rest of the partial columns share one `partial_schema` (and
    // one .col file), and compare_at is then performed inline inside _update_source_chunk_by_upt
    // against the already-read source/upt chunks.
    ASSIGN_OR_RETURN(int32_t condition_cid, _resolve_condition_cid(txn_meta, *params.tablet_schema));

    // FLEXIBLE (per-row heterogeneous) partial update: each row updates a different column subset, encoded
    // as a per-row set-id in the hidden "__cset__" column + the per-rowset distinct_column_sets dictionary.
    // Packing requires ALL the batch's value columns to land in ONE union `.spcols`, so we force a single
    // column batch (like the condition-update path). Decode the dictionary into per-set covered-uid lists
    // once here so the per-(rssid) tasks can build the packed chunk without touching the proto again.
    const bool flexible_mode = txn_meta.flexible_partial_update() && txn_meta.distinct_column_sets_size() > 0;
    // Flexible + merge_condition is unsupported: a merge_condition forces sdcg_enabled=false (so the packed
    // `.spcols` sparse_schema is never built) AND forces flex_column=false (the masked-dense path is gated
    // on condition<0), leaving NO flexible-aware emit path -- the packed branch would deref a null
    // sparse_schema (crash) and the auto/dense fallback would clobber omitted columns (silent corruption).
    // Reject cleanly here (defense in depth; delta_writer also rejects it before the write). A future
    // masked-dense-with-inline-condition path could lift this.
    if (flexible_mode && condition_cid >= 0) {
        return Status::NotSupported("flexible partial update combined with merge_condition is not supported");
    }
    std::vector<std::vector<ColumnUID>> distinct_column_sets;
    if (flexible_mode) {
        distinct_column_sets.reserve(txn_meta.distinct_column_sets_size());
        for (const auto& set_pb : txn_meta.distinct_column_sets()) {
            std::vector<ColumnUID> uids;
            uids.reserve(set_pb.column_unique_ids_size());
            for (uint32_t uid : set_pb.column_unique_ids()) {
                uids.push_back(static_cast<ColumnUID>(uid));
            }
            distinct_column_sets.push_back(std::move(uids));
        }
    }

    const size_t BATCH_HANDLE_COLUMN_CNT =
            ((condition_cid >= 0 || flexible_mode) && !update_column_ids.empty())
                    ? update_column_ids.size()
                    : static_cast<size_t>(config::vertical_compaction_max_columns_per_group);

    // 2. getter all rss_rowid_to_update_rowid, and prepare .col writer by the way
    // rss_id -> update file id -> <rowid, update rowid>
    std::map<uint32_t, UptidToRowidPairs> rss_upt_id_to_rowid_pairs;

    // For COLUMN_UPSERT_MODE: save insert_rowids before clearing _partial_update_states
    if (insert_rowids_by_segment != nullptr) {
        insert_rowids_by_segment->resize(_partial_update_states.size());
    }

    for (int upt_id = 0; upt_id < _partial_update_states.size(); upt_id++) {
        for (const auto& each_rss : _partial_update_states[upt_id].rss_rowid_to_update_rowid) {
            for (const auto& each : each_rss.second) {
                rss_upt_id_to_rowid_pairs[each_rss.first][upt_id].emplace_back(each.first, each.second);
            }
            TRACE_COUNTER_INCREMENT("pcu_update_cnt", each_rss.second.size());
        }
        TRACE_COUNTER_INCREMENT("pcu_insert_rows", _partial_update_states[upt_id].insert_rowids.size());

        if (insert_rowids_by_segment != nullptr) {
            // insert_rowids are already physical positions in the update segment file
            // (build_rss_rowid_to_update_rowid applied upt_segment_physical_rowid_offset),
            // exactly what the downstream fetch_values_by_rowid reads expect.
            (*insert_rowids_by_segment)[upt_id] = std::move(_partial_update_states[upt_id].insert_rowids);
        }
    }

    // M8 guard: a load FE marked flexible (per-row column sets) but whose distinct_column_sets dictionary
    // did NOT arrive at this node (RPC field lost, CN restart between open and eos, or a registry miss)
    // resolves flexible_mode=false above (~line 1221) and would then apply as a homogeneous union --
    // silently overwriting the columns a row did NOT declare with their NULL placeholders. A real flexible
    // JSON load always interns >=1 set, so flexible_partial_update() with empty sets means the dict is
    // missing: FAIL the apply rather than corrupt. Gated on there being update rows (map populated above),
    // so an empty load -- and the homogeneous non-flexible paths, which never set the flexible bit -- are
    // unaffected.
    if (txn_meta.flexible_partial_update() && txn_meta.distinct_column_sets_size() == 0 &&
        !rss_upt_id_to_rowid_pairs.empty()) {
        return Status::InternalError(
                "flexible partial update: column-set dictionary is empty/missing on this node; refusing to "
                "apply as homogeneous (would clobber undeclared columns)");
    }

    const size_t partial_update_states_size = _partial_update_states.size();
    _partial_update_states.clear();
    // must record unique column id in delta column group
    // dcg_column_ids and dcg_column_files are mapped one to the other. E.g.
    // {{1,2}, {3,4}} -> {"aaa.cols", "bbb.cols"}
    // It means column_1 and column_2 are stored in aaa.cols, and column_3 and column_4 are stored in bbb.cols
    std::map<uint32_t, std::vector<std::vector<ColumnUID>>> dcg_column_ids;
    std::map<uint32_t, std::vector<std::pair<std::string, std::string>>> dcg_column_file_with_encryption_metas;
    // Parallel to dcg_column_file_with_encryption_metas: byte size of each `.cols` file,
    // captured from finalize() so readers can avoid a stat/HeadObject when opening the segment.
    std::map<uint32_t, std::vector<int64_t>> dcg_column_file_sizes;
    // SDCG parallel arrays (per rssid, 1:1 with the file lists above): per-file kind and, for sparse
    // files, the row count K. Dense files carry DENSE_COLS / 0. Built only when enable_sparse_dcg picks
    // the sparse path; otherwise stay all-DENSE (byte-identical to the pre-SDCG meta).
    std::map<uint32_t, std::vector<DeltaColumnFileKindPB>> dcg_column_file_kinds;
    std::map<uint32_t, std::vector<int64_t>> dcg_column_sparse_row_counts;
    // Per-file presence summary (1:1 with the file lists above): the [min, max] source_rowid range + K of
    // each sparse `.spcols` file; dense files carry an empty SparsePresencePB. Lets readers skip layers
    // whose range excludes the requested rowid without opening the file.
    std::map<uint32_t, std::vector<SparsePresencePB>> dcg_column_presences;
    // Per-COLUMN presence list (1:1 with the file lists above): for a PACKED (flexible) `.spcols` file, one
    // ColumnSparsePresencePB per UPDATE column carrying the EXACT covered-rowid roaring (the apply gate).
    // Dense files and HOMOGENEOUS sparse files carry an empty ColumnPresenceListPB (reader gates on the
    // file-level presence). Only emitted when at least one packed file has a non-empty list.
    std::map<uint32_t, std::vector<ColumnPresenceListPB>> dcg_column_presence_lists;
    // Per-rssid base segment row count M (fingerprint), recorded into the DCG meta. 0 = unknown.
    std::map<uint32_t, int64_t> dcg_source_segment_num_rows;

    // SDCG decision is enabled only with the flag on AND no merge_condition: the sparse path performs no
    // source-segment read, so it cannot evaluate a condition that compares against the base value. With a
    // condition present we fall back to the (untouched) dense path. M is resolved per rssid up front so
    // the per-(column_batch, rssid) tasks can decide without touching the filesystem concurrently.
    // (A GIN-indexed value column never reaches this column-mode apply: FE forces ROW mode for such an
    // update -- see Load.effectivePartialUpdateMode -- because neither a sparse `.spcols` nor a dense
    // `.cols` overlay carries a GIN index, so MATCH over the overlay could not be answered. The
    // segment_iterator inverted-index sparse guard remains the read-side belt.)
    const bool sdcg_enabled = config::enable_sparse_dcg && condition_cid < 0;
    if (sdcg_enabled) {
        for (const auto& each : rss_upt_id_to_rowid_pairs) {
            const uint32_t rssid = each.first;
            auto m_or = _resolve_source_segment_num_rows(params, rssid);
            // A failure to resolve M just disables the sparse path for this rssid (M stays 0 => dense);
            // it must not fail the whole publish.
            dcg_source_segment_num_rows[rssid] = m_or.ok() ? m_or.value() : 0;
        }
    }
    // 3. read from raw segment file and update file, and generate `.col` files
    // The inner segment loop is parallelized: each (column_batch, rssid) combination is independent
    // since they read different source segments and write to different .col files (UUID-based names).
    for (uint32_t col_index = 0; col_index < update_column_ids.size(); col_index += BATCH_HANDLE_COLUMN_CNT) {
        // 3.1 build column id range (shared across all segments in this column batch)
        std::vector<ColumnId> selective_update_column_ids =
                append_fixed_batch(update_column_ids, col_index, BATCH_HANDLE_COLUMN_CNT);
        std::vector<ColumnUID> selective_unique_update_column_ids =
                append_fixed_batch(unique_update_column_ids, col_index, BATCH_HANDLE_COLUMN_CNT);
        auto partial_tschema = TabletSchema::create_with_uid(params.tablet_schema, selective_unique_update_column_ids);
        Schema partial_schema = ChunkHelper::convert_schema(params.tablet_schema, selective_update_column_ids);

        // SDCG: synthetic schema = [source_rowid] + value columns, shared by every rssid task in this
        // column batch (the per-rssid sparse chunk is built/written independently). Built once here even
        // when most rssids may take the dense path; cheap and keeps the task body lock-free.
        std::shared_ptr<TabletSchema> sparse_tschema;
        std::shared_ptr<Schema> sparse_schema;
        if (sdcg_enabled) {
            sparse_tschema = build_sparse_tablet_schema(params.tablet_schema, partial_tschema);
            sparse_schema = std::make_shared<Schema>(ChunkHelper::convert_schema(sparse_tschema));
        }

        // When condition update is enabled, the single-batch invariant ensures the condition column
        // is present in this batch's partial schema; locate its index for inline compare_at.
        int32_t condition_idx_in_partial_schema = -1;
        if (condition_cid >= 0) {
            ASSIGN_OR_RETURN(condition_idx_in_partial_schema,
                             _locate_condition_idx_in_partial_schema(selective_update_column_ids, condition_cid));
        }

        // Create thread pool token for segment-level parallelism
        std::unique_ptr<ThreadPoolToken> token;
        if (config::enable_pk_index_parallel_execution) {
            token = RuntimeEnv::GetInstance()->lake_partial_update_thread_pool()->new_token(
                    ThreadPool::ExecutionMode::CONCURRENT);
        }

        std::mutex result_mutex;
        Status shared_status;

        for (const auto& each : rss_upt_id_to_rowid_pairs) {
            uint32_t rssid = each.first;
            // `each`/`each.second` are loop-local bindings whose storage may be reused as the
            // loop advances, so capturing a reference to `each.second` from the thread-pool task
            // would dangle. The map itself is stable for the duration of execute(); capture the
            // address of the map entry's value by value and dereference inside the task.
            const auto* upt_pairs_ptr = &each.second;

            const int64_t source_num_rows = sdcg_enabled ? dcg_source_segment_num_rows[rssid] : 0;
            auto func = [this, &params, &partial_schema, &partial_tschema, &sparse_tschema, &sparse_schema,
                         &selective_unique_update_column_ids, rssid, upt_pairs_ptr, condition_idx_in_partial_schema,
                         sdcg_enabled, flexible_mode, &distinct_column_sets, source_num_rows, &dcg_column_ids,
                         &dcg_column_file_with_encryption_metas, &dcg_column_file_sizes, &dcg_column_file_kinds,
                         &dcg_column_sparse_row_counts, &dcg_column_presences, &dcg_column_presence_lists,
                         &result_mutex, &shared_status]() {
                // SDCG density decision (per (column_batch, rssid)): K = distinct source_rowids for this
                // rssid; sparse iff K is small both absolutely (< sdcg_sparse_max_rows) and relative to M
                // (K/M < sdcg_dense_threshold). M==0 (unknown) or no rows => dense fallback.
                bool take_sparse = false;
                int64_t K = 0;
                if (sdcg_enabled && source_num_rows > 0) {
                    std::set<uint32_t> distinct;
                    for (const auto& each_upt : *upt_pairs_ptr) {
                        for (const auto& pair : each_upt.second) {
                            distinct.insert(pair.first);
                        }
                    }
                    K = static_cast<int64_t>(distinct.size());
                    // Cost-model gate (P0-B): sparse only on large-enough segments — its per-file framing +
                    // 8B/row source_rowid + read-amp pay off only when dense's M×col-width rewrite is
                    // genuinely costly, which scales with M (source_num_rows). Small M (or K spread thin
                    // across many small segments) -> dense is faster and reads flatter. Then the K/M ratio +
                    // absolute K cap as before.
                    take_sparse = K > 0 && K < config::sdcg_sparse_max_rows &&
                                  source_num_rows >= config::sdcg_sparse_min_segment_rows &&
                                  (static_cast<double>(K) / static_cast<double>(source_num_rows) <
                                   config::sdcg_dense_threshold);
                }

                // Unified read-amp budget (config::sdcg_read_amp_budget): R_max==0 (read-strict) forbids any
                // append-overlay (sparse/packed) whose read cost grows with chain depth. FIXED-column path ->
                // DENSE (column-dense supersede, zero read-amp) here; FLEXIBLE path -> MASKED-DENSE
                // (flex_column, row-complete `.cols`, also read-flat) via the flex_column gate below. Both
                // honor the same zero-read-amp contract.
                const bool read_strict = (config::sdcg_read_amp_budget == 0);
                if (read_strict && !flexible_mode) {
                    take_sparse = false;
                }

                // FLEXIBLE routing. A heterogeneous per-row load can NEVER use the flexible-UNAWARE plain
                // dense overlay (_update_source_chunk_by_upt overlays the full union including the NULL
                // placeholders a row did NOT declare, destroying those base values). It takes one of two
                // flexible-AWARE paths instead:
                //   * enable_sparse_dcg ON (no condition)  -> PACKED SPARSE (.spcols + per-column roaring).
                //   * enable_sparse_dcg OFF, OR read-strict (no condition) -> FLEX_COLUMN dense: a per-row MASKED
                //     dense overlay (_update_source_chunk_by_upt_flexible) that overlays a value column ONLY at
                //     the rows whose set covers it, writing a row-complete `.cols` for the union columns
                //     (read-neutral, no source_rowid, no per-column roaring). read-strict picks it so a flexible
                //     table honors the zero-read-amp contract (a packed `.spcols` would read-amp).
                //     condition+flexible keeps the packed path (masked-dense can't carry the inline condition).
                // Non-const: for partial_update_mode=auto the adaptive selector (below) may re-route a
                // flexible batch to masked-dense (flex_column) even with enable_sparse_dcg on, when the cost
                // model prefers a row-complete dense overlay over packed sparse.
                bool flex_column =
                        flexible_mode && (!sdcg_enabled || read_strict) && condition_idx_in_partial_schema < 0;
                // Flexible picks EXACTLY ONE of {packed sparse (take_sparse), masked-dense (flex_column)}. Force
                // take_sparse=!flex_column so the K/M-derived take_sparse above cannot leave BOTH set (which
                // would take the packed path and defeat a read-strict flex_column choice).
                if (flexible_mode) {
                    take_sparse = !flex_column;
                }

                // SDCG convergence SAFETY VALVE: convergence of the sparse overlay chain is normally
                // done by BACKGROUND lake PK compaction (it reads input segments through the DCG overlay
                // and emits fresh dense segments, dropping the chain). Only as a last resort, if a chain
                // somehow reaches the HIGH sdcg_promotion_hard_count cap before compaction converges it,
                // do we fall back to a synchronous in-place dense rewrite here. The old per-batch cum_K/M
                // (sdcg_promotion_threshold) trigger has been removed -- it forced a depth-16 dense
                // rewrite on the load's publish critical path roughly every 16 partial-update batches,
                // which was the column-scaling p95 spike. Skip even the safety valve for flexible: it
                // would flip take_sparse back to false and route the apply onto the flexible-unaware
                // dense path (corrupting omitted columns); flexible chains converge via compaction only.
                if (take_sparse && !flexible_mode) {
                    // Convergence of the sparse overlay chain is handled by BACKGROUND compaction: a
                    // normal lake PK compaction reads input segments THROUGH the DCG overlay (the
                    // dcg_loader wired at rowset.cpp for primary keys) and emits fresh DENSE segments,
                    // and apply_opcompaction drops the old sparse-chain DCG entries (orphaning the
                    // .spcols files for GC). The synchronous in-place dense rewrite below is therefore
                    // demoted to a rare SAFETY VALVE that fires only if a chain reaches a HIGH hard cap
                    // before compaction catches it -- it is no longer a per-batch convergence mechanism.
                    // The old cum_K/M (sdcg_promotion_threshold) trigger is removed: a moderately-covered
                    // segment no longer pays a publish-time dense rewrite, which was the column-scaling
                    // p95 cost (a depth-16 promotion fired on the load's critical path). Chain depth is
                    // bounded proactively by the DCG-depth-aware compaction score (compaction_policy).
                    int32_t chain_len = 0;
                    int64_t cum_k = 0;
                    inspect_existing_sparse_chain(params.metadata, rssid, selective_unique_update_column_ids,
                                                  &chain_len, &cum_k);
                    if ((chain_len + 1) > config::sdcg_promotion_hard_count) {
                        take_sparse = false;
                        LOG(INFO) << fmt::format(
                                "SDCG safety-valve dense rewrite at chain cap, tablet_id: {} txn_id: {} "
                                "rssid: {} chain_len: {} cum_K: {} K: {} M: {}",
                                params.tablet->id(), _txn_id, rssid, chain_len, cum_k, K, source_num_rows);
                    }
                }

                // partial_update_mode=auto SHADOW: compute the adaptive selector's recommendation and log it
                // against the current boolean decision. NO behavior change yet -- this calibrates the cost
                // model against production before it takes over the decision.
                {
                    int32_t sh_chain = 0;
                    int64_t sh_cum_k = 0;
                    inspect_existing_sparse_chain(params.metadata, rssid, selective_unique_update_column_ids, &sh_chain,
                                                  &sh_cum_k);
                    SdcgAutoSignals sig;
                    sig.M = source_num_rows;
                    sig.K = K;
                    sig.K_union = K;
                    // Shadow proxies (schema-based); execution wiring will measure W_upd from the upt chunk.
                    sig.W_upd = static_cast<double>(selective_unique_update_column_ids.size()) * 8.0;
                    sig.W_row = static_cast<double>(params.tablet_schema->num_columns()) * 8.0;
                    sig.chain_depth = sh_chain;
                    sig.heterogeneous = flexible_mode && distinct_column_sets.size() > 1;
                    sig.read_weight = (config::sdcg_read_amp_budget == 0) ? 1.0 : 0.0;
                    sig.sticky = -1;
                    const bool auto_resolved = params.op_write.txn_meta().partial_update_auto_resolved();
                    if (auto_resolved) {
                        // EXECUTE: the cost model drives the column write FORMAT (ROW is a per-load level-1
                        // choice at the writer, so allow_row=false). heterogeneous (>1 distinct set) chooses
                        // packed vs masked-dense; a single set (homogeneous -- incl. fixed-column auto, which
                        // FE marks flexible so BE sees the uniform set) chooses sparse vs dense, EMITTED via
                        // the same packed/masked paths (packed-single-set == homogeneous sparse; masked-dense
                        // single-set == dense). The chain-cap valve still bounds depth when read_weight==0.
                        const SdcgAutoMode rec = select_write_mode(sig, /*allow_row=*/false);
                        // CRITICAL: gate on sdcg_enabled. The sparse `.spcols` path needs sparse_schema, which
                        // is built ONLY when sdcg_enabled (line ~1315). enable_sparse_dcg can be false at APPLY
                        // time even though the cost model recommends sparse (e.g. the config was flipped between
                        // write and apply, or auto is used with sparse globally off) -> taking sparse then would
                        // dereference a null sparse_schema and SIGSEGV. When sparse is unavailable, fall back to
                        // the read-flat dense/masked-dense format (also the correct choice with no sparse infra).
                        bool want_sparse = sdcg_enabled &&
                                           (rec == SdcgAutoMode::COLUMN_SPARSE || rec == SdcgAutoMode::PACKED_SPARSE);
                        if (want_sparse && (sh_chain + 1) > config::sdcg_promotion_hard_count) {
                            want_sparse = false;
                        }
                        take_sparse = want_sparse;
                        // dense choice: emit masked-dense (row-complete `.cols`) for a flexible batch even with
                        // enable_sparse_dcg on; a non-flexible batch takes the plain dense path (flex_column false).
                        flex_column = (!want_sparse && flexible_mode && condition_idx_in_partial_schema < 0);
                        const char* chosen = want_sparse ? (flexible_mode ? "packed" : "sparse")
                                                         : (flex_column ? "masked_dense" : "dense");
                        VLOG(1) << fmt::format(
                                "SDCG auto execute: tablet_id: {} rssid: {} K: {} M: {} chain: {} het: {} chose: {}",
                                params.tablet->id(), rssid, K, source_num_rows, sh_chain, sig.heterogeneous, chosen);
                        auto* sm = StorageMetrics::instance();
                        if (want_sparse && flexible_mode) {
                            sm->sdcg_write_mode_packed_total.increment(1);
                        } else if (want_sparse) {
                            sm->sdcg_write_mode_sparse_total.increment(1);
                        } else if (flex_column) {
                            sm->sdcg_write_mode_masked_dense_total.increment(1);
                        } else {
                            sm->sdcg_write_mode_dense_total.increment(1);
                        }
                    } else {
                        // SHADOW (explicit column/flexible): log the full-picture recommendation (incl. ROW)
                        // against the actual decision; no behavior change.
                        const SdcgAutoMode rec = select_write_mode(sig, /*allow_row=*/true);
                        VLOG(2) << fmt::format(
                                "SDCG auto shadow: tablet_id: {} rssid: {} K: {} M: {} chain: {} het: {} recommend: {} "
                                "actual_take_sparse: {}",
                                params.tablet->id(), rssid, K, source_num_rows, sh_chain, sig.heterogeneous,
                                sdcg_auto_mode_name(rec), take_sparse);
                    }
                }

                if (take_sparse && flexible_mode) {
                    // FLEXIBLE PACKED path: different upt rows update different column subsets. Build ONE
                    // union `.spcols` packing ALL the batch's value columns (rows = union of every column's
                    // covered source_rowids; columns physically NULL-padded where absent) and emit PER-COLUMN
                    // presence (exact covered roaring) so the reader applies each column only at its covered
                    // rows. Packed files always go to a FILE (the inline patch has no per-column presence
                    // encoding), so no inline gate here.
                    int64_t packed_rows = 0;
                    int64_t min_source_rowid = kSDCGPresenceUnknown;
                    int64_t max_source_rowid = kSDCGPresenceUnknown;
                    std::vector<PackedColumnPresence> column_presences;
                    auto packed_or = _build_packed_sparse_chunk_from_upt(
                            *upt_pairs_ptr, partial_schema, *sparse_schema, selective_unique_update_column_ids,
                            distinct_column_sets, source_num_rows, &packed_rows, &min_source_rowid, &max_source_rowid,
                            &column_presences);
                    if (!packed_or.ok()) {
                        std::lock_guard<std::mutex> l(result_mutex);
                        shared_status.update(packed_or.status());
                        return;
                    }
                    auto packed_chunk_ptr = std::move(packed_or.value());
                    const size_t packed_chunk_size = packed_chunk_ptr->memory_usage();
                    _tracker->consume(packed_chunk_size);
                    DeferOp tracker_defer([&]() { _tracker->release(packed_chunk_size); });

                    padding_char_columns(*sparse_schema, sparse_tschema, packed_chunk_ptr.get());

                    auto writer_or = _prepare_sparse_delta_column_group_writer(params, sparse_tschema);
                    if (!writer_or.ok()) {
                        std::lock_guard<std::mutex> l(result_mutex);
                        shared_status.update(writer_or.status());
                        return;
                    }
                    auto packed_writer = std::move(writer_or.value());
                    uint64_t segment_file_size = 0;
                    uint64_t index_size = 0;
                    uint64_t footer_position = 0;
                    auto st = packed_writer->append_chunk(*packed_chunk_ptr);
                    if (st.ok()) {
                        st = packed_writer->finalize(&segment_file_size, &index_size, &footer_position);
                    }

                    std::lock_guard<std::mutex> l(result_mutex);
                    shared_status.update(st);
                    if (shared_status.ok()) {
                        // The packed file's DCG entry carries only the UPDATE columns that actually cover >=1
                        // row (column_presences is exactly that set); a column covering no row is dropped from
                        // both the uid list and the per-column presence list, so the reader never resolves it
                        // to this file.
                        std::vector<ColumnUID> covered_uids;
                        ColumnPresenceListPB presence_list_pb;
                        for (const auto& cp : column_presences) {
                            covered_uids.push_back(cp.column_uid);
                            auto* entry = presence_list_pb.add_entries();
                            entry->set_column_uid(static_cast<uint32_t>(cp.column_uid));
                            entry->set_min_source_rowid(cp.min_source_rowid);
                            entry->set_max_source_rowid(cp.max_source_rowid);
                            entry->set_count(cp.count);
                            entry->set_roaring(cp.roaring);
                        }
                        dcg_column_ids[rssid].push_back(std::move(covered_uids));
                        const std::string spcols_name = file_name(packed_writer->segment_path());
                        dcg_column_file_with_encryption_metas[rssid].emplace_back(spcols_name,
                                                                                  packed_writer->encryption_meta());
                        dcg_column_file_sizes[rssid].push_back(static_cast<int64_t>(segment_file_size));
                        dcg_column_file_kinds[rssid].push_back(SPARSE_PERCOL);
                        dcg_column_sparse_row_counts[rssid].push_back(packed_rows);
                        // File-level presence = the union [min,max]+K_union (cheap range skip). The per-column
                        // presence list is the authoritative apply gate.
                        SparsePresencePB presence;
                        if (min_source_rowid != kSDCGPresenceUnknown) presence.set_min_source_rowid(min_source_rowid);
                        if (max_source_rowid != kSDCGPresenceUnknown) presence.set_max_source_rowid(max_source_rowid);
                        presence.set_row_count(packed_rows);
                        dcg_column_presences[rssid].push_back(std::move(presence));
                        dcg_column_presence_lists[rssid].push_back(std::move(presence_list_pb));
                        std::string cols_str;
                        for (size_t c = 0; c < column_presences.size(); ++c) {
                            if (c > 0) cols_str += ",";
                            cols_str += std::to_string(column_presences[c].column_uid);
                        }
                        LOG(INFO) << fmt::format(
                                "SDCG packed sparse write: tablet_id: {} txn_id: {} rssid: {} file: {} K_union: {} "
                                "M: {} min_rowid: {} max_rowid: {} cols: {}",
                                params.tablet->id(), _txn_id, rssid, spcols_name, packed_rows, source_num_rows,
                                min_source_rowid, max_source_rowid, cols_str);
                    }
                    TRACE_COUNTER_INCREMENT("pcu_packed_sparse_handle_cnt", 1);
                    return;
                }

                if (take_sparse) {
                    // SPARSE path: build a K-row [source_rowid + values] chunk from the `.upt` payload
                    // (no source-segment read) and write it into a `.spcols` FILE (a one-time immutable
                    // object). Inline-in-meta carriage was removed (deprecated 2026-06-30); see design §5.4.
                    int64_t sparse_rows = 0;
                    int64_t min_source_rowid = kSDCGPresenceUnknown;
                    int64_t max_source_rowid = kSDCGPresenceUnknown;
                    auto sparse_chunk_or = _build_sparse_chunk_from_upt(*upt_pairs_ptr, partial_schema, *sparse_schema,
                                                                        source_num_rows, &sparse_rows,
                                                                        &min_source_rowid, &max_source_rowid);
                    if (!sparse_chunk_or.ok()) {
                        std::lock_guard<std::mutex> l(result_mutex);
                        shared_status.update(sparse_chunk_or.status());
                        return;
                    }
                    auto sparse_chunk_ptr = std::move(sparse_chunk_or.value());
                    const size_t sparse_chunk_size = sparse_chunk_ptr->memory_usage();
                    _tracker->consume(sparse_chunk_size);
                    DeferOp tracker_defer([&]() { _tracker->release(sparse_chunk_size); });

                    // Pad char value columns (column 0 is the synthetic source_rowid uint32, untouched).
                    padding_char_columns(*sparse_schema, sparse_tschema, sparse_chunk_ptr.get());

                    auto writer_or = _prepare_sparse_delta_column_group_writer(params, sparse_tschema);
                    if (!writer_or.ok()) {
                        std::lock_guard<std::mutex> l(result_mutex);
                        shared_status.update(writer_or.status());
                        return;
                    }
                    auto sparse_writer = std::move(writer_or.value());
                    uint64_t segment_file_size = 0;
                    uint64_t index_size = 0;
                    uint64_t footer_position = 0;
                    auto st = sparse_writer->append_chunk(*sparse_chunk_ptr);
                    if (st.ok()) {
                        st = sparse_writer->finalize(&segment_file_size, &index_size, &footer_position);
                    }

                    std::lock_guard<std::mutex> l(result_mutex);
                    shared_status.update(st);
                    if (shared_status.ok()) {
                        // DCG entry carries the UPDATE column uids only (NOT the reserved source_rowid uid).
                        dcg_column_ids[rssid].push_back(selective_unique_update_column_ids);
                        const std::string spcols_name = file_name(sparse_writer->segment_path());
                        dcg_column_file_with_encryption_metas[rssid].emplace_back(spcols_name,
                                                                                  sparse_writer->encryption_meta());
                        dcg_column_file_sizes[rssid].push_back(static_cast<int64_t>(segment_file_size));
                        dcg_column_file_kinds[rssid].push_back(SPARSE_PERCOL);
                        dcg_column_sparse_row_counts[rssid].push_back(sparse_rows);
                        // Record the presence summary [min, max]+K so readers can skip out-of-range layers.
                        SparsePresencePB presence;
                        if (min_source_rowid != kSDCGPresenceUnknown) presence.set_min_source_rowid(min_source_rowid);
                        if (max_source_rowid != kSDCGPresenceUnknown) presence.set_max_source_rowid(max_source_rowid);
                        presence.set_row_count(sparse_rows);
                        dcg_column_presences[rssid].push_back(std::move(presence));
                        // Homogeneous (single equivalence class) file: no per-column override; the reader
                        // gates on the file-level presence. Push an empty list to keep the array 1:1 with
                        // the file list (needed when this rssid also has a packed file).
                        dcg_column_presence_lists[rssid].push_back(ColumnPresenceListPB());
                        // Observability: the PoC logged nothing on a sparse write, which made cluster
                        // verification hard. One INFO line per `.spcols` with filename, K, M and the column set.
                        std::string cols_str;
                        for (size_t c = 0; c < selective_unique_update_column_ids.size(); ++c) {
                            if (c > 0) cols_str += ",";
                            cols_str += std::to_string(selective_unique_update_column_ids[c]);
                        }
                        LOG(INFO) << fmt::format(
                                "SDCG sparse write: tablet_id: {} txn_id: {} rssid: {} file: {} K: {} M: {} "
                                "min_rowid: {} max_rowid: {} cols: {}",
                                params.tablet->id(), _txn_id, rssid, spcols_name, sparse_rows, source_num_rows,
                                min_source_rowid, max_source_rowid, cols_str);
                    }
                    TRACE_COUNTER_INCREMENT("pcu_sparse_handle_cnt", 1);
                    return;
                }

                // DENSE path (unchanged): read the full source segment column, overlay upt values, write
                // a row-complete `.cols` file.
                // 3.3 read from source segment
                auto source_chunk_or = _read_from_source_segment(params, partial_schema, rssid);
                if (!source_chunk_or.ok()) {
                    std::lock_guard<std::mutex> l(result_mutex);
                    shared_status.update(source_chunk_or.status());
                    return;
                }
                auto source_chunk_ptr = std::move(source_chunk_or.value());
                const size_t source_chunk_size = source_chunk_ptr->memory_usage();
                _tracker->consume(source_chunk_size);
                DeferOp tracker_defer([&]() { _tracker->release(source_chunk_size); });

                // 3.4 read from update segment and apply updates. FLEX_COLUMN takes the per-row MASKED
                // overlay (omitted columns keep base); the homogeneous fixed-colset path takes the plain one.
                Status st;
                if (flex_column) {
                    st = _update_source_chunk_by_upt_flexible(*upt_pairs_ptr, partial_schema,
                                                              selective_unique_update_column_ids, distinct_column_sets,
                                                              &source_chunk_ptr);
                } else {
                    st = _update_source_chunk_by_upt(*upt_pairs_ptr, partial_schema, &source_chunk_ptr,
                                                     condition_idx_in_partial_schema);
                }
                if (!st.ok()) {
                    std::lock_guard<std::mutex> l(result_mutex);
                    shared_status.update(st);
                    return;
                }

                padding_char_columns(partial_schema, partial_tschema, source_chunk_ptr.get());

                // 3.5 write delta column group (.col file with UUID name, no collision)
                auto writer_or = _prepare_delta_column_group_writer(params, partial_tschema);
                if (!writer_or.ok()) {
                    std::lock_guard<std::mutex> l(result_mutex);
                    shared_status.update(writer_or.status());
                    return;
                }
                auto delta_column_group_writer = std::move(writer_or.value());

                uint64_t segment_file_size = 0;
                uint64_t index_size = 0;
                uint64_t footer_position = 0;
                st = delta_column_group_writer->append_chunk(*source_chunk_ptr);
                if (st.ok()) {
                    st = delta_column_group_writer->finalize(&segment_file_size, &index_size, &footer_position);
                }

                // 3.6 collect results under lock
                std::lock_guard<std::mutex> l(result_mutex);
                shared_status.update(st);
                if (shared_status.ok()) {
                    dcg_column_ids[rssid].push_back(selective_unique_update_column_ids);
                    dcg_column_file_with_encryption_metas[rssid].emplace_back(
                            file_name(delta_column_group_writer->segment_path()),
                            delta_column_group_writer->encryption_meta());
                    dcg_column_file_sizes[rssid].push_back(static_cast<int64_t>(segment_file_size));
                    dcg_column_file_kinds[rssid].push_back(DENSE_COLS);
                    dcg_column_sparse_row_counts[rssid].push_back(0);
                    // Dense entry: empty presence keeps the array 1:1 with the file list (unknown == no skip).
                    dcg_column_presences[rssid].push_back(SparsePresencePB());
                    // Dense entry: empty per-column list (reader gates on file-level presence == none).
                    dcg_column_presence_lists[rssid].push_back(ColumnPresenceListPB());
                }
                TRACE_COUNTER_INCREMENT("pcu_handle_cnt", 1);
            };

            if (token) {
                auto submit_st = token->submit_func(func);
                std::lock_guard<std::mutex> l(result_mutex);
                shared_status.update(submit_st);
            } else {
                func();
                RETURN_IF_ERROR(shared_status);
            }
        }

        if (token) {
            TRACE_COUNTER_SCOPE_LATENCY_US("pcu_parallel_dcg_wait_us");
            token->wait();
        }
        RETURN_IF_ERROR(shared_status);
    }
    // 4 generate delta columngroup
    for (const auto& each : rss_upt_id_to_rowid_pairs) {
        const uint32_t rssid = each.first;
        const auto& kinds = dcg_column_file_kinds[rssid];
        const bool rssid_has_sparse =
                std::any_of(kinds.begin(), kinds.end(), [](DeltaColumnFileKindPB k) { return k == SPARSE_PERCOL; });
        if (sdcg_enabled && rssid_has_sparse) {
            // Density-aware: pass per-file kinds + sparse row counts (1:1 with the file list) plus the
            // base segment row count M. The arrays were built in lockstep with the file list inside the
            // task, so they stay aligned even with mixed dense/sparse files for a single rssid.
            // Per-column presence lists (packed flexible files). Only present when a packed file was
            // written for this rssid; otherwise empty (append_dcg pads + omits the array entirely).
            static const std::vector<ColumnPresenceListPB> kNoColumnPresenceLists;
            auto cpl_it = dcg_column_presence_lists.find(rssid);
            const auto& column_presence_lists =
                    cpl_it != dcg_column_presence_lists.end() ? cpl_it->second : kNoColumnPresenceLists;
            builder->append_dcg(rssid, dcg_column_file_with_encryption_metas[rssid], dcg_column_ids[rssid],
                                dcg_column_file_sizes[rssid], dcg_column_file_kinds[rssid],
                                dcg_column_sparse_row_counts[rssid], dcg_column_presences[rssid],
                                dcg_source_segment_num_rows[rssid], column_presence_lists);
        } else {
            // No sparse file/inline for this rssid: byte-identical to pre-SDCG behavior (all-DENSE, no
            // sparse counts, no source row count) regardless of the flag.
            builder->append_dcg(rssid, dcg_column_file_with_encryption_metas[rssid], dcg_column_ids[rssid],
                                dcg_column_file_sizes[rssid]);
        }
    }
    builder->apply_column_mode_partial_update(params.op_write);

    TRACE_COUNTER_INCREMENT("pcu_rss_cnt", rss_upt_id_to_rowid_pairs.size());
    TRACE_COUNTER_INCREMENT("pcu_upt_cnt", partial_update_states_size);
    TRACE_COUNTER_INCREMENT("pcu_column_cnt", update_column_ids.size());
    return Status::OK();
}

// A DCG version chain is replayable iff its conflicting (version > compact_version) layers are all
// plain SPARSE_PERCOL `.spcols` overlays: their (source_rowid -> value) rows can be remapped onto the
// compaction output via the rows mapper. Flexible/packed presence lists and a dense conflicting layer
// are NOT handled by the simple rowid-remap replay and force a discard.
static bool dcg_conflict_is_replayable(const DeltaColumnGroupVerPB& dcg, int64_t compact_version) {
    // PACKED (flexible / per-row heterogeneous) racing layers ARE replayable: a packed file is a
    // SPARSE_PERCOL file carrying per-column presence roaring; the replay reader gates winner cells by
    // that roaring. So no blanket column_presence_lists rejection here -- only the file kind matters below.
    for (int i = 0; i < dcg.column_files_size(); ++i) {
        const int64_t ver = i < dcg.versions_size() ? dcg.versions(i) : 0;
        if (ver > compact_version) {
            const DeltaColumnFileKindPB kind = i < dcg.file_kinds_size() ? dcg.file_kinds(i) : DENSE_COLS;
            // SPARSE_PERCOL and DENSE_COLS racing layers are both remappable via the rows mapper (dense =
            // whole-column rewrite read by base ordinal; sparse = per-source_rowid). Any other kind is not.
            if (kind != SPARSE_PERCOL && kind != DENSE_COLS) {
                return false;
            }
        }
    }
    return true;
}

CompactionConflictKind CompactionUpdateConflictChecker::classify_conflict(const TxnLogPB_OpCompaction& op_compaction,
                                                                          const TabletMetadata& metadata) {
    const bool has_dcg = !metadata.dcg_meta().dcgs().empty();
    const bool has_idg = metadata.has_idg_meta() && !metadata.idg_meta().idgs().empty();
    if (!has_dcg && !has_idg) {
        return CompactionConflictKind::NONE;
    }
    std::unordered_set<uint32_t> input_rowsets; // all rowsets that have been compacted
    std::vector<uint32_t> input_segments;       // all segment that have been compacted
    for (uint32_t input_rowset : op_compaction.input_rowsets()) {
        input_rowsets.insert(input_rowset);
    }
    // 1. find all segments that have been compacted
    for (const auto& rowset : metadata.rowsets()) {
        if (input_rowsets.count(rowset.id()) > 0 && rowset.segment_metas_size() > 0) {
            for (int i = 0; i < rowset.segment_metas_size(); ++i) {
                input_segments.push_back(get_rssid(rowset, i));
            }
        }
    }
    // 2. find out if these segments have been updated (DCG) or had indexes added (IDG) since the
    //    compaction started. An IDG race or a non-replayable DCG race => MUST_DISCARD; a race that is
    //    only plain-sparse DCG overlays => REPLAYABLE_DCG. Scan ALL segments so the kind reflects the
    //    worst conflict (MUST_DISCARD dominates).
    bool found_replayable = false;
    for (uint32_t segment : input_segments) {
        if (has_idg) {
            auto idg_ver_iter = metadata.idg_meta().idgs().find(segment);
            if (idg_ver_iter != metadata.idg_meta().idgs().end()) {
                for (const auto& entry : idg_ver_iter->second.entries()) {
                    if (entry.has_version() && entry.version() > op_compaction.compact_version()) {
                        return CompactionConflictKind::MUST_DISCARD;
                    }
                }
            }
        }
        if (has_dcg) {
            auto dcg_ver_iter = metadata.dcg_meta().dcgs().find(segment);
            if (dcg_ver_iter != metadata.dcg_meta().dcgs().end()) {
                bool conflict = false;
                for (int64_t ver : dcg_ver_iter->second.versions()) {
                    if (ver > op_compaction.compact_version()) {
                        conflict = true;
                        break;
                    }
                }
                if (conflict) {
                    if (!dcg_conflict_is_replayable(dcg_ver_iter->second, op_compaction.compact_version())) {
                        return CompactionConflictKind::MUST_DISCARD;
                    }
                    found_replayable = true;
                }
            }
        }
    }
    return found_replayable ? CompactionConflictKind::REPLAYABLE_DCG : CompactionConflictKind::NONE;
}

bool CompactionUpdateConflictChecker::conflict_check(const TxnLogPB_OpCompaction& op_compaction, int64_t txn_id,
                                                     const TabletMetadata& metadata, MetaFileBuilder* builder) {
    const CompactionConflictKind kind = classify_conflict(op_compaction, metadata);
    if (kind == CompactionConflictKind::NONE) {
        return false;
    }
    builder->apply_opcompaction_with_conflict(op_compaction);
    if (kind == CompactionConflictKind::REPLAYABLE_DCG) {
        StorageMetrics::instance()->sdcg_compaction_conflict_replayable_total.increment(1);
    } else {
        StorageMetrics::instance()->sdcg_compaction_conflict_discard_total.increment(1);
    }
    LOG(INFO) << fmt::format("PK compaction conflict ({}), tablet_id: {} txn_id: {} op_compaction: {}",
                             kind == CompactionConflictKind::REPLAYABLE_DCG ? "replayable-dcg" : "must-discard",
                             metadata.id(), txn_id, op_compaction.ShortDebugString());
    return true;
}

namespace {

// Preconditions for PK-keyed replay, checked after classify_conflict==REPLAYABLE_DCG (which guarantees
// every racing layer is SPARSE_PERCOL or DENSE_COLS, no packed, no IDG). The unified per-uid winner +
// always-packed emit handle heterogeneous updated-column sets across sparse/dense layers, so uniform-uid
// is NO LONGER required. A racing DELETE is no longer a discard condition either: replay runs on the KEPT
// output AFTER the compaction-conflict resolver / persistent-index try_replace has already appended that
// output's delvec (which deletes every racing-deleted/upserted output row), so replay just re-applies the
// racing SPARSE overlays and a racing-deleted row carries a harmless dead overlay filtered by the delvec at
// read. Remaining gate: at least one racing sparse/dense file exists (else nothing to replay). Any
// violation -> caller falls back to safe discard.
bool replay_preconditions_ok(const TxnLogPB_OpCompaction& op_compaction, const TabletMetadata& metadata) {
    const int64_t compact_version = op_compaction.compact_version();
    std::unordered_set<uint32_t> input_rowsets(op_compaction.input_rowsets().begin(),
                                               op_compaction.input_rowsets().end());
    bool any_racing = false;
    for (const auto& rowset : metadata.rowsets()) {
        if (input_rowsets.count(rowset.id()) == 0) continue;
        for (int i = 0; i < rowset.segment_metas_size(); ++i) {
            const uint32_t rssid = get_rssid(rowset, i);
            auto dcg_it = metadata.dcg_meta().dcgs().find(rssid);
            if (dcg_it == metadata.dcg_meta().dcgs().end()) continue;
            const auto& ver = dcg_it->second;
            for (int j = 0; j < ver.column_files_size(); ++j) {
                const int64_t v = j < ver.versions_size() ? ver.versions(j) : 0;
                if (v <= compact_version) continue;
                const DeltaColumnFileKindPB kind = j < ver.file_kinds_size() ? ver.file_kinds(j) : DENSE_COLS;
                // classify_conflict already restricts to SPARSE_PERCOL/DENSE_COLS; defensive re-check.
                if (kind != SPARSE_PERCOL && kind != DENSE_COLS) return false;
                any_racing = true;
            }
        }
    }
    return any_racing;
}

// Standalone `.spcols` writer for the replay path (decoupled from RowsetUpdateStateParams/_txn_id, which
// do not exist at compaction publish). Mirrors _prepare_sparse_delta_column_group_writer.
StatusOr<std::unique_ptr<SegmentWriter>> prepare_replay_spcols_writer(
        const Tablet& tablet, int64_t txn_id, const std::shared_ptr<TabletSchema>& sparse_tschema) {
    const std::string path = tablet.segment_location(gen_spcols_filename(txn_id));
    WritableFileOptions opts{.sync_on_close = true, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
    SegmentWriterOptions writer_options;
    if (config::enable_transparent_data_encryption) {
        ASSIGN_OR_RETURN(auto pair, KeyCache::instance().create_encryption_meta_pair_using_current_kek());
        opts.encryption_info = pair.info;
        writer_options.encryption_meta = std::move(pair.encryption_meta);
    }
    ASSIGN_OR_RETURN(auto wfile, fs::new_writable_file(opts, path));
    auto segment_writer = std::make_unique<SegmentWriter>(std::move(wfile), 0, sparse_tschema, writer_options);
    RETURN_IF_ERROR(segment_writer->init(false));
    return std::move(segment_writer);
}

} // namespace

CompactionConflictKind CompactionUpdateConflictChecker::check_and_maybe_discard(
        const TxnLogPB_OpCompaction& op_compaction, int64_t txn_id, const TabletMetadata& metadata,
        MetaFileBuilder* builder, bool* out_discarded) {
    const CompactionConflictKind kind = classify_conflict(op_compaction, metadata);
    if (kind == CompactionConflictKind::NONE) {
        *out_discarded = false;
        return CompactionConflictKind::NONE;
    }
    const bool replay_enabled = config::enable_sparse_dcg && config::enable_sdcg_compaction_conflict_replay;
    // Replay maps racing input rows -> output rows via the compaction rows mapper (.lcrm); without it
    // (e.g. a full/non-light publish that wrote no mapper) we cannot remap, so fall back to discard.
    if (kind == CompactionConflictKind::REPLAYABLE_DCG && replay_enabled && op_compaction.has_lcrm_file() &&
        replay_preconditions_ok(op_compaction, metadata)) {
        // KEEP the compaction output; the caller will replay the racing overlays onto it before commit.
        StorageMetrics::instance()->sdcg_compaction_conflict_replayable_total.increment(1);
        LOG(INFO) << fmt::format(
                "PK compaction conflict (replayable-dcg: REPLAYING onto output), tablet_id: {} txn_id: {} "
                "op_compaction: {}",
                metadata.id(), txn_id, op_compaction.ShortDebugString());
        *out_discarded = false;
        return CompactionConflictKind::REPLAYABLE_DCG;
    }
    // Discard (orphan) exactly as before: MUST_DISCARD, or REPLAYABLE_DCG with replay disabled / preconditions unmet.
    builder->apply_opcompaction_with_conflict(op_compaction);
    if (kind == CompactionConflictKind::REPLAYABLE_DCG) {
        StorageMetrics::instance()->sdcg_compaction_conflict_replayable_total.increment(1);
    } else {
        StorageMetrics::instance()->sdcg_compaction_conflict_discard_total.increment(1);
    }
    LOG(INFO) << fmt::format("PK compaction conflict ({}), tablet_id: {} txn_id: {} op_compaction: {}",
                             kind == CompactionConflictKind::REPLAYABLE_DCG
                                     ? "replayable-dcg: discarded (replay off or preconditions unmet)"
                                     : "must-discard",
                             metadata.id(), txn_id, op_compaction.ShortDebugString());
    *out_discarded = true;
    return kind;
}

Status CompactionUpdateConflictChecker::replay_sparse_overlays_onto_output(
        const TxnLogPB_OpCompaction& op_compaction, int64_t txn_id, const TabletMetadataPtr& metadata,
        const Tablet& tablet, const Rowset& output_rowset, MetaFileBuilder* builder) {
    const int64_t compact_version = op_compaction.compact_version();
    const TabletSchemaPtr tablet_schema = output_rowset.tablet_schema();
    LakeIOOptions lake_io_opts{.fill_data_cache = false, .skip_disk_cache = false};
    std::unordered_set<uint32_t> input_rowsets(op_compaction.input_rowsets().begin(),
                                               op_compaction.input_rowsets().end());

    // uid -> TabletColumn lookup (value columns are addressed by unique_id in the `.spcols`).
    std::unordered_map<ColumnUID, TabletColumn> uid_to_col;
    for (size_t c = 0; c < tablet_schema->num_columns(); ++c) {
        uid_to_col.emplace(tablet_schema->column(c).unique_id(), tablet_schema->column(c));
    }

    // Unified per-uid winner across SPARSE_PERCOL + DENSE_COLS racing layers, keyed by
    // (input_rssid<<32 | base_rowid). A Cell points at a value inside a loaded layer; layers are read
    // ascending-version per input rssid so a later write to the same (key,uid) overwrites => last-writer
    // -wins. sparse row_index = position k in the K-row .spcols; dense row_index = the base ordinal.
    struct Cell {
        int layer_idx = -1;
        int col_pos = 0; // position of the uid within layers[layer_idx].uids/.values
        uint32_t row_index = 0;
    };
    struct LoadedLayer {
        uint32_t input_rssid = 0;
        bool is_dense = false;
        std::vector<ColumnUID> uids;          // this layer's updated uids (sorted), 1:1 with values
        std::vector<MutableColumnPtr> values; // one per uid; sparse: K rows (by k), dense: M rows (by ordinal)
    };
    std::vector<LoadedLayer> layers;
    // vector-of-(uid,cell) per row is lighter than a nested map -- important because a DENSE layer
    // populates EVERY base row of its segment.
    std::unordered_map<uint64_t, std::vector<std::pair<ColumnUID, Cell>>> winner;
    std::unordered_set<uint32_t> racing_rssids;
    std::set<ColumnUID> union_uids;

    auto set_winner = [&](uint64_t key, ColumnUID uid, Cell cell) {
        auto& vec = winner[key];
        for (auto& p : vec) {
            if (p.first == uid) {
                p.second = cell; // higher-version layer overwrites
                return;
            }
        }
        vec.emplace_back(uid, cell);
    };
    auto find_winner = [&](uint64_t key, ColumnUID uid) -> const Cell* {
        auto it = winner.find(key);
        if (it == winner.end()) return nullptr;
        for (const auto& p : it->second) {
            if (p.first == uid) return &p.second;
        }
        return nullptr;
    };

    // ---- 1. Read every racing SPARSE_PERCOL / DENSE_COLS layer (ascending version) into the winner. ----
    for (const auto& rowset : metadata->rowsets()) {
        if (input_rowsets.count(rowset.id()) == 0) continue;
        for (int i = 0; i < rowset.segment_metas_size(); ++i) {
            const uint32_t rssid = get_rssid(rowset, i);
            auto dcg_it = metadata->dcg_meta().dcgs().find(rssid);
            if (dcg_it == metadata->dcg_meta().dcgs().end()) continue;
            const DeltaColumnGroupVerPB& ver_pb = dcg_it->second;
            std::vector<int> racing;
            for (int j = 0; j < ver_pb.column_files_size(); ++j) {
                const int64_t v = j < ver_pb.versions_size() ? ver_pb.versions(j) : 0;
                const DeltaColumnFileKindPB kind = j < ver_pb.file_kinds_size() ? ver_pb.file_kinds(j) : DENSE_COLS;
                if (v > compact_version && (kind == SPARSE_PERCOL || kind == DENSE_COLS)) racing.push_back(j);
            }
            if (racing.empty()) continue;
            std::sort(racing.begin(), racing.end(),
                      [&](int a, int b) { return ver_pb.versions(a) < ver_pb.versions(b); });

            const auto& seg_meta = rowset.segment_metas(i);
            FileInfo seg_info{.path = tablet.segment_location(seg_meta.filename())};
            if (seg_meta.has_encryption_meta()) seg_info.encryption_meta = seg_meta.encryption_meta();
            if (seg_meta.has_size()) seg_info.size = seg_meta.size();
            if (seg_meta.has_bundle_file_offset()) seg_info.bundle_file_offset = seg_meta.bundle_file_offset();
            ASSIGN_OR_RETURN(auto seg_fs, FileSystemFactory::CreateSharedFromString(seg_info.path));
            const int seg_id_in_rowset = static_cast<int>(get_segment_idx(rowset, i));
            ASSIGN_OR_RETURN(auto base_seg, Segment::open(seg_fs, seg_info, seg_id_in_rowset, tablet_schema, nullptr,
                                                          nullptr, lake_io_opts));
            racing_rssids.insert(rssid);
            auto dcg_ptr = std::make_shared<DeltaColumnGroup>();
            RETURN_IF_ERROR(dcg_ptr->load(metadata->version(), ver_pb));

            for (int j : racing) {
                const DeltaColumnFileKindPB kind = j < ver_pb.file_kinds_size() ? ver_pb.file_kinds(j) : DENSE_COLS;
                std::vector<ColumnUID> uids(ver_pb.unique_column_ids(j).column_ids().begin(),
                                            ver_pb.unique_column_ids(j).column_ids().end());
                std::sort(uids.begin(), uids.end());
                std::vector<TabletColumn> layer_cols;
                layer_cols.reserve(uids.size());
                for (ColumnUID uid : uids) {
                    auto cit = uid_to_col.find(uid);
                    if (cit == uid_to_col.end()) {
                        return Status::InternalError(
                                fmt::format("sdcg replay: updated column uid {} absent from output schema", uid));
                    }
                    layer_cols.push_back(cit->second);
                    union_uids.insert(uid);
                }
                LoadedLayer layer;
                layer.input_rssid = rssid;
                layer.is_dense = (kind == DENSE_COLS);
                layer.uids = uids;
                const int gl = static_cast<int>(layers.size());

                OlapReaderStatistics stats;
                ColumnIteratorOptions iter_opts;
                iter_opts.stats = &stats;

                if (kind == SPARSE_PERCOL) {
                    ASSIGN_OR_RETURN(auto spcols_seg,
                                     base_seg->new_sparse_dcg_segment(*dcg_ptr, j, tablet_schema, lake_io_opts));
                    const int64_t K = j < ver_pb.sparse_row_counts_size() ? ver_pb.sparse_row_counts(j) : 0;
                    if (K <= 0) continue;
                    // PACKED (flexible) layer: a per-column roaring gates which of the K union rows actually
                    // carry each uid (the rest are NULL placeholders). Homogeneous sparse has no presence
                    // list -> every (row,uid) is a real cell. Deserialize per uid (portable readSafe).
                    std::unordered_map<ColumnUID, roaring::Roaring> col_presence;
                    if (j < ver_pb.column_presence_lists_size() && ver_pb.column_presence_lists(j).entries_size() > 0) {
                        for (const auto& e : ver_pb.column_presence_lists(j).entries()) {
                            col_presence.emplace(static_cast<ColumnUID>(e.column_uid()),
                                                 roaring::Roaring::readSafe(e.roaring().data(), e.roaring().size()));
                        }
                    }
                    RandomAccessFileOptions raf_opts;
                    if (spcols_seg->encryption_info()) raf_opts.encryption_info = *spcols_seg->encryption_info();
                    ASSIGN_OR_RETURN(auto rfile, seg_fs->new_random_access_file(raf_opts, spcols_seg->file_name()));
                    iter_opts.read_file = rfile.get();
                    TabletColumn rowid_col;
                    rowid_col.set_unique_id(kSDCGSourceRowidUid);
                    rowid_col.set_name("__sdcg_source_rowid__");
                    rowid_col.set_type(TYPE_BIGINT);
                    rowid_col.set_is_nullable(false);
                    rowid_col.set_length(sizeof(int64_t));
                    ASSIGN_OR_RETURN(auto rid_iter, spcols_seg->new_column_iterator(rowid_col, nullptr));
                    RETURN_IF_ERROR(rid_iter->init(iter_opts));
                    RETURN_IF_ERROR(rid_iter->seek_to_first());
                    auto rid_holder = ChunkFactory::column_from_field_type(TYPE_BIGINT, false);
                    {
                        auto rem = static_cast<size_t>(K);
                        while (rem > 0) {
                            size_t n = rem;
                            RETURN_IF_ERROR(rid_iter->next_batch(&n, rid_holder.get()));
                            if (n == 0) break;
                            rem -= n;
                        }
                    }
                    if (static_cast<int64_t>(rid_holder->size()) != K) {
                        return Status::Corruption("sdcg replay: source_rowid row count mismatch");
                    }
                    std::vector<uint32_t> srowids(K);
                    for (int64_t r = 0; r < K; ++r) srowids[r] = static_cast<uint32_t>(rid_holder->get(r).get_int64());
                    for (size_t c = 0; c < layer_cols.size(); ++c) {
                        ASSIGN_OR_RETURN(auto vit, spcols_seg->new_column_iterator(layer_cols[c], nullptr));
                        RETURN_IF_ERROR(vit->init(iter_opts));
                        RETURN_IF_ERROR(vit->seek_to_first());
                        auto vcol =
                                ChunkFactory::column_from_field_type(layer_cols[c].type(), layer_cols[c].is_nullable());
                        auto rem = static_cast<size_t>(K);
                        while (rem > 0) {
                            size_t n = rem;
                            RETURN_IF_ERROR(vit->next_batch(&n, vcol.get()));
                            if (n == 0) break;
                            rem -= n;
                        }
                        if (static_cast<int64_t>(vcol->size()) != K) {
                            return Status::Corruption("sdcg replay: value column row count mismatch");
                        }
                        layer.values.push_back(std::move(vcol));
                    }
                    for (int64_t r = 0; r < K; ++r) {
                        const uint64_t key = (static_cast<uint64_t>(rssid) << 32) | srowids[r];
                        for (size_t c = 0; c < layer.uids.size(); ++c) {
                            // packed: skip placeholder rows (this uid's roaring does not cover base_rowid).
                            auto pit = col_presence.find(layer.uids[c]);
                            if (pit != col_presence.end() && !pit->second.contains(srowids[r])) {
                                continue;
                            }
                            set_winner(key, layer.uids[c], Cell{gl, static_cast<int>(c), static_cast<uint32_t>(r)});
                        }
                    }
                } else { // DENSE_COLS: row-complete whole-column rewrite, read by base ordinal.
                    ASSIGN_OR_RETURN(auto dense_seg,
                                     base_seg->new_dcg_segment(*dcg_ptr, j, tablet_schema, lake_io_opts));
                    const int64_t M = static_cast<int64_t>(dense_seg->num_rows());
                    if (M <= 0) continue;
                    RandomAccessFileOptions raf_opts;
                    if (dense_seg->encryption_info()) raf_opts.encryption_info = *dense_seg->encryption_info();
                    ASSIGN_OR_RETURN(auto rfile, seg_fs->new_random_access_file(raf_opts, dense_seg->file_name()));
                    iter_opts.read_file = rfile.get();
                    for (size_t c = 0; c < layer_cols.size(); ++c) {
                        ASSIGN_OR_RETURN(auto vit, dense_seg->new_column_iterator(layer_cols[c], nullptr));
                        RETURN_IF_ERROR(vit->init(iter_opts));
                        RETURN_IF_ERROR(vit->seek_to_first());
                        auto vcol =
                                ChunkFactory::column_from_field_type(layer_cols[c].type(), layer_cols[c].is_nullable());
                        auto rem = static_cast<size_t>(M);
                        while (rem > 0) {
                            size_t n = rem;
                            RETURN_IF_ERROR(vit->next_batch(&n, vcol.get()));
                            if (n == 0) break;
                            rem -= n;
                        }
                        if (static_cast<int64_t>(vcol->size()) != M) {
                            return Status::Corruption("sdcg replay: dense value row count mismatch");
                        }
                        layer.values.push_back(std::move(vcol));
                    }
                    for (int64_t r = 0; r < M; ++r) {
                        const uint64_t key = (static_cast<uint64_t>(rssid) << 32) | static_cast<uint32_t>(r);
                        for (size_t c = 0; c < layer.uids.size(); ++c) {
                            set_winner(key, layer.uids[c], Cell{gl, static_cast<int>(c), static_cast<uint32_t>(r)});
                        }
                    }
                }
                layers.push_back(std::move(layer));
            }
        }
    }

    VLOG(2) << fmt::format("SDCG replay enum: tablet_id: {} txn_id: {} racing_layers: {} uids: {} winner_keys: {}",
                           tablet.id(), txn_id, layers.size(), union_uids.size(), winner.size());
    // DEFENSIVE: classify said REPLAYABLE_DCG + preconditions passed, so racing layers MUST exist. An empty
    // load is an inconsistency; FAIL (not silent OK) so we never publish the kept output missing the racing
    // update (silent data loss).
    if (layers.empty() || winner.empty()) {
        return Status::InternalError("sdcg replay: classified REPLAYABLE_DCG but loaded no racing layers");
    }

    // ---- 2. Map racing (input_rssid, base_rowid) -> (output_rssid, output_rowid) via the .lcrm rows mapper. ----
    FileInfo lcrm_info;
    ASSIGN_OR_RETURN(lcrm_info.path,
                     lake_rows_mapper_filename(tablet.tablet_mgr(), tablet.id(), op_compaction.lcrm_file().name()));
    if (op_compaction.lcrm_file().size() > 0) lcrm_info.size = op_compaction.lcrm_file().size();
    RowsMapperIterator mapper;
    RETURN_IF_ERROR(mapper.open(lcrm_info));

    const uint32_t out_rowset_id = metadata->next_rowset_id();
    const auto& out_meta = output_rowset.metadata();
    std::map<uint32_t, int64_t> out_num_rows;
    std::unordered_map<uint64_t, uint64_t> inverse; // input_key -> (output_rssid<<32 | output_rowid)
    inverse.reserve(winner.size());
    // Racing DELETEs need NO handling here: replay runs on the KEPT compaction output AFTER the normal PK
    // compaction-conflict resolver / persistent-index try_replace has already computed and appended that
    // output's delvec (update_manager.cpp light/full publish, step 3, before this call), which deletes every
    // racing-deleted (and racing-upserted) output row. Replay only re-applies the racing SPARSE overlays; a
    // row that was racing-deleted simply carries a harmless dead overlay value filtered by the delvec at read.
    // (An earlier version re-derived and append_delvec'd the deletes here, which REDUNDANTLY CLOBBERED the
    // resolver's already-correct delvec -- removed. Convergence under a racing delete is enabled purely by
    // NOT discarding: replay_preconditions_ok no longer rejects a conflict just because an input delvec
    // advanced past compact_version.)
    for (int s = 0; s < out_meta.segment_metas_size(); ++s) {
        const int32_t sidx = static_cast<int32_t>(get_segment_idx(out_meta, s));
        if (sidx != s) {
            return Status::InternalError("sdcg replay: non-positional output segment_idx; cannot map rows");
        }
        const uint32_t out_rssid = out_rowset_id + sidx;
        const int64_t nrows = out_meta.segment_metas(s).num_rows();
        out_num_rows[out_rssid] = nrows;
        int64_t done = 0;
        while (done < nrows) {
            const size_t want = static_cast<size_t>(std::min<int64_t>(nrows - done, 65536));
            std::vector<uint64_t> vals;
            RETURN_IF_ERROR(mapper.next_values(want, &vals));
            if (vals.empty()) break;
            for (size_t i = 0; i < vals.size(); ++i) {
                const uint64_t input_key = vals[i]; // (input_rssid<<32 | input_rowid)
                const uint32_t in_rssid = static_cast<uint32_t>(input_key >> 32);
                const uint32_t out_rowid = static_cast<uint32_t>(done + i);
                if (racing_rssids.count(in_rssid) == 0) continue;
                if (winner.find(input_key) == winner.end()) continue;
                inverse[input_key] = (static_cast<uint64_t>(out_rssid) << 32) | out_rowid;
            }
            done += static_cast<int64_t>(vals.size());
        }
    }
    RETURN_IF_ERROR(mapper.status());

    // out_rssid -> list of (output_rowid, input_key). A winner key absent from inverse (its input row was
    // deleted/merged by the compaction) is skipped.
    std::map<uint32_t, std::vector<std::pair<uint32_t, uint64_t>>> out_groups;
    size_t skipped = 0;
    for (const auto& kv : winner) {
        auto it = inverse.find(kv.first);
        if (it == inverse.end()) {
            ++skipped;
            continue;
        }
        out_groups[static_cast<uint32_t>(it->second >> 32)].emplace_back(static_cast<uint32_t>(it->second & 0xffffffff),
                                                                         kv.first);
    }
    VLOG(2) << fmt::format("SDCG replay remap(lcrm): txn_id: {} winners: {} skipped: {} out_groups: {}", txn_id,
                           winner.size(), skipped, out_groups.size());
    // Outcome metrics (distinct from *_replayable_total, which also counts replayable-but-discarded): this
    // is a conflict whose racing overlays are actually being re-applied onto the kept output.
    StorageMetrics::instance()->sdcg_compaction_conflict_replay_executed_total.increment(1);
    StorageMetrics::instance()->sdcg_compaction_conflict_replay_winner_rows_total.increment(
            static_cast<int64_t>(winner.size()));
    StorageMetrics::instance()->sdcg_compaction_conflict_replay_skipped_rows_total.increment(
            static_cast<int64_t>(skipped));
    if (out_groups.empty()) {
        return Status::OK(); // every racing row deleted/merged by the compaction; nothing to overlay
    }

    // ---- 3. Per output segment: build a packed `.spcols` (per-column presence) covering exactly the uids
    //         that segment's rows carry; homogeneous (every row covers every covered uid) emits with empty
    //         column_presence_lists (byte-identical to the pure-sparse path). ----
    int64_t total_overlay_bytes = 0; // bytes the discard path would have orphaned (space-reclaim proof)
    for (auto& [out_rssid, rows] : out_groups) {
        std::sort(rows.begin(), rows.end(), [](const auto& a, const auto& b) { return a.first < b.first; });
        auto nr_it = out_num_rows.find(out_rssid);
        if (nr_it == out_num_rows.end()) {
            return Status::InternalError(fmt::format("sdcg replay: output rssid {} not in output rowset", out_rssid));
        }
        const int64_t seg_num_rows = nr_it->second;

        // covered uids for THIS segment (union of its rows' winner uids), sorted -> the file's column set.
        std::set<ColumnUID> covered_set;
        for (const auto& row : rows) {
            auto wit = winner.find(row.second);
            if (wit == winner.end()) continue;
            for (const auto& p : wit->second) covered_set.insert(p.first);
        }
        std::vector<ColumnUID> seg_uids(covered_set.begin(), covered_set.end());
        std::vector<TabletColumn> seg_cols;
        seg_cols.reserve(seg_uids.size());
        for (ColumnUID uid : seg_uids) seg_cols.push_back(uid_to_col.at(uid));
        auto value_tschema = TabletSchema::copy(*tablet_schema, seg_cols);
        auto sparse_tschema = ColumnModePartialUpdateHandler::build_sparse_tablet_schema(tablet_schema, value_tschema);
        std::vector<ColumnId> sparse_cids;
        sparse_cids.reserve(sparse_tschema->num_columns());
        for (size_t c = 0; c < sparse_tschema->num_columns(); ++c) sparse_cids.push_back(static_cast<ColumnId>(c));
        Schema sparse_schema = ChunkHelper::convert_schema(sparse_tschema, sparse_cids);

        auto chunk = ChunkFactory::new_chunk(sparse_schema, rows.size());
        chunk->reset();
        auto* rowid_col = chunk->get_column_raw_ptr_by_index(0);
        uint32_t prev = 0;
        bool first = true;
        for (const auto& row : rows) {
            const uint32_t out_rowid = row.first;
            if (!first && out_rowid == prev) {
                return Status::InternalError("sdcg replay: duplicate output_rowid (PK uniqueness violated)");
            }
            if (static_cast<int64_t>(out_rowid) >= seg_num_rows) {
                return Status::InternalError(fmt::format("sdcg replay: output_rowid {} >= output segment num_rows {}",
                                                         out_rowid, seg_num_rows));
            }
            rowid_col->append_datum(Datum(static_cast<int64_t>(out_rowid)));
            prev = out_rowid;
            first = false;
        }

        // per-column value gather + OUTPUT-space presence roaring, built from EXACTLY the emitted rows.
        std::vector<roaring::Roaring> col_roaring(seg_uids.size());
        std::vector<int64_t> col_count(seg_uids.size(), 0);
        bool homogeneous = true;
        for (size_t uc = 0; uc < seg_uids.size(); ++uc) {
            auto* dst = chunk->get_column_raw_ptr_by_index(uc + 1);
            for (const auto& row : rows) {
                const Cell* cell = find_winner(row.second, seg_uids[uc]);
                if (cell != nullptr) {
                    dst->append(*layers[cell->layer_idx].values[cell->col_pos], cell->row_index, 1);
                    col_roaring[uc].add(row.first);
                    col_count[uc]++;
                } else {
                    dst->append_default(1);
                    homogeneous = false; // this row does not carry seg_uids[uc]
                }
            }
        }
        auto char_field_indexes = ChunkSchemaHelper::get_char_field_indexes(sparse_schema);
        ChunkHelper::padding_char_columns(char_field_indexes, sparse_schema, sparse_tschema, chunk.get());

        ASSIGN_OR_RETURN(auto writer, prepare_replay_spcols_writer(tablet, txn_id, sparse_tschema));
        uint64_t file_size = 0, index_size = 0, footer_pos = 0;
        RETURN_IF_ERROR(writer->append_chunk(*chunk));
        RETURN_IF_ERROR(writer->finalize(&file_size, &index_size, &footer_pos));
        total_overlay_bytes += static_cast<int64_t>(file_size);

        const std::string spcols_name = file_name(writer->segment_path());
        const int64_t krows = static_cast<int64_t>(rows.size());
        SparsePresencePB presence;
        presence.set_min_source_rowid(rows.front().first);
        presence.set_max_source_rowid(rows.back().first);
        presence.set_row_count(krows);

        // Heterogeneous per-row column sets -> emit per-column presence (packed). Homogeneous (every row
        // covers every covered uid, e.g. pure-sparse-uniform or pure-dense) -> empty list (byte-identical).
        std::vector<ColumnPresenceListPB> cpls;
        if (!homogeneous) {
            ColumnPresenceListPB cpl;
            for (size_t uc = 0; uc < seg_uids.size(); ++uc) {
                // Every seg_uid is covered by >=1 row (covered_set is the union of rows' uids), so the
                // roaring is non-empty by construction.
                col_roaring[uc].runOptimize();
                if (col_roaring[uc].maximum() >= static_cast<uint32_t>(seg_num_rows)) {
                    return Status::Corruption(fmt::format("sdcg replay: presence roaring max {} >= seg_num_rows {}",
                                                          col_roaring[uc].maximum(), seg_num_rows));
                }
                if (static_cast<int64_t>(col_roaring[uc].cardinality()) != col_count[uc]) {
                    return Status::Corruption("sdcg replay: presence roaring cardinality != covered cell count");
                }
                auto* entry = cpl.add_entries();
                entry->set_column_uid(seg_uids[uc]);
                entry->set_count(col_count[uc]);
                entry->set_min_source_rowid(static_cast<int64_t>(col_roaring[uc].minimum()));
                entry->set_max_source_rowid(static_cast<int64_t>(col_roaring[uc].maximum()));
                std::string buf;
                buf.resize(col_roaring[uc].getSizeInBytes());
                col_roaring[uc].write(buf.data());
                entry->set_roaring(std::move(buf));
            }
            cpls.push_back(std::move(cpl));
        }

        builder->append_dcg(out_rssid, {{spcols_name, writer->encryption_meta()}}, {seg_uids},
                            {static_cast<int64_t>(file_size)}, {SPARSE_PERCOL}, {krows}, {presence}, seg_num_rows,
                            cpls);
        VLOG(2) << fmt::format(
                "SDCG conflict replay: tablet_id: {} txn_id: {} out_rssid: {} file: {} K': {} out_seg_rows: {} "
                "cols: {} packed: {}",
                tablet.id(), txn_id, out_rssid, spcols_name, krows, seg_num_rows, seg_uids.size(), !homogeneous);
    }
    StorageMetrics::instance()->sdcg_compaction_conflict_replay_overlay_bytes_total.increment(total_overlay_bytes);
    return Status::OK();
}
} // namespace starrocks::lake

// SDCG auto-mode: full-scope rebuild marker (defeat OSS package squat for --scope all FE deploy).
