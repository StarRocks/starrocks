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
#include "common/config_compaction_fwd.h"
#include "common/config_primary_key_fwd.h"
#include "common/config_rowset_fwd.h"
#include "common/tracer.h"
#include "fs/fs_factory.h"
#include "fs/fs_util.h"
#include "fs/key_cache.h"
#include "gutil/strings/substitute.h"
#include "runtime/current_thread.h"
#include "runtime/env/global_env.h"
#include "serde/column_array_serde.h"
#include "storage/chunk_helper.h"
#include "storage/delta_column_group.h"
#include "storage/flexible_partial_update.h"
#include "storage/lake/column_mode_partial_update_handler.h"
#include "storage/lake/filenames.h"
#include "storage/lake/meta_file.h"
#include "storage/lake/update_manager.h"
#include "storage/primary_key_encoder.h"
#include "storage/rowset/column_iterator.h"
#include "storage/rowset/default_value_column_iterator.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/rowset_options.h"
#include "storage/rowset/segment_options.h"
#include "storage/rowset/segment_rewriter.h"
#include "storage/tablet.h"

namespace starrocks::lake {

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

std::shared_ptr<TabletSchema> ColumnModePartialUpdateHandler::_build_sparse_tablet_schema(
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

// Literal name of the hidden per-row set-id column (the SET-ID SPINE). FE injects it as a load slot
// immediately before "__op"; it survives into the `.upt` as a real partial-update column. The writer
// (Agent B's lake delta_writer) appends it with the reserved unique-id kCsetReservedColumnUid; the
// reader below resolves it against the `.upt` footer BY THAT UNIQUE-ID (segment v2 maps schema fields
// to physical columns by unique_id), not by name -- the name is kept only for diagnostics. Reconciled
// with Agent B: kSDCGCsetColumnName == flexible_partial_update.h LOAD_CSET_COLUMN ("__cset__").
static constexpr const char* kSDCGCsetColumnName = "__cset__";

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
    //    column_in_set[set_id] is a bitmask over the batch's value-column positions [0, num_value_cols).
    //    A uid in the set that is not part of this batch is ignored (it lands in another column batch).
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
    std::set<uint32_t> union_source_rowids;

    for (const auto& each : upt_id_to_rowid_pairs) {
        const uint32_t upt_id = each.first;
        ASSIGN_OR_RETURN(std::vector<int32_t> set_ids, _read_cset_column_from_upt(upt_id));
        for (const auto& pair : each.second) {
            const uint32_t source_rowid = pair.first;
            const uint32_t upt_rowid = pair.second;
            if (upt_rowid >= set_ids.size()) {
                return Status::InternalError(fmt::format(
                        "ColumnModePartialUpdateHandler: upt_rowid {} out of range of `{}` column (size {})", upt_rowid,
                        kSDCGCsetColumnName, set_ids.size()));
            }
            const int32_t set_id = set_ids[upt_rowid];
            if (set_id < 0 || static_cast<size_t>(set_id) >= set_covers.size()) {
                return Status::InternalError(fmt::format(
                        "ColumnModePartialUpdateHandler: set-id {} out of range of distinct_column_sets (size {})",
                        set_id, set_covers.size()));
            }
            const auto& covers = set_covers[set_id];
            bool covers_any = false;
            for (size_t c = 0; c < num_value_cols; ++c) {
                if (covers[c]) {
                    per_column_pairs[c].push_back(ColumnPair{source_rowid, upt_id, upt_rowid});
                    covers_any = true;
                }
            }
            if (covers_any) {
                union_source_rowids.insert(source_rowid);
            }
        }
    }

    const int64_t K_union = static_cast<int64_t>(union_source_rowids.size());
    *out_num_rows = K_union;
    if (K_union == 0) {
        return Status::InternalError("ColumnModePartialUpdateHandler: empty packed sparse class");
    }
    *out_min_source_rowid = static_cast<int64_t>(*union_source_rowids.begin());
    *out_max_source_rowid = static_cast<int64_t>(*union_source_rowids.rbegin());
    // Fingerprint guard: every union source_rowid must be a valid ordinal of the base segment.
    if (source_segment_num_rows > 0) {
        DCHECK_LT(static_cast<int64_t>(*union_source_rowids.rbegin()), source_segment_num_rows);
        if (static_cast<int64_t>(*union_source_rowids.rbegin()) >= source_segment_num_rows) {
            return Status::InternalError(
                    fmt::format("ColumnModePartialUpdateHandler: packed source_rowid {} >= source segment num_rows {}",
                                *union_source_rowids.rbegin(), source_segment_num_rows));
        }
    }

    // base_rowid -> union ordinal [0, K_union). The set is ascending, so this is exactly column 0's order.
    std::unordered_map<uint32_t, uint32_t> base_rowid_to_union;
    base_rowid_to_union.reserve(union_source_rowids.size());
    {
        uint32_t local = 0;
        for (uint32_t base_rowid : union_source_rowids) {
            base_rowid_to_union.emplace(base_rowid, local++);
        }
    }

    // 2. Build the K_union-row packed chunk: column 0 = ascending union source_rowids; value columns
    //    default-filled to K_union (placeholders) then overlaid only at their covered union ordinals.
    auto sparse_chunk = ChunkFactory::new_chunk(sparse_schema, K_union);
    {
        auto& rowid_column = sparse_chunk->get_column_by_index(0);
        std::vector<int64_t> sorted_rowids(union_source_rowids.begin(), union_source_rowids.end());
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
            for (const auto& cp : col_pairs[c]) {
                upt_rowids.push_back(cp.upt_rowid);
                const uint32_t uord = base_rowid_to_union.at(cp.source_rowid);
                union_ordinals.push_back(uord);
                column_roaring[c].add(cp.source_rowid);
            }
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

StatusOr<InlineSparsePatchPB> ColumnModePartialUpdateHandler::_build_inline_patch_from_sparse_chunk(
        const Chunk& sparse_chunk, const std::vector<ColumnUID>& unique_update_column_ids, int64_t row_count,
        int64_t min_source_rowid, int64_t max_source_rowid, size_t* out_patch_bytes) {
    // sparse_chunk layout: column 0 = source_rowid (BIGINT, ascending), columns 1..N = value columns,
    // 1:1 with unique_update_column_ids by position. This is the same K-row chunk that the file path
    // would have written into a `.spcols`; here we serialize it into the meta instead.
    DCHECK_EQ(sparse_chunk.num_columns(), unique_update_column_ids.size() + 1);
    DCHECK_EQ(static_cast<int64_t>(sparse_chunk.num_rows()), row_count);

    InlineSparsePatchPB patch;
    // version is left unset here; MetaFileBuilder::append_dcg stamps it with the publish version so the
    // patch interleaves with the new file entries' versions[]. out_patch_bytes therefore slightly
    // under-counts (by the eventual version varint), which is harmless for the byte gates.
    patch.set_row_count(row_count);
    if (min_source_rowid != kSDCGPresenceUnknown) patch.set_min_source_rowid(min_source_rowid);
    if (max_source_rowid != kSDCGPresenceUnknown) patch.set_max_source_rowid(max_source_rowid);

    // source_rowids -> K x uint32 little-endian, ascending. Column 0 is a BIGINT (Int64) column (no
    // unsigned encoding exists), but every value is a valid base-segment ordinal < M <= UINT32_MAX, so
    // it narrows to uint32 safely. Read element-wise to stay byte-order explicit (BE is LE in practice).
    {
        const auto& rowid_col = sparse_chunk.get_column_by_index(0);
        std::string rowid_bytes;
        rowid_bytes.resize(static_cast<size_t>(row_count) * sizeof(uint32_t));
        for (int64_t i = 0; i < row_count; ++i) {
            const int64_t v = rowid_col->get(i).get_int64();
            DCHECK_GE(v, 0);
            const uint32_t u = static_cast<uint32_t>(v);
            std::memcpy(rowid_bytes.data() + static_cast<size_t>(i) * sizeof(uint32_t), &u, sizeof(uint32_t));
        }
        patch.set_source_rowids(std::move(rowid_bytes));
    }

    // One ColumnArraySerde blob per value column, in unique_update_column_ids order. encode_level 0 keeps
    // the blob self-describing (header included by the serde) so the reader can deserialize without extra
    // side metadata. A type the serde cannot handle yields max_serialized_size == 0 -> bail to the file path.
    for (size_t c = 0; c < unique_update_column_ids.size(); ++c) {
        patch.add_column_uids(static_cast<uint32_t>(unique_update_column_ids[c]));
        const auto& value_col = sparse_chunk.get_column_by_index(c + 1);
        const int64_t max_size = serde::ColumnArraySerde::max_serialized_size(*value_col);
        if (max_size <= 0) {
            return Status::NotSupported(
                    fmt::format("SDCG inline patch: column uid {} type is not serializable by ColumnArraySerde",
                                unique_update_column_ids[c]));
        }
        std::string blob;
        blob.resize(static_cast<size_t>(max_size));
        ASSIGN_OR_RETURN(uint8_t * end,
                         serde::ColumnArraySerde::serialize(*value_col, reinterpret_cast<uint8_t*>(blob.data())));
        // serialize returns the one-past-the-end pointer; the real size may be < max_serialized_size.
        blob.resize(static_cast<size_t>(end - reinterpret_cast<uint8_t*>(blob.data())));
        patch.add_column_values(std::move(blob));
    }

    *out_patch_bytes = patch.ByteSizeLong();
    return patch;
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
    // Inline patches are sparse overlays on the SAME axis as `.spcols` files for promotion purposes:
    // each existing inline patch that covers any batch column counts toward chain depth and cum_K, so a
    // long run of micro-batches (which prefer inline) still triggers in-place promotion to a dense rewrite.
    for (const auto& patch : dcg.inline_patches()) {
        bool covers_batch_col = false;
        for (uint32_t uid : patch.column_uids()) {
            if (batch_uids.count(static_cast<ColumnUID>(uid)) > 0) {
                covers_batch_col = true;
                break;
            }
        }
        if (!covers_batch_col) continue;
        ++(*out_chain_len);
        *out_cum_k += patch.row_count();
    }
}

Status ColumnModePartialUpdateHandler::execute(const RowsetUpdateStateParams& params, MetaFileBuilder* builder,
                                               std::vector<std::vector<uint32_t>>* insert_rowids_by_segment) {
    TRACE_COUNTER_SCOPE_LATENCY_US("pcu_execute_us");
    // 1. load update state first
    RETURN_IF_ERROR(_load_update_state(params));

    const auto& txn_meta = params.op_write.txn_meta();

    std::vector<ColumnId> update_column_ids;
    std::vector<ColumnUID> unique_update_column_ids;
    for (ColumnId cid : txn_meta.partial_update_column_ids()) {
        if (cid >= params.tablet_schema->num_key_columns()) {
            if (!params.tablet_schema->column(cid).is_auto_increment()) {
                update_column_ids.push_back(cid);
            }
        }
    }
    for (uint32_t uid : txn_meta.partial_update_column_unique_ids()) {
        auto cid = params.tablet_schema->field_index(uid);
        if (cid == -1) {
            std::string msg = strings::Substitute("column with unique id:$0 does not exist. tablet:$1", uid,
                                                  params.tablet->tablet_id());
            LOG(ERROR) << msg;
            return Status::InternalError(msg);
        }
        if (!params.tablet_schema->column(cid).is_key() && !params.tablet_schema->column(cid).is_auto_increment()) {
            unique_update_column_ids.push_back(uid);
        }
    }

    DCHECK(update_column_ids.size() == unique_update_column_ids.size());

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
    // SDCG inline patches per rssid: micro-batch sparse overlays carried IN-META (no `.spcols` file),
    // a SEPARATE axis from the file lists above. Filled when a sparse-eligible (column-batch, rssid)
    // patch fits the inline byte gates; otherwise that batch takes the `.spcols` file path. Riding the
    // per-publish meta PUT, inline patches cost ZERO new objects.
    std::map<uint32_t, std::vector<InlineSparsePatchPB>> dcg_inline_patches;
    // Per-rssid byte size of the EXISTING DCG meta entry (before this publish), used by the cumulative
    // meta-ceiling gate (sdcg_dcg_meta_max_bytes_per_segment). Resolved once up front (no per-task IO).
    std::map<uint32_t, size_t> dcg_existing_entry_bytes;
    // Per-rssid base segment row count M (fingerprint), recorded into the DCG meta. 0 = unknown.
    std::map<uint32_t, int64_t> dcg_source_segment_num_rows;

    // SDCG decision is enabled only with the flag on AND no merge_condition: the sparse path performs no
    // source-segment read, so it cannot evaluate a condition that compares against the base value. With a
    // condition present we fall back to the (untouched) dense path. M is resolved per rssid up front so
    // the per-(column_batch, rssid) tasks can decide without touching the filesystem concurrently.
    const bool sdcg_enabled = config::enable_sparse_dcg && condition_cid < 0;
    if (sdcg_enabled) {
        for (const auto& each : rss_upt_id_to_rowid_pairs) {
            const uint32_t rssid = each.first;
            auto m_or = _resolve_source_segment_num_rows(params, rssid);
            // A failure to resolve M just disables the sparse path for this rssid (M stays 0 => dense);
            // it must not fail the whole publish.
            dcg_source_segment_num_rows[rssid] = m_or.ok() ? m_or.value() : 0;
            // Existing DCG meta byte size for this rssid (the inline patches it already carries are
            // re-uploaded every publish). Resolved once from the in-memory metadata the publish already
            // loaded (no IO). 0 when this segment has no DCG entry yet.
            size_t existing_bytes = 0;
            if (params.metadata != nullptr) {
                const auto& dcgs = params.metadata->dcg_meta().dcgs();
                auto it = dcgs.find(rssid);
                if (it != dcgs.end()) {
                    existing_bytes = it->second.ByteSizeLong();
                }
            }
            dcg_existing_entry_bytes[rssid] = existing_bytes;
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
            sparse_tschema = _build_sparse_tablet_schema(params.tablet_schema, partial_tschema);
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
            token = GlobalEnv::GetInstance()->lake_partial_update_thread_pool()->new_token(
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
            const size_t existing_entry_bytes = sdcg_enabled ? dcg_existing_entry_bytes[rssid] : 0;

            auto func = [this, &params, &partial_schema, &partial_tschema, &sparse_tschema, &sparse_schema,
                         &selective_unique_update_column_ids, rssid, upt_pairs_ptr, condition_idx_in_partial_schema,
                         sdcg_enabled, flexible_mode, &distinct_column_sets, source_num_rows, existing_entry_bytes,
                         &dcg_column_ids, &dcg_column_file_with_encryption_metas, &dcg_column_file_sizes,
                         &dcg_column_file_kinds, &dcg_column_sparse_row_counts, &dcg_column_presences,
                         &dcg_column_presence_lists, &dcg_inline_patches, &result_mutex, &shared_status]() {
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
                    take_sparse = K > 0 && K < config::sdcg_sparse_max_rows &&
                                  (static_cast<double>(K) / static_cast<double>(source_num_rows) <
                                   config::sdcg_dense_threshold);
                }

                // SDCG flexible: a heterogeneous per-row load CANNOT be applied via the dense union
                // path (_update_source_chunk_by_upt overlays the full union, including NULL
                // placeholders for the columns a given row did NOT declare, destroying those base
                // values). Honoring per-row presence is correctness-critical, not a perf option, so
                // a flexible apply MUST take the packed-presence path regardless of the SDCG density
                // decision (enable_sparse_dcg, the K/M gate, or M being unknown). The packed path
                // tolerates source_num_rows==0 (it uses M only as a guard) and builds presence from
                // the actual covered source_rowids.
                if (flexible_mode) {
                    take_sparse = true;
                }

                // SDCG convergence (in-place promotion): even when the density decision favors a new sparse
                // layer, force the dense path when the EXISTING sparse chain for this rssid's batch columns
                // is already deep. Two independent triggers:
                //   (a) chain_len + 1 > sdcg_promotion_hard_count  -- a hard cap on overlay depth, and
                //   (b) cum_K + K >= sdcg_promotion_threshold * M  -- the accumulated touched rows across
                //       the chain plus this batch reach a fraction of the segment, at which point a full
                //       rewrite is no costlier to read than walking the chain.
                // The dense rewrite reads the source THROUGH the overlay readers, materializing the whole
                // chain, and append_dcg's dense-supersede then orphans every superseded sparse layer --
                // convergence without any background compaction worker.
                // Skip in-place promotion for flexible: it would flip take_sparse back to false and
                // route the apply onto the flexible-unaware dense path (corrupting omitted columns).
                // Flexible chains converge via compaction, not the promotion dense-rewrite.
                if (take_sparse && !flexible_mode) {
                    int32_t chain_len = 0;
                    int64_t cum_k = 0;
                    inspect_existing_sparse_chain(params.metadata, rssid, selective_unique_update_column_ids,
                                                  &chain_len, &cum_k);
                    const bool hit_hard_count = (chain_len + 1) > config::sdcg_promotion_hard_count;
                    const bool hit_threshold = source_num_rows > 0 && (static_cast<double>(cum_k + K) >=
                                                                       config::sdcg_promotion_threshold *
                                                                               static_cast<double>(source_num_rows));
                    if (hit_hard_count || hit_threshold) {
                        take_sparse = false;
                        LOG(INFO) << fmt::format(
                                "SDCG promotion: forcing dense rewrite to converge sparse chain, tablet_id: {} "
                                "txn_id: {} rssid: {} chain_len: {} cum_K: {} K: {} M: {} trigger: {}",
                                params.tablet->id(), _txn_id, rssid, chain_len, cum_k, K, source_num_rows,
                                hit_hard_count ? "hard_count" : "threshold");
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
                    // (no source-segment read). The same chunk feeds either the in-meta INLINE patch or
                    // the `.spcols` FILE — both are sparse overlays, identical content, different carrier.
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

                    // INLINE decision: build the patch from the SAME chunk and apply two byte gates:
                    //   (1) serialized patch bytes <= sdcg_inline_patch_max_bytes  -- per-patch budget, and
                    //   (2) existing meta entry bytes + patch bytes <= sdcg_dcg_meta_max_bytes_per_segment
                    //       -- cumulative ceiling (meta is re-uploaded every publish; exceeding it biases
                    //       to a FILE, a one-time immutable object). Promotion accounting already counted
                    //       inline patches, so a long inline run still triggers the dense rewrite above.
                    // A type ColumnArraySerde cannot handle (NotSupported) just falls back to the file path.
                    {
                        size_t patch_bytes = 0;
                        auto patch_or = _build_inline_patch_from_sparse_chunk(
                                *sparse_chunk_ptr, selective_unique_update_column_ids, sparse_rows, min_source_rowid,
                                max_source_rowid, &patch_bytes);
                        if (patch_or.ok()) {
                            const bool fits_per_patch =
                                    static_cast<int64_t>(patch_bytes) <= config::sdcg_inline_patch_max_bytes;
                            const bool fits_meta_ceiling = static_cast<int64_t>(existing_entry_bytes + patch_bytes) <=
                                                           config::sdcg_dcg_meta_max_bytes_per_segment;
                            if (fits_per_patch && fits_meta_ceiling) {
                                std::lock_guard<std::mutex> l(result_mutex);
                                if (shared_status.ok()) {
                                    dcg_inline_patches[rssid].push_back(std::move(patch_or.value()));
                                    std::string cols_str;
                                    for (size_t c = 0; c < selective_unique_update_column_ids.size(); ++c) {
                                        if (c > 0) cols_str += ",";
                                        cols_str += std::to_string(selective_unique_update_column_ids[c]);
                                    }
                                    LOG(INFO) << fmt::format(
                                            "SDCG inline patch write: tablet_id: {} txn_id: {} rssid: {} K: {} "
                                            "bytes: {} M: {} min_rowid: {} max_rowid: {} cols: {}",
                                            params.tablet->id(), _txn_id, rssid, sparse_rows, patch_bytes,
                                            source_num_rows, min_source_rowid, max_source_rowid, cols_str);
                                }
                                TRACE_COUNTER_INCREMENT("pcu_inline_patch_cnt", 1);
                                return;
                            }
                        }
                        // else: not serializable or over budget -> fall through to the `.spcols` file path.
                    }

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

                // 3.4 read from update segment and apply updates
                auto st = _update_source_chunk_by_upt(*upt_pairs_ptr, partial_schema, &source_chunk_ptr,
                                                      condition_idx_in_partial_schema);
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
        // Inline patches are a SEPARATE axis from files: a rssid may have NO `.spcols` file yet still
        // carry inline patches (the common micro-batch case). Either makes the entry SDCG-active.
        auto inline_it = dcg_inline_patches.find(rssid);
        const bool rssid_has_inline = inline_it != dcg_inline_patches.end() && !inline_it->second.empty();
        if (sdcg_enabled && (rssid_has_sparse || rssid_has_inline)) {
            // Density-aware: pass per-file kinds + sparse row counts (1:1 with the file list) plus the
            // base segment row count M and any inline patches. The arrays were built in lockstep with the
            // file list inside the task, so they stay aligned even with mixed dense/sparse files for a
            // single rssid. Inline patches ride the meta on a separate axis (no file).
            static const std::vector<InlineSparsePatchPB> kNoInlinePatches;
            const auto& inline_patches = rssid_has_inline ? inline_it->second : kNoInlinePatches;
            // Per-column presence lists (packed flexible files). Only present when a packed file was
            // written for this rssid; otherwise empty (append_dcg pads + omits the array entirely).
            static const std::vector<ColumnPresenceListPB> kNoColumnPresenceLists;
            auto cpl_it = dcg_column_presence_lists.find(rssid);
            const auto& column_presence_lists =
                    cpl_it != dcg_column_presence_lists.end() ? cpl_it->second : kNoColumnPresenceLists;
            builder->append_dcg(rssid, dcg_column_file_with_encryption_metas[rssid], dcg_column_ids[rssid],
                                dcg_column_file_sizes[rssid], dcg_column_file_kinds[rssid],
                                dcg_column_sparse_row_counts[rssid], dcg_column_presences[rssid],
                                dcg_source_segment_num_rows[rssid], inline_patches, column_presence_lists);
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

bool CompactionUpdateConflictChecker::conflict_check(const TxnLogPB_OpCompaction& op_compaction, int64_t txn_id,
                                                     const TabletMetadata& metadata, MetaFileBuilder* builder) {
    const bool has_dcg = !metadata.dcg_meta().dcgs().empty();
    const bool has_idg = metadata.has_idg_meta() && !metadata.idg_meta().idgs().empty();
    if (!has_dcg && !has_idg) {
        return false;
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
    // 2. find out if these segments have been updated (DCG) or had indexes
    //    added (IDG) since the compaction started. Either race forces the
    //    compaction to land as an "with_conflict" no-op so the newer delta
    //    is preserved.
    for (uint32_t segment : input_segments) {
        if (has_dcg) {
            auto dcg_ver_iter = metadata.dcg_meta().dcgs().find(segment);
            if (dcg_ver_iter != metadata.dcg_meta().dcgs().end()) {
                for (int64_t ver : dcg_ver_iter->second.versions()) {
                    if (ver > op_compaction.compact_version()) {
                        builder->apply_opcompaction_with_conflict(op_compaction);
                        LOG(INFO) << fmt::format(
                                "PK compaction conflict with partial column update, tablet_id: {} txn_id: {} "
                                "op_compaction: {}",
                                metadata.id(), txn_id, op_compaction.ShortDebugString());
                        return true;
                    }
                }
            }
        }
        if (has_idg) {
            auto idg_ver_iter = metadata.idg_meta().idgs().find(segment);
            if (idg_ver_iter != metadata.idg_meta().idgs().end()) {
                for (const auto& entry : idg_ver_iter->second.entries()) {
                    if (entry.has_version() && entry.version() > op_compaction.compact_version()) {
                        builder->apply_opcompaction_with_conflict(op_compaction);
                        LOG(INFO) << fmt::format(
                                "Compaction conflict with ADD INDEX fast path, tablet_id: {} txn_id: {} "
                                "op_compaction: {}",
                                metadata.id(), txn_id, op_compaction.ShortDebugString());
                        return true;
                    }
                }
            }
        }
    }

    return false;
}

} // namespace starrocks::lake
