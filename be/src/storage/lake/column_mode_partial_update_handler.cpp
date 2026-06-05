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
    const size_t BATCH_HANDLE_COLUMN_CNT =
            (condition_cid >= 0 && !update_column_ids.empty())
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

            auto func = [this, &params, &partial_schema, &partial_tschema, &sparse_tschema, &sparse_schema,
                         &selective_unique_update_column_ids, rssid, upt_pairs_ptr, condition_idx_in_partial_schema,
                         sdcg_enabled, source_num_rows, &dcg_column_ids, &dcg_column_file_with_encryption_metas,
                         &dcg_column_file_sizes, &dcg_column_file_kinds, &dcg_column_sparse_row_counts,
                         &dcg_column_presences, &result_mutex, &shared_status]() {
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
                if (take_sparse) {
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

                if (take_sparse) {
                    // SPARSE path: build a K-row [source_rowid + values] chunk from the `.upt` payload
                    // (no source-segment read) and write it to a `.spcols` file.
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
            // task, so they stay aligned even with mixed dense/sparse files for a single rssid. Only when
            // this rssid actually has a sparse file do we record M (avoids carrying SDCG meta on an
            // all-dense rssid even when the flag is on).
            builder->append_dcg(rssid, dcg_column_file_with_encryption_metas[rssid], dcg_column_ids[rssid],
                                dcg_column_file_sizes[rssid], dcg_column_file_kinds[rssid],
                                dcg_column_sparse_row_counts[rssid], dcg_column_presences[rssid],
                                dcg_source_segment_num_rows[rssid]);
        } else {
            // No sparse file for this rssid: byte-identical to pre-SDCG behavior (all-DENSE, no sparse
            // counts, no source row count) regardless of the flag.
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
