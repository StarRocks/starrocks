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
#include <climits>
#include <filesystem>

#include "base/debug/trace.h"
#include "base/phmap/phmap.h"
#include "base/testutil/sync_point.h"
#include "base/time/time.h"
#include "base/utility/defer_op.h"
#include "column/chunk_factory.h"
#include "column/chunk_schema_helper.h"
#include "column/fixed_length_column.h"
#include "column/serde/column_array_serde.h"
#include "column/vectorized_fwd.h"
#include "common/config_compaction_fwd.h"
#include "common/config_primary_key_fwd.h"
#include "common/config_rowset_fwd.h"
#include "common/tracer.h"
#include "fs/fs.h"
#include "fs/fs_factory.h"
#include "fs/fs_util.h"
#include "gutil/strings/substitute.h"
#include "platform/key_cache.h"
#include "runtime/current_thread.h"
#include "runtime/runtime_env.h"
#include "storage/chunk_helper.h"
#include "storage/data_dir.h"
#include "storage/delta_column_group.h"
#include "storage/lake/column_mode_partial_update_handler.h"
#include "storage/lake/filenames.h"
#include "storage/lake/meta_file.h"
#include "storage/lake/parallel_task_runner.h"
#include "storage/lake/update_manager.h"
#include "storage/rowset/column_iterator.h"
#include "storage/rowset/column_reader.h"
#include "storage/rowset/default_value_column_iterator.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/rowset_options.h"
#include "storage/rowset/segment_options.h"
#include "storage/rowset/segment_rewriter.h"
#include "storage/storage_engine.h"
#include "storage/tablet.h"
#include "storage_primitive/primary_key_encoder.h"

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
    _release_spill();
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

    ASSIGN_OR_RETURN(_upt_memory_usage_per_row, _calc_upt_memory_usage_per_row(*params.tablet_schema));

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
    // Only a SPLIT child's cross publish gets one; nullptr on every ordinary publish, and then every
    // row is owned. Held by the handler because the iterators keep referencing it.
    ASSIGN_OR_RETURN(_row_selector, CrossPublishRowSelector::create_if_needed(*params.metadata, params.tablet_schema,
                                                                              _rowset_ptr->metadata()));

    // Create lazy-load SegmentPKIterators with deferred first load.
    // defer_data_load=true avoids loading the first chunk during init(), so that
    // all iterators can be created without a memory spike. The actual data load
    // happens on-demand when each iterator is first consumed in batch_parallel_get_rss_rowids.
    std::vector<SegmentPKIteratorPtr> pk_iters(num_segments);
    for (uint32_t i = 0; i < num_segments; i++) {
        pk_iters[i] = std::make_unique<SegmentPKIterator>();
        // Must precede init(): the selection is built inside _load().
        pk_iters[i]->set_row_selector(_row_selector.get());
        RETURN_IF_ERROR(pk_iters[i]->init(segment_iters[i], pkey_schema, true /*lazy_load*/, pk_encoding_type,
                                          true /*defer_data_load*/));
    }

    // Parallel query PK index: each segment's PKs are loaded chunk-by-chunk (lazy)
    // and each chunk is queried against the index in parallel via thread pool.
    std::vector<std::vector<uint64_t>> rss_rowids_per_segment;
    std::vector<Filter> owned_per_segment;
    RETURN_IF_ERROR(params.tablet->update_mgr()->batch_get_rss_rowids_from_pkindex(
            params.tablet->id(), _base_version, pk_iters, &rss_rowids_per_segment, false /*need_lock*/,
            &owned_per_segment));

    // Build rss_rowid_to_update_rowid mapping for each update segment. Pass each
    // segment's physical rowid base (range_start, captured by the iterator during
    // the query above) so insert_rowids are physical positions in the segment file
    // — see ColumnPartialUpdateState::build_rss_rowid_to_update_rowid.
    _partial_update_states.resize(num_segments);
    for (uint32_t i = 0; i < num_segments; i++) {
        _partial_update_states[i].src_rss_rowids = std::move(rss_rowids_per_segment[i]);
        _partial_update_states[i].build_rss_rowid_to_update_rowid(pk_iters[i]->physical_rowid_base(),
                                                                  owned_per_segment[i]);
        _partial_update_states[i].inited = true;
    }

    return Status::OK();
}

StatusOr<int64_t> ColumnModePartialUpdateHandler::_calc_upt_memory_usage_per_row(const TabletSchema& tablet_schema) {
    // RowsetMetadataPB only exposes compressed file sizes. The segment footer's per-column
    // total_mem_footprint is accumulated from Column::byte_size() by SegmentWriter, which matches
    // the total_update_row_size accounting used by the shared-nothing update rowset writer.
    LakeIOOptions lake_io_opts{.fill_data_cache = false, .fill_metadata_cache = true};
    ASSIGN_OR_RETURN(auto segments, _rowset_ptr->segments(lake_io_opts));

    int64_t total_update_row_size = 0;
    int64_t num_rows_upt = 0;
    for (const auto& segment : segments) {
        if (segment == nullptr) {
            continue;
        }
        num_rows_upt += segment->num_rows();
        // Sum every column that is actually present in the partial-update segment, including PK
        // columns. This intentionally retains the conservative shared-nothing accounting even when
        // the source segment is processed one update-column group at a time.
        for (size_t cid = 0; cid < tablet_schema.num_columns(); ++cid) {
            const auto* column_reader = segment->column_with_uid(tablet_schema.column(cid).unique_id());
            if (column_reader == nullptr) {
                continue;
            }
            const uint64_t column_size = column_reader->total_mem_footprint();
            if (column_size > static_cast<uint64_t>(INT64_MAX - total_update_row_size)) {
                return Status::Corruption("partial update row memory footprint overflows int64");
            }
            total_update_row_size += static_cast<int64_t>(column_size);
        }
    }

    int64_t result = RowsetColumnUpdateState::calc_upt_memory_usage_per_row(total_update_row_size, num_rows_upt);
    TEST_SYNC_POINT_CALLBACK("ColumnModePartialUpdateHandler::_calc_upt_memory_usage_per_row", &result);
    return result;
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

Status ColumnModePartialUpdateHandler::_read_from_source_segment_and_update(
        const RowsetUpdateStateParams& params, const Schema& schema, uint32_t rssid,
        const std::function<Status(StreamChunkContainer)>& update_func) {
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
    seg_options.chunk_size = config::vector_chunk_size;
    ASSIGN_OR_RETURN(auto seg_iter, segment->new_iterator(schema, seg_options));
    auto source_chunk_ptr = ChunkFactory::new_chunk(schema, config::vector_chunk_size);
    auto tmp_chunk_ptr = ChunkFactory::new_chunk(schema, config::vector_chunk_size);
    uint32_t start_rowid = 0;
    // Accumulate the source bytes incrementally. bytes_usage() may walk every value for
    // object-backed columns, while appending a batch adds exactly that batch's bytes.
    int64_t source_chunk_bytes = 0;
    auto emit_container = [&]() {
        StreamChunkContainer container = {
                .chunk_ptr = source_chunk_ptr.get(),
                .start_rowid = start_rowid,
                .end_rowid = start_rowid + static_cast<uint32_t>(source_chunk_ptr->num_rows())};
        TEST_SYNC_POINT_CALLBACK("ColumnModePartialUpdateHandler::_read_from_source_segment_and_update:emit",
                                 &container);
        RETURN_IF_ERROR(update_func(container));
        start_rowid += static_cast<uint32_t>(source_chunk_ptr->num_rows());
        source_chunk_ptr->reset();
        source_chunk_bytes = 0;
        return Status::OK();
    };
    while (true) {
        tmp_chunk_ptr->reset();
        auto st = seg_iter->get_next(tmp_chunk_ptr.get());
        if (st.is_end_of_file()) {
            break;
        } else if (!st.ok()) {
            return st;
        } else {
            // Check before appending from it: appending reads the source offsets, and reading
            // offsets that have already wrapped is what throws or silently copies from the wrong
            // address. Note this accumulator has no byte budget at all -- it is sized at the whole
            // segment -- so the column limit is the only thing bounding it.
            RETURN_IF_ERROR(ChunkHelper::reject_if_over_capacity(*tmp_chunk_ptr,
                                                                 "column mode partial update source segment read batch",
                                                                 params.tablet->id(), _txn_id));
            const int64_t batch_bytes = static_cast<int64_t>(tmp_chunk_ptr->bytes_usage());
            const int64_t rows_after = static_cast<int64_t>(source_chunk_ptr->num_rows()) +
                                       static_cast<int64_t>(tmp_chunk_ptr->num_rows());
            if (!source_chunk_ptr->is_empty() &&
                (rows_after >= INT32_MAX || source_chunk_bytes + batch_bytes + rows_after * _upt_memory_usage_per_row >
                                                    config::partial_update_memory_limit_per_worker)) {
                RETURN_IF_ERROR(emit_container());
            }
            // Keep an oversized iterator batch intact. The capacity check below remains the hard
            // safety bound, while the memory limit controls accumulation across batches.
            source_chunk_ptr->append(*tmp_chunk_ptr);
            source_chunk_bytes += batch_bytes;
            RETURN_IF_ERROR(ChunkHelper::reject_if_over_capacity(
                    *source_chunk_ptr, "column mode partial update source chunk", params.tablet->id(), _txn_id));
        }
    }
    if (!source_chunk_ptr->is_empty()) {
        RETURN_IF_ERROR(emit_container());
    }
    return Status::OK();
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
                                                                   const Schema& partial_schema,
                                                                   StreamChunkContainer container,
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
        // A whole .upt file lands in one chunk, because the upt rowids below index into it. Check
        // before append_selective() reads its offsets.
        RETURN_IF_ERROR(ChunkHelper::reject_if_over_capacity(*upt_chunk, "column mode partial update upt file chunk",
                                                             _rowset_ptr->tablet_id(), _txn_id));
        const size_t upt_chunk_size = upt_chunk->memory_usage();
        _tracker->consume(upt_chunk_size);
        DeferOp tracker_defer([&]() { _tracker->release(upt_chunk_size); });
        // 2. update source chunk
        std::vector<uint32_t> sorted_source_rowids;
        std::vector<uint32_t> unsorted_upt_rowids;
        // Keep only source rowids in this streamed chunk and align them to the chunk's rowid base.
        split_rowid_pairs(each.second, &sorted_source_rowids, &unsorted_upt_rowids, &container);
        DCHECK(sorted_source_rowids.size() == unsorted_upt_rowids.size());

        // When condition update is enabled, compare the condition column value in
        // source chunk vs upt chunk and keep winners (old <= new); equal values let
        // the new row win, matching the upsert path.
        if (condition_idx_in_partial_schema >= 0) {
            const auto& source_cond = container.chunk_ptr->get_column_by_index(condition_idx_in_partial_schema);
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
        RETURN_IF_EXCEPTION(container.chunk_ptr->update_rows(*tmp_chunk, sorted_source_rowids.data()));
        // The merge writes values wider than the ones it replaces, so the result can be over the
        // limit even though both inputs were under it, and the next .upt file in this loop reads
        // these offsets again.
        RETURN_IF_ERROR(ChunkHelper::reject_if_over_capacity(*container.chunk_ptr,
                                                             "column mode partial update merged source chunk",
                                                             _rowset_ptr->tablet_id(), _txn_id));
    }
    return Status::OK();
}

std::string ColumnModePartialUpdateHandler::_spill_dir() {
    if (!config::lake_pk_column_mode_spill_dir.empty()) {
        return config::lake_pk_column_mode_spill_dir;
    }
    auto* engine = StorageEngine::instance();
    if (engine != nullptr) {
        auto stores = engine->get_stores();
        if (!stores.empty()) {
            return stores[0]->path() + "/lake_pcu_spill";
        }
    }
    return std::filesystem::temp_directory_path().string() + "/lake_pcu_spill";
}

void ColumnModePartialUpdateHandler::_release_spill() {
    if (_spill.writer != nullptr) {
        (void)_spill.writer->close();
        _spill.writer.reset();
    }
    if (!_spill.path.empty()) {
        auto st = FileSystem::Default()->delete_file(_spill.path);
        if (!st.ok() && !st.is_not_found()) {
            LOG(WARNING) << "Fail to delete column-mode partial update spill file " << _spill.path << ": " << st;
        }
        _spill.path.clear();
    }
    _spill.records.clear();
    _spill.size = 0;
    _spill.ready = false;
}

Status ColumnModePartialUpdateHandler::_spill_update_rows(
        const std::map<uint32_t, UptidToRowidPairs>& rss_upt_id_to_rowid_pairs, const Schema& partial_schema) {
    TRACE_COUNTER_SCOPE_LATENCY_US("pcu_spill_write_us");
    _release_spill();
    const uint32_t num_upt = _rowset_ptr->num_segments();

    // Invert rssid -> upt_id -> (source rowid, upt rowid) into one list per update file, ordered by
    // upt rowid, so the file can be streamed once and each row routed to its segment on the way.
    struct Entry {
        uint32_t upt_rowid;
        uint32_t rssid;
        uint32_t src_rowid;
    };
    std::vector<std::vector<Entry>> per_upt(num_upt);
    for (const auto& [rssid, upt_map] : rss_upt_id_to_rowid_pairs) {
        for (const auto& [upt_id, pairs] : upt_map) {
            RETURN_ERROR_IF_FALSE(upt_id < num_upt, fmt::format("update file id {} out of range {}, tablet {} txn {}",
                                                                upt_id, num_upt, _rowset_ptr->tablet_id(), _txn_id));
            for (const auto& p : pairs) {
                per_upt[upt_id].push_back({p.second, rssid, p.first});
            }
        }
    }
    for (auto& entries : per_upt) {
        std::sort(entries.begin(), entries.end(),
                  [](const Entry& a, const Entry& b) { return a.upt_rowid < b.upt_rowid; });
    }

    const std::string dir = _spill_dir();
    RETURN_IF_ERROR(FileSystem::Default()->create_dir_recursive(dir));
    _spill.path = fmt::format("{}/pcu_{}_{}_{}.spill", dir, _rowset_ptr->tablet_id(), _txn_id, _spill_seq++);
    ASSIGN_OR_RETURN(_spill.writer, FileSystem::Default()->new_writable_file(_spill.path));

    // One buffer per source segment: this batch's columns plus the source rowid of each row.
    struct Buffer {
        ChunkUniquePtr chunk;
        UInt32Column::MutablePtr rowids;
    };
    std::map<uint32_t, Buffer> buffers;
    int64_t buffered_bytes = 0;
    DeferOp release_buffered([&]() { _tracker->release(buffered_bytes); });

    auto flush = [&](uint32_t upt_id) -> Status {
        std::string payload;
        for (auto& [rssid, buffer] : buffers) {
            if (buffer.chunk == nullptr || buffer.chunk->num_rows() == 0) {
                continue;
            }
            int64_t max_size = 0;
            for (const auto& column : buffer.chunk->columns()) {
                const int64_t n = serde::ColumnArraySerde::max_serialized_size(*column);
                RETURN_ERROR_IF_FALSE(n > 0, "column-mode partial update cannot spill this column type");
                max_size += n;
            }
            max_size += serde::ColumnArraySerde::max_serialized_size(*buffer.rowids);
            payload.resize(max_size);
            auto* cur = reinterpret_cast<uint8_t*>(payload.data());
            for (const auto& column : buffer.chunk->columns()) {
                ASSIGN_OR_RETURN(cur, serde::ColumnArraySerde::serialize(*column, cur));
            }
            ASSIGN_OR_RETURN(cur, serde::ColumnArraySerde::serialize(*buffer.rowids, cur));
            const int64_t length = cur - reinterpret_cast<uint8_t*>(payload.data());
            RETURN_IF_ERROR(_spill.writer->append(Slice(payload.data(), length)));
            _spill.records[rssid].push_back(SpillRecord{.upt_id = upt_id,
                                                        .num_rows = static_cast<uint32_t>(buffer.chunk->num_rows()),
                                                        .offset = _spill.size,
                                                        .length = length});
            _spill.size += length;
            TRACE_COUNTER_INCREMENT("pcu_spill_records", 1);
            buffer.chunk->reset();
            buffer.rowids->reset_column();
        }
        _tracker->release(buffered_bytes);
        buffered_bytes = 0;
        return Status::OK();
    };

    for (uint32_t upt_id = 0; upt_id < num_upt; upt_id++) {
        auto& entries = per_upt[upt_id];
        if (entries.empty()) {
            continue;
        }
        OlapReaderStatistics stats;
        ASSIGN_OR_RETURN(auto segment_iters, _rowset_ptr->get_each_segment_iterator(partial_schema, true, &stats));
        RETURN_ERROR_IF_FALSE(segment_iters.size() == num_upt);
        DeferOp close_iters([&]() {
            for (auto& iter : segment_iters) {
                if (iter != nullptr) {
                    iter->close();
                }
            }
        });
        if (segment_iters[upt_id] == nullptr) {
            // A lost update-file segment ignored via experimental_lake_ignore_lost_segment produces
            // no rowid pairs, so this should not happen; keep the old behaviour of skipping it.
            LOG(WARNING) << "column-mode partial update skips a null update-file segment iterator, tablet: "
                         << _rowset_ptr->tablet_id() << ", upt_id: " << upt_id
                         << " (a lost segment via experimental_lake_ignore_lost_segment, or an unexpected empty slot)";
            continue;
        }
        TEST_SYNC_POINT_CALLBACK("ColumnModePartialUpdateHandler::_spill_update_rows:upt_read", &upt_id);
        auto& iter = segment_iters[upt_id];
        auto chunk = ChunkFactory::new_chunk(partial_schema, DEFAULT_CHUNK_SIZE);
        uint32_t base = 0;
        size_t pos = 0;
        std::vector<uint32_t> indexes;
        std::vector<uint32_t> src_rowids;
        while (pos < entries.size()) {
            chunk->reset();
            auto st = iter->get_next(chunk.get());
            if (st.is_end_of_file()) {
                break;
            } else if (!st.ok()) {
                return st;
            }
            const uint32_t n = chunk->num_rows();
            const uint32_t end = base + n;
            const size_t start = pos;
            while (pos < entries.size() && entries[pos].upt_rowid < end) {
                pos++;
            }
            if (start < pos) {
                RETURN_IF_ERROR(ChunkHelper::reject_if_over_capacity(*chunk, "column mode partial update upt chunk",
                                                                     _rowset_ptr->tablet_id(), _txn_id));
                const int64_t row_bytes = static_cast<int64_t>(chunk->bytes_usage()) / n + sizeof(uint32_t);
                // Group this chunk's rows by segment; the stable sort keeps upt rowid order inside a group.
                std::stable_sort(entries.begin() + start, entries.begin() + pos,
                                 [](const Entry& a, const Entry& b) { return a.rssid < b.rssid; });
                size_t i = start;
                while (i < pos) {
                    const uint32_t rssid = entries[i].rssid;
                    indexes.clear();
                    src_rowids.clear();
                    for (; i < pos && entries[i].rssid == rssid; i++) {
                        indexes.push_back(entries[i].upt_rowid - base);
                        src_rowids.push_back(entries[i].src_rowid);
                    }
                    auto& buffer = buffers[rssid];
                    if (buffer.chunk == nullptr) {
                        buffer.chunk = ChunkFactory::new_chunk(partial_schema, 0);
                        buffer.rowids = UInt32Column::create();
                    }
                    TRY_CATCH_BAD_ALLOC(buffer.chunk->append_selective(*chunk, indexes.data(), 0, indexes.size()));
                    buffer.rowids->append_numbers(src_rowids.data(), src_rowids.size() * sizeof(uint32_t));
                    const int64_t bytes = row_bytes * static_cast<int64_t>(indexes.size());
                    _tracker->consume(bytes);
                    buffered_bytes += bytes;
                }
                if (buffered_bytes > config::lake_pk_column_mode_spill_buffer_bytes) {
                    RETURN_IF_ERROR(flush(upt_id));
                }
            }
            base = end;
        }
        // Flush at the update-file boundary so every record holds rows of one update file.
        RETURN_IF_ERROR(flush(upt_id));
    }
    RETURN_IF_ERROR(_spill.writer->close());
    _spill.writer.reset();
    _spill.ready = true;
    TRACE_COUNTER_INCREMENT("pcu_spill_bytes", _spill.size);
    return Status::OK();
}

Status ColumnModePartialUpdateHandler::_update_source_chunk_from_spill(uint32_t rssid, const Schema& partial_schema,
                                                                       StreamChunkContainer container,
                                                                       int32_t condition_idx_in_partial_schema) {
    TRACE_COUNTER_SCOPE_LATENCY_US("pcu_update_source_by_upt_us");
    RETURN_ERROR_IF_FALSE(_spill.ready, "column-mode partial update spill is not ready");
    auto it = _spill.records.find(rssid);
    if (it == _spill.records.end()) {
        return Status::OK();
    }
    // read_at_fully is a seek followed by a read, so the segment tasks must not share one stream:
    // each call opens its own reader on the local file.
    ASSIGN_OR_RETURN(auto reader, FileSystem::Default()->new_random_access_file(_spill.path));
    std::string payload;
    std::vector<std::pair<uint32_t, uint32_t>> rows; // (source rowid in container, row in record)
    for (const auto& rec : it->second) {
        payload.resize(rec.length);
        RETURN_IF_ERROR(reader->read_at_fully(rec.offset, payload.data(), rec.length));
        auto upt_chunk = ChunkFactory::new_chunk(partial_schema, rec.num_rows);
        auto rowids = UInt32Column::create();
        const auto* cur = reinterpret_cast<const uint8_t*>(payload.data());
        const auto* end = cur + rec.length;
        for (auto& column : upt_chunk->columns()) {
            ASSIGN_OR_RETURN(cur, serde::ColumnArraySerde::deserialize(cur, end, column->as_mutable_raw_ptr()));
        }
        ASSIGN_OR_RETURN(cur, serde::ColumnArraySerde::deserialize(cur, end, rowids.get()));
        RETURN_ERROR_IF_FALSE(upt_chunk->num_rows() == rec.num_rows && rowids->size() == rec.num_rows,
                              "column-mode partial update spill record is corrupted");

        // Keep only the rows of this streamed source range, ordered by source rowid like the
        // whole-file path does.
        rows.clear();
        const auto& src = rowids->get_data();
        for (uint32_t i = 0; i < rec.num_rows; i++) {
            if (src[i] >= container.start_rowid && src[i] < container.end_rowid) {
                rows.emplace_back(src[i] - container.start_rowid, i);
            }
        }
        if (rows.empty()) {
            continue;
        }
        std::stable_sort(rows.begin(), rows.end(), [](const auto& a, const auto& b) { return a.first < b.first; });
        std::vector<uint32_t> sorted_source_rowids;
        std::vector<uint32_t> unsorted_upt_rowids;
        sorted_source_rowids.reserve(rows.size());
        unsorted_upt_rowids.reserve(rows.size());
        for (const auto& [source_rowid, upt_rowid] : rows) {
            sorted_source_rowids.push_back(source_rowid);
            unsorted_upt_rowids.push_back(upt_rowid);
        }

        if (condition_idx_in_partial_schema >= 0) {
            const auto& source_cond = container.chunk_ptr->get_column_by_index(condition_idx_in_partial_schema);
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
        RETURN_IF_EXCEPTION(container.chunk_ptr->update_rows(*tmp_chunk, sorted_source_rowids.data()));
        RETURN_IF_ERROR(ChunkHelper::reject_if_over_capacity(*container.chunk_ptr,
                                                             "column mode partial update merged source chunk",
                                                             _rowset_ptr->tablet_id(), _txn_id));
    }
    return Status::OK();
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

        // When condition update is enabled, the single-batch invariant ensures the condition column
        // is present in this batch's partial schema; locate its index for inline compare_at.
        int32_t condition_idx_in_partial_schema = -1;
        if (condition_cid >= 0) {
            ASSIGN_OR_RETURN(condition_idx_in_partial_schema,
                             _locate_condition_idx_in_partial_schema(selective_update_column_ids, condition_cid));
        }

        // Read each update file once and hand every source segment its own rows (see
        // _spill_update_rows). Declared before the runner so the spill outlives every task.
        const bool spill_update_rows = config::lake_pk_column_mode_spill_update_rows;
        if (spill_update_rows) {
            RETURN_IF_ERROR(_spill_update_rows(rss_upt_id_to_rowid_pairs, partial_schema));
        }
        DeferOp release_spill([&]() { _release_spill(); });

        // Create thread pool token for segment-level parallelism
        auto token = RuntimeEnv::GetInstance()->lake_partial_update_thread_pool()->new_token(
                ThreadPool::ExecutionMode::CONCURRENT);

        // Declared before the runner so it outlives the join in ~ParallelTaskRunner: the tasks lock
        // it, and reverse declaration order would otherwise destroy it first.
        // Guards the shared dcg_* result maps only; the status is the runner's business now.
        std::mutex result_mutex;
        ParallelTaskRunner runner(token.get());

        for (const auto& each : rss_upt_id_to_rowid_pairs) {
            uint32_t rssid = each.first;
            // `each`/`each.second` are loop-local bindings whose storage may be reused as the
            // loop advances, so capturing a reference to `each.second` from the thread-pool task
            // would dangle. The map itself is stable for the duration of execute(); capture the
            // address of the map entry's value by value and dereference inside the task.
            const auto* upt_pairs_ptr = &each.second;

            auto func = [this, &params, &partial_schema, &partial_tschema, &selective_unique_update_column_ids, rssid,
                         upt_pairs_ptr, condition_idx_in_partial_schema, spill_update_rows, &dcg_column_ids,
                         &dcg_column_file_with_encryption_metas, &dcg_column_file_sizes, &result_mutex]() -> Status {
                // 3.3 prepare one DCG writer, then stream source-segment chunks through update and append.
                ASSIGN_OR_RETURN(auto delta_column_group_writer,
                                 _prepare_delta_column_group_writer(params, partial_tschema));
                auto st = _read_from_source_segment_and_update(
                        params, partial_schema, rssid, [&](StreamChunkContainer container) {
                            const size_t source_chunk_size = container.chunk_ptr->memory_usage();
                            _tracker->consume(source_chunk_size);
                            DeferOp tracker_defer([&]() { _tracker->release(source_chunk_size); });

                            // 3.4 apply this source range's update rows (from the spill, or by
                            // reading the update files).
                            if (spill_update_rows) {
                                RETURN_IF_ERROR(_update_source_chunk_from_spill(rssid, partial_schema, container,
                                                                                condition_idx_in_partial_schema));
                            } else {
                                RETURN_IF_ERROR(_update_source_chunk_by_upt(*upt_pairs_ptr, partial_schema, container,
                                                                            condition_idx_in_partial_schema));
                            }
                            padding_char_columns(partial_schema, partial_tschema, container.chunk_ptr);
                            RETURN_IF_ERROR(ChunkHelper::reject_if_over_capacity(
                                    *container.chunk_ptr, "column mode partial update padded source chunk",
                                    params.tablet->id(), _txn_id));

                            // 3.5 append this bounded source range to the same DCG file.
                            RETURN_IF_ERROR(delta_column_group_writer->append_chunk(*container.chunk_ptr));
                            return Status::OK();
                        });
                RETURN_IF_ERROR(st);

                uint64_t segment_file_size = 0;
                uint64_t index_size = 0;
                uint64_t footer_position = 0;
                RETURN_IF_ERROR(delta_column_group_writer->finalize(&segment_file_size, &index_size, &footer_position));

                // 3.6 collect results under lock
                std::lock_guard<std::mutex> l(result_mutex);
                dcg_column_ids[rssid].push_back(selective_unique_update_column_ids);
                dcg_column_file_with_encryption_metas[rssid].emplace_back(
                        file_name(delta_column_group_writer->segment_path()),
                        delta_column_group_writer->encryption_meta());
                dcg_column_file_sizes[rssid].push_back(static_cast<int64_t>(segment_file_size));
                TRACE_COUNTER_INCREMENT("pcu_handle_cnt", 1);
                return Status::OK();
            };

            runner.run(func);
        }

        {
            TRACE_COUNTER_SCOPE_LATENCY_US("pcu_parallel_dcg_wait_us");
            RETURN_IF_ERROR(runner.join());
        }
    }
    // 4 generate delta columngroup
    for (const auto& each : rss_upt_id_to_rowid_pairs) {
        builder->append_dcg(each.first, dcg_column_file_with_encryption_metas[each.first], dcg_column_ids[each.first],
                            dcg_column_file_sizes[each.first]);
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
