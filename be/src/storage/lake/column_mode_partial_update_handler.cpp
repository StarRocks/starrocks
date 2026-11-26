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

#include <openssl/rand.h>

#include <algorithm>
#include <climits>
#include <cstring>
#include <functional>
#include <limits>
#include <mutex>
#include <numeric>
#include <unordered_map>
#include <unordered_set>

#include "base/crypto/aes_util.h"
#include "base/debug/trace.h"
#include "base/phmap/phmap.h"
#include "base/testutil/sync_point.h"
#include "base/time/time.h"
#include "base/uid_util.h"
#include "base/utility/defer_op.h"
#include "column/chunk_factory.h"
#include "column/chunk_schema_helper.h"
#include "column/fixed_length_column.h"
#include "column/serde/column_array_serde.h"
#include "column/vectorized_fwd.h"
#include "common/config_compaction_fwd.h"
#include "common/config_exec_flow_fwd.h"
#include "common/config_primary_key_fwd.h"
#include "common/config_rowset_fwd.h"
#include "common/tracer.h"
#include "compute_env/spill/log_block_manager.h"
#include "fs/fs.h"
#include "fs/fs_factory.h"
#include "fs/fs_util.h"
#include "gutil/strings/substitute.h"
#include "io/input_stream.h"
#include "platform/key_cache.h"
#include "runtime/current_thread.h"
#include "runtime/runtime_env.h"
#include "storage/chunk_helper.h"
#include "storage/delta_column_group.h"
#include "storage/lake/column_mode_partial_update_handler.h"
#include "storage/lake/filenames.h"
#include "storage/lake/meta_file.h"
#include "storage/lake/parallel_task_runner.h"
#include "storage/lake/primary_key_manager.h"
#include "storage/pk_vector_chunk_size.h"
#include "storage/rowset/column_iterator.h"
#include "storage/rowset/column_reader.h"
#include "storage/rowset/default_value_column_iterator.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/rowset_options.h"
#include "storage/rowset/segment_options.h"
#include "storage/rowset/segment_rewriter.h"
#include "storage/storage_env.h"
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
    ASSIGN_OR_RETURN(auto segment_iters,
                     _rowset_ptr->get_each_segment_iterator(pkey_schema, true, &stats, get_pk_vector_chunk_size()));
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
    RETURN_IF_ERROR(params.tablet->primary_key_mgr()->batch_get_rss_rowids_from_pkindex(
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
    seg_options.chunk_size = get_pk_vector_chunk_size();
    ASSIGN_OR_RETURN(auto seg_iter, segment->new_iterator(schema, seg_options));
    auto source_chunk_ptr = ChunkFactory::new_chunk(schema, seg_options.chunk_size);
    auto tmp_chunk_ptr = ChunkFactory::new_chunk(schema, seg_options.chunk_size);
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

// Condition update: keep the pairs whose old condition value is not greater than the new one, so
// ties go to the new row like in the upsert path. The two vectors are parallel and stay so.
static void filter_by_condition(const Column& source_cond, const Column& upt_cond,
                                std::vector<uint32_t>* sorted_source_rowids, std::vector<uint32_t>* upt_rowids) {
    const size_t original_size = sorted_source_rowids->size();
    size_t kept = 0;
    for (size_t i = 0; i < original_size; ++i) {
        const uint32_t src_rowid = (*sorted_source_rowids)[i];
        const uint32_t upt_rowid = (*upt_rowids)[i];
        if (source_cond.compare_at(src_rowid, upt_rowid, upt_cond, -1) <= 0) {
            (*sorted_source_rowids)[kept] = src_rowid;
            (*upt_rowids)[kept] = upt_rowid;
            ++kept;
        }
    }
    sorted_source_rowids->resize(kept);
    upt_rowids->resize(kept);
    TRACE_COUNTER_INCREMENT("pcu_condition_kept_cnt", kept);
    TRACE_COUNTER_INCREMENT("pcu_condition_dropped_cnt", original_size - kept);
}

// read from upt files and update rows in source chunk.
// When condition_idx_in_partial_schema >= 0, pairs where the old condition value is
// strictly greater than the new one are dropped before applying the update, so the
// corresponding source rows keep their previous values. Equal values let the new row
// win, matching the existing upsert/row-mode condition-update semantics (see
// PrimaryKeyManager::_process_single_chunk_update_with_condition).
Status ColumnModePartialUpdateHandler::_update_source_chunk_by_upt(const UptidToRowidPairs& upt_id_to_rowid_pairs,
                                                                   const Schema& partial_schema,
                                                                   StreamChunkContainer container,
                                                                   int32_t condition_idx_in_partial_schema) {
    TRACE_COUNTER_SCOPE_LATENCY_US("pcu_update_source_by_upt_us");
    // build iterators
    OlapReaderStatistics stats;
    ASSIGN_OR_RETURN(auto segment_iters,
                     _rowset_ptr->get_each_segment_iterator(partial_schema, true, &stats, get_pk_vector_chunk_size()));
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
        ChunkUniquePtr upt_chunk = ChunkFactory::new_chunk(partial_schema, get_pk_vector_chunk_size());
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

        if (condition_idx_in_partial_schema >= 0) {
            filter_by_condition(*container.chunk_ptr->get_column_by_index(condition_idx_in_partial_schema),
                                *upt_chunk->get_column_by_index(condition_idx_in_partial_schema), &sorted_source_rowids,
                                &unsorted_upt_rowids);
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

// AES-128-CTR over one spill record, in place. `seq` is the number of the record in its spill, which
// keeps the counter blocks of two records apart under the spill's key. No-op without a key.
static Status crypt_spill_record(const std::string& key, uint64_t seq, bool encrypt, char* data, int64_t length) {
    if (key.empty() || length == 0) {
        return Status::OK();
    }
    char iv[16] = {};
    memcpy(iv, &seq, sizeof(seq));
    auto* buf = reinterpret_cast<unsigned char*>(data);
    const auto* k = reinterpret_cast<const unsigned char*>(key.data());
    const auto len = static_cast<uint32_t>(length);
    const int n = encrypt ? AesUtil::encrypt_ex(AES_128_CTR, buf, len, k, key.size(), iv, sizeof(iv), false, buf)
                          : AesUtil::decrypt_ex(AES_128_CTR, buf, len, k, key.size(), iv, sizeof(iv), false, buf);
    RETURN_ERROR_IF_FALSE(n == length, "column-mode partial update cannot encrypt or decrypt a spill record");
    return Status::OK();
}

void ColumnModePartialUpdateHandler::_release_spill() {
    // The records hold the blocks and the blocks their container files: with the manager gone too,
    // the files are deleted and their space goes back to the spill directory.
    _spill.records.clear();
    _spill.block_mgr.reset();
    _tracker->release(_spill.index_bytes);
    _spill.index_bytes = 0;
    _spill.size = 0;
    _spill.num_records = 0;
    _spill.ready = false;
    _spill.key.clear();
}

Status ColumnModePartialUpdateHandler::_spill_update_rows(
        const std::map<uint32_t, UptidToRowidPairs>& rss_upt_id_to_rowid_pairs, const Schema& partial_schema) {
    TRACE_COUNTER_SCOPE_LATENCY_US("pcu_spill_write_us");
    _release_spill();
    const uint32_t num_upt = _rowset_ptr->num_segments();

    // Each update file is streamed once with its rows routed to their source segments on the way,
    // which needs the rssid -> upt_id -> (source rowid, upt rowid) pairs inverted into one list per
    // update file, ordered by upt rowid. Only the per-file row counts are taken up front: the list
    // itself is built one update file at a time inside the loop below, sized exactly and charged
    // to the tracker while it lives, so the pass never holds a second copy of every mapped row next
    // to rss_upt_id_to_rowid_pairs.
    struct Entry {
        uint32_t upt_rowid;
        uint32_t rssid;
        uint32_t src_rowid;
    };
    std::vector<size_t> rows_per_upt(num_upt, 0);
    for (const auto& [rssid, upt_map] : rss_upt_id_to_rowid_pairs) {
        for (const auto& [upt_id, pairs] : upt_map) {
            RETURN_ERROR_IF_FALSE(upt_id < num_upt, fmt::format("update file id {} out of range {}, tablet {} txn {}",
                                                                upt_id, num_upt, _rowset_ptr->tablet_id(), _txn_id));
            rows_per_upt[upt_id] += pairs.size();
        }
    }

    // The rows go to the local spill directories the query and load spill use, through the same block
    // manager, so the space they take counts against the same per-directory limit. Only local blocks
    // are used: when no directory has room the caller falls back to the per-segment re-read, which
    // needs no local disk.
    auto* dir_mgr = StorageEnv::GetInstance()->spill_dir_mgr();
    if (dir_mgr == nullptr) {
        return Status::InternalError("column-mode partial update: no local spill directory");
    }
    // The spill is written uncompressed and stays on disk until the column batch is done, and the
    // query and load spill fail when these directories are full. So spill only what fits in half of
    // the room that is left; a larger update takes the re-read. The estimate is the matched rows at
    // the row size of the whole update, which is on the high side for a batch of some of its columns.
    {
        double room = 0;
        for (const auto& dir : dir_mgr->dirs()) {
            if (!dir->is_remote()) {
                room += std::max<double>(
                        0, static_cast<double>(dir->get_max_size()) - static_cast<double>(dir->get_current_size()));
            }
        }
        double estimate = 0;
        for (const size_t rows : rows_per_upt) {
            estimate += static_cast<double>(rows) * static_cast<double>(_upt_memory_usage_per_row);
        }
        TEST_SYNC_POINT_CALLBACK("ColumnModePartialUpdateHandler::_spill_update_rows:estimate", &estimate);
        if (estimate * 2 > room) {
            return Status::CapacityLimitExceed(fmt::format(
                    "column-mode partial update: about {} bytes to spill, {} bytes left in the local spill directories",
                    static_cast<int64_t>(estimate), static_cast<int64_t>(std::min<double>(room, 9e18))));
        }
    }
    spill::AcquireBlockOptions block_opts;
    // One directory per spill under the spill root, so a publish retried on the same tablet and txn
    // never shares files with an earlier attempt.
    block_opts.query_id = UniqueId::gen_uid().to_thrift();
    block_opts.fragment_instance_id = UniqueId(_rowset_ptr->tablet_id(), _txn_id).to_thrift();
    block_opts.plan_node_id = 0;
    block_opts.name = "pcu_spill";
    _spill.block_mgr = std::make_unique<spill::LogBlockManager>(block_opts.query_id, dir_mgr);
    if (config::enable_transparent_data_encryption) {
        // The spill blocks are plain local files, so the records are encrypted here. They never
        // outlive this handler: the key is random and stays in memory.
        _spill.key.resize(16);
        RETURN_ERROR_IF_FALSE(RAND_bytes(reinterpret_cast<unsigned char*>(_spill.key.data()),
                                         static_cast<int>(_spill.key.size())) == 1,
                              "column-mode partial update cannot generate a spill key");
    }
    TEST_SYNC_POINT_CALLBACK("ColumnModePartialUpdateHandler::_spill_update_rows:opened", &block_opts);

    // One buffer per source segment: this batch's columns plus the source rowid of each row. The
    // rows arrive one update file after another, so `runs` says which update file each stretch of
    // rows came from without storing an id per row.
    struct Buffer {
        ChunkUniquePtr chunk;
        UInt32Column::MutablePtr rowids;
        std::vector<SpillRun> runs;
        // What this buffer is charged to the tracker for, so a flush can release it on its own.
        int64_t bytes = 0;
    };
    std::map<uint32_t, Buffer> buffers;
    int64_t buffered_bytes = 0;
    DeferOp release_buffered([&]() { _tracker->release(buffered_bytes); });

    // The serde writes the byte length of every buffer as a uint32_t, so a record must stay below
    // that: a larger buffer is split by rows, and a single row that cannot fit is an error (the
    // caller then falls back to the per-segment re-read, which does not serialize).
    // The cipher takes an int length, which sets the limit below the serde's.
    int64_t max_record_bytes = std::numeric_limits<int32_t>::max();
    TEST_SYNC_POINT_CALLBACK("ColumnModePartialUpdateHandler::_spill_update_rows:max_record_bytes", &max_record_bytes);

    // The runs of rows [from, from + count) of a buffer whose rows are described by `runs`.
    auto slice_runs = [](const std::vector<SpillRun>& runs, size_t from, size_t count) {
        std::vector<SpillRun> out;
        size_t pos = 0;
        for (const auto& run : runs) {
            const size_t begin = std::max(pos, from);
            const size_t end = std::min(pos + run.num_rows, from + count);
            if (begin < end) {
                out.push_back(SpillRun{.upt_id = run.upt_id, .num_rows = static_cast<uint32_t>(end - begin)});
            }
            pos += run.num_rows;
        }
        return out;
    };

    // A flush serializes every buffer into one payload and writes it as one block, so the block
    // manager is asked once per flush, not once per source segment: acquiring a block, reserving its
    // space and handing it back cost about the same whatever the block holds. A record is a byte
    // range of the block of its flush.
    struct PendingRecord {
        uint32_t rssid;
        uint32_t num_rows;
        uint64_t seq;
        int64_t offset;
        int64_t length;
        std::vector<SpillRun> runs;
    };
    auto flush = [&]() -> Status {
        // The payload grows while the buffers it is made from are dropped one by one, so the two
        // together stay around one buffer limit. Only the written part of the reservation is
        // charged: the rest has not been touched yet. The reservation adds room for the length
        // headers the serde writes per column and record. A payload that still outgrows it (object
        // columns serialize to a size unrelated to their memory usage) is charged at its new
        // capacity.
        std::string payload;
        int64_t payload_bytes = 0;
        DeferOp release_payload([&]() { _tracker->release(payload_bytes); });
        const size_t payload_reserved =
                static_cast<size_t>(buffered_bytes) + buffers.size() * (partial_schema.num_fields() + 1) * 16;
        TRY_CATCH_BAD_ALLOC(payload.reserve(payload_reserved));
        int64_t payload_size = 0;
        std::vector<PendingRecord> pending;
        std::function<Status(uint32_t, const Chunk&, const UInt32Column&, const std::vector<SpillRun>&)>
                serialize_record = [&](uint32_t rssid, const Chunk& chunk, const UInt32Column& rowids,
                                       const std::vector<SpillRun>& runs) -> Status {
            int64_t max_size = 0;
            for (const auto& column : chunk.columns()) {
                const int64_t n = serde::ColumnArraySerde::max_serialized_size(*column);
                RETURN_ERROR_IF_FALSE(n > 0, "column-mode partial update cannot spill this column type");
                max_size += n;
            }
            max_size += serde::ColumnArraySerde::max_serialized_size(rowids);
            if (max_size > max_record_bytes) {
                const size_t num_rows = chunk.num_rows();
                if (num_rows <= 1) {
                    return Status::CapacityLimitExceed(
                            fmt::format("column-mode partial update: one row needs {} bytes in the spill, over the {} "
                                        "byte record limit, tablet {} txn {}",
                                        max_size, max_record_bytes, _rowset_ptr->tablet_id(), _txn_id));
                }
                const size_t half = num_rows / 2;
                for (const auto& [from, count] :
                     {std::pair<size_t, size_t>{0, half}, std::pair<size_t, size_t>{half, num_rows - half}}) {
                    auto part = chunk.clone_empty(count);
                    auto part_rowids = UInt32Column::create();
                    TRY_CATCH_BAD_ALLOC(part->append(chunk, from, count));
                    TRY_CATCH_BAD_ALLOC(part_rowids->append(rowids, from, count));
                    const int64_t part_bytes = static_cast<int64_t>(part->memory_usage() + part_rowids->memory_usage());
                    _tracker->consume(part_bytes);
                    DeferOp release_part([&]() { _tracker->release(part_bytes); });
                    RETURN_IF_ERROR(serialize_record(rssid, *part, *part_rowids, slice_runs(runs, from, count)));
                }
                return Status::OK();
            }
            TRY_CATCH_BAD_ALLOC(payload.resize(payload_size + max_size));
            const int64_t in_use = payload.capacity() > payload_reserved ? static_cast<int64_t>(payload.capacity())
                                                                         : payload_size + max_size;
            const int64_t grown = in_use - payload_bytes;
            if (grown > 0) {
                _tracker->consume(grown);
                payload_bytes += grown;
            }
            TEST_SYNC_POINT_CALLBACK("ColumnModePartialUpdateHandler::_spill_update_rows:payload", &payload_bytes);
            char* begin = payload.data() + payload_size;
            auto* cur = reinterpret_cast<uint8_t*>(begin);
            for (const auto& column : chunk.columns()) {
                ASSIGN_OR_RETURN(cur, serde::ColumnArraySerde::serialize(*column, cur));
            }
            ASSIGN_OR_RETURN(cur, serde::ColumnArraySerde::serialize(rowids, cur));
            int64_t length = cur - reinterpret_cast<uint8_t*>(begin);
            std::pair<const char*, int64_t> plain{begin, length};
            TEST_SYNC_POINT_CALLBACK("ColumnModePartialUpdateHandler::_spill_update_rows:plain", &plain);
            const uint64_t seq = _spill.num_records++;
            RETURN_IF_ERROR(crypt_spill_record(_spill.key, seq, true, begin, length));
            pending.push_back(PendingRecord{.rssid = rssid,
                                            .num_rows = static_cast<uint32_t>(chunk.num_rows()),
                                            .seq = seq,
                                            .offset = payload_size,
                                            .length = length,
                                            .runs = runs});
            payload_size += length;
            TEST_SYNC_POINT_CALLBACK("ColumnModePartialUpdateHandler::_spill_update_rows:record", &length);
            return Status::OK();
        };
        for (auto& [rssid, buffer] : buffers) {
            if (buffer.chunk == nullptr || buffer.chunk->num_rows() == 0) {
                continue;
            }
            RETURN_IF_ERROR(serialize_record(rssid, *buffer.chunk, *buffer.rowids, buffer.runs));
            // Its rows are in the payload now. Drop the buffer rather than reset it: a reset chunk
            // keeps its capacity, and with the buffers living for the whole partition pass that
            // would pin about one buffer limit per segment that ever took a large share of a flush,
            // while the tracker reports zero.
            buffer.chunk.reset();
            buffer.rowids.reset();
            _tracker->release(buffer.bytes);
            buffered_bytes -= buffer.bytes;
            buffer.bytes = 0;
        }
        buffers.clear();
        _tracker->release(buffered_bytes);
        buffered_bytes = 0;
        if (!pending.empty()) {
            block_opts.block_size = payload_size;
            ASSIGN_OR_RETURN(auto block, _spill.block_mgr->acquire_block(block_opts));
            auto write_st = block->append({Slice(payload.data(), payload_size)});
            if (write_st.ok()) {
                write_st = block->flush();
            }
            // Hand the block back whichever way the write went: the manager keeps its container file
            // for the next flush and deletes it in the end.
            auto release_st = _spill.block_mgr->release_block(block);
            RETURN_IF_ERROR(write_st);
            RETURN_IF_ERROR(release_st);
            TEST_SYNC_POINT_CALLBACK("ColumnModePartialUpdateHandler::_spill_update_rows:block", &block);
            // The record index lives until the column batch is done, one descriptor per segment and
            // flush plus one run per update file in it, so its growth is charged as well (a map node
            // per new segment, vector capacity, the runs).
            for (auto& rec : pending) {
                auto [index_it, inserted] = _spill.records.try_emplace(rec.rssid);
                auto& index = index_it->second;
                const size_t old_capacity = index.capacity();
                index.push_back(SpillRecord{.num_rows = rec.num_rows,
                                            .seq = rec.seq,
                                            .block = block,
                                            .offset = rec.offset,
                                            .length = rec.length,
                                            .runs = std::move(rec.runs)});
                const int64_t index_grown =
                        (inserted ? static_cast<int64_t>(sizeof(decltype(_spill.records)::value_type) +
                                                         4 * sizeof(void*))
                                  : 0) +
                        static_cast<int64_t>((index.capacity() - old_capacity) * sizeof(SpillRecord)) +
                        static_cast<int64_t>(index.back().runs.capacity() * sizeof(SpillRun));
                if (index_grown > 0) {
                    _tracker->consume(index_grown);
                    _spill.index_bytes += index_grown;
                }
                TRACE_COUNTER_INCREMENT("pcu_spill_records", 1);
            }
            _spill.size += payload_size;
            TRACE_COUNTER_INCREMENT("pcu_spill_blocks", 1);
        }
        size_t live_buffers = buffers.size();
        TEST_SYNC_POINT_CALLBACK("ColumnModePartialUpdateHandler::_spill_update_rows:flushed", &live_buffers);
        return Status::OK();
    };

    // Build the update-file iterators once for the whole pass. get_each_segment_iterator() loads every
    // update segment and constructs one iterator per segment, so calling it inside the loop below
    // would construct and close num_upt squared iterators while consuming only one per iteration.
    OlapReaderStatistics stats;
    ASSIGN_OR_RETURN(auto segment_iters,
                     _rowset_ptr->get_each_segment_iterator(partial_schema, true, &stats, get_pk_vector_chunk_size()));
    RETURN_ERROR_IF_FALSE(segment_iters.size() == num_upt);
    // Only the update files the loop never reaches (no rows, or an early error) are still open here:
    // every file that was read is closed by the per-file defer below, so an iterator's decoded pages,
    // scan chunks and column files never outlive the file they belong to.
    DeferOp close_iters([&]() {
        for (auto& iter : segment_iters) {
            if (iter != nullptr) {
                iter->close();
            }
        }
    });

    for (uint32_t upt_id = 0; upt_id < num_upt; upt_id++) {
        if (rows_per_upt[upt_id] == 0) {
            continue;
        }
        if (segment_iters[upt_id] == nullptr) {
            // A lost update-file segment ignored via experimental_lake_ignore_lost_segment produces
            // no rowid pairs, so this should not happen; keep the old behaviour of skipping it.
            LOG(WARNING) << "column-mode partial update skips a null update-file segment iterator, tablet: "
                         << _rowset_ptr->tablet_id() << ", upt_id: " << upt_id
                         << " (a lost segment via experimental_lake_ignore_lost_segment, or an unexpected empty slot)";
            continue;
        }
        std::vector<Entry> entries;
        entries.reserve(rows_per_upt[upt_id]);
        for (const auto& [rssid, upt_map] : rss_upt_id_to_rowid_pairs) {
            auto it = upt_map.find(upt_id);
            if (it == upt_map.end()) {
                continue;
            }
            for (const auto& p : it->second) {
                entries.push_back({p.second, rssid, p.first});
            }
        }
        std::sort(entries.begin(), entries.end(),
                  [](const Entry& a, const Entry& b) { return a.upt_rowid < b.upt_rowid; });
        int64_t entries_bytes = static_cast<int64_t>(entries.capacity() * sizeof(Entry));
        _tracker->consume(entries_bytes);
        DeferOp release_entries([&]() { _tracker->release(entries_bytes); });
        TEST_SYNC_POINT_CALLBACK("ColumnModePartialUpdateHandler::_spill_update_rows:routed", &entries_bytes);
        TEST_SYNC_POINT_CALLBACK("ColumnModePartialUpdateHandler::_spill_update_rows:upt_read", &upt_id);
        auto& iter = segment_iters[upt_id];
        // Release this file's iterator as soon as it is done with, whichever way the loop body exits.
        // Construction is hoisted above, but an iterator that has been read holds its scan chunks,
        // each column's current page and its column files until close(), none of it charged to
        // _tracker, so keeping every read file open until the pass ends would grow with num_upt.
        DeferOp close_iter([&]() {
            iter->close();
            iter.reset();
            size_t live = std::count_if(segment_iters.begin(), segment_iters.end(),
                                        [](const auto& it) { return it != nullptr; });
            TEST_SYNC_POINT_CALLBACK("ColumnModePartialUpdateHandler::_spill_update_rows:upt_closed", &live);
        });
        auto chunk = ChunkFactory::new_chunk(partial_schema, get_pk_vector_chunk_size());
        // The decoded chunk holds every row read, the ones routed below and the ones left out (inserts
        // in COLUMN_UPSERT_MODE), and reset() keeps its capacity: charge its high-water mark for as
        // long as it lives, next to the buffers and the serialized payload built from it.
        int64_t chunk_bytes = 0;
        DeferOp release_chunk([&]() { _tracker->release(chunk_bytes); });
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
            if (const auto now = static_cast<int64_t>(chunk->memory_usage()); now > chunk_bytes) {
                _tracker->consume(now - chunk_bytes);
                chunk_bytes = now;
            }
            TEST_SYNC_POINT_CALLBACK("ColumnModePartialUpdateHandler::_spill_update_rows:chunk", &chunk_bytes);
            const uint32_t n = chunk->num_rows();
            const uint32_t end = base + n;
            const size_t start = pos;
            while (pos < entries.size() && entries[pos].upt_rowid < end) {
                pos++;
            }
            if (start < pos) {
                RETURN_IF_ERROR(ChunkHelper::reject_if_over_capacity(*chunk, "column mode partial update upt chunk",
                                                                     _rowset_ptr->tablet_id(), _txn_id));
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
                    // Charge what the selection actually appended. An average row width over the whole
                    // chunk undercounts when the routed rows are wider than the rows left out (inserts
                    // in COLUMN_UPSERT_MODE), letting the buffers outgrow the limit before a flush.
                    // Measure only the appended range: the whole-chunk size walks every buffered value
                    // of object columns (JSON, BITMAP, HLL), quadratic over a flush interval.
                    const size_t from = buffer.chunk->num_rows();
                    TRY_CATCH_BAD_ALLOC(buffer.chunk->append_selective(*chunk, indexes.data(), 0, indexes.size()));
                    // A buffer collects a segment's rows of several update files until the next
                    // flush, and the buffer limit is a mutable config with no upper bound.
                    RETURN_IF_ERROR(ChunkHelper::reject_if_over_capacity(*buffer.chunk,
                                                                         "column mode partial update spill buffer",
                                                                         _rowset_ptr->tablet_id(), _txn_id));
                    buffer.rowids->append_numbers(src_rowids.data(), src_rowids.size() * sizeof(uint32_t));
                    int64_t bytes = static_cast<int64_t>(buffer.chunk->bytes_usage(from, indexes.size())) +
                                    static_cast<int64_t>(src_rowids.size() * sizeof(uint32_t));
                    if (buffer.runs.empty() || buffer.runs.back().upt_id != upt_id) {
                        buffer.runs.push_back(SpillRun{.upt_id = upt_id, .num_rows = 0});
                        bytes += static_cast<int64_t>(sizeof(SpillRun));
                    }
                    buffer.runs.back().num_rows += static_cast<uint32_t>(indexes.size());
                    _tracker->consume(bytes);
                    buffer.bytes += bytes;
                    buffered_bytes += bytes;
                }
                if (buffered_bytes > config::lake_pk_column_mode_spill_buffer_bytes) {
                    RETURN_IF_ERROR(flush());
                }
            }
            base = end;
        }
        if (pos != entries.size()) {
            return Status::Corruption(fmt::format(
                    "column-mode partial update: update file {} ended after {} rows, but {} routed rows point past "
                    "them, tablet {} txn {}",
                    upt_id, base, entries.size() - pos, _rowset_ptr->tablet_id(), _txn_id));
        }
    }
    // The buffers are flushed only when they are over the limit and here, not at every update-file
    // boundary: a write costs about the same whatever its size, and a flush per update file writes
    // (source segments x update files) records where this writes one per source segment and flush.
    RETURN_IF_ERROR(flush());
    _spill.ready = true;
    TRACE_COUNTER_INCREMENT("pcu_spill_bytes", _spill.size);
    return Status::OK();
}

Status ColumnModePartialUpdateHandler::_update_source_chunk_from_spill(uint32_t rssid, const Schema& partial_schema,
                                                                       StreamChunkContainer container,
                                                                       int32_t condition_idx_in_partial_schema,
                                                                       bool* spill_unreadable) {
    TRACE_COUNTER_SCOPE_LATENCY_US("pcu_update_source_by_upt_us");
    *spill_unreadable = false;
    RETURN_ERROR_IF_FALSE(_spill.ready, "column-mode partial update spill is not ready");
    auto it = _spill.records.find(rssid);
    if (it == _spill.records.end()) {
        return Status::OK();
    }
    // The read buffer is reused across records and keeps the capacity of the largest one, so that
    // capacity stays charged for as long as the buffer lives, as on the write side.
    std::string payload;
    int64_t payload_bytes = 0;
    DeferOp release_payload([&]() { _tracker->release(payload_bytes); });
    std::vector<uint32_t> selected; // rows of one record that fall into this source range
    int64_t selected_bytes = 0;     // capacity of `selected` charged so far
    DeferOp release_selected([&]() { _tracker->release(selected_bytes); });
    const auto& records = it->second;
    // The rows of the update file being collected. A record holds this segment's rows of several
    // update files, one run per file, and a flush in the middle of an update file leaves that
    // file's rows in the last run of one record and the first run of the next. update_rows()
    // rebuilds a variable-length column whenever a value changes its length, so the rows of one
    // update file are applied with a single call, whichever runs they came from. Different update
    // files stay apart: a later file must overwrite an earlier one.
    ChunkUniquePtr upt_chunk;
    std::vector<uint32_t> source_rowids; // row i of upt_chunk updates this row of the container
    int64_t upt_bytes = 0;
    DeferOp release_upt([&]() { _tracker->release(upt_bytes); });
    uint32_t pending_upt_id = 0;
    auto apply_pending = [&]() -> Status {
        DeferOp reset_pending([&]() {
            upt_chunk.reset();
            source_rowids.clear();
            _tracker->release(upt_bytes);
            upt_bytes = 0;
        });
        if (source_rowids.empty()) {
            return Status::OK();
        }
        // The two row-id arrays below hold 8 bytes per collected row for the whole apply. With a
        // narrow update column that is as much as the rows themselves, so charge them too.
        int64_t rowid_bytes = static_cast<int64_t>(source_rowids.size() * 2 * sizeof(uint32_t));
        _tracker->consume(rowid_bytes);
        DeferOp release_rowids([&]() { _tracker->release(rowid_bytes); });
        TEST_SYNC_POINT_CALLBACK("ColumnModePartialUpdateHandler::_update_source_chunk_from_spill:rowids",
                                 &rowid_bytes);
        // Order by source rowid like the whole-file path does.
        std::vector<uint32_t> unsorted_upt_rowids;
        std::vector<uint32_t> sorted_source_rowids;
        TRY_CATCH_BAD_ALLOC(unsorted_upt_rowids.resize(source_rowids.size()));
        TRY_CATCH_BAD_ALLOC(sorted_source_rowids.reserve(source_rowids.size()));
        std::iota(unsorted_upt_rowids.begin(), unsorted_upt_rowids.end(), 0);
        std::stable_sort(unsorted_upt_rowids.begin(), unsorted_upt_rowids.end(),
                         [&](uint32_t a, uint32_t b) { return source_rowids[a] < source_rowids[b]; });
        for (const uint32_t upt_rowid : unsorted_upt_rowids) {
            sorted_source_rowids.push_back(source_rowids[upt_rowid]);
        }

        if (condition_idx_in_partial_schema >= 0) {
            filter_by_condition(*container.chunk_ptr->get_column_by_index(condition_idx_in_partial_schema),
                                *upt_chunk->get_column_by_index(condition_idx_in_partial_schema), &sorted_source_rowids,
                                &unsorted_upt_rowids);
        }
        if (sorted_source_rowids.empty()) {
            return Status::OK();
        }
        auto tmp_chunk = ChunkFactory::new_chunk(partial_schema, unsorted_upt_rowids.size());
        TRY_CATCH_BAD_ALLOC(
                tmp_chunk->append_selective(*upt_chunk, unsorted_upt_rowids.data(), 0, unsorted_upt_rowids.size()));
        const int64_t tmp_bytes = static_cast<int64_t>(tmp_chunk->memory_usage());
        _tracker->consume(tmp_bytes);
        DeferOp release_tmp([&]() { _tracker->release(tmp_bytes); });
        // The sorted copy is all update_rows() needs. Drop the collected rows first: update_rows()
        // allocates a whole new column when it has to rebuild a variable-length one.
        upt_chunk.reset();
        _tracker->release(upt_bytes);
        upt_bytes = 0;
        TRACE_COUNTER_INCREMENT("pcu_spill_apply_cnt", 1);
        TEST_SYNC_POINT_CALLBACK("ColumnModePartialUpdateHandler::_update_source_chunk_from_spill:apply",
                                 &pending_upt_id);
        RETURN_IF_EXCEPTION(container.chunk_ptr->update_rows(*tmp_chunk, sorted_source_rowids.data()));
        return ChunkHelper::reject_if_over_capacity(*container.chunk_ptr,
                                                    "column mode partial update merged source chunk",
                                                    _rowset_ptr->tablet_id(), _txn_id);
    };
    // Reading a record back and decoding it. A failure here is a problem of the local spill (disk
    // error, file removed, damaged bytes), not of the update, and is reported through
    // `spill_unreadable` so the caller can apply this range from the update files instead.
    auto read_record = [&](const SpillRecord& rec, Chunk* rec_chunk, UInt32Column* rowids) -> Status {
        const int64_t length = rec.length;
        TRY_CATCH_BAD_ALLOC(payload.resize(length));
        if (const int64_t grown = static_cast<int64_t>(payload.capacity()) - payload_bytes; grown > 0) {
            _tracker->consume(grown);
            payload_bytes += grown;
        }
        Status st;
        TEST_SYNC_POINT_CALLBACK("ColumnModePartialUpdateHandler::_update_source_chunk_from_spill:read", &st);
        RETURN_IF_ERROR(st);
        // The records of one flush share a block, and every read opens its own stream on it: the
        // segment tasks share no file position.
        ASSIGN_OR_RETURN(auto readable, rec.block->get_readable());
        RETURN_IF_ERROR(readable->skip(rec.offset));
        RETURN_IF_ERROR(readable->read_fully(payload.data(), length));
        RETURN_IF_ERROR(crypt_spill_record(_spill.key, rec.seq, false, payload.data(), length));
        const auto* cur = reinterpret_cast<const uint8_t*>(payload.data());
        const auto* end = cur + length;
        for (auto& column : rec_chunk->columns()) {
            ASSIGN_OR_RETURN(cur, serde::ColumnArraySerde::deserialize(cur, end, column->as_mutable_raw_ptr()));
        }
        ASSIGN_OR_RETURN(cur, serde::ColumnArraySerde::deserialize(cur, end, rowids));
        RETURN_ERROR_IF_FALSE(rec_chunk->num_rows() == rec.num_rows && rowids->size() == rec.num_rows,
                              "column-mode partial update spill record is corrupted");
        uint64_t run_rows = 0;
        for (const auto& run : rec.runs) {
            run_rows += run.num_rows;
        }
        RETURN_ERROR_IF_FALSE(run_rows == rec.num_rows, "column-mode partial update spill record runs are corrupted");
        return Status::OK();
    };
    for (const auto& rec : records) {
        auto rec_chunk = ChunkFactory::new_chunk(partial_schema, rec.num_rows);
        auto rowids = UInt32Column::create();
        if (auto st = read_record(rec, rec_chunk.get(), rowids.get()); !st.ok()) {
            *spill_unreadable = !st.is_mem_limit_exceeded();
            return st;
        }
        // Charge the record's decoded copies (chunk, rowids) for as long as they live, as the
        // whole-file path charges its update chunk: segment tasks run concurrently and a record can
        // be most of a buffer limit. The payload is charged above.
        int64_t rec_bytes =
                static_cast<int64_t>(rec_chunk->memory_usage()) + static_cast<int64_t>(rowids->memory_usage());
        _tracker->consume(rec_bytes);
        DeferOp release_rec([&]() { _tracker->release(rec_bytes); });
        TEST_SYNC_POINT_CALLBACK("ColumnModePartialUpdateHandler::_update_source_chunk_from_spill:charged", &rec_bytes);

        const auto& src = rowids->get_data();
        uint32_t row = 0;
        for (const auto& run : rec.runs) {
            if (run.upt_id != pending_upt_id) {
                RETURN_IF_ERROR(apply_pending());
                pending_upt_id = run.upt_id;
            }
            // Keep only the rows of this streamed source range.
            selected.clear();
            for (uint32_t i = row; i < row + run.num_rows; i++) {
                if (src[i] >= container.start_rowid && src[i] < container.end_rowid) {
                    selected.push_back(i);
                    source_rowids.push_back(src[i] - container.start_rowid);
                }
            }
            row += run.num_rows;
            // `selected` is reused across runs and records, so its capacity lives until the end.
            if (const int64_t grown = static_cast<int64_t>(selected.capacity() * sizeof(uint32_t)) - selected_bytes;
                grown > 0) {
                _tracker->consume(grown);
                selected_bytes += grown;
            }
            if (selected.empty()) {
                continue;
            }
            if (upt_chunk == nullptr) {
                upt_chunk = ChunkFactory::new_chunk(partial_schema, 0);
            }
            const size_t from = upt_chunk->num_rows();
            TRY_CATCH_BAD_ALLOC(upt_chunk->append_selective(*rec_chunk, selected.data(), 0, selected.size()));
            RETURN_IF_ERROR(ChunkHelper::reject_if_over_capacity(*upt_chunk, "column mode partial update spill rows",
                                                                 _rowset_ptr->tablet_id(), _txn_id));
            const int64_t bytes = static_cast<int64_t>(upt_chunk->bytes_usage(from, selected.size())) +
                                  static_cast<int64_t>(selected.size() * sizeof(uint32_t));
            _tracker->consume(bytes);
            upt_bytes += bytes;
        }
    }
    return apply_pending();
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
    // How often the re-read path reads an update file: once for every source segment the file has
    // rows of. Averaged over the update files, weighted by their matched rows so that a large file
    // spread over many source segments is not hidden by small files that each fall into one. The
    // map does not change below, so this is computed once for all column batches.
    double upt_reads_weighted = 0; // sum over update files of (source segments fed) * (matched rows)
    double upt_rows_matched = 0;
    {
        std::unordered_map<uint32_t, std::pair<uint64_t, uint64_t>> per_upt; // upt_id -> (segments, rows)
        for (const auto& [rssid, upt_map] : rss_upt_id_to_rowid_pairs) {
            for (const auto& [upt_id, pairs] : upt_map) {
                auto& [segments, rows] = per_upt[upt_id];
                segments++;
                rows += pairs.size();
            }
        }
        for (const auto& [upt_id, stat] : per_upt) {
            upt_reads_weighted += static_cast<double>(stat.first) * static_cast<double>(stat.second);
            upt_rows_matched += static_cast<double>(stat.second);
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

        // When condition update is enabled, the single-batch invariant ensures the condition column
        // is present in this batch's partial schema; locate its index for inline compare_at.
        int32_t condition_idx_in_partial_schema = -1;
        if (condition_cid >= 0) {
            ASSIGN_OR_RETURN(condition_idx_in_partial_schema,
                             _locate_condition_idx_in_partial_schema(selective_update_column_ids, condition_cid));
        }

        // Read each update file once and hand every source segment its own rows (see
        // _spill_update_rows). Declared before the runner so the spill outlives every task.
        // The spill pays off only when an update file would otherwise be read more than once. The
        // re-read path reads an update file once per source range of every source segment the file
        // has rows of (see _read_from_source_segment_and_update), so the check is on the update
        // files rather than on the source segments of the whole update: update files that each
        // fall into a different source segment are read once either way, and there the spill would
        // add a serialize, local write, local read and deserialize pass for nothing. The update is
        // spilled when its update files feed at least the configured number of source segments on
        // average (computed above). The default is where the saved remote reads start to outweigh
        // the cost of the spill. The count is of segments, not ranges: a segment too large for one
        // range reads each update file once per range on the re-read path, and only a lower
        // threshold, down to 1 for always, spills that case. A load that matched no existing key
        // (an insert-only COLUMN_UPSERT_MODE load) has no segment task to feed and never spills.
        const int32_t spill_min_source_segments = config::lake_pk_column_mode_spill_min_source_segments;
        bool spill_update_rows =
                spill_min_source_segments > 0 && upt_rows_matched > 0 &&
                upt_reads_weighted >= upt_rows_matched * static_cast<double>(spill_min_source_segments);
        if (spill_update_rows) {
            auto st = _spill_update_rows(rss_upt_id_to_rowid_pairs, partial_schema);
            // Whatever stopped the partition pass, short of corruption, falls back: the per-segment
            // re-read below needs neither the spill nor local disk, so a local problem (disk full,
            // spill directory not usable, a column the spill cannot serialize) must not block the
            // publish. An error that is not local, such as a failed read of an update file, falls
            // back too and the re-read reports it itself if it is still there. Corruption is
            // returned as is, since the re-read would index past the same rows.
            if (!st.ok() && !st.is_corruption()) {
                LOG(WARNING) << "Column-mode partial update falls back to reading the update files per source "
                             << "segment, tablet: " << params.tablet->id() << ", txn: " << _txn_id << ": " << st;
                TRACE_COUNTER_INCREMENT("pcu_spill_fallback_cnt", 1);
                TEST_SYNC_POINT("ColumnModePartialUpdateHandler::execute:spill_fallback");
                _release_spill();
                spill_update_rows = false;
            } else {
                RETURN_IF_ERROR(st);
            }
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
                // Set once this task fails to read its rows back from the spill.
                bool spill_unreadable = false;
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
                            bool from_spill = spill_update_rows && !spill_unreadable;
                            if (from_spill) {
                                auto spill_st = _update_source_chunk_from_spill(rssid, partial_schema, container,
                                                                                condition_idx_in_partial_schema,
                                                                                &spill_unreadable);
                                if (!spill_unreadable) {
                                    RETURN_IF_ERROR(spill_st);
                                } else {
                                    // The local spill cannot be read back. The update files are
                                    // still there, so this range and the rest of the segment are
                                    // applied from them. Update files of this range that the spill
                                    // already applied are applied again, in the same order, which
                                    // gives the same rows: a plain update overwrites the same
                                    // values, and a condition update keeps the row it kept before.
                                    LOG(WARNING) << "Column-mode partial update cannot read its spill back and "
                                                 << "re-reads the update files, tablet: " << params.tablet->id()
                                                 << ", txn: " << _txn_id << ", rssid: " << rssid << ": " << spill_st;
                                    TRACE_COUNTER_INCREMENT("pcu_spill_fallback_cnt", 1);
                                    TEST_SYNC_POINT("ColumnModePartialUpdateHandler::execute:spill_read_fallback");
                                    from_spill = false;
                                }
                            }
                            if (!from_spill) {
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
