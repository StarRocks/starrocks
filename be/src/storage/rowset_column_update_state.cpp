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

#include "rowset_column_update_state.h"

#include "common/tracer.h"
#include "fs/fs_util.h"
#include "gutil/strings/substitute.h"
#include "serde/column_array_serde.h"
#include "storage/chunk_helper.h"
#include "storage/delta_column_group.h"
#include "storage/primary_key_encoder.h"
#include "storage/rowset/column_iterator.h"
#include "storage/rowset/default_value_column_iterator.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/rowset_options.h"
#include "storage/rowset/segment_options.h"
#include "storage/rowset/segment_rewriter.h"
#include "storage/tablet.h"
#include "storage/tablet_meta_manager.h"
#include "storage/update_manager.h"
#include "util/defer_op.h"
#include "util/phmap/phmap.h"
#include "util/stack_util.h"
#include "util/time.h"

namespace starrocks {

RowsetColumnUpdateState::RowsetColumnUpdateState() = default;

RowsetColumnUpdateState::~RowsetColumnUpdateState() {
    if (!_status.ok()) {
        LOG(WARNING) << "bad RowsetColumnUpdateState released tablet:" << _tablet_id;
    }
}

Status RowsetColumnUpdateState::load(Tablet* tablet, Rowset* rowset, MemTracker* update_mem_tracker) {
    if (UNLIKELY(!_status.ok())) {
        return _status;
    }
    std::call_once(_load_once_flag, [&] {
        _tablet_id = tablet->tablet_id();
        _status = _do_load(tablet, rowset, update_mem_tracker);
        if (!_status.ok()) {
            LOG(WARNING) << "load RowsetColumnUpdateState error: " << _status << " tablet:" << _tablet_id << " stack:\n"
                         << get_stack_trace();
            if (_status.is_mem_limit_exceeded()) {
                LOG(WARNING) << CurrentThread::mem_tracker()->debug_string();
            }
        }
    });
    return _status;
}

void RowsetColumnUpdateState::_release_upserts(uint32_t start_idx, uint32_t end_idx) {
    for (uint32_t idx = start_idx; idx < _upserts.size() && idx < end_idx; idx++) {
        if (_upserts[idx] != nullptr) {
            if (_upserts[idx]->is_last(idx)) {
                _memory_usage -= _upserts[idx]->upserts->memory_usage();
            }
            _upserts[idx].reset();
        }
    }
}

Status RowsetColumnUpdateState::_load_upserts(Rowset* rowset, MemTracker* update_mem_tracker, uint32_t start_idx,
                                              uint32_t* end_idx) {
    CHECK_MEM_LIMIT("RowsetColumnUpdateState::_load_upserts");
    RowsetReleaseGuard guard(rowset->shared_from_this());
    if (_upserts.size() == 0) {
        _upserts.resize(rowset->num_update_files());
    } else {
        // update files should be immutable
        DCHECK(_upserts.size() == rowset->num_update_files());
    }
    if (_upserts.size() == 0) {
        return Status::OK();
    }
    if (_upserts[start_idx] != nullptr) {
        *end_idx = _upserts[start_idx]->end_idx;
        return Status::OK();
    }

    OlapReaderStatistics stats;
    auto& schema = rowset->schema();
    vector<uint32_t> pk_columns;
    for (size_t i = 0; i < schema->num_key_columns(); i++) {
        pk_columns.push_back((uint32_t)i);
    }
    Schema pkey_schema = ChunkHelper::convert_schema(schema, pk_columns);
    MutableColumnPtr pk_column;
    if (!PrimaryKeyEncoder::create_column(pkey_schema, &pk_column).ok()) {
        std::string err_msg = fmt::format("create column for primary key encoder failed, tablet_id: {}", _tablet_id);
        DCHECK(false) << err_msg;
        return Status::InternalError(err_msg);
    }

    std::shared_ptr<Chunk> chunk_shared_ptr;
    TRY_CATCH_BAD_ALLOC(chunk_shared_ptr = ChunkHelper::new_chunk(pkey_schema, DEFAULT_CHUNK_SIZE));

    // alloc first BatchPKsPtr
    auto header_ptr = std::make_shared<BatchPKs>();
    header_ptr->upserts = pk_column->clone();
    header_ptr->start_idx = start_idx;
    for (uint32_t idx = start_idx; idx < rowset->num_update_files(); idx++) {
        header_ptr->offsets.push_back(header_ptr->upserts->size());
        auto chunk = chunk_shared_ptr.get();
        auto col = pk_column->clone();
        ASSIGN_OR_RETURN(auto itr, rowset->get_update_file_iterator(pkey_schema, idx, &stats));
        DeferOp iter_defer([&]() {
            if (itr != nullptr) {
                itr->close();
            }
        });
        if (itr != nullptr) {
            TRY_CATCH_BAD_ALLOC(col->reserve(DEFAULT_CHUNK_SIZE));
            while (true) {
                chunk->reset();
                auto st = itr->get_next(chunk);
                if (st.is_end_of_file()) {
                    break;
                } else if (!st.ok()) {
                    return st;
                } else {
                    TRY_CATCH_BAD_ALLOC(
                            PrimaryKeyEncoder::encode(pkey_schema, *chunk, 0, chunk->num_rows(), col.get()));
                }
            }
        }
        // merge pk column into BatchPKs
        TRY_CATCH_BAD_ALLOC(header_ptr->upserts->append(*col));
        // all idx share same ptr with start idx
        _upserts[idx] = header_ptr;
        *end_idx = idx + 1;
        // quit merge PK into BatchPKs when hiting memory limit
        if (header_ptr->upserts->memory_usage() + _memory_usage > config::primary_key_batch_get_index_memory_limit ||
            update_mem_tracker->any_limit_exceeded()) {
            break;
        }
    }
    // push end offset
    TRY_CATCH_BAD_ALLOC(header_ptr->offsets.push_back(header_ptr->upserts->size()));
    header_ptr->end_idx = *end_idx;
    DCHECK(header_ptr->offsets.size() == header_ptr->end_idx - header_ptr->start_idx + 1);
    // This is a little bit trick. If pk column is a binary column, we will call function `raw_data()` in the following
    // And the function `raw_data()` will build slice of pk column which will increase the memory usage of pk column
    // So we try build slice in advance in here to make sure the correctness of memory statistics
    TRY_CATCH_BAD_ALLOC(header_ptr->upserts->raw_data());
    _memory_usage += header_ptr->upserts->memory_usage();

    return Status::OK();
}

Status RowsetColumnUpdateState::_do_load(Tablet* tablet, Rowset* rowset, MemTracker* update_mem_tracker) {
    auto span = Tracer::Instance().start_trace_txn_tablet("rowset_column_update_state_load", rowset->txn_id(),
                                                          tablet->tablet_id());
    if (rowset->num_update_files() > 0) {
        uint32_t end_idx = 0;
        RETURN_IF_ERROR(_load_upserts(rowset, update_mem_tracker, 0, &end_idx));
        DCHECK(end_idx > 0);
        RETURN_IF_ERROR(_prepare_partial_update_states(tablet, rowset, 0, end_idx, true));
    }

    return Status::OK();
}

Status RowsetColumnUpdateState::_prepare_partial_update_states(Tablet* tablet, Rowset* rowset, uint32_t start_idx,
                                                               uint32_t end_idx, bool need_lock) {
    CHECK_MEM_LIMIT("RowsetColumnUpdateState::_prepare_partial_update_states");
    if (_partial_update_states.size() == 0) {
        _partial_update_states.resize(rowset->num_update_files());
    } else {
        // update files should be immutable
        DCHECK(_partial_update_states.size() == rowset->num_update_files());
    }

    if (_partial_update_states[start_idx].inited) {
        // assume that states between [start_idx, end_idx) should be inited
        RETURN_ERROR_IF_FALSE(_partial_update_states[end_idx - 1].inited);
        return Status::OK();
    }

    const auto& txn_meta = rowset->rowset_meta()->get_meta_pb_without_schema().txn_meta();
    for (auto& entry : txn_meta.column_to_expr_value()) {
        _column_to_expr_value.insert({entry.first, entry.second});
    }

    EditVersion read_version;
    TRY_CATCH_BAD_ALLOC(_upserts[start_idx]->src_rss_rowids.resize(_upserts[start_idx]->upserts_size()));
    int64_t t_start = MonotonicMillis();
    if (need_lock) {
        RETURN_IF_ERROR(tablet->updates()->get_rss_rowids_by_pk(tablet, *(_upserts[start_idx]->upserts), &read_version,
                                                                &(_upserts[start_idx]->src_rss_rowids)));
    } else {
        RETURN_IF_ERROR(tablet->updates()->get_rss_rowids_by_pk_unlock(
                tablet, *(_upserts[start_idx]->upserts), &read_version, &(_upserts[start_idx]->src_rss_rowids)));
    }
    int64_t t_read_rss = MonotonicMillis();

    for (uint32_t idx = start_idx; idx < end_idx; idx++) {
        _upserts[idx]->split_src_rss_rowids(idx, _partial_update_states[idx].src_rss_rowids);
        // build `rss_rowid_to_update_rowid`
        _partial_update_states[idx].read_version = read_version;
        TRY_CATCH_BAD_ALLOC(_partial_update_states[idx].build_rss_rowid_to_update_rowid());
        _partial_update_states[idx].inited = true;
    }
    int64_t t_end = MonotonicMillis();

    LOG(INFO) << strings::Substitute(
            "prepare ColumnPartialUpdateState tablet:$0 segment:[$1, $2) "
            "time:$3ms(src_rss:$4)",
            _tablet_id, start_idx, end_idx, t_end - t_start, t_read_rss - t_start);
    return Status::OK();
}

Status RowsetColumnUpdateState::_resolve_conflict(Tablet* tablet, uint32_t rowset_id, uint32_t start_idx,
                                                  uint32_t end_idx, EditVersion latest_applied_version,
                                                  const PrimaryIndex& index) {
    CHECK_MEM_LIMIT("RowsetColumnUpdateState::_resolve_conflict");
    int64_t t_start = MonotonicMillis();
    // rebuild src_rss_rowids;
    TRY_CATCH_BAD_ALLOC(_upserts[start_idx]->src_rss_rowids.resize(_upserts[start_idx]->upserts_size(), 0));
    RETURN_IF_ERROR(index.get(*(_upserts[start_idx]->upserts), &(_upserts[start_idx]->src_rss_rowids)));
    int64_t t_read_index = MonotonicMillis();
    for (uint32_t idx = start_idx; idx < end_idx; idx++) {
        TRY_CATCH_BAD_ALLOC({
            _partial_update_states[idx].src_rss_rowids.clear();
            _upserts[idx]->split_src_rss_rowids(idx, _partial_update_states[idx].src_rss_rowids);
            // rebuild rss_rowid_to_update_rowid
            _partial_update_states[idx].build_rss_rowid_to_update_rowid();
        });
    }
    int64_t t_end = MonotonicMillis();
    LOG(INFO) << strings::Substitute(
            "_resolve_conflict_column_mode tablet:$0 rowset:$1 segment:[$2, $3) version:($4 $5) "
            "time:$6ms(index:$7/build:$8)",
            tablet->tablet_id(), rowset_id, start_idx, end_idx,
            _partial_update_states[start_idx].read_version.to_string(), latest_applied_version.to_string(),
            t_end - t_start, t_read_index - t_start, t_end - t_read_index);

    return Status::OK();
}

Status RowsetColumnUpdateState::_check_and_resolve_conflict(Tablet* tablet, uint32_t rowset_id, uint32_t start_idx,
                                                            uint32_t end_idx, EditVersion latest_applied_version,
                                                            const PrimaryIndex& index) {
    if (_partial_update_states.size() < end_idx || !_partial_update_states[start_idx].inited ||
        !_partial_update_states[end_idx - 1].inited) {
        std::string msg = strings::Substitute(
                "_check_and_reslove_conflict tablet:$0 rowset:$1 segment:[$2, $3) failed, partial_update_states "
                "size:$4",
                tablet->tablet_id(), rowset_id, start_idx, end_idx, _partial_update_states.size());
        LOG(WARNING) << msg;
        return Status::InternalError(msg);
    }

    VLOG(2) << "latest_applied_version is " << latest_applied_version.to_string() << " read version is "
            << _partial_update_states[start_idx].read_version.to_string();
    if (latest_applied_version == _partial_update_states[start_idx].read_version) {
        // _read_version is equal to latest_applied_version which means there is no other rowset is applied.
        // skip resolve conflict
        return Status::OK();
    }

    return _resolve_conflict(tablet, rowset_id, start_idx, end_idx, latest_applied_version, index);
}

Status RowsetColumnUpdateState::_finalize_partial_update_state(Tablet* tablet, Rowset* rowset,
                                                               MemTracker* update_mem_tracker,
                                                               EditVersion latest_applied_version,
                                                               const PrimaryIndex& index) {
    const auto& rowset_meta_pb = rowset->rowset_meta()->get_meta_pb_without_schema();
    if (!rowset_meta_pb.has_txn_meta() || rowset->num_update_files() == 0 ||
        rowset_meta_pb.txn_meta().has_merge_condition()) {
        return Status::OK();
    }
    RETURN_IF_ERROR(_init_rowset_seg_id(tablet));
    for (uint32_t i = 0; i < rowset->num_update_files();) {
        uint32_t end_idx = 0;
        RETURN_IF_ERROR(_load_upserts(rowset, update_mem_tracker, i, &end_idx));
        DCHECK(end_idx > i);
        // check and resolve conflict
        if (_partial_update_states.size() == 0 || !_partial_update_states[i].inited) {
            RETURN_IF_ERROR(_prepare_partial_update_states(tablet, rowset, i, end_idx, false));
        } else {
            // reslove conflict
            RETURN_IF_ERROR(_check_and_resolve_conflict(tablet, rowset->rowset_meta()->get_rowset_seg_id(), i, end_idx,
                                                        latest_applied_version, index));
        }
        _release_upserts(i, end_idx);
        i = end_idx;
    }

    return Status::OK();
}

int64_t RowsetColumnUpdateState::calc_upt_memory_usage_per_row(Rowset* rowset) {
    // `num_rows_upt` could be zero after upgrade from old version,
    // then we will return zero and no limit.
    if ((rowset->num_rows_upt()) <= 0) return 0;
    return rowset->total_update_row_size() / rowset->num_rows_upt();
}

// Read chunk from source segment file and call `update_func` to update it.
// `update_func` accept ChunkUniquePtr and [start_rowid, end_rowid) range of this chunk.
static Status read_from_source_segment_and_update(
        Rowset* rowset, const Schema& schema, Tablet* tablet, OlapReaderStatistics* stats, int64_t version,
        RowsetSegmentId rowset_seg_id, const std::string& path,
        const std::function<Status(StreamChunkContainer, bool, int64_t)>& update_func) {
    CHECK_MEM_LIMIT("RowsetColumnUpdateState::read_from_source_segment");
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(rowset->rowset_path()));
    // We need to estimate each update rows size before it has been actually updated.
    const int64_t upt_memory_usage_per_row = RowsetColumnUpdateState::calc_upt_memory_usage_per_row(rowset);
    auto segment = Segment::open(fs, FileInfo{path}, rowset_seg_id.segment_id, rowset->schema());
    if (!segment.ok()) {
        LOG(WARNING) << "Fail to open " << path << ": " << segment.status();
        return segment.status();
    }
    if ((*segment)->num_rows() == 0) {
        return Status::InternalError("empty segment");
    }
    SegmentReadOptions seg_options;
    seg_options.fs = fs;
    seg_options.stats = stats;
    seg_options.is_primary_keys = true;
    seg_options.tablet_id = rowset->rowset_meta()->tablet_id();
    seg_options.rowset_id = rowset_seg_id.sequence_rowset_id;
    seg_options.version = version;
    // not use delvec loader
    seg_options.dcg_loader = std::make_shared<LocalDeltaColumnGroupLoader>(tablet->data_dir()->get_meta());
    seg_options.chunk_size = config::vector_chunk_size;
    ASSIGN_OR_RETURN(auto seg_iter, (*segment)->new_iterator(schema, seg_options));
    ChunkUniquePtr source_chunk_ptr;
    ChunkUniquePtr tmp_chunk_ptr;
    TRY_CATCH_BAD_ALLOC(source_chunk_ptr = ChunkHelper::new_chunk(schema, config::vector_chunk_size));
    TRY_CATCH_BAD_ALLOC(tmp_chunk_ptr = ChunkHelper::new_chunk(schema, config::vector_chunk_size));
    uint32_t start_rowid = 0;
    while (true) {
        tmp_chunk_ptr->reset();
        auto st = seg_iter->get_next(tmp_chunk_ptr.get());
        if (st.is_end_of_file()) {
            break;
        } else if (!st.ok()) {
            return st;
        } else {
            source_chunk_ptr->append(*tmp_chunk_ptr);
            // Avoid too many memory usage and Column overflow, we will limit source chunk's size.
            if (source_chunk_ptr->num_rows() >= INT32_MAX ||
                (int64_t)source_chunk_ptr->num_rows() * upt_memory_usage_per_row >
                        config::partial_update_memory_limit_per_worker) {
                // Because we will handle columns group by group (define by config::vertical_compaction_max_columns_per_group),
                // so use `upt_memory_usage_per_row` to estimate source chunk future memory cost will be overvalued.
                // But it's better to be overvalued than undervalued.
                StreamChunkContainer container = {
                        .chunk_ptr = source_chunk_ptr.get(),
                        .start_rowid = start_rowid,
                        .end_rowid = start_rowid + static_cast<uint32_t>(source_chunk_ptr->num_rows())};
                RETURN_IF_ERROR(update_func(container, true /*print log*/, upt_memory_usage_per_row));
                start_rowid += static_cast<uint32_t>(source_chunk_ptr->num_rows());
                source_chunk_ptr->reset();
            }
        }
    }
    if (!source_chunk_ptr->is_empty()) {
        StreamChunkContainer container = {
                .chunk_ptr = source_chunk_ptr.get(),
                .start_rowid = start_rowid,
                .end_rowid = start_rowid + static_cast<uint32_t>(source_chunk_ptr->num_rows())};
        RETURN_IF_ERROR(update_func(container, false /*print log*/, upt_memory_usage_per_row));
        start_rowid += static_cast<uint32_t>(source_chunk_ptr->num_rows());
        source_chunk_ptr->reset();
    }
    return Status::OK();
}

// this function build delta writer for delta column group's file.(end with `.col`)
StatusOr<std::unique_ptr<SegmentWriter>> RowsetColumnUpdateState::_prepare_delta_column_group_writer(
        Rowset* rowset, const std::shared_ptr<TabletSchema>& tschema, uint32_t rssid, int64_t ver, int idx) {
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(rowset->rowset_path()));
    ASSIGN_OR_RETURN(auto rowsetid_segid, _find_rowset_seg_id(rssid));
    // always 0 file suffix here, because alter table will execute after this version has been applied only.
    const std::string path = Rowset::delta_column_group_path(rowset->rowset_path(), rowsetid_segid.unique_rowset_id,
                                                             rowsetid_segid.segment_id, ver, idx);
    (void)fs->delete_file(path); // delete .cols if already exist
    WritableFileOptions opts{.sync_on_close = true};
    ASSIGN_OR_RETURN(auto wfile, fs->new_writable_file(opts, path));
    SegmentWriterOptions writer_options;
    auto segment_writer =
            std::make_unique<SegmentWriter>(std::move(wfile), rowsetid_segid.segment_id, tschema, writer_options);
    RETURN_IF_ERROR(segment_writer->init(false));
    return std::move(segment_writer);
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

// cut rowid pairs by source rowid's order. E.g.
// rowid_pairs -> <101, 2>, <202, 3>, <303, 4>, <102, 5>, <203, 6>
// After cut, it will be:
//  inorder_source_rowids -> <101, 202, 303>, <102, 203>
//  inorder_upt_rowids -> <2, 3, 4>, <5, 6>
static void cut_rowids_in_order(const std::vector<RowidPairs>& rowid_pairs,
                                std::vector<std::vector<uint32_t>>* inorder_source_rowids,
                                std::vector<std::vector<uint32_t>>* inorder_upt_rowids,
                                StreamChunkContainer container) {
    uint32_t last_source_rowid = 0;
    std::vector<uint32_t> current_source_rowids;
    std::vector<uint32_t> current_upt_rowids;
    auto cut_rowids_fn = [&]() {
        inorder_source_rowids->push_back({});
        inorder_upt_rowids->push_back({});
        inorder_source_rowids->back().swap(current_source_rowids);
        inorder_upt_rowids->back().swap(current_upt_rowids);
    };
    for (const auto& each : rowid_pairs) {
        if (!container.contains(each.first)) {
            // skip in this round
            continue;
        }
        if (each.first < last_source_rowid) {
            // cut
            cut_rowids_fn();
        }
        // Align rowid
        current_source_rowids.push_back(each.first - container.start_rowid);
        current_upt_rowids.push_back(each.second);
        last_source_rowid = each.first;
    }
    if (!current_source_rowids.empty()) {
        cut_rowids_fn();
    }
}

// read from upt files and update rows in source chunk.
Status RowsetColumnUpdateState::_update_source_chunk_by_upt(const UptidToRowidPairs& upt_id_to_rowid_pairs,
                                                            const Schema& partial_schema, Rowset* rowset,
                                                            OlapReaderStatistics* stats, MemTracker* tracker,
                                                            StreamChunkContainer container) {
    CHECK_MEM_LIMIT("RowsetColumnUpdateState::_update_source_chunk_by_upt");
    // handle upt files one by one
    for (const auto& each : upt_id_to_rowid_pairs) {
        const uint32_t upt_id = each.first;
        // 1. get chunk from upt file
        ChunkUniquePtr upt_chunk;
        TRY_CATCH_BAD_ALLOC(upt_chunk = ChunkHelper::new_chunk(partial_schema, DEFAULT_CHUNK_SIZE));
        ASSIGN_OR_RETURN(auto update_iterator, rowset->get_update_file_iterator(partial_schema, upt_id, stats));
        DeferOp iter_defer([&]() {
            if (update_iterator != nullptr) {
                update_iterator->close();
            }
        });
        RETURN_IF_ERROR(read_chunk_from_update_file(update_iterator, upt_chunk));
        const size_t upt_chunk_size = upt_chunk->memory_usage();
        tracker->consume(upt_chunk_size);
        DeferOp tracker_defer([&]() { tracker->release(upt_chunk_size); });
        // 2. update source chunk
        std::vector<std::vector<uint32_t>> inorder_source_rowids;
        std::vector<std::vector<uint32_t>> inorder_upt_rowids;
        cut_rowids_in_order(each.second, &inorder_source_rowids, &inorder_upt_rowids, container);
        DCHECK(inorder_source_rowids.size() == inorder_upt_rowids.size());
        for (int i = 0; i < inorder_source_rowids.size(); i++) {
            auto tmp_chunk = ChunkHelper::new_chunk(partial_schema, inorder_upt_rowids[i].size());
            TRY_CATCH_BAD_ALLOC(tmp_chunk->append_selective(*upt_chunk, inorder_upt_rowids[i].data(), 0,
                                                            inorder_upt_rowids[i].size()));
            RETURN_IF_EXCEPTION(container.chunk_ptr->update_rows(*tmp_chunk, inorder_source_rowids[i].data()));
        }
    }
    return Status::OK();
}

// this function build segment writer for segment files
StatusOr<std::unique_ptr<SegmentWriter>> RowsetColumnUpdateState::_prepare_segment_writer(
        Rowset* rowset, const TabletSchemaCSPtr& tablet_schema, int segment_id) {
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(rowset->rowset_path()));
    const std::string path = Rowset::segment_file_path(rowset->rowset_path(), rowset->rowset_id(), segment_id);
    (void)fs->delete_file(path); // delete .dat if already exist
    WritableFileOptions opts{.sync_on_close = true};
    ASSIGN_OR_RETURN(auto wfile, fs->new_writable_file(opts, path));
    SegmentWriterOptions writer_options;
    auto segment_writer = std::make_unique<SegmentWriter>(std::move(wfile), segment_id, tablet_schema, writer_options);
    RETURN_IF_ERROR(segment_writer->init());
    return std::move(segment_writer);
}

static std::pair<std::vector<uint32_t>, std::vector<uint32_t>> get_read_update_columns_ids(
        const RowsetTxnMetaPB& txn_meta, const TabletSchemaCSPtr& tablet_schema) {
    std::vector<uint32_t> update_column_ids(txn_meta.partial_update_column_ids().begin(),
                                            txn_meta.partial_update_column_ids().end());
    std::set<uint32_t> update_columns_set(update_column_ids.begin(), update_column_ids.end());

    std::vector<uint32_t> read_column_ids;
    for (uint32_t i = 0; i < tablet_schema->num_columns(); i++) {
        if (update_columns_set.find(i) == update_columns_set.end()) {
            read_column_ids.push_back(i);
        }
    }

    return {read_column_ids, update_column_ids};
}

Status RowsetColumnUpdateState::_fill_default_columns(const TabletSchemaCSPtr& tablet_schema,
                                                      const std::vector<uint32_t>& column_ids, const int64_t row_cnt,
                                                      vector<ColumnPtr>* columns) {
    for (auto i = 0; i < column_ids.size(); ++i) {
        const TabletColumn& tablet_column = tablet_schema->column(column_ids[i]);

        bool has_default_value = tablet_column.has_default_value();
        std::string default_value = has_default_value ? tablet_column.default_value() : "";
        auto iter = _column_to_expr_value.find(std::string(tablet_column.name()));
        if (iter != _column_to_expr_value.end()) {
            has_default_value = true;
            default_value = iter->second;
        }
        if (has_default_value) {
            const TypeInfoPtr& type_info = get_type_info(tablet_column);
            std::unique_ptr<DefaultValueColumnIterator> default_value_iter =
                    std::make_unique<DefaultValueColumnIterator>(true, default_value, tablet_column.is_nullable(),
                                                                 type_info, tablet_column.length(), row_cnt);
            ColumnIteratorOptions iter_opts;
            RETURN_IF_ERROR(default_value_iter->init(iter_opts));
            RETURN_IF_ERROR(
                    default_value_iter->fetch_values_by_rowid(nullptr, row_cnt, (*columns)[column_ids[i]].get()));
        } else {
            TRY_CATCH_BAD_ALLOC((*columns)[column_ids[i]]->append_default(row_cnt));
        }
    }
    return Status::OK();
}

Status RowsetColumnUpdateState::_update_primary_index(const TabletSchemaCSPtr& tablet_schema, Tablet* tablet,
                                                      const EditVersion& edit_version, uint32_t rowset_id,
                                                      std::map<int, ChunkUniquePtr>& segid_to_chunk,
                                                      int64_t insert_row_cnt, PersistentIndexMetaPB& index_meta,
                                                      vector<std::pair<uint32_t, DelVectorPtr>>& delvecs,
                                                      PrimaryIndex& index) {
    // 1. build pk column
    vector<uint32_t> pk_column_ids;
    for (size_t i = 0; i < tablet_schema->num_key_columns(); i++) {
        pk_column_ids.push_back((uint32_t)i);
    }
    Schema pkey_schema = ChunkHelper::convert_schema(tablet_schema, pk_column_ids);

    // 2. update pk index
    PrimaryIndex::DeletesMap new_deletes;
    RETURN_IF_ERROR(index.prepare(edit_version, insert_row_cnt));
    for (const auto& each_chunk : segid_to_chunk) {
        new_deletes[rowset_id + each_chunk.first] = {};
        MutableColumnPtr pk_column;
        RETURN_IF_ERROR(PrimaryKeyEncoder::create_column(pkey_schema, &pk_column));
        PrimaryKeyEncoder::encode(pkey_schema, *each_chunk.second, 0, each_chunk.second->num_rows(), pk_column.get());
        RETURN_IF_ERROR(index.upsert(rowset_id + each_chunk.first, 0, *pk_column, &new_deletes));
    }
    RETURN_IF_ERROR(index.commit(&index_meta));
    for (auto& new_delete : new_deletes) {
        // record delvec
        auto delvec = std::make_shared<DelVector>();
        auto& del_ids = new_delete.second;
        delvec->init(edit_version.major_number(), del_ids.data(), del_ids.size());
        delvecs.emplace_back(new_delete.first, delvec);
    }
    return Status::OK();
}

Status RowsetColumnUpdateState::_update_rowset_meta(const RowsetSegmentStat& stat, Rowset* rowset) {
    rowset->rowset_meta()->set_num_rows(stat.num_rows_written);
    rowset->rowset_meta()->set_total_row_size(stat.total_row_size);
    rowset->rowset_meta()->set_total_disk_size(stat.total_data_size);
    rowset->rowset_meta()->set_data_disk_size(stat.total_data_size);
    rowset->rowset_meta()->set_index_disk_size(stat.total_index_size);
    rowset->rowset_meta()->set_empty(stat.num_rows_written == 0);
    rowset->rowset_meta()->set_num_segments(stat.num_segment);
    if (stat.num_segment <= 1) {
        rowset->rowset_meta()->set_segments_overlap_pb(NONOVERLAPPING);
    }
    (void)rowset->reload();
    return Status::OK();
}

static void padding_char_columns(const Schema& schema, const TabletSchemaCSPtr& tschema, Chunk* chunk) {
    auto char_field_indexes = ChunkHelper::get_char_field_indexes(schema);
    ChunkHelper::padding_char_columns(char_field_indexes, schema, tschema, chunk);
}

// handle new rows, generate segment files and update primary index
Status RowsetColumnUpdateState::_insert_new_rows(const TabletSchemaCSPtr& tablet_schema, Tablet* tablet,
                                                 const EditVersion& edit_version, Rowset* rowset, uint32_t rowset_id,
                                                 PersistentIndexMetaPB& index_meta,
                                                 vector<std::pair<uint32_t, DelVectorPtr>>& delvecs,
                                                 PrimaryIndex& index) {
    int segid = 0;
    RowsetSegmentStat stat;
    const auto& txn_meta = rowset->rowset_meta()->get_meta_pb_without_schema().txn_meta();
    auto schema = ChunkHelper::convert_schema(tablet_schema);
    auto read_update_column_ids = get_read_update_columns_ids(txn_meta, tablet_schema);
    std::map<int, ChunkUniquePtr> segid_to_chunk;
    OlapReaderStatistics stats;
    Schema partial_schema = ChunkHelper::convert_schema(tablet_schema, read_update_column_ids.second);
    for (int upt_id = 0; upt_id < _partial_update_states.size(); upt_id++) {
        if (_partial_update_states[upt_id].insert_rowids.size() > 0) {
            ASSIGN_OR_RETURN(auto update_iterator, rowset->get_update_file_iterator(partial_schema, upt_id, &stats));
            DeferOp iter_defer([&]() {
                if (update_iterator != nullptr) {
                    update_iterator->close();
                }
            });
            // 1. generate segment file
            auto chunk_ptr = ChunkHelper::new_chunk(schema, _partial_update_states[upt_id].insert_rowids.size());
            ChunkUniquePtr partial_chunk_ptr = ChunkHelper::new_chunk(partial_schema, DEFAULT_CHUNK_SIZE);
            ASSIGN_OR_RETURN(auto writer, _prepare_segment_writer(rowset, tablet_schema, segid));
            RETURN_IF_ERROR(read_chunk_from_update_file(update_iterator, partial_chunk_ptr));
            for (uint32_t column_id : read_update_column_ids.second) {
                chunk_ptr->get_column_by_id(column_id)->append_selective(
                        *partial_chunk_ptr->get_column_by_id(column_id), _partial_update_states[upt_id].insert_rowids);
            }
            // fill default columns
            RETURN_IF_ERROR(_fill_default_columns(tablet_schema, read_update_column_ids.first, chunk_ptr->num_rows(),
                                                  &chunk_ptr->columns()));
            uint64_t segment_file_size = 0;
            uint64_t index_size = 0;
            uint64_t footer_position = 0;
            padding_char_columns(schema, tablet_schema, chunk_ptr.get());
            RETURN_IF_ERROR(writer->append_chunk(*chunk_ptr));
            RETURN_IF_ERROR(writer->finalize(&segment_file_size, &index_size, &footer_position));
            // update statisic
            stat.num_segment++;
            stat.total_data_size += segment_file_size;
            stat.total_index_size += index_size;
            stat.num_rows_written += static_cast<int64_t>(chunk_ptr->num_rows());
            stat.total_row_size += static_cast<int64_t>(chunk_ptr->bytes_usage());
            segid_to_chunk[segid] = std::move(chunk_ptr);
            segid++;
        }
    }
    if (stat.num_segment > 0) {
        // 2. update pk index
        RETURN_IF_ERROR(_update_primary_index(tablet_schema, tablet, edit_version, rowset_id, segid_to_chunk,
                                              stat.num_rows_written, index_meta, delvecs, index));
        // 3. update meta, add segment to rowset
        RETURN_IF_ERROR(_update_rowset_meta(stat, rowset));
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

Status RowsetColumnUpdateState::finalize(Tablet* tablet, Rowset* rowset, uint32_t rowset_id,
                                         PersistentIndexMetaPB& index_meta, MemTracker* tracker,
                                         vector<std::pair<uint32_t, DelVectorPtr>>& delvecs, PrimaryIndex& index) {
    CHECK_MEM_LIMIT("RowsetColumnUpdateState::finalize");
    if (_finalize_finished) return Status::OK();
    std::stringstream cost_str;
    MonotonicStopWatch watch;
    watch.start();

    DCHECK(rowset->num_update_files() == _partial_update_states.size());
    DCHECK(rowset->rowset_meta()->get_meta_pb_without_schema().has_txn_meta())
            << fmt::format("tablet_id: {} rowset_id: {}", tablet->tablet_id(), rowset_id);
    const auto& txn_meta = rowset->rowset_meta()->get_meta_pb_without_schema().txn_meta();

    // 1. resolve conflicts and generate `ColumnPartialUpdateState` finally.
    EditVersion latest_applied_version;
    RETURN_IF_ERROR(tablet->updates()->get_latest_applied_version(&latest_applied_version));
    RETURN_IF_ERROR(_finalize_partial_update_state(tablet, rowset, tracker, latest_applied_version, index));

    std::vector<ColumnId> update_column_ids;
    std::vector<ColumnUID> update_column_uids;
    std::vector<ColumnUID> unique_update_column_ids;
    const auto& tschema = rowset->schema();
    for (ColumnId cid : txn_meta.partial_update_column_ids()) {
        if (cid >= tschema->num_key_columns()) {
            update_column_ids.push_back(cid);
            update_column_uids.push_back((ColumnUID)cid);
        }
    }
    for (uint32_t uid : txn_meta.partial_update_column_unique_ids()) {
        auto cid = tschema->field_index(uid);
        if (cid == -1) {
            std::string msg =
                    strings::Substitute("column with unique id:$0 does not exist. tablet:$1", uid, tablet->tablet_id());
            LOG(ERROR) << msg;
            return Status::InternalError(msg);
        }
        if (!tschema->column(cid).is_key()) {
            unique_update_column_ids.push_back(uid);
        }
    }

    DCHECK(update_column_ids.size() == unique_update_column_ids.size());
    const size_t BATCH_HANDLE_COLUMN_CNT = config::vertical_compaction_max_columns_per_group;

    auto build_writer_fn = [&](uint32_t rssid, const std::shared_ptr<TabletSchema>& partial_tschema, int idx) {
        // we can generate delta column group by new version
        return _prepare_delta_column_group_writer(rowset, partial_tschema, rssid,
                                                  latest_applied_version.major_number() + 1, idx);
    };
    // 2. getter all rss_rowid_to_update_rowid, and prepare .col writer by the way
    int64_t insert_rows = 0;
    int64_t update_rows = 0;
    // rss_id -> update file id -> <rowid, update rowid>
    std::map<uint32_t, UptidToRowidPairs> rss_upt_id_to_rowid_pairs;
    for (int upt_id = 0; upt_id < _partial_update_states.size(); upt_id++) {
        for (const auto& each_rss : _partial_update_states[upt_id].rss_rowid_to_update_rowid) {
            for (const auto& each : each_rss.second) {
                rss_upt_id_to_rowid_pairs[each_rss.first][upt_id].emplace_back(each.first, each.second);
            }
            update_rows += each_rss.second.size();
        }
        insert_rows += _partial_update_states[upt_id].insert_rowids.size();
    }
    cost_str << " [generate delta column group writer] " << watch.elapsed_time();
    watch.reset();
    OlapReaderStatistics stats;
    int64_t total_do_update_time = 0;
    int64_t total_finalize_dcg_time = 0;
    int64_t handle_cnt = 0;
    // must record unique column id in delta column group
    // dcg_column_ids and dcg_column_files are mapped one to the other. E.g.
    // {{1,2}, {3,4}} -> {"aaa.cols", "bbb.cols"}
    // It means column_1 and column_2 are stored in aaa.cols, and column_3 and column_4 are stored in bbb.cols
    std::map<uint32_t, std::vector<std::vector<ColumnUID>>> dcg_column_ids;
    std::map<uint32_t, std::vector<std::string>> dcg_column_files;
    // 3. read from raw segment file and update file, and generate `.col` files one by one
    int idx = 0; // It is used for generate different .cols filename
    for (uint32_t col_index = 0; col_index < update_column_ids.size(); col_index += BATCH_HANDLE_COLUMN_CNT) {
        for (const auto& each : rss_upt_id_to_rowid_pairs) {
            int64_t t1 = MonotonicMillis();
            // 3.1 build column id range
            std::vector<ColumnId> selective_update_column_ids =
                    append_fixed_batch(update_column_ids, col_index, BATCH_HANDLE_COLUMN_CNT);
            std::vector<ColumnUID> selective_update_column_uids =
                    append_fixed_batch(update_column_uids, col_index, BATCH_HANDLE_COLUMN_CNT);
            std::vector<ColumnUID> selective_unique_update_column_ids =
                    append_fixed_batch(unique_update_column_ids, col_index, BATCH_HANDLE_COLUMN_CNT);
            // 3.2 build partial schema
            auto partial_tschema = TabletSchema::create(tschema, selective_update_column_uids);
            Schema partial_schema = ChunkHelper::convert_schema(tschema, selective_update_column_ids);
            ASSIGN_OR_RETURN(auto delta_column_group_writer, build_writer_fn(each.first, partial_tschema, idx));
            // 3.3 read from source segment
            ASSIGN_OR_RETURN(auto rowsetid_segid, _find_rowset_seg_id(each.first));
            const std::string seg_path = Rowset::segment_file_path(
                    rowset->rowset_path(), rowsetid_segid.unique_rowset_id, rowsetid_segid.segment_id);
            RETURN_IF_ERROR(read_from_source_segment_and_update(
                    rowset, partial_schema, tablet, &stats, latest_applied_version.major_number(), rowsetid_segid,
                    seg_path, [&](StreamChunkContainer container, bool print_log, int64_t upt_memory_usage_per_row) {
                        if (print_log) {
                            LOG(INFO) << "RowsetColumnUpdateState read from source segment: tablet id:"
                                      << tablet->tablet_id() << " [byte usage: " << container.chunk_ptr->bytes_usage()
                                      << " row cnt: " << container.chunk_ptr->num_rows() << "] row range : ["
                                      << container.start_rowid << ", " << container.end_rowid
                                      << ") upt_memory_usage_per_row : " << upt_memory_usage_per_row
                                      << " update column cnt : " << update_column_ids.size();
                        }
                        const size_t source_chunk_size = container.chunk_ptr->memory_usage();
                        tracker->consume(source_chunk_size);
                        DeferOp tracker_defer([&]() { tracker->release(source_chunk_size); });
                        // 3.4 read from update segment and do update
                        RETURN_IF_ERROR(_update_source_chunk_by_upt(each.second, partial_schema, rowset, &stats,
                                                                    tracker, container));
                        padding_char_columns(partial_schema, partial_tschema, container.chunk_ptr);
                        RETURN_IF_ERROR(delta_column_group_writer->append_chunk(*container.chunk_ptr));
                        return Status::OK();
                    }));
            int64_t t2 = MonotonicMillis();
            uint64_t segment_file_size = 0;
            uint64_t index_size = 0;
            uint64_t footer_position = 0;
            RETURN_IF_ERROR(delta_column_group_writer->finalize(&segment_file_size, &index_size, &footer_position));
            int64_t t3 = MonotonicMillis();
            total_do_update_time += t2 - t1;
            total_finalize_dcg_time += t3 - t2;
            // 3.6 prepare column id list and dcg file list
            dcg_column_ids[each.first].push_back(selective_unique_update_column_ids);
            dcg_column_files[each.first].push_back(file_name(delta_column_group_writer->segment_path()));
            handle_cnt++;
        }
        idx++;
    }
    // 4 generate delta columngroup
    for (const auto& each : rss_upt_id_to_rowid_pairs) {
        _rssid_to_delta_column_group[each.first] = std::make_shared<DeltaColumnGroup>();
        _rssid_to_delta_column_group[each.first]->init(latest_applied_version.major_number() + 1,
                                                       dcg_column_ids[each.first], dcg_column_files[each.first]);
    }
    cost_str << " [generate delta column group] " << watch.elapsed_time();
    watch.reset();
    // 5. generate segment file for insert data
    if (txn_meta.partial_update_mode() == PartialUpdateMode::COLUMN_UPSERT_MODE) {
        // ignore insert missing rows if partial_update_mode == COLUMN_UPDATE_MODE
        RETURN_IF_ERROR(_insert_new_rows(tschema, tablet, EditVersion(latest_applied_version.major_number() + 1, 0),
                                         rowset, rowset_id, index_meta, delvecs, index));
        cost_str << " [insert missing rows] " << watch.elapsed_time();
        watch.reset();
    }
    cost_str << strings::Substitute(" total_do_update_time(ms):$0 total_finalize_dcg_time(ms):$1 ",
                                    total_do_update_time, total_finalize_dcg_time);
    cost_str << strings::Substitute(
            "rss_cnt:$0 update_cnt:$1 column_cnt:$2 update_rows:$3 handle_cnt:$4 insert_rows:$5",
            rss_upt_id_to_rowid_pairs.size(), _partial_update_states.size(), update_column_ids.size(), update_rows,
            handle_cnt, insert_rows);

    LOG(INFO) << "RowsetColumnUpdateState tablet_id: " << tablet->tablet_id() << ", txn_id: " << rowset->txn_id()
              << ", finalize cost:" << cost_str.str();
    _finalize_finished = true;
    return Status::OK();
}

Status RowsetColumnUpdateState::_init_rowset_seg_id(Tablet* tablet) {
    std::vector<RowsetSharedPtr> rowsets;
    int64_t version;
    std::vector<uint32_t> rowset_ids;
    RETURN_IF_ERROR(tablet->updates()->get_apply_version_and_rowsets(&version, &rowsets, &rowset_ids));
    for (const auto& rs_ptr : rowsets) {
        for (uint32_t seg_id = 0; seg_id < rs_ptr->num_segments(); seg_id++) {
            _rssid_to_rowsetid_segid[rs_ptr->rowset_meta()->get_rowset_seg_id() + seg_id] =
                    RowsetSegmentId{.unique_rowset_id = rs_ptr->rowset_id(),
                                    .sequence_rowset_id = rs_ptr->rowset_meta()->get_rowset_seg_id(),
                                    .segment_id = seg_id};
        }
    }
    return Status::OK();
}

StatusOr<RowsetSegmentId> RowsetColumnUpdateState::_find_rowset_seg_id(uint32_t rssid) {
    auto rowsetid_segid = _rssid_to_rowsetid_segid.find(rssid);
    if (rowsetid_segid == _rssid_to_rowsetid_segid.end()) {
        LOG(ERROR) << "prepare_segment_writer meet invalid rssid: " << rssid;
        return Status::InternalError("prepare_segment_writer meet invalid rssid");
    }
    return rowsetid_segid->second;
}

std::string RowsetColumnUpdateState::to_string() const {
    return strings::Substitute("RowsetColumnUpdateState tablet:$0", _tablet_id);
}

} // namespace starrocks
