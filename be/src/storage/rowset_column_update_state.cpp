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
        _status = _do_load(tablet, rowset);
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

// check if we have memory to preload update data
void RowsetColumnUpdateState::_check_if_preload_column_mode_update_data(Rowset* rowset,
                                                                        MemTracker* update_mem_tracker) {
    if (update_mem_tracker->limit_exceeded() ||
        update_mem_tracker->consumption() + rowset->total_update_row_size() >= update_mem_tracker->limit()) {
        _enable_preload_column_mode_update_data = false;
    } else {
        _enable_preload_column_mode_update_data = true;
    }
}

Status RowsetColumnUpdateState::_load_upserts(Rowset* rowset, uint32_t idx) {
    RowsetReleaseGuard guard(rowset->shared_from_this());
    if (_upserts.size() == 0) {
        _upserts.resize(rowset->num_update_files());
        _update_chunk_cache.resize(rowset->num_update_files());
    } else {
        // update files should be immutable
        DCHECK(_upserts.size() == rowset->num_update_files());
        DCHECK(_update_chunk_cache.size() == rowset->num_update_files());
    }
    if (_upserts.size() == 0 || _upserts[idx] != nullptr) {
        return Status::OK();
    }

    const int64_t DEFAULT_CONTAINER_SIZE = 4096;
    OlapReaderStatistics stats;
    auto& schema = rowset->schema();
    vector<uint32_t> pk_columns;
    for (size_t i = 0; i < schema.num_key_columns(); i++) {
        pk_columns.push_back((uint32_t)i);
    }
    std::vector<uint32_t> update_columns;
    std::vector<uint32_t> update_columns_with_keys;
    const auto& txn_meta = rowset->rowset_meta()->get_meta_pb().txn_meta();
    for (uint32_t cid : txn_meta.partial_update_column_ids()) {
        update_columns_with_keys.push_back(cid);
        if (cid >= schema.num_key_columns()) {
            update_columns.push_back(cid);
        }
    }
    Schema pkey_schema = ChunkHelper::convert_schema(schema, pk_columns);
    Schema update_schema = ChunkHelper::convert_schema(schema, update_columns);
    Schema update_schema_with_keys = ChunkHelper::convert_schema(schema, update_columns_with_keys);
    std::unique_ptr<Column> pk_column;
    if (!PrimaryKeyEncoder::create_column(pkey_schema, &pk_column).ok()) {
        std::string err_msg = fmt::format("create column for primary key encoder failed, tablet_id: {}", _tablet_id);
        DCHECK(false) << err_msg;
        return Status::InternalError(err_msg);
    }

    // iterators for each update segments;
    std::vector<ChunkIteratorPtr> itrs;

    std::shared_ptr<Chunk> chunk_shared_ptr;
    // if we want to preload update cache, create chunk with update columns
    if (_enable_preload_column_mode_update_data) {
        _update_chunk_cache[idx] = ChunkHelper::new_chunk(update_schema, DEFAULT_CONTAINER_SIZE);
        chunk_shared_ptr = ChunkHelper::new_chunk(update_schema_with_keys, DEFAULT_CONTAINER_SIZE);
        ASSIGN_OR_RETURN(itrs, rowset->get_update_file_iterators(update_schema_with_keys, &stats));
    } else {
        _update_chunk_cache[idx] = ChunkHelper::new_chunk(update_schema, 0);
        chunk_shared_ptr = ChunkHelper::new_chunk(pkey_schema, DEFAULT_CONTAINER_SIZE);
        ASSIGN_OR_RETURN(itrs, rowset->get_update_file_iterators(pkey_schema, &stats));
    }

    if (itrs.size() != rowset->num_update_files()) {
        std::string err_msg = fmt::format("itrs.size {} != num_update_files {}, tablet_id: {}", itrs.size(),
                                          rowset->num_update_files(), _tablet_id);
        DCHECK(false) << err_msg;
        return Status::InternalError(err_msg);
    }
    auto chunk = chunk_shared_ptr.get();
    auto& dest = _upserts[idx];
    auto& dest_chunk = _update_chunk_cache[idx];
    auto col = pk_column->clone();
    auto itr = itrs[idx].get();
    if (itr != nullptr) {
        col->reserve(DEFAULT_CONTAINER_SIZE);
        while (true) {
            chunk->reset();
            auto st = itr->get_next(chunk);
            if (st.is_end_of_file()) {
                break;
            } else if (!st.ok()) {
                return st;
            } else {
                PrimaryKeyEncoder::encode(pkey_schema, *chunk, 0, chunk->num_rows(), col.get());
                if (_enable_preload_column_mode_update_data) {
                    for (int i = pk_columns.size(); i < update_columns_with_keys.size(); i++) {
                        _memory_usage += chunk->columns()[i]->byte_size();
                        dest_chunk->columns()[i - pk_columns.size()]->append(*chunk->columns()[i]);
                    }
                }
            }
        }
    }
    for (const auto& itr : itrs) {
        itr->close();
    }
    dest = std::move(col);
    // This is a little bit trick. If pk column is a binary column, we will call function `raw_data()` in the following
    // And the function `raw_data()` will build slice of pk column which will increase the memory usage of pk column
    // So we try build slice in advance in here to make sure the correctness of memory statistics
    dest->raw_data();
    _memory_usage += dest != nullptr ? dest->memory_usage() : 0;

    return Status::OK();
}

Status RowsetColumnUpdateState::_do_load(Tablet* tablet, Rowset* rowset) {
    auto span = Tracer::Instance().start_trace_txn_tablet("rowset_column_update_state_load", rowset->txn_id(),
                                                          tablet->tablet_id());

    for (size_t i = 0; i < rowset->num_update_files(); i++) {
        RETURN_IF_ERROR(_load_upserts(rowset, i));
    }

    for (size_t i = 0; i < rowset->num_update_files(); i++) {
        RETURN_IF_ERROR(_prepare_partial_update_states(tablet, rowset, i, true));
    }

    return Status::OK();
}

Status RowsetColumnUpdateState::_prepare_partial_update_states(Tablet* tablet, Rowset* rowset, uint32_t idx,
                                                               bool need_lock) {
    if (_partial_update_states.size() == 0) {
        _partial_update_states.resize(rowset->num_update_files());
    } else {
        // update files should be immutable
        DCHECK(_partial_update_states.size() == rowset->num_update_files());
    }

    if (_partial_update_states[idx].inited) {
        return Status::OK();
    }

    int64_t t_start = MonotonicMillis();

    _partial_update_states[idx].src_rss_rowids.resize(_upserts[idx]->size());

    int64_t t_read_rss = MonotonicMillis();
    if (need_lock) {
        RETURN_IF_ERROR(tablet->updates()->get_rss_rowids_by_pk(tablet, *_upserts[idx],
                                                                &(_partial_update_states[idx].read_version),
                                                                &(_partial_update_states[idx].src_rss_rowids)));
    } else {
        RETURN_IF_ERROR(tablet->updates()->get_rss_rowids_by_pk_unlock(tablet, *_upserts[idx],
                                                                       &(_partial_update_states[idx].read_version),
                                                                       &(_partial_update_states[idx].src_rss_rowids)));
    }
    // build `rss_rowid_to_update_rowid`
    _partial_update_states[idx].build_rss_rowid_to_update_rowid();
    int64_t t_end = MonotonicMillis();

    _partial_update_states[idx].inited = true;

    LOG(INFO) << strings::Substitute(
            "prepare ColumnPartialUpdateState tablet:$0 segment:$1 "
            "time:$2ms(src_rss:$3)",
            _tablet_id, idx, t_end - t_start, t_end - t_read_rss);
    return Status::OK();
}

Status RowsetColumnUpdateState::_resolve_conflict(Tablet* tablet, uint32_t rowset_id, uint32_t segment_id,
                                                  EditVersion latest_applied_version, const PrimaryIndex& index) {
    int64_t t_start = MonotonicMillis();
    // rebuild src_rss_rowids;
    DCHECK(_upserts[segment_id]->size() == _partial_update_states[segment_id].src_rss_rowids.size());
    // @TODO should handle error?
    RETURN_IF_ERROR(index.get(*_upserts[segment_id], &_partial_update_states[segment_id].src_rss_rowids));
    int64_t t_read_index = MonotonicMillis();
    // rebuild rss_rowid_to_update_rowid
    _partial_update_states[segment_id].build_rss_rowid_to_update_rowid();
    int64_t t_end = MonotonicMillis();
    LOG(INFO) << strings::Substitute(
            "_resolve_conflict_column_mode tablet:$0 rowset:$1 segment:$2 version:($3 $4) "
            "time:$5ms(index:$6/build:$7)",
            tablet->tablet_id(), rowset_id, segment_id, _partial_update_states[segment_id].read_version.to_string(),
            latest_applied_version.to_string(), t_end - t_start, t_read_index - t_start, t_end - t_read_index);

    return Status::OK();
}

Status RowsetColumnUpdateState::_check_and_resolve_conflict(Tablet* tablet, uint32_t rowset_id, uint32_t segment_id,
                                                            EditVersion latest_applied_version,
                                                            const PrimaryIndex& index) {
    if (_partial_update_states.size() <= segment_id || !_partial_update_states[segment_id].inited) {
        std::string msg = strings::Substitute(
                "_check_and_reslove_conflict tablet:$0 rowset:$1 segment:$2 failed, partial_update_states size:$3",
                tablet->tablet_id(), rowset_id, segment_id, _partial_update_states.size());
        LOG(WARNING) << msg;
        return Status::InternalError(msg);
    }

    LOG(INFO) << "latest_applied_version is " << latest_applied_version.to_string() << " read version is "
              << _partial_update_states[segment_id].read_version.to_string();
    if (latest_applied_version == _partial_update_states[segment_id].read_version) {
        // _read_version is equal to latest_applied_version which means there is no other rowset is applied.
        // skip resolve conflict
        return Status::OK();
    }

    return _resolve_conflict(tablet, rowset_id, segment_id, latest_applied_version, index);
}

Status RowsetColumnUpdateState::_finalize_partial_update_state(Tablet* tablet, Rowset* rowset,
                                                               EditVersion latest_applied_version,
                                                               const PrimaryIndex& index) {
    const auto& rowset_meta_pb = rowset->rowset_meta()->get_meta_pb();
    if (!rowset_meta_pb.has_txn_meta() || rowset->num_update_files() == 0 ||
        rowset_meta_pb.txn_meta().has_merge_condition()) {
        return Status::OK();
    }
    RETURN_IF_ERROR(_init_rowset_seg_id(tablet));
    for (uint32_t i = 0; i < rowset->num_update_files(); i++) {
        // check and resolve conflict
        if (_partial_update_states.size() == 0 || !_partial_update_states[i].inited) {
            RETURN_IF_ERROR(_prepare_partial_update_states(tablet, rowset, i, false));
        } else {
            // reslove conflict
            RETURN_IF_ERROR(_check_and_resolve_conflict(tablet, rowset->rowset_meta()->get_rowset_seg_id(), i,
                                                        latest_applied_version, index));
        }
    }

    return Status::OK();
}

static StatusOr<ChunkPtr> read_from_source_segment(Rowset* rowset, const Schema& schema, Tablet* tablet,
                                                   OlapReaderStatistics* stats, int64_t version,
                                                   RowsetSegmentId rowset_seg_id, const std::string& path) {
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(rowset->rowset_path()));
    auto segment = Segment::open(fs, path, rowset_seg_id.segment_id, &rowset->schema());
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
    ASSIGN_OR_RETURN(auto seg_iter, (*segment)->new_iterator(schema, seg_options));
    auto source_chunk_ptr = ChunkHelper::new_chunk(schema, (*segment)->num_rows());
    auto tmp_chunk_ptr = ChunkHelper::new_chunk(schema, 1024);
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

// this function build delta writer for delta column group's file.(end with `.col`)
StatusOr<std::unique_ptr<SegmentWriter>> RowsetColumnUpdateState::_prepare_delta_column_group_writer(
        Rowset* rowset, const std::shared_ptr<TabletSchema>& tschema, uint32_t rssid, int64_t ver) {
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(rowset->rowset_path()));
    ASSIGN_OR_RETURN(auto rowsetid_segid, _find_rowset_seg_id(rssid));
    const std::string path = Rowset::delta_column_group_path(rowset->rowset_path(), rowsetid_segid.unique_rowset_id,
                                                             rowsetid_segid.segment_id, ver);
    (void)fs->delete_file(path); // delete .cols if already exist
    WritableFileOptions opts{.sync_on_close = true};
    ASSIGN_OR_RETURN(auto wfile, fs->new_writable_file(opts, path));
    SegmentWriterOptions writer_options;
    auto segment_writer =
            std::make_unique<SegmentWriter>(std::move(wfile), rowsetid_segid.segment_id, tschema.get(), writer_options);
    RETURN_IF_ERROR(segment_writer->init(false));
    return std::move(segment_writer);
}

static Status read_chunk_from_update_file(const ChunkIteratorPtr& iter, const ChunkPtr& result_chunk) {
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

// this function have two goals:
// 1. get the rows from update files, store in `result_chunk`
// 2. generate `rowids`, the rowid list marks the rows in source segment file which be updated.
Status RowsetColumnUpdateState::_read_chunk_from_update(const RowidsToUpdateRowids& rowid_to_update_rowid,
                                                        std::vector<ChunkIteratorPtr>& update_iterators,
                                                        std::vector<uint32_t>& rowids, Chunk* result_chunk) {
    // We split the task into multiple rounds according to the update file where the updated rows are located.
    std::vector<uint32_t> batch_append_rowids;
    uint32_t cur_update_file_id = UINT32_MAX;
    for (const auto& each : rowid_to_update_rowid) {
        rowids.push_back(each.first);
        if (cur_update_file_id == UINT32_MAX) {
            // begin to handle new round.
            cur_update_file_id = each.second.first;
            batch_append_rowids.push_back(each.second.second);
        } else if (cur_update_file_id == each.second.first) {
            // same update file, batch them in one round, handle them later.
            batch_append_rowids.push_back(each.second.second);
        } else {
            // meet different update file, handle this round.
            if (!_enable_preload_column_mode_update_data) {
                _update_chunk_cache[cur_update_file_id]->reserve(4096);
                RETURN_IF_ERROR(read_chunk_from_update_file(update_iterators[cur_update_file_id],
                                                            _update_chunk_cache[cur_update_file_id]));
                result_chunk->append_selective(*_update_chunk_cache[cur_update_file_id], batch_append_rowids.data(), 0,
                                               batch_append_rowids.size());
                _update_chunk_cache[cur_update_file_id]->reset();
            } else {
                result_chunk->append_selective(*_update_chunk_cache[cur_update_file_id], batch_append_rowids.data(), 0,
                                               batch_append_rowids.size());
            }
            cur_update_file_id = each.second.first;
            batch_append_rowids.clear();
            batch_append_rowids.push_back(each.second.second);
        }
    }
    if (!batch_append_rowids.empty()) {
        // finish last round.
        if (!_enable_preload_column_mode_update_data) {
            _update_chunk_cache[cur_update_file_id]->reserve(4096);
            RETURN_IF_ERROR(read_chunk_from_update_file(update_iterators[cur_update_file_id],
                                                        _update_chunk_cache[cur_update_file_id]));
            result_chunk->append_selective(*_update_chunk_cache[cur_update_file_id], batch_append_rowids.data(), 0,
                                           batch_append_rowids.size());
            _update_chunk_cache[cur_update_file_id]->reset();
        } else {
            result_chunk->append_selective(*_update_chunk_cache[cur_update_file_id], batch_append_rowids.data(), 0,
                                           batch_append_rowids.size());
        }
    }
    return Status::OK();
}

Status RowsetColumnUpdateState::finalize(Tablet* tablet, Rowset* rowset, const PrimaryIndex& index) {
    if (_finalize_finished) return Status::OK();
    std::stringstream cost_str;
    MonotonicStopWatch watch;
    watch.start();

    DCHECK(rowset->num_update_files() == _partial_update_states.size());
    const auto& txn_meta = rowset->rowset_meta()->get_meta_pb().txn_meta();

    // 1. resolve conflicts and generate `ColumnPartialUpdateState` finally.
    EditVersion latest_applied_version;
    RETURN_IF_ERROR(tablet->updates()->get_latest_applied_version(&latest_applied_version));
    RETURN_IF_ERROR(_finalize_partial_update_state(tablet, rowset, latest_applied_version, index));

    std::vector<int32_t> update_column_ids;
    std::vector<uint32_t> update_column_uids;
    std::vector<uint32_t> unique_update_column_ids;
    const auto& tschema = rowset->schema();
    for (int32_t cid : txn_meta.partial_update_column_ids()) {
        if (cid >= tschema.num_key_columns()) {
            update_column_ids.push_back(cid);
            update_column_uids.push_back((uint32_t)cid);
        }
    }
    for (uint32_t cid : txn_meta.partial_update_column_unique_ids()) {
        auto& column = tschema.column(cid);
        if (!column.is_key()) {
            unique_update_column_ids.push_back(cid);
        }
    }
    auto partial_tschema = TabletSchema::create(tschema, update_column_ids);
    Schema partial_schema = ChunkHelper::convert_schema(tschema, update_column_uids);

    // rss_id -> delta column group writer
    std::map<uint32_t, std::unique_ptr<SegmentWriter>> delta_column_group_writer;
    // 2. getter all rss_rowid_to_update_rowid, and prepare .col writer by the way
    // rss_id -> rowid -> <update file id, update_rowids>
    std::map<uint32_t, RowidsToUpdateRowids> rss_rowid_to_update_rowid;
    for (int upt_id = 0; upt_id < _partial_update_states.size(); upt_id++) {
        for (const auto& each : _partial_update_states[upt_id].rss_rowid_to_update_rowid) {
            auto rssid = (uint32_t)(each.first >> 32);
            auto rowid = (uint32_t)(each.first & ROWID_MASK);
            rss_rowid_to_update_rowid[rssid][rowid] = std::make_pair(upt_id, each.second);
            // prepare delta column writers by the way
            if (delta_column_group_writer.count(rssid) == 0) {
                // we can generate delta column group by new version
                ASSIGN_OR_RETURN(auto writer, _prepare_delta_column_group_writer(rowset, partial_tschema, rssid,
                                                                                 latest_applied_version.major() + 1));
                delta_column_group_writer[rssid] = std::move(writer);
            }
        }
    }
    cost_str << " [generate delta column group writer] " << watch.elapsed_time();
    watch.reset();
    // 3. create update file's iterator
    OlapReaderStatistics stats;
    std::vector<ChunkIteratorPtr> update_file_iters;
    if (!_enable_preload_column_mode_update_data) {
        ASSIGN_OR_RETURN(update_file_iters, rowset->get_update_file_iterators(partial_schema, &stats));
    }
    cost_str << " [prepare upt column iter] " << watch.elapsed_time();
    watch.reset();

    int64_t total_seek_source_segment_time = 0;
    int64_t total_read_column_from_update_time = 0;
    int64_t total_finalize_dcg_time = 0;
    int64_t total_merge_column_time = 0;
    int64_t update_rows = 0;
    // 4. read from raw segment file and update file, and generate `.col` files one by one
    for (const auto& each : rss_rowid_to_update_rowid) {
        update_rows += each.second.size();
        int64_t t1 = MonotonicMillis();
        ASSIGN_OR_RETURN(auto rowsetid_segid, _find_rowset_seg_id(each.first));
        const std::string seg_path = Rowset::segment_file_path(rowset->rowset_path(), rowsetid_segid.unique_rowset_id,
                                                               rowsetid_segid.segment_id);
        // 4.1 read from source segment
        ASSIGN_OR_RETURN(auto source_chunk_ptr,
                         read_from_source_segment(rowset, partial_schema, tablet, &stats,
                                                  latest_applied_version.major(), rowsetid_segid, seg_path));
        // 4.2 read from update segment
        int64_t t2 = MonotonicMillis();
        std::vector<uint32_t> rowids;
        auto update_chunk_ptr = ChunkHelper::new_chunk(partial_schema, each.second.size());
        RETURN_IF_ERROR(_read_chunk_from_update(each.second, update_file_iters, rowids, update_chunk_ptr.get()));
        int64_t t3 = MonotonicMillis();
        // 4.3 merge source chunk and update chunk
        source_chunk_ptr->update_rows(*update_chunk_ptr, rowids.data());
        // 4.4 write column to delta column file
        int64_t t4 = MonotonicMillis();
        uint64_t segment_file_size = 0;
        uint64_t index_size = 0;
        uint64_t footer_position = 0;
        RETURN_IF_ERROR(delta_column_group_writer[each.first]->append_chunk(*source_chunk_ptr));
        RETURN_IF_ERROR(
                delta_column_group_writer[each.first]->finalize(&segment_file_size, &index_size, &footer_position));
        int64_t t5 = MonotonicMillis();
        total_seek_source_segment_time += t2 - t1;
        total_read_column_from_update_time += t3 - t2;
        total_merge_column_time += t4 - t3;
        total_finalize_dcg_time += t5 - t4;
        // 4.5 generate delta columngroup
        _rssid_to_delta_column_group[each.first] = std::make_shared<DeltaColumnGroup>();
        // must record unique column id in delta column group
        _rssid_to_delta_column_group[each.first]->init(
                latest_applied_version.major() + 1, unique_update_column_ids,
                file_name(delta_column_group_writer[each.first]->segment_path()));
    }
    cost_str << " [generate delta column group] " << watch.elapsed_time();
    watch.reset();
    cost_str << strings::Substitute(
            " seek_source_segment(ms):$0 read_column_from_update(ms):$1 avg_merge_column_time(ms):$2 "
            "avg_finalize_dcg_time(ms):$3 ",
            total_seek_source_segment_time, total_read_column_from_update_time, total_merge_column_time,
            total_finalize_dcg_time);
    cost_str << strings::Substitute("rss_cnt:$0 update_cnt:$1 column_cnt:$2 update_rows:$3",
                                    rss_rowid_to_update_rowid.size(), _partial_update_states.size(),
                                    update_column_ids.size(), update_rows);

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
