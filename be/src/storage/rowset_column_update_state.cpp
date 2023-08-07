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

void RowsetColumnUpdateState::_release_upserts(uint32_t idx) {
    if (idx >= _upserts.size()) {
        return;
    }
    if (_upserts[idx] != nullptr) {
        _memory_usage -= _upserts[idx]->memory_usage();
        _upserts[idx].reset();
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
    for (size_t i = 0; i < schema->num_key_columns(); i++) {
        pk_columns.push_back((uint32_t)i);
    }
    std::vector<uint32_t> update_columns;
    std::vector<uint32_t> update_columns_with_keys;
    const auto& txn_meta = rowset->rowset_meta()->get_meta_pb().txn_meta();
    for (uint32_t cid : txn_meta.partial_update_column_ids()) {
        update_columns_with_keys.push_back(cid);
        if (cid >= schema->num_key_columns()) {
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
    if (rowset->num_update_files() > 0) {
        RETURN_IF_ERROR(_load_upserts(rowset, 0));
        RETURN_IF_ERROR(_prepare_partial_update_states(tablet, rowset, 0, true));
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
    index.get(*_upserts[segment_id], &_partial_update_states[segment_id].src_rss_rowids);
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

    VLOG(2) << "latest_applied_version is " << latest_applied_version.to_string() << " read version is "
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
        RETURN_IF_ERROR(_load_upserts(rowset, i));
        // check and resolve conflict
        if (_partial_update_states.size() == 0 || !_partial_update_states[i].inited) {
            RETURN_IF_ERROR(_prepare_partial_update_states(tablet, rowset, i, false));
        } else {
            // reslove conflict
            RETURN_IF_ERROR(_check_and_resolve_conflict(tablet, rowset->rowset_meta()->get_rowset_seg_id(), i,
                                                        latest_applied_version, index));
        }
        _release_upserts(i);
    }

    return Status::OK();
}

static StatusOr<ChunkPtr> read_from_source_segment(Rowset* rowset, const Schema& schema, Tablet* tablet,
                                                   OlapReaderStatistics* stats, int64_t version,
                                                   RowsetSegmentId rowset_seg_id, const std::string& path) {
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(rowset->rowset_path()));
    auto segment = Segment::open(fs, path, rowset_seg_id.segment_id, rowset->schema());
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
    // always 0 file suffix here, because alter table will execute after this version has been applied only.
    const std::string path = Rowset::delta_column_group_path(rowset->rowset_path(), rowsetid_segid.unique_rowset_id,
                                                             rowsetid_segid.segment_id, ver, 0);
    (void)fs->delete_file(path); // delete .cols if already exist
    WritableFileOptions opts{.sync_on_close = true};
    ASSIGN_OR_RETURN(auto wfile, fs->new_writable_file(opts, path));
    SegmentWriterOptions writer_options;
    auto segment_writer =
            std::make_unique<SegmentWriter>(std::move(wfile), rowsetid_segid.segment_id, tschema, writer_options);
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

// this function build segment writer for segment files
StatusOr<std::unique_ptr<SegmentWriter>> RowsetColumnUpdateState::_prepare_segment_writer(
        Rowset* rowset, const TabletSchema& tablet_schema, int segment_id) {
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(rowset->rowset_path()));
    const std::string path = Rowset::segment_file_path(rowset->rowset_path(), rowset->rowset_id(), segment_id);
    (void)fs->delete_file(path); // delete .dat if already exist
    WritableFileOptions opts{.sync_on_close = true};
    ASSIGN_OR_RETURN(auto wfile, fs->new_writable_file(opts, path));
    SegmentWriterOptions writer_options;
    auto segment_writer = std::make_unique<SegmentWriter>(std::move(wfile), segment_id, &tablet_schema, writer_options);
    RETURN_IF_ERROR(segment_writer->init());
    return std::move(segment_writer);
}

static std::pair<std::vector<uint32_t>, std::vector<uint32_t>> get_read_update_columns_ids(
        const RowsetTxnMetaPB& txn_meta, const TabletSchema& tablet_schema) {
    std::vector<uint32_t> update_column_ids(txn_meta.partial_update_column_ids().begin(),
                                            txn_meta.partial_update_column_ids().end());
    std::set<uint32_t> update_columns_set(update_column_ids.begin(), update_column_ids.end());

    std::vector<uint32_t> read_column_ids;
    for (uint32_t i = 0; i < tablet_schema.num_columns(); i++) {
        if (update_columns_set.find(i) == update_columns_set.end()) {
            read_column_ids.push_back(i);
        }
    }

    return {read_column_ids, update_column_ids};
}

Status RowsetColumnUpdateState::_fill_default_columns(const TabletSchema& tablet_schema,
                                                      const std::vector<uint32_t>& column_ids, const int64_t row_cnt,
                                                      vector<std::shared_ptr<Column>>* columns) {
    for (auto i = 0; i < column_ids.size(); ++i) {
        const TabletColumn& tablet_column = tablet_schema.column(column_ids[i]);
        if (tablet_column.has_default_value()) {
            const TypeInfoPtr& type_info = get_type_info(tablet_column);
            std::unique_ptr<DefaultValueColumnIterator> default_value_iter =
                    std::make_unique<DefaultValueColumnIterator>(
                            tablet_column.has_default_value(), tablet_column.default_value(),
                            tablet_column.is_nullable(), type_info, tablet_column.length(), row_cnt);
            ColumnIteratorOptions iter_opts;
            RETURN_IF_ERROR(default_value_iter->init(iter_opts));
            default_value_iter->fetch_values_by_rowid(nullptr, row_cnt, (*columns)[column_ids[i]].get());
        } else {
            (*columns)[column_ids[i]]->append_default(row_cnt);
        }
    }
    return Status::OK();
}

Status RowsetColumnUpdateState::_update_primary_index(const TabletSchema& tablet_schema, Tablet* tablet,
                                                      const EditVersion& edit_version, uint32_t rowset_id,
                                                      std::map<int, ChunkUniquePtr>& segid_to_chunk,
                                                      int64_t insert_row_cnt, PersistentIndexMetaPB& index_meta,
                                                      vector<std::pair<uint32_t, DelVectorPtr>>& delvecs,
                                                      PrimaryIndex& index) {
    // 1. build pk column
    vector<uint32_t> pk_column_ids;
    for (size_t i = 0; i < tablet_schema.num_key_columns(); i++) {
        pk_column_ids.push_back((uint32_t)i);
    }
    Schema pkey_schema = ChunkHelper::convert_schema(tablet_schema, pk_column_ids);

    // 2. update pk index
    PrimaryIndex::DeletesMap new_deletes;
    RETURN_IF_ERROR(index.prepare(edit_version, insert_row_cnt));
    for (const auto& each_chunk : segid_to_chunk) {
        new_deletes[rowset_id + each_chunk.first] = {};
        std::unique_ptr<Column> pk_column;
        RETURN_IF_ERROR(PrimaryKeyEncoder::create_column(pkey_schema, &pk_column));
        PrimaryKeyEncoder::encode(pkey_schema, *each_chunk.second, 0, each_chunk.second->num_rows(), pk_column.get());
        RETURN_IF_ERROR(index.upsert(rowset_id + each_chunk.first, 0, *pk_column, &new_deletes));
    }
    RETURN_IF_ERROR(index.commit(&index_meta));
    for (auto& new_delete : new_deletes) {
        // record delvec
        auto delvec = std::make_shared<DelVector>();
        auto& del_ids = new_delete.second;
        delvec->init(edit_version.major(), del_ids.data(), del_ids.size());
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
    rowset->rowset_meta()->clear_txn_meta();
    return Status::OK();
}

// handle new rows, generate segment files and update primary index
Status RowsetColumnUpdateState::_insert_new_rows(const TabletSchema& tablet_schema, Tablet* tablet,
                                                 const EditVersion& edit_version, Rowset* rowset, uint32_t rowset_id,
                                                 PersistentIndexMetaPB& index_meta,
                                                 vector<std::pair<uint32_t, DelVectorPtr>>& delvecs,
                                                 PrimaryIndex& index) {
    int segid = 0;
    RowsetSegmentStat stat;
    const auto& txn_meta = rowset->rowset_meta()->get_meta_pb().txn_meta();
    auto schema = ChunkHelper::convert_schema(tablet_schema);
    auto read_update_column_ids = get_read_update_columns_ids(txn_meta, tablet_schema);
    std::map<int, ChunkUniquePtr> segid_to_chunk;
    std::vector<ChunkIteratorPtr> update_iterators;
    OlapReaderStatistics stats;
    Schema partial_schema = ChunkHelper::convert_schema(tablet_schema, read_update_column_ids.second);
    ASSIGN_OR_RETURN(update_iterators, rowset->get_update_file_iterators(partial_schema, &stats));
    for (int upt_id = 0; upt_id < _partial_update_states.size(); upt_id++) {
        if (_partial_update_states[upt_id].insert_rowids.size() > 0) {
            // 1. generate segment file
            auto chunk_ptr = ChunkHelper::new_chunk(schema, _partial_update_states[upt_id].insert_rowids.size());
            ChunkPtr partial_chunk_ptr = ChunkHelper::new_chunk(partial_schema, 4096);
            ASSIGN_OR_RETURN(auto writer, _prepare_segment_writer(rowset, tablet_schema, segid));
            RETURN_IF_ERROR(read_chunk_from_update_file(update_iterators[upt_id], partial_chunk_ptr));
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

Status RowsetColumnUpdateState::finalize(Tablet* tablet, Rowset* rowset, uint32_t rowset_id,
                                         PersistentIndexMetaPB& index_meta,
                                         vector<std::pair<uint32_t, DelVectorPtr>>& delvecs, PrimaryIndex& index) {
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
        if (cid >= tschema->num_key_columns()) {
            update_column_ids.push_back(cid);
            update_column_uids.push_back((uint32_t)cid);
        }
    }
    for (uint32_t cid : txn_meta.partial_update_column_unique_ids()) {
        auto& column = tschema->column(cid);
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
    ASSIGN_OR_RETURN(update_file_iters, rowset->get_update_file_iterators(partial_schema, &stats));
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
        RETURN_IF_EXCEPTION(source_chunk_ptr->update_rows(*update_chunk_ptr, rowids.data()));
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
        std::vector<std::vector<uint32_t>> dcg_column_ids{unique_update_column_ids};
        std::vector<std::string> dcg_column_files{file_name(delta_column_group_writer[each.first]->segment_path())};
        _rssid_to_delta_column_group[each.first]->init(latest_applied_version.major() + 1, dcg_column_ids,
                                                       dcg_column_files);
    }
    cost_str << " [generate delta column group] " << watch.elapsed_time();
    watch.reset();
    // generate segment file for insert data
    if (txn_meta.partial_update_mode() == PartialUpdateMode::COLUMN_UPSERT_MODE) {
        // ignore insert missing rows if partial_update_mode == COLUMN_UPDATE_MODE
        RETURN_IF_ERROR(_insert_new_rows(tschema, tablet, EditVersion(latest_applied_version.major() + 1, 0), rowset,
                                         rowset_id, index_meta, delvecs, index));
        cost_str << " [insert missing rows] " << watch.elapsed_time();
        watch.reset();
    }
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
