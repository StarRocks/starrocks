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

#include "common/tracer.h"
#include "fs/fs_util.h"
#include "fs/key_cache.h"
#include "gutil/strings/substitute.h"
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
#include "util/defer_op.h"
#include "util/phmap/phmap.h"
#include "util/stack_util.h"
#include "util/time.h"
#include "util/trace.h"

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

void ColumnModePartialUpdateHandler::_release_upserts(uint32_t start_idx, uint32_t end_idx) {
    for (uint32_t idx = start_idx; idx < _upserts.size() && idx < end_idx; idx++) {
        if (_upserts[idx] != nullptr) {
            if (_upserts[idx]->is_last(idx)) {
                const auto upserts_memory_usage = _upserts[idx]->upserts->memory_usage();
                _memory_usage -= upserts_memory_usage;
                _tracker->release(upserts_memory_usage);
            }
            _upserts[idx].reset();
        }
    }
}

Status ColumnModePartialUpdateHandler::_load_upserts(const RowsetUpdateStateParams& params, const Schema& pkey_schema,
                                                     const std::vector<ChunkIteratorPtr>& segment_iters,
                                                     uint32_t start_idx, uint32_t* end_idx) {
    if (_upserts.size() == 0) {
        _upserts.resize(_rowset_ptr->num_segments());
    } else {
        // update files should be immutable
        DCHECK(_upserts.size() == _rowset_ptr->num_segments());
    }
    if (_upserts.size() == 0) {
        return Status::OK();
    }
    if (_upserts[start_idx] != nullptr) {
        *end_idx = _upserts[start_idx]->end_idx;
        return Status::OK();
    }

    // 2. build schema.
    MutableColumnPtr pk_column;
    if (!PrimaryKeyEncoder::create_column(pkey_schema, &pk_column).ok()) {
        std::string err_msg =
                fmt::format("create column for primary key encoder failed, tablet_id: {}", params.tablet->id());
        DCHECK(false) << err_msg;
        return Status::InternalError(err_msg);
    }

    std::shared_ptr<Chunk> chunk_shared_ptr = ChunkHelper::new_chunk(pkey_schema, DEFAULT_CHUNK_SIZE);

    // alloc first BatchPKsPtr
    auto header_ptr = std::make_shared<BatchPKs>();
    header_ptr->upserts = pk_column->clone();
    header_ptr->start_idx = start_idx;
    for (uint32_t idx = start_idx; idx < _rowset_ptr->num_segments(); idx++) {
        header_ptr->offsets.push_back(header_ptr->upserts->size());
        auto chunk = chunk_shared_ptr.get();
        auto col = pk_column->clone();
        DeferOp iter_defer([&]() {
            if (segment_iters[idx] != nullptr) {
                segment_iters[idx]->close();
            }
        });
        if (segment_iters[idx] != nullptr) {
            col->reserve(DEFAULT_CHUNK_SIZE);
            while (true) {
                chunk->reset();
                auto st = segment_iters[idx]->get_next(chunk);
                if (st.is_end_of_file()) {
                    break;
                } else if (!st.ok()) {
                    return st;
                } else {
                    PrimaryKeyEncoder::encode(pkey_schema, *chunk, 0, chunk->num_rows(), col.get());
                }
            }
        }
        // merge pk column into BatchPKs
        header_ptr->upserts->append(*col);
        // all idx share same ptr with start idx
        _upserts[idx] = header_ptr;
        *end_idx = idx + 1;
        // quit merge PK into BatchPKs when hiting memory limit
        if (header_ptr->upserts->memory_usage() > config::primary_key_batch_get_index_memory_limit ||
            _tracker->any_limit_exceeded()) {
            break;
        }
    }
    // push end offset
    header_ptr->offsets.push_back(header_ptr->upserts->size());
    header_ptr->end_idx = *end_idx;
    DCHECK(header_ptr->offsets.size() == header_ptr->end_idx - header_ptr->start_idx + 1);
    // This is a little bit trick. If pk column is a binary column, we will call function `raw_data()` in the following
    // And the function `raw_data()` will build slice of pk column which will increase the memory usage of pk column
    // So we try build slice in advance in here to make sure the correctness of memory statistics
    header_ptr->upserts->raw_data();
    const auto upserts_memory_usage = header_ptr->upserts->memory_usage();
    _tracker->consume(upserts_memory_usage);
    _memory_usage += upserts_memory_usage;

    return Status::OK();
}

Status ColumnModePartialUpdateHandler::_load_update_state(const RowsetUpdateStateParams& params) {
    if (_rowset_ptr == nullptr) {
        _rowset_meta_ptr = std::make_unique<const RowsetMetadata>(params.op_write.rowset());
        _rowset_ptr = std::make_unique<Rowset>(params.tablet->tablet_mgr(), params.tablet->id(), _rowset_meta_ptr.get(),
                                               -1 /*unused*/, params.tablet_schema);
    }
    vector<uint32_t> pk_columns;
    for (size_t i = 0; i < params.tablet_schema->num_key_columns(); i++) {
        pk_columns.push_back((uint32_t)i);
    }
    Schema pkey_schema = ChunkHelper::convert_schema(params.tablet_schema, pk_columns);
    OlapReaderStatistics stats;
    ASSIGN_OR_RETURN(auto segment_iters, _rowset_ptr->get_each_segment_iterator(pkey_schema, true, &stats));
    RETURN_ERROR_IF_FALSE(segment_iters.size() == _rowset_ptr->num_segments());
    for (uint32_t i = 0; i < _rowset_ptr->num_segments();) {
        TRACE_COUNTER_INCREMENT("pcu_load_update_state_cnt", 1);
        uint32_t end_idx = 0;
        {
            TRACE_COUNTER_SCOPE_LATENCY_US("pcu_load_upsets_us");
            RETURN_IF_ERROR(_load_upserts(params, pkey_schema, segment_iters, i, &end_idx));
        }
        DCHECK(end_idx > i);
        {
            TRACE_COUNTER_SCOPE_LATENCY_US("pcu_prepare_partial_update_states_us");
            RETURN_IF_ERROR(_prepare_partial_update_states(params, i, end_idx, false));
        }
        _release_upserts(i, end_idx);
        i = end_idx;
    }

    return Status::OK();
}

Status ColumnModePartialUpdateHandler::_prepare_partial_update_states(const RowsetUpdateStateParams& params,
                                                                      uint32_t start_idx, uint32_t end_idx,
                                                                      bool need_lock) {
    if (_partial_update_states.size() == 0) {
        _partial_update_states.resize(_rowset_ptr->num_segments());
    } else {
        // update files should be immutable
        DCHECK(_partial_update_states.size() == _rowset_ptr->num_segments());
    }

    if (_partial_update_states[start_idx].inited) {
        // assume that states between [start_idx, end_idx) should be inited
        RETURN_ERROR_IF_FALSE(_partial_update_states[end_idx - 1].inited);
        return Status::OK();
    }

    EditVersion read_version;
    _upserts[start_idx]->src_rss_rowids.resize(_upserts[start_idx]->upserts_size());
    RETURN_IF_ERROR(params.tablet->update_mgr()->get_rowids_from_pkindex(
            params.tablet->id(), _base_version, _upserts[start_idx]->upserts, &_upserts[start_idx]->src_rss_rowids,
            need_lock));

    for (uint32_t idx = start_idx; idx < end_idx; idx++) {
        _upserts[idx]->split_src_rss_rowids(idx, _partial_update_states[idx].src_rss_rowids);
        // build `rss_rowid_to_update_rowid`
        _partial_update_states[idx].read_version = read_version;
        _partial_update_states[idx].build_rss_rowid_to_update_rowid();
        _partial_update_states[idx].inited = true;
    }
    return Status::OK();
}

// this function build delta writer for delta column group's file.(end with `.col`)
StatusOr<std::unique_ptr<SegmentWriter>> ColumnModePartialUpdateHandler::_prepare_delta_column_group_writer(
        const RowsetUpdateStateParams& params, const std::shared_ptr<TabletSchema>& tschema) {
    const std::string path = params.tablet->segment_location(gen_cols_filename(_txn_id));
    WritableFileOptions opts{.sync_on_close = true, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
    SegmentWriterOptions writer_options;
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
    uint32_t rowset_id = params.container.rssid_to_rowid().at(rssid);
    // 2. load segment meta.
    ASSIGN_OR_RETURN(auto segment, params.tablet->tablet_mgr()->load_segment(
                                           fileinfo, rssid - rowset_id /* segment id inside rowset */,
                                           &footer_size_hint, lake_io_opts, true, params.tablet_schema));
    SegmentReadOptions seg_options;
    ASSIGN_OR_RETURN(seg_options.fs, FileSystem::CreateSharedFromString(fileinfo.path));
    seg_options.stats = &stats;
    seg_options.is_primary_keys = true;
    seg_options.tablet_id = params.tablet->id();
    seg_options.rowset_id = rowset_id;
    seg_options.version = _base_version;
    seg_options.tablet_schema = params.tablet_schema;
    // not use delvec loader
    seg_options.dcg_loader = std::make_shared<LakeDeltaColumnGroupLoader>(params.metadata);
    ASSIGN_OR_RETURN(auto seg_iter, segment->new_iterator(schema, seg_options));
    auto source_chunk_ptr = ChunkHelper::new_chunk(schema, segment->num_rows());
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

// cut rowid pairs by source rowid's order. E.g.
// rowid_pairs -> <101, 2>, <202, 3>, <303, 4>, <102, 5>, <203, 6>
// After cut, it will be:
//  inorder_source_rowids -> <101, 202, 303>, <102, 203>
//  inorder_upt_rowids -> <2, 3, 4>, <5, 6>
static void cut_rowids_in_order(const std::vector<RowidPairs>& rowid_pairs,
                                std::vector<std::vector<uint32_t>>* inorder_source_rowids,
                                std::vector<std::vector<uint32_t>>* inorder_upt_rowids) {
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
        if (each.first < last_source_rowid) {
            // cut
            cut_rowids_fn();
        }
        current_source_rowids.push_back(each.first);
        current_upt_rowids.push_back(each.second);
        last_source_rowid = each.first;
    }
    if (!current_source_rowids.empty()) {
        cut_rowids_fn();
    }
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
Status ColumnModePartialUpdateHandler::_update_source_chunk_by_upt(const UptidToRowidPairs& upt_id_to_rowid_pairs,
                                                                   const Schema& partial_schema,
                                                                   ChunkPtr* source_chunk) {
    TRACE_COUNTER_SCOPE_LATENCY_US("pcu_update_source_by_upt_us");
    // build iterators
    OlapReaderStatistics stats;
    ASSIGN_OR_RETURN(auto segment_iters, _rowset_ptr->get_each_segment_iterator(partial_schema, true, &stats));
    RETURN_ERROR_IF_FALSE(segment_iters.size() == _rowset_ptr->num_segments());
    // handle upt files one by one
    for (const auto& each : upt_id_to_rowid_pairs) {
        const uint32_t upt_id = each.first;
        // 1. get chunk from upt file
        ChunkUniquePtr upt_chunk = ChunkHelper::new_chunk(partial_schema, DEFAULT_CHUNK_SIZE);
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
        std::vector<std::vector<uint32_t>> inorder_source_rowids;
        std::vector<std::vector<uint32_t>> inorder_upt_rowids;
        cut_rowids_in_order(each.second, &inorder_source_rowids, &inorder_upt_rowids);
        DCHECK(inorder_source_rowids.size() == inorder_upt_rowids.size());
        for (int i = 0; i < inorder_source_rowids.size(); i++) {
            auto tmp_chunk = ChunkHelper::new_chunk(partial_schema, inorder_upt_rowids[i].size());
            tmp_chunk->append_selective(*upt_chunk, inorder_upt_rowids[i].data(), 0, inorder_upt_rowids[i].size());
            RETURN_IF_EXCEPTION((*source_chunk)->update_rows(*tmp_chunk, inorder_source_rowids[i].data()));
        }
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
    auto char_field_indexes = ChunkHelper::get_char_field_indexes(schema);
    ChunkHelper::padding_char_columns(char_field_indexes, schema, tschema, chunk);
}

Status ColumnModePartialUpdateHandler::execute(const RowsetUpdateStateParams& params, MetaFileBuilder* builder) {
    TRACE_COUNTER_SCOPE_LATENCY_US("pcu_execute_us");
    // 1. load update state first
    RETURN_IF_ERROR(_load_update_state(params));

    const auto& txn_meta = params.op_write.txn_meta();

    std::vector<ColumnId> update_column_ids;
    std::vector<ColumnUID> unique_update_column_ids;
    for (ColumnId cid : txn_meta.partial_update_column_ids()) {
        if (cid >= params.tablet_schema->num_key_columns()) {
            update_column_ids.push_back(cid);
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
        if (!params.tablet_schema->column(cid).is_key()) {
            unique_update_column_ids.push_back(uid);
        }
    }

    DCHECK(update_column_ids.size() == unique_update_column_ids.size());
    const size_t BATCH_HANDLE_COLUMN_CNT = config::vertical_compaction_max_columns_per_group;

    // 2. getter all rss_rowid_to_update_rowid, and prepare .col writer by the way
    // rss_id -> update file id -> <rowid, update rowid>
    std::map<uint32_t, UptidToRowidPairs> rss_upt_id_to_rowid_pairs;
    for (int upt_id = 0; upt_id < _partial_update_states.size(); upt_id++) {
        for (const auto& each_rss : _partial_update_states[upt_id].rss_rowid_to_update_rowid) {
            for (const auto& each : each_rss.second) {
                rss_upt_id_to_rowid_pairs[each_rss.first][upt_id].emplace_back(each.first, each.second);
            }
            TRACE_COUNTER_INCREMENT("pcu_update_cnt", each_rss.second.size());
        }
        TRACE_COUNTER_INCREMENT("pcu_insert_rows", _partial_update_states[upt_id].insert_rowids.size());
    }
    _partial_update_states.clear();
    // must record unique column id in delta column group
    // dcg_column_ids and dcg_column_files are mapped one to the other. E.g.
    // {{1,2}, {3,4}} -> {"aaa.cols", "bbb.cols"}
    // It means column_1 and column_2 are stored in aaa.cols, and column_3 and column_4 are stored in bbb.cols
    std::map<uint32_t, std::vector<std::vector<ColumnUID>>> dcg_column_ids;
    std::map<uint32_t, std::vector<std::pair<std::string, std::string>>> dcg_column_file_with_encryption_metas;
    // 3. read from raw segment file and update file, and generate `.col` files one by one
    for (uint32_t col_index = 0; col_index < update_column_ids.size(); col_index += BATCH_HANDLE_COLUMN_CNT) {
        for (const auto& each : rss_upt_id_to_rowid_pairs) {
            // 3.1 build column id range
            std::vector<ColumnId> selective_update_column_ids =
                    append_fixed_batch(update_column_ids, col_index, BATCH_HANDLE_COLUMN_CNT);
            std::vector<ColumnUID> selective_unique_update_column_ids =
                    append_fixed_batch(unique_update_column_ids, col_index, BATCH_HANDLE_COLUMN_CNT);
            // 3.2 build partial schema and iterators
            auto partial_tschema =
                    TabletSchema::create_with_uid(params.tablet_schema, selective_unique_update_column_ids);
            Schema partial_schema = ChunkHelper::convert_schema(params.tablet_schema, selective_update_column_ids);
            // 3.3 read from source segment
            ASSIGN_OR_RETURN(auto source_chunk_ptr, _read_from_source_segment(params, partial_schema, each.first));
            const size_t source_chunk_size = source_chunk_ptr->memory_usage();
            _tracker->consume(source_chunk_size);
            DeferOp tracker_defer([&]() { _tracker->release(source_chunk_size); });
            // 3.2 read from update segment
            RETURN_IF_ERROR(_update_source_chunk_by_upt(each.second, partial_schema, &source_chunk_ptr));
            uint64_t segment_file_size = 0;
            uint64_t index_size = 0;
            uint64_t footer_position = 0;
            padding_char_columns(partial_schema, partial_tschema, source_chunk_ptr.get());
            ASSIGN_OR_RETURN(auto delta_column_group_writer,
                             _prepare_delta_column_group_writer(params, partial_tschema));
            {
                TRACE_COUNTER_SCOPE_LATENCY_US("pcu_finalize_dcg_us");
                RETURN_IF_ERROR(delta_column_group_writer->append_chunk(*source_chunk_ptr));
                RETURN_IF_ERROR(delta_column_group_writer->finalize(&segment_file_size, &index_size, &footer_position));
            }
            // 3.6 prepare column id list and dcg file list
            dcg_column_ids[each.first].push_back(selective_unique_update_column_ids);
            dcg_column_file_with_encryption_metas[each.first].emplace_back(
                    file_name(delta_column_group_writer->segment_path()), delta_column_group_writer->encryption_meta());
            TRACE_COUNTER_INCREMENT("pcu_handle_cnt", 1);
        }
    }
    // 4 generate delta columngroup
    for (const auto& each : rss_upt_id_to_rowid_pairs) {
        builder->append_dcg(each.first, dcg_column_file_with_encryption_metas[each.first], dcg_column_ids[each.first]);
    }
    builder->apply_column_mode_partial_update(params.op_write);

    TRACE_COUNTER_INCREMENT("pcu_rss_cnt", rss_upt_id_to_rowid_pairs.size());
    TRACE_COUNTER_INCREMENT("pcu_upt_cnt", _partial_update_states.size());
    TRACE_COUNTER_INCREMENT("pcu_column_cnt", update_column_ids.size());
    return Status::OK();
}

bool CompactionUpdateConflictChecker::conflict_check(const TxnLogPB_OpCompaction& op_compaction, int64_t txn_id,
                                                     const TabletMetadata& metadata, MetaFileBuilder* builder) {
    if (metadata.dcg_meta().dcgs().empty()) {
        return false;
    }
    std::unordered_set<uint32_t> input_rowsets; // all rowsets that have been compacted
    std::vector<uint32_t> input_segments;       // all segment that have been compacted
    for (uint32_t input_rowset : op_compaction.input_rowsets()) {
        input_rowsets.insert(input_rowset);
    }
    // 1. find all segments that have been compacted
    for (const auto& rowset : metadata.rowsets()) {
        if (input_rowsets.count(rowset.id()) > 0 && rowset.segments_size() > 0) {
            std::vector<int> temp(rowset.segments_size());
            std::iota(temp.begin(), temp.end(), rowset.id());
            input_segments.insert(input_segments.end(), temp.begin(), temp.end());
        }
    }
    // 2. find out if these segments have been updated
    for (uint32_t segment : input_segments) {
        auto dcg_ver_iter = metadata.dcg_meta().dcgs().find(segment);
        if (dcg_ver_iter != metadata.dcg_meta().dcgs().end()) {
            for (int64_t ver : dcg_ver_iter->second.versions()) {
                if (ver > op_compaction.compact_version()) {
                    // conflict happens
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

    return false;
}

} // namespace starrocks::lake