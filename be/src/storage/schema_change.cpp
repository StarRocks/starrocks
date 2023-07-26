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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/schema_change.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "storage/schema_change.h"

#include <csignal>
#include <memory>
#include <utility>
#include <vector>

#include "exec/sorting/sorting.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "gutil/strings/substitute.h"
#include "runtime/current_thread.h"
#include "runtime/mem_pool.h"
#include "runtime/runtime_state.h"
#include "storage/chunk_aggregator.h"
#include "storage/convert_helper.h"
#include "storage/memtable.h"
#include "storage/memtable_rowset_writer_sink.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_id_generator.h"
#include "storage/storage_engine.h"
#include "storage/tablet.h"
#include "storage/tablet_manager.h"
#include "storage/tablet_meta_manager.h"
#include "storage/tablet_updates.h"
#include "util/unaligned_access.h"

namespace starrocks {

using ChunkRow = std::pair<size_t, Chunk*>;

int compare_chunk_row(const ChunkRow& lhs, const ChunkRow& rhs) {
    for (uint16_t i = 0; i < lhs.second->schema()->num_key_fields(); ++i) {
        int res = lhs.second->get_column_by_index(i)->compare_at(lhs.first, rhs.first,
                                                                 *rhs.second->get_column_by_index(i), -1);
        if (res != 0) {
            return res;
        }
    }
    return 0;
}

// TODO: optimize it with vertical sort
class ChunkMerger {
public:
    explicit ChunkMerger(TabletSharedPtr tablet);
    virtual ~ChunkMerger();

    Status merge(std::vector<ChunkPtr>& chunk_arr, RowsetWriter* rowset_writer);
    static void aggregate_chunk(ChunkAggregator& aggregator, ChunkPtr& chunk, RowsetWriter* rowset_writer);

private:
    struct MergeElement {
        bool operator<(const MergeElement& other) const {
            return compare_chunk_row(std::make_pair(row_index, chunk), std::make_pair(other.row_index, other.chunk)) >
                   0;
        }

        Chunk* chunk;
        size_t row_index;
    };

    bool _make_heap(std::vector<ChunkPtr>& chunk_arr);
    void _pop_heap();

    TabletSharedPtr _tablet;
    std::priority_queue<MergeElement> _heap;
    std::unique_ptr<ChunkAggregator> _aggregator;
};

bool ChunkSorter::sort(ChunkPtr& chunk, const TabletSharedPtr& new_tablet) {
    Schema new_schema = ChunkHelper::convert_schema(new_tablet->tablet_schema());
    if (_swap_chunk == nullptr || _max_allocated_rows < chunk->num_rows()) {
        _swap_chunk = ChunkHelper::new_chunk(new_schema, chunk->num_rows());
        if (_swap_chunk == nullptr) {
            LOG(WARNING) << "allocate swap chunk for sort failed";
            return false;
        }
        _max_allocated_rows = chunk->num_rows();
    }

    _swap_chunk->reset();

    std::vector<ColumnId> sort_key_idxes;
    if (new_schema.sort_key_idxes().empty()) {
        int num_key_columns = chunk->schema()->num_key_fields();
        for (ColumnId i = 0; i < num_key_columns; ++i) {
            sort_key_idxes.push_back(i);
        }
    } else {
        sort_key_idxes = new_schema.sort_key_idxes();
    }
    Columns key_columns;
    for (const auto sort_key_idx : sort_key_idxes) {
        key_columns.push_back(chunk->get_column_by_index(sort_key_idx));
    }

    SmallPermutation perm = create_small_permutation(chunk->num_rows());
    Status st =
            stable_sort_and_tie_columns(false, key_columns, SortDescs::asc_null_first(sort_key_idxes.size()), &perm);
    CHECK(st.ok());
    std::vector<uint32_t> selective;
    permutate_to_selective(perm, &selective);
    _swap_chunk = chunk->clone_empty_with_schema();
    _swap_chunk->append_selective(*chunk, selective.data(), 0, chunk->num_rows());

    chunk->swap_chunk(*_swap_chunk);
    return true;
}

ChunkMerger::ChunkMerger(TabletSharedPtr tablet) : _tablet(std::move(tablet)), _aggregator(nullptr) {}

ChunkMerger::~ChunkMerger() {
    if (_aggregator != nullptr) {
        _aggregator->close();
    }
}

void ChunkMerger::aggregate_chunk(ChunkAggregator& aggregator, ChunkPtr& chunk, RowsetWriter* rowset_writer) {
    aggregator.aggregate();
    while (aggregator.is_finish()) {
        (void)rowset_writer->add_chunk(*aggregator.aggregate_result());
        aggregator.aggregate_reset();
        aggregator.aggregate();
    }

    DCHECK(aggregator.source_exhausted());
    aggregator.update_source(chunk);
    aggregator.aggregate();

    while (aggregator.is_finish()) {
        (void)rowset_writer->add_chunk(*aggregator.aggregate_result());
        aggregator.aggregate_reset();
        aggregator.aggregate();
    }
}

Status ChunkMerger::merge(std::vector<ChunkPtr>& chunk_arr, RowsetWriter* rowset_writer) {
    auto process_err = [this] {
        VLOG(3) << "merge chunk failed";
        while (!_heap.empty()) {
            _heap.pop();
        }
    };

    _make_heap(chunk_arr);
    size_t nread = 0;
    Schema new_schema = ChunkHelper::convert_schema(_tablet->tablet_schema());
    ChunkPtr tmp_chunk = ChunkHelper::new_chunk(new_schema, config::vector_chunk_size);
    if (_tablet->keys_type() == KeysType::AGG_KEYS) {
        _aggregator = std::make_unique<ChunkAggregator>(&new_schema, config::vector_chunk_size, 0);
    }

    StorageEngine* storage_engine = StorageEngine::instance();
    bool bg_worker_stopped = storage_engine->bg_worker_stopped();
    while (!_heap.empty() && !bg_worker_stopped) {
        if (tmp_chunk->capacity_limit_reached() || nread >= config::vector_chunk_size) {
            if (_tablet->keys_type() == KeysType::AGG_KEYS) {
                aggregate_chunk(*_aggregator, tmp_chunk, rowset_writer);
            } else {
                (void)rowset_writer->add_chunk(*tmp_chunk);
            }
            tmp_chunk->reset();
            nread = 0;
        }

        tmp_chunk->append(*_heap.top().chunk, _heap.top().row_index, 1);
        nread += 1;
        _pop_heap();
        bg_worker_stopped = StorageEngine::instance()->bg_worker_stopped();
    }

    if (bg_worker_stopped) {
        return Status::InternalError("back ground worker stopped, BE maybe exit");
    }

    if (_tablet->keys_type() == KeysType::AGG_KEYS) {
        aggregate_chunk(*_aggregator, tmp_chunk, rowset_writer);
        if (_aggregator->has_aggregate_data()) {
            _aggregator->aggregate();
            (void)rowset_writer->add_chunk(*_aggregator->aggregate_result());
        }
    } else {
        (void)rowset_writer->add_chunk(*tmp_chunk);
    }

    if (auto st = rowset_writer->flush(); !st.ok()) {
        LOG(WARNING) << "failed to finalizing writer: " << st;
        process_err();
        return st;
    }

    return Status::OK();
}

bool ChunkMerger::_make_heap(std::vector<ChunkPtr>& chunk_arr) {
    for (const auto& chunk : chunk_arr) {
        MergeElement element;
        element.chunk = chunk.get();
        element.row_index = 0;

        _heap.push(element);
    }

    return true;
}

void ChunkMerger::_pop_heap() {
    MergeElement element = _heap.top();
    _heap.pop();

    if (++element.row_index >= element.chunk->num_rows()) {
        return;
    }

    _heap.push(element);
}

Status LinkedSchemaChange::process(TabletReader* reader, RowsetWriter* new_rowset_writer, TabletSharedPtr new_tablet,
                                   TabletSharedPtr base_tablet, RowsetSharedPtr rowset) {
#ifndef BE_TEST
    Status st = CurrentThread::mem_tracker()->check_mem_limit("LinkedSchemaChange");
    if (!st.ok()) {
        LOG(WARNING) << alter_msg_header() << "fail to execute schema change: " << st.message() << std::endl;
        return st;
    }
#endif

    Status status =
            new_rowset_writer->add_rowset_for_linked_schema_change(rowset, _chunk_changer->get_schema_mapping());
    if (!status.ok()) {
        LOG(WARNING) << alter_msg_header() << "fail to convert rowset."
                     << ", new_tablet=" << new_tablet->full_name() << ", base_tablet=" << base_tablet->full_name()
                     << ", version=" << new_rowset_writer->version();
        return status;
    }
    return Status::OK();
}

Status LinkedSchemaChange::generate_delta_column_group_and_cols(const Tablet* new_tablet, const Tablet* base_tablet,
                                                                const RowsetSharedPtr& src_rowset, RowsetId rid,
                                                                int64_t version, ChunkChanger* chunk_changer,
                                                                DeltaColumnGroupList& dcgs,
                                                                std::vector<int> last_dcg_counts) {
    // This function is just used for adding generated column if src_rowset
    // contain some segment files
    bool no_segment_file = (last_dcg_counts.size() == 0);
    if (chunk_changer->get_mc_exprs()->size() == 0 || no_segment_file) {
        return Status::OK();
    }

    // Get read schema chunk and new partial schema(schema with new added generated column only) chunk
    std::vector<uint32_t> new_columns_ids;
    std::vector<uint32_t> all_ref_columns_ids;
    std::set<uint32_t> s_ref_columns_ids;
    for (const auto& iter : *chunk_changer->get_mc_exprs()) {
        new_columns_ids.emplace_back(iter.first);
        // get read schema only through associated column in all generate columns' expr
        Expr* root = (iter.second)->root();
        std::vector<SlotId> slot_ids;
        root->get_slot_ids(&slot_ids);

        s_ref_columns_ids.insert(slot_ids.begin(), slot_ids.end());
    }
    all_ref_columns_ids.resize(s_ref_columns_ids.size());
    all_ref_columns_ids.assign(s_ref_columns_ids.begin(), s_ref_columns_ids.end());
    std::sort(all_ref_columns_ids.begin(), all_ref_columns_ids.end());
    std::sort(new_columns_ids.begin(), new_columns_ids.end());

    // If all expression is constant, all_ref_columns_ids will be empty.
    // we just append 0 into it to construct the read schema for simplicity.
    if (all_ref_columns_ids.size() == 0) {
        all_ref_columns_ids.emplace_back(0);
    }

    Schema read_schema = ChunkHelper::convert_schema(base_tablet->tablet_schema(), all_ref_columns_ids);
    ChunkPtr read_chunk = ChunkHelper::new_chunk(read_schema, config::vector_chunk_size);

    Schema new_schema = ChunkHelper::convert_schema(new_tablet->tablet_schema(), new_columns_ids);
    ChunkPtr new_chunk = ChunkHelper::new_chunk(new_schema, config::vector_chunk_size);

    OlapReaderStatistics stats;
    RowsetReleaseGuard guard(src_rowset->shared_from_this());
    auto res = src_rowset->get_segment_iterators2(read_schema, nullptr, version, &stats,
                                                  base_tablet->data_dir()->get_meta());
    if (!res.ok()) {
        return res.status();
    }

    auto seg_iterators = res.value();

    // Fetch the new columns value into the new_chunk
    for (int idx = 0; idx < seg_iterators.size(); ++idx) {
        auto seg_iterator = seg_iterators[idx];
        if (seg_iterator.get() == nullptr) {
            std::stringstream ss;
            ss << "Failed to get segment iterator, segment id: " << idx;
            LOG(WARNING) << ss.str();
            continue;
        }

        new_chunk->reset();

        while (true) {
            read_chunk->reset();
            Status status = seg_iterator->get_next(read_chunk.get());
            if (!status.ok()) {
                if (status.is_end_of_file()) {
                    break;
                } else {
                    std::stringstream ss;
                    ss << "segment iterator failed to get next chunk, status is:" << status.to_string();
                    LOG(WARNING) << ss.str();
                    return Status::InternalError(ss.str());
                }
            }
            status = chunk_changer->append_materialized_columns(read_chunk, new_chunk, all_ref_columns_ids,
                                                                base_tablet->tablet_schema().num_columns());
            if (!status.ok()) {
                LOG(WARNING) << "failed to append materialized columns";
                return Status::InternalError("failed to append materialized columns");
            }
        }

        // Write cols file with current new_chunk
        ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(new_tablet->schema_hash_path()));
        const std::string path = Rowset::delta_column_group_path(new_tablet->schema_hash_path(), rid, idx, version,
                                                                 last_dcg_counts[idx]);
        // must record unique column id in delta column group
        std::vector<uint32_t> unique_column_ids;
        for (const auto& iter : *chunk_changer->get_mc_exprs()) {
            ColumnUID unique_id = new_tablet->tablet_schema().column(iter.first).unique_id();
            unique_column_ids.emplace_back(unique_id);
        }
        std::sort(unique_column_ids.begin(), unique_column_ids.end());
        auto cols_file_schema = TabletSchema::create_with_uid(new_tablet->tablet_schema(), unique_column_ids);

        (void)fs->delete_file(path); // delete .cols if already exist
        WritableFileOptions opts{.sync_on_close = true};
        ASSIGN_OR_RETURN(auto wfile, fs->new_writable_file(opts, path));
        SegmentWriterOptions writer_options;
        auto segment_writer =
                std::make_unique<SegmentWriter>(std::move(wfile), idx, cols_file_schema.get(), writer_options);
        RETURN_IF_ERROR(segment_writer->init(false));

        uint64_t segment_file_size = 0;
        uint64_t index_size = 0;
        uint64_t footer_position = 0;
        RETURN_IF_ERROR(segment_writer->append_chunk(*new_chunk));
        RETURN_IF_ERROR(segment_writer->finalize(&segment_file_size, &index_size, &footer_position));

        // abort schema change if cols file' size is larger than max_segment_file_size
        // It is nearly impossible happen unless the expression result of the generated column
        // is extremely large or there are too many generated column to added.
        if (UNLIKELY(segment_file_size > config::max_segment_file_size)) {
            (void)fs->delete_file(path);
            std::stringstream ss;
            ss << "cols file' size is larger than max_segment_file_size: " << config::max_segment_file_size;
            LOG(WARNING) << ss.str();
            return Status::InternalError(ss.str());
        }

        // Get DeltaColumnGroup for current cols file
        auto dcg = std::make_shared<DeltaColumnGroup>();
        std::vector<std::vector<uint32_t>> dcg_column_ids{unique_column_ids};
        std::vector<std::string> dcg_column_files{file_name(segment_writer->segment_path())};
        dcg->init(version, dcg_column_ids, dcg_column_files);
        dcgs.emplace_back(dcg);
    }

    return Status::OK();
}

Status SchemaChangeDirectly::process(TabletReader* reader, RowsetWriter* new_rowset_writer, TabletSharedPtr new_tablet,
                                     TabletSharedPtr base_tablet, RowsetSharedPtr rowset) {
    Schema base_schema =
            ChunkHelper::convert_schema(base_tablet->tablet_schema(), _chunk_changer->get_selected_column_indexes());
    ChunkPtr base_chunk = ChunkHelper::new_chunk(base_schema, config::vector_chunk_size);
    Schema new_schema = ChunkHelper::convert_schema(new_tablet->tablet_schema());
    auto char_field_indexes = ChunkHelper::get_char_field_indexes(new_schema);

    ChunkPtr new_chunk = ChunkHelper::new_chunk(new_schema, config::vector_chunk_size);

    std::unique_ptr<MemPool> mem_pool(new MemPool());
    do {
        Status st;
        bool bg_worker_stopped = StorageEngine::instance()->bg_worker_stopped();
        if (bg_worker_stopped) {
            return Status::InternalError(alter_msg_header() + "bg_worker_stopped");
        }
#ifndef BE_TEST
        st = CurrentThread::mem_tracker()->check_mem_limit("DirectSchemaChange");
        if (!st.ok()) {
            LOG(WARNING) << alter_msg_header() << "fail to execute schema change: " << st.message() << std::endl;
            return st;
        }
#endif
        st = reader->do_get_next(base_chunk.get());

        if (!st.ok()) {
            if (st.is_end_of_file()) {
                break;
            } else {
                LOG(WARNING) << alter_msg_header()
                             << "tablet reader failed to get next chunk, status: " << st.get_error_msg();
                return st;
            }
        }
        if (!_chunk_changer->change_chunk_v2(base_chunk, new_chunk, base_schema, new_schema, mem_pool.get())) {
            std::string err_msg = strings::Substitute("failed to convert chunk data. base tablet:$0, new tablet:$1",
                                                      base_tablet->tablet_id(), new_tablet->tablet_id());
            LOG(WARNING) << alter_msg_header() + err_msg;
            return Status::InternalError(alter_msg_header() + err_msg);
        }

        if (auto st = _chunk_changer->fill_materialized_columns(new_chunk); !st.ok()) {
            LOG(WARNING) << alter_msg_header() << "fill materialized columns failed: " << st.get_error_msg();
            return st;
        }

        ChunkHelper::padding_char_columns(char_field_indexes, new_schema, new_tablet->tablet_schema(), new_chunk.get());

        if (st = new_rowset_writer->add_chunk(*new_chunk); !st.ok()) {
            std::string err_msg = strings::Substitute(
                    "failed to execute schema change. base tablet:$0, new_tablet:$1. err msg: failed to add chunk to "
                    "rowset writer: $2",
                    base_tablet->tablet_id(), new_tablet->tablet_id(), st.get_error_msg());
            LOG(WARNING) << alter_msg_header() << err_msg;
            return Status::InternalError(alter_msg_header() + err_msg);
        }
        base_chunk->reset();
        new_chunk->reset();
        mem_pool->clear();
    } while (base_chunk->num_rows() == 0);

    if (base_chunk->num_rows() != 0) {
        if (!_chunk_changer->change_chunk_v2(base_chunk, new_chunk, base_schema, new_schema, mem_pool.get())) {
            std::string err_msg = strings::Substitute("failed to convert chunk data. base tablet:$0, new tablet:$1",
                                                      base_tablet->tablet_id(), new_tablet->tablet_id());
            LOG(WARNING) << alter_msg_header() << err_msg;
            return Status::InternalError(alter_msg_header() + err_msg);
        }
        if (auto st = _chunk_changer->fill_materialized_columns(new_chunk); !st.ok()) {
            LOG(WARNING) << alter_msg_header() << "fill materialized columns failed: " << st.get_error_msg();
            return st;
        }
        if (auto st = new_rowset_writer->add_chunk(*new_chunk); !st.ok()) {
            LOG(WARNING) << alter_msg_header() << "rowset writer add chunk failed: " << st;
            return st;
        }
    }

    if (auto st = new_rowset_writer->flush(); !st.ok()) {
        LOG(WARNING) << alter_msg_header() << "failed to flush rowset writer: " << st;
        return st;
    }

    return Status::OK();
}

SchemaChangeWithSorting::SchemaChangeWithSorting(ChunkChanger* chunk_changer, size_t memory_limitation)
        : SchemaChange(), _chunk_changer(chunk_changer), _memory_limitation(memory_limitation) {}

Status SchemaChangeWithSorting::process(TabletReader* reader, RowsetWriter* new_rowset_writer,
                                        TabletSharedPtr new_tablet, TabletSharedPtr base_tablet,
                                        RowsetSharedPtr rowset) {
    MemTableRowsetWriterSink mem_table_sink(new_rowset_writer);
    Schema base_schema =
            ChunkHelper::convert_schema(base_tablet->tablet_schema(), _chunk_changer->get_selected_column_indexes());
    Schema new_schema = ChunkHelper::convert_schema(new_tablet->tablet_schema());
    auto char_field_indexes = ChunkHelper::get_char_field_indexes(new_schema);

    // memtable max buffer size set default 80% of memory limit so that it will do _merge() if reach limit
    // set max memtable size to 4G since some column has limit size, it will make invalid data
    size_t max_buffer_size = std::min<size_t>(
            4294967296, static_cast<size_t>(_memory_limitation * config::memory_ratio_for_sorting_schema_change));
    auto mem_table = std::make_unique<MemTable>(new_tablet->tablet_id(), &new_schema, &mem_table_sink, max_buffer_size,
                                                CurrentThread::mem_tracker());

    auto selective = std::make_unique<std::vector<uint32_t>>();
    selective->resize(config::vector_chunk_size);
    for (uint32_t i = 0; i < config::vector_chunk_size; i++) {
        (*selective)[i] = i;
    }

    std::unique_ptr<MemPool> mem_pool(new MemPool());

    StorageEngine* storage_engine = StorageEngine::instance();
    bool bg_worker_stopped = storage_engine->bg_worker_stopped();
    while (!bg_worker_stopped) {
#ifndef BE_TEST
        auto cur_usage = CurrentThread::mem_tracker()->consumption();
        // we check memory usage exceeds 90% since tablet reader use some memory
        // it will return fail if memory is exhausted
        if (cur_usage > CurrentThread::mem_tracker()->limit() * 0.9) {
            RETURN_IF_ERROR_WITH_WARN(mem_table->finalize(), alter_msg_header() + "failed to finalize mem table");
            RETURN_IF_ERROR_WITH_WARN(mem_table->flush(), alter_msg_header() + "failed to flush mem table");
            mem_table = std::make_unique<MemTable>(new_tablet->tablet_id(), &new_schema, &mem_table_sink,
                                                   max_buffer_size, CurrentThread::mem_tracker());
            VLOG(1) << alter_msg_header() << "SortSchemaChange memory usage: " << cur_usage << " after mem table flush "
                    << CurrentThread::mem_tracker()->consumption();
        }
#endif
        ChunkPtr base_chunk = ChunkHelper::new_chunk(base_schema, config::vector_chunk_size);
        Status status = reader->do_get_next(base_chunk.get());
        if (!status.ok()) {
            if (!status.is_end_of_file()) {
                LOG(WARNING) << alter_msg_header() << "failed to get next chunk, status is:" << status.to_string();
                return status;
            } else if (base_chunk->num_rows() <= 0) {
                break;
            }
        }

        ChunkPtr new_chunk = ChunkHelper::new_chunk(new_schema, base_chunk->num_rows());

        if (!_chunk_changer->change_chunk_v2(base_chunk, new_chunk, base_schema, new_schema, mem_pool.get())) {
            std::string err_msg = strings::Substitute("failed to convert chunk data. base tablet:$0, new tablet:$1",
                                                      base_tablet->tablet_id(), new_tablet->tablet_id());
            LOG(WARNING) << alter_msg_header() << err_msg;
            return Status::InternalError(alter_msg_header() + err_msg);
        }

        ChunkHelper::padding_char_columns(char_field_indexes, new_schema, new_tablet->tablet_schema(), new_chunk.get());

        bool full = mem_table->insert(*new_chunk, selective->data(), 0, new_chunk->num_rows());
        if (full) {
            RETURN_IF_ERROR_WITH_WARN(mem_table->finalize(), alter_msg_header() + "failed to finalize mem table");
            RETURN_IF_ERROR_WITH_WARN(mem_table->flush(), alter_msg_header() + "failed to flush mem table");
            mem_table = std::make_unique<MemTable>(new_tablet->tablet_id(), &new_schema, &mem_table_sink,
                                                   max_buffer_size, CurrentThread::mem_tracker());
        }

        mem_pool->clear();
        bg_worker_stopped = storage_engine->bg_worker_stopped();
    }

    RETURN_IF_ERROR_WITH_WARN(mem_table->finalize(), alter_msg_header() + "failed to finalize mem table");
    RETURN_IF_ERROR_WITH_WARN(mem_table->flush(), alter_msg_header() + "failed to flush mem table");

    if (bg_worker_stopped) {
        return Status::InternalError(alter_msg_header() + "bg_worker_stopped");
    }

    if (auto st = new_rowset_writer->flush(); !st.ok()) {
        LOG(WARNING) << alter_msg_header() << "failed to flush rowset writer: " << st;
        return st;
    }

    return Status::OK();
}

Status SchemaChangeWithSorting::_internal_sorting(std::vector<ChunkPtr>& chunk_arr, RowsetWriter* new_rowset_writer,
                                                  TabletSharedPtr tablet) {
    if (chunk_arr.size() == 1) {
        Status st;
        if (st = new_rowset_writer->add_chunk(*chunk_arr[0]); !st.ok()) {
            LOG(WARNING) << "failed to add chunk: " << st;
            return st;
        }
        if (st = new_rowset_writer->flush(); !st.ok()) {
            LOG(WARNING) << "failed to finalizing writer: " << st;
        }
        return st;
    }

    ChunkMerger merger(std::move(tablet));
    if (auto st = merger.merge(chunk_arr, new_rowset_writer); !st.ok()) {
        LOG(WARNING) << "merge chunk arr failed";
        return st;
    }

    return Status::OK();
}

Status SchemaChangeHandler::process_alter_tablet_v2(const TAlterTabletReqV2& request) {
    LOG(INFO) << _alter_msg_header << "begin to do request alter tablet: base_tablet_id=" << request.base_tablet_id
              << ", base_schema_hash=" << request.base_schema_hash << ", new_tablet_id=" << request.new_tablet_id
              << ", new_schema_hash=" << request.new_schema_hash << ", alter_version=" << request.alter_version;

    MonotonicStopWatch timer;
    timer.start();

    // Lock schema_change_lock util schema change info is stored in tablet header
    if (!StorageEngine::instance()->tablet_manager()->try_schema_change_lock(request.base_tablet_id)) {
        LOG(WARNING) << _alter_msg_header << "failed to obtain schema change lock. "
                     << "base_tablet=" << request.base_tablet_id;
        return Status::InternalError(_alter_msg_header + "failed to obtain schema change lock");
    }

    DeferOp release_lock(
            [&] { StorageEngine::instance()->tablet_manager()->release_schema_change_lock(request.base_tablet_id); });

    Status status = _do_process_alter_tablet_v2(request);
    LOG(INFO) << _alter_msg_header << "finished alter tablet process, status=" << status.to_string()
              << " duration: " << timer.elapsed_time() / 1000000
              << "ms, peak_mem_usage: " << CurrentThread::mem_tracker()->peak_consumption() << " bytes";
    return status;
}

Status SchemaChangeHandler::_do_process_alter_tablet_v2(const TAlterTabletReqV2& request) {
    TabletSharedPtr base_tablet = StorageEngine::instance()->tablet_manager()->get_tablet(request.base_tablet_id);
    if (base_tablet == nullptr) {
        LOG(WARNING) << _alter_msg_header << "fail to find base tablet. base_tablet=" << request.base_tablet_id
                     << ", base_schema_hash=" << request.base_schema_hash;
        return Status::InternalError("failed to find base tablet");
    }

    // new tablet has to exist
    TabletSharedPtr new_tablet = StorageEngine::instance()->tablet_manager()->get_tablet(request.new_tablet_id);
    if (new_tablet == nullptr) {
        LOG(WARNING) << _alter_msg_header << "fail to find new tablet."
                     << " new_tablet=" << request.new_tablet_id << ", new_schema_hash=" << request.new_schema_hash;
        return Status::InternalError(_alter_msg_header + "failed to find new tablet");
    }

    // check if tablet's state is not_ready, if it is ready, it means the tablet already finished
    // check whether the tablet's max continuous version == request.version
    if (new_tablet->tablet_state() != TABLET_NOTREADY) {
        Status st = _validate_alter_result(new_tablet, request);
        LOG(INFO) << _alter_msg_header << "tablet's state=" << new_tablet->tablet_state()
                  << " the convert job already finished, check its version"
                  << " res=" << st.to_string();
        return st;
    }

    LOG(INFO) << _alter_msg_header
              << "finish to validate alter tablet request. begin to convert data from base tablet to new tablet"
              << " base_tablet=" << base_tablet->full_name() << " new_tablet=" << new_tablet->full_name();

    std::shared_lock base_migration_rlock(base_tablet->get_migration_lock(), std::try_to_lock);
    if (!base_migration_rlock.owns_lock()) {
        return Status::InternalError(_alter_msg_header + "base tablet get migration r_lock failed");
    }
    if (Tablet::check_migrate(base_tablet)) {
        return Status::InternalError(
                strings::Substitute(_alter_msg_header + "tablet $0 is doing disk balance", base_tablet->tablet_id()));
    }
    std::shared_lock new_migration_rlock(new_tablet->get_migration_lock(), std::try_to_lock);
    if (!new_migration_rlock.owns_lock()) {
        return Status::InternalError(_alter_msg_header + "new tablet get migration r_lock failed");
    }
    if (Tablet::check_migrate(new_tablet)) {
        return Status::InternalError(_alter_msg_header +
                                     strings::Substitute("tablet $0 is doing disk balance", new_tablet->tablet_id()));
    }

    SchemaChangeParams sc_params;
    sc_params.base_tablet = base_tablet;
    sc_params.new_tablet = new_tablet;
    sc_params.chunk_changer = std::make_unique<ChunkChanger>(sc_params.new_tablet->tablet_schema());

    if (request.__isset.materialized_view_params && request.materialized_view_params.size() > 0) {
        if (!request.__isset.query_options || !request.__isset.query_globals) {
            return Status::InternalError(_alter_msg_header +
                                         "change materialized view but query_options/query_globals is not set");
        }
        sc_params.chunk_changer->init_runtime_state(request.query_options, request.query_globals);
    }

    // materialized column index in new schema
    std::unordered_set<int> materialized_column_idxs;
    if (request.materialized_column_req.mc_exprs.size() != 0) {
        for (auto it : request.materialized_column_req.mc_exprs) {
            materialized_column_idxs.insert(it.first);
        }
    }

    // primary key do not support materialized view, initialize materialized_params_map here,
    // just for later column_mapping of _parse_request.
    SchemaChangeUtils::init_materialized_params(request, &sc_params.materialized_params_map);
    Status status = SchemaChangeUtils::parse_request(base_tablet->tablet_schema(), new_tablet->tablet_schema(),
                                                     sc_params.chunk_changer.get(), sc_params.materialized_params_map,
                                                     !base_tablet->delete_predicates().empty(), &sc_params.sc_sorting,
                                                     &sc_params.sc_directly, &materialized_column_idxs);

    if (!status.ok()) {
        LOG(WARNING) << _alter_msg_header << "failed to parse the request. res=" << status.get_error_msg();
        return status;
    }

    if (request.__isset.materialized_column_req && request.materialized_column_req.mc_exprs.size() != 0) {
        // Currently, a schema change task for materialized column is just
        // ADD/DROP/MODIFY a single materialized column, so it is impossible
        // that sc_sorting == true, for materialized column can not be a KEY.
        DCHECK_EQ(sc_params.sc_sorting, false);

        sc_params.chunk_changer->init_runtime_state(request.materialized_column_req.query_options,
                                                    request.materialized_column_req.query_globals);

        for (auto it : request.materialized_column_req.mc_exprs) {
            ExprContext* ctx = nullptr;
            RETURN_IF_ERROR(Expr::create_expr_tree(sc_params.chunk_changer->get_object_pool(), it.second, &ctx,
                                                   sc_params.chunk_changer->get_runtime_state()));
            RETURN_IF_ERROR(ctx->prepare(sc_params.chunk_changer->get_runtime_state()));
            RETURN_IF_ERROR(ctx->open(sc_params.chunk_changer->get_runtime_state()));

            sc_params.chunk_changer->get_mc_exprs()->insert({it.first, ctx});
        }
    }

    if (base_tablet->keys_type() == KeysType::PRIMARY_KEYS) {
        const auto& base_sort_key_idxes = base_tablet->tablet_schema().sort_key_idxes();
        const auto& new_sort_key_idxes = new_tablet->tablet_schema().sort_key_idxes();
        std::vector<int32_t> base_sort_key_unique_ids;
        std::vector<int32_t> new_sort_key_unique_ids;
        for (auto idx : base_sort_key_idxes) {
            base_sort_key_unique_ids.emplace_back(base_tablet->tablet_schema().column(idx).unique_id());
        }
        for (auto idx : new_sort_key_idxes) {
            new_sort_key_unique_ids.emplace_back(new_tablet->tablet_schema().column(idx).unique_id());
        }
        if (std::mismatch(new_sort_key_unique_ids.begin(), new_sort_key_unique_ids.end(),
                          base_sort_key_unique_ids.begin())
                    .first != new_sort_key_unique_ids.end()) {
            sc_params.sc_directly = !(sc_params.sc_sorting = true);
        }
        if (sc_params.sc_directly) {
            status = new_tablet->updates()->convert_from(base_tablet, request.alter_version,
                                                         sc_params.chunk_changer.get(), _alter_msg_header);
        } else if (sc_params.sc_sorting) {
            status = new_tablet->updates()->reorder_from(base_tablet, request.alter_version,
                                                         sc_params.chunk_changer.get(), _alter_msg_header);
        } else {
            status = new_tablet->updates()->link_from(base_tablet.get(), request.alter_version,
                                                      sc_params.chunk_changer.get(), _alter_msg_header);
        }
        if (!status.ok()) {
            LOG(WARNING) << _alter_msg_header << "schema change new tablet load snapshot error: " << status.to_string();
            return status;
        }
        return Status::OK();
    } else {
        return _do_process_alter_tablet_v2_normal(request, sc_params, base_tablet, new_tablet);
    }
}

Status SchemaChangeHandler::_do_process_alter_tablet_v2_normal(const TAlterTabletReqV2& request,
                                                               SchemaChangeParams& sc_params,
                                                               const TabletSharedPtr& base_tablet,
                                                               const TabletSharedPtr& new_tablet) {
    // begin to find deltas to convert from base tablet to new tablet so that
    // obtain base tablet and new tablet's push lock and header write lock to prevent loading data
    RowsetSharedPtr max_rowset;
    std::vector<RowsetSharedPtr> rowsets_to_change;
    int32_t end_version = -1;
    Status status;
    std::vector<std::unique_ptr<TabletReader>> readers;
    {
        std::lock_guard l1(base_tablet->get_push_lock());
        std::lock_guard l2(new_tablet->get_push_lock());
        std::shared_lock l3(base_tablet->get_header_lock());
        std::lock_guard l4(new_tablet->get_header_lock());

        std::vector<Version> versions_to_be_changed;
        status = _get_versions_to_be_changed(base_tablet, &versions_to_be_changed);
        if (!status.ok()) {
            LOG(WARNING) << _alter_msg_header << "fail to get version to be changed. status: " << status;
            return status;
        }
        VLOG(3) << "versions to be changed size:" << versions_to_be_changed.size();

        Schema base_schema;
        base_schema = ChunkHelper::convert_schema(base_tablet->tablet_schema(),
                                                  sc_params.chunk_changer->get_selected_column_indexes());

        for (auto& version : versions_to_be_changed) {
            rowsets_to_change.push_back(base_tablet->get_rowset_by_version(version));
            if (rowsets_to_change.back() == nullptr) {
                std::vector<Version> base_tablet_versions;
                base_tablet->list_versions(&base_tablet_versions);
                std::stringstream ss;
                ss << " rs_version_map: ";
                for (auto& ver : base_tablet_versions) {
                    ss << ver << ",";
                }
                ss << " versions_to_be_changed: ";
                for (auto& ver : versions_to_be_changed) {
                    ss << ver << ",";
                }
                LOG(WARNING) << _alter_msg_header << "fail to get rowset by version: " << version << ". " << ss.str();
                return Status::InternalError(_alter_msg_header + "fail to get rowset by version");
            }
            // prepare tablet reader to prevent rowsets being compacted
            std::unique_ptr<TabletReader> tablet_reader =
                    std::make_unique<TabletReader>(base_tablet, version, base_schema);
            RETURN_IF_ERROR(tablet_reader->prepare());
            readers.emplace_back(std::move(tablet_reader));
        }
        VLOG(3) << "rowsets_to_change size is:" << rowsets_to_change.size();

        Version max_version = base_tablet->max_version();
        max_rowset = base_tablet->rowset_with_max_version();
        if (max_rowset == nullptr || max_version.second < request.alter_version) {
            LOG(WARNING) << _alter_msg_header
                         << "base tablet's max version=" << (max_rowset == nullptr ? 0 : max_rowset->end_version())
                         << " is less than request version=" << request.alter_version;
            return Status::InternalError(_alter_msg_header + "base tablet's max version is less than request version");
        }

        LOG(INFO) << _alter_msg_header << "begin to remove all data from new tablet to prevent rewrite."
                  << " new_tablet=" << new_tablet->full_name();
        std::vector<RowsetSharedPtr> rowsets_to_delete;
        std::vector<Version> new_tablet_versions;
        new_tablet->list_versions(&new_tablet_versions);
        for (auto& version : new_tablet_versions) {
            if (version.second <= max_rowset->end_version()) {
                rowsets_to_delete.push_back(new_tablet->get_rowset_by_version(version));
            }
        }
        VLOG(3) << "rowsets_to_delete size is:" << rowsets_to_delete.size()
                << " version is:" << max_rowset->end_version();
        new_tablet->modify_rowsets(std::vector<RowsetSharedPtr>(), rowsets_to_delete, nullptr);
        new_tablet->set_cumulative_layer_point(-1);
        new_tablet->save_meta();
        for (auto& rowset : rowsets_to_delete) {
            // do not call rowset.remove directly, using gc thread to delete it
            StorageEngine::instance()->add_unused_rowset(rowset);
        }

        // init one delete handler
        for (auto& version : versions_to_be_changed) {
            if (version.second > end_version) {
                end_version = version.second;
            }
        }
    }

    Version delete_predicates_version(0, max_rowset->version().second);
    TabletReaderParams read_params;
    read_params.reader_type = ReaderType::READER_ALTER_TABLE;
    read_params.skip_aggregation = false;
    read_params.chunk_size = config::vector_chunk_size;

    // open tablet readers out of lock for open is heavy because of io
    for (auto& tablet_reader : readers) {
        tablet_reader->set_delete_predicates_version(delete_predicates_version);
        RETURN_IF_ERROR(tablet_reader->open(read_params));
    }

    sc_params.rowset_readers = std::move(readers);
    sc_params.version = Version(0, end_version);
    sc_params.rowsets_to_change = rowsets_to_change;

    status = _convert_historical_rowsets(sc_params);
    if (!status.ok()) {
        return status;
    }

    Status res = Status::OK();
    {
        // set state to ready
        std::unique_lock new_wlock(new_tablet->get_header_lock());
        res = new_tablet->set_tablet_state(TabletState::TABLET_RUNNING);
        if (!res.ok()) {
            LOG(WARNING) << _alter_msg_header << "failed to alter tablet. base_tablet=" << base_tablet->full_name()
                         << ", drop new_tablet=" << new_tablet->full_name();
            // do not drop the new tablet and its data. GC thread will
            return res;
        }
        // link schema change will not generate low cardinality dict for new column
        // so that we need disable shortchut compaction make sure dict will generate by further compaction
        if (!sc_params.sc_directly && !sc_params.sc_sorting) {
            new_tablet->tablet_meta()->set_enable_shortcut_compaction(false);
        }
        new_tablet->save_meta();
    }

    // _validate_alter_result should be outside the above while loop.
    // to avoid requiring the header lock twice.
    status = _validate_alter_result(new_tablet, request);
    if (!status.ok()) {
        LOG(WARNING) << _alter_msg_header << "failed to alter tablet. base_tablet=" << base_tablet->full_name()
                     << ", drop new_tablet=" << new_tablet->full_name();
        // do not drop the new tablet and its data. GC thread will
        return status;
    }
    LOG(INFO) << _alter_msg_header << "success to alter tablet. base_tablet=" << base_tablet->full_name();
    return Status::OK();
}

Status SchemaChangeHandler::_get_versions_to_be_changed(const TabletSharedPtr& base_tablet,
                                                        std::vector<Version>* versions_to_be_changed) {
    RowsetSharedPtr rowset = base_tablet->rowset_with_max_version();
    if (rowset == nullptr) {
        LOG(WARNING) << _alter_msg_header << "Tablet has no version. base_tablet=" << base_tablet->full_name();
        return Status::InternalError(_alter_msg_header + "tablet alter version does not exists");
    }
    std::vector<Version> span_versions;
    if (!base_tablet->capture_consistent_versions(Version(0, rowset->version().second), &span_versions).ok()) {
        return Status::InternalError(_alter_msg_header + "capture consistent versions failed");
    }
    versions_to_be_changed->insert(std::end(*versions_to_be_changed), std::begin(span_versions),
                                   std::end(span_versions));
    return Status::OK();
}

Status SchemaChangeHandler::_convert_historical_rowsets(SchemaChangeParams& sc_params) {
    LOG(INFO) << _alter_msg_header << "begin to convert historical rowsets for new_tablet from base_tablet."
              << " base_tablet=" << sc_params.base_tablet->full_name()
              << ", new_tablet=" << sc_params.new_tablet->full_name();
    DeferOp save_meta([&sc_params] {
        std::unique_lock new_wlock(sc_params.new_tablet->get_header_lock());
        sc_params.new_tablet->save_meta();
    });

    std::unique_ptr<SchemaChange> sc_procedure;
    auto chunk_changer = sc_params.chunk_changer.get();
    if (sc_params.sc_sorting) {
        LOG(INFO) << _alter_msg_header << "doing schema change with sorting for base_tablet "
                  << sc_params.base_tablet->full_name();
        size_t memory_limitation =
                static_cast<size_t>(config::memory_limitation_per_thread_for_schema_change) * 1024 * 1024 * 1024;
        sc_procedure = std::make_unique<SchemaChangeWithSorting>(chunk_changer, memory_limitation);
    } else if (sc_params.sc_directly) {
        LOG(INFO) << _alter_msg_header << "doing directly schema change for base_tablet "
                  << sc_params.base_tablet->full_name();
        sc_procedure = std::make_unique<SchemaChangeDirectly>(chunk_changer);
    } else {
        LOG(INFO) << _alter_msg_header << "doing linked schema change for base_tablet "
                  << sc_params.base_tablet->full_name();
        sc_procedure = std::make_unique<LinkedSchemaChange>(chunk_changer);
    }

    if (sc_procedure == nullptr) {
        LOG(WARNING) << _alter_msg_header << "failed to malloc SchemaChange. "
                     << "malloc_size=" << sizeof(SchemaChangeWithSorting);
        return Status::InternalError(_alter_msg_header + "failed to malloc SchemaChange");
    }
    sc_procedure->set_alter_msg_header(_alter_msg_header);

    Status status;
    std::vector<std::vector<DeltaColumnGroupList>> all_historical_dcgs;
    std::vector<RowsetId> new_rowset_ids;

    for (int i = 0; i < sc_params.rowset_readers.size(); ++i) {
        VLOG(3) << "begin to convert a history rowset. version=" << sc_params.rowsets_to_change[i]->version();

        TabletSharedPtr new_tablet = sc_params.new_tablet;
        TabletSharedPtr base_tablet = sc_params.base_tablet;
        RowsetWriterContext writer_context;
        writer_context.rowset_id = StorageEngine::instance()->next_rowset_id();
        writer_context.tablet_uid = new_tablet->tablet_uid();
        writer_context.tablet_id = new_tablet->tablet_id();
        writer_context.partition_id = new_tablet->partition_id();
        writer_context.tablet_schema_hash = new_tablet->schema_hash();
        writer_context.rowset_path_prefix = new_tablet->schema_hash_path();
        writer_context.tablet_schema = &new_tablet->tablet_schema();
        writer_context.rowset_state = VISIBLE;
        writer_context.version = sc_params.rowsets_to_change[i]->version();
        writer_context.segments_overlap = sc_params.rowsets_to_change[i]->rowset_meta()->segments_overlap();

        if (sc_params.sc_sorting) {
            writer_context.schema_change_sorting = true;
        }

        std::unique_ptr<RowsetWriter> rowset_writer;
        status = RowsetFactory::create_rowset_writer(writer_context, &rowset_writer);
        if (!status.ok()) {
            return Status::InternalError(_alter_msg_header + "build rowset writer failed");
        }

        auto st = sc_procedure->process(sc_params.rowset_readers[i].get(), rowset_writer.get(), new_tablet, base_tablet,
                                        sc_params.rowsets_to_change[i]);
        if (!st.ok()) {
            LOG(WARNING) << _alter_msg_header << "failed to process the schema change. from tablet "
                         << base_tablet->get_tablet_info().to_string() << " to tablet "
                         << new_tablet->get_tablet_info().to_string() << " version=" << sc_params.version.first << "-"
                         << sc_params.version.second << " error " << st;
            return st;
        }
        sc_params.rowset_readers[i]->close();
        auto new_rowset = rowset_writer->build();
        if (!new_rowset.ok()) {
            LOG(WARNING) << _alter_msg_header << "failed to build rowset: " << new_rowset.status()
                         << ". exit alter process";
            break;
        }
        if (sc_params.rowsets_to_change[i]->rowset_meta()->has_delete_predicate()) {
            (*new_rowset)
                    ->mutable_delete_predicate()
                    ->CopyFrom(sc_params.rowsets_to_change[i]->rowset_meta()->delete_predicate());
        }
        status = sc_params.new_tablet->add_rowset(*new_rowset, false);
        if (status.is_already_exist()) {
            LOG(WARNING) << _alter_msg_header << "version already exist, version revert occurred. "
                         << "tablet=" << sc_params.new_tablet->full_name() << ", version='" << sc_params.version.first
                         << "-" << sc_params.version.second;
            StorageEngine::instance()->add_unused_rowset(*new_rowset);
            status = Status::OK();
        } else if (!status.ok()) {
            LOG(WARNING) << _alter_msg_header << "failed to register new version. "
                         << " tablet=" << sc_params.new_tablet->full_name() << ", version=" << sc_params.version.first
                         << "-" << sc_params.version.second;
            StorageEngine::instance()->add_unused_rowset(*new_rowset);
            break;
        } else {
            VLOG(3) << "register new version. tablet=" << sc_params.new_tablet->full_name()
                    << ", version=" << sc_params.version.first << "-" << sc_params.version.second;
        }

        std::vector<DeltaColumnGroupList> historical_dcgs;
        historical_dcgs.resize(sc_params.rowsets_to_change[i]->num_segments());
        for (uint32_t j = 0; j < sc_params.rowsets_to_change[i]->num_segments(); j++) {
            int64_t tablet_id = sc_params.rowsets_to_change[i]->rowset_meta()->tablet_id();
            RowsetId rowsetid = sc_params.rowsets_to_change[i]->rowset_meta()->rowset_id();
            RETURN_IF_ERROR(TabletMetaManager::get_delta_column_group(new_tablet->data_dir()->get_meta(), tablet_id,
                                                                      rowsetid, j, INT64_MAX, &historical_dcgs[j]));
        }
        if (!sc_params.sc_sorting && !sc_params.sc_directly && chunk_changer->get_mc_exprs()->size() != 0) {
            // new added dcgs info for every segment in rowset.
            DeltaColumnGroupList dcgs;
            std::vector<int> last_dcg_counts;
            for (uint32_t j = 0; j < sc_params.rowsets_to_change[i]->num_segments(); j++) {
                // check the lastest historical_dcgs version if it is equal to schema change version
                // of the rowset. If it is, we should merge the dcg info.
                last_dcg_counts.emplace_back((historical_dcgs[j].size() != 0 &&
                                              historical_dcgs[j].front()->version() == sc_params.version.second)
                                                     ? historical_dcgs[j].front()->relative_column_files().size()
                                                     : 0);
            }

            RETURN_IF_ERROR(LinkedSchemaChange::generate_delta_column_group_and_cols(
                    new_tablet.get(), base_tablet.get(), sc_params.rowsets_to_change[i], (*new_rowset)->rowset_id(),
                    sc_params.version.second, chunk_changer, dcgs, last_dcg_counts));

            // merge dcg info if necessary
            if (dcgs.size() != 0) {
                if (dcgs.size() != sc_params.rowsets_to_change[i]->num_segments()) {
                    std::stringstream ss;
                    ss << "The size of dcgs and segment file in src rowset is different, "
                       << "base tablet id: " << base_tablet->tablet_id() << " "
                       << "new tablet id: " << new_tablet->tablet_id();
                    LOG(WARNING) << ss.str();
                    return Status::InternalError(ss.str());
                }
                for (uint32_t j = 0; j < dcgs.size(); j++) {
                    if (dcgs[j]->merge_into_by_version(historical_dcgs[j], new_tablet->schema_hash_path(),
                                                       (*new_rowset)->rowset_id(), j) == 0) {
                        // In this case, historical_dcgs[j] contain no suitable dcg:
                        // 1. no version of dcg in historical_dcgs[j] satisfy the src rowset version.
                        // 2. historical_dcgs[j] is empty
                        // So nothing can be merged, and we should just insert the dcgs[j] into historical_dcgs
                        historical_dcgs[j].insert(historical_dcgs[j].begin(), dcgs[j]); /* reverse order by version */
                    }
                }
                (*new_rowset)->rowset_meta()->set_partial_schema_change(true);
            }
        }
        all_historical_dcgs.emplace_back(historical_dcgs);
        new_rowset_ids.emplace_back((*new_rowset)->rowset_meta()->rowset_id());

        VLOG(10) << "succeed to convert a history version."
                 << " version=" << sc_params.version.first << "-" << sc_params.version.second;
    }

    auto data_dir = sc_params.new_tablet->data_dir();
    rocksdb::WriteBatch wb;
    RETURN_IF_ERROR(TabletMetaManager::clear_delta_column_group(data_dir, &wb, sc_params.new_tablet->tablet_id()));

    // for sorting and directly mode, cols files has been compacted even they exist before schema change.
    if (!sc_params.sc_sorting && !sc_params.sc_directly) {
        for (int i = 0; i < sc_params.rowsets_to_change.size(); ++i) {
            for (uint32_t j = 0; j < sc_params.rowsets_to_change[i]->num_segments(); j++) {
                RETURN_IF_ERROR(
                        TabletMetaManager::put_delta_column_group(data_dir, &wb, sc_params.new_tablet->tablet_id(),
                                                                  new_rowset_ids[i], j, all_historical_dcgs[i][j]));
            }
        }
    }

    status = sc_params.new_tablet->data_dir()->get_meta()->write_batch(&wb);
    if (!status.ok()) {
        LOG(WARNING) << "Fail to delete old dcg and write new dcg" << sc_params.new_tablet->tablet_id() << ": "
                     << status;
        return Status::InternalError("Fail to delete old dcg and write new dcg");
    }

    if (status.ok()) {
        status = sc_params.new_tablet->check_version_integrity(sc_params.version);
    }

    LOG(INFO) << _alter_msg_header << "finish converting rowsets for new_tablet from base_tablet. "
              << "base_tablet=" << sc_params.base_tablet->full_name()
              << ", new_tablet=" << sc_params.new_tablet->full_name() << ", status is " << status.to_string();

    return status;
}

Status SchemaChangeHandler::_validate_alter_result(const TabletSharedPtr& new_tablet,
                                                   const TAlterTabletReqV2& request) {
    int64_t max_continuous_version = new_tablet->max_continuous_version();
    LOG(INFO) << _alter_msg_header << "find max continuous version of tablet=" << new_tablet->full_name()
              << ", version=" << max_continuous_version;
    if (max_continuous_version >= request.alter_version) {
        return Status::OK();
    } else {
        return Status::InternalError(_alter_msg_header + "version missed");
    }
}

} // namespace starrocks
