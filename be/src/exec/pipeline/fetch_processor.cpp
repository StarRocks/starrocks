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

#include "exec/pipeline/fetch_processor.h"

#include <algorithm>
#include <memory>
#include <mutex>
#include <vector>

#include "agent/master_info.h"
#include "column/chunk.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"
#include "common/config.h"
#include "common/global_types.h"
#include "common/logging.h"
#include "exec/pipeline/fetch_task.h"
#include "exec/sorting/sorting.h"
#include "exec/tablet_info.h"
#include "fmt/format.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/lookup_stream_mgr.h"
#include "serde/column_array_serde.h"
#include "util/brpc_stub_cache.h"
#include "util/defer_op.h"
#include "util/disposable_closure.h"
#include "util/runtime_profile.h"
#include "util/uuid_generator.h"

namespace starrocks::pipeline {


Status FetchProcessor::prepare(RuntimeState* state, RuntimeProfile* runtime_profile) {
    // @TODO we can move this to fetch_node prepare
    if (auto opt = get_backend_id(); opt.has_value()) {
        _local_be_id = opt.value();
    } else {
        return Status::InternalError("can't get local backend id");
    }

    runtime_profile->add_info_string("LookUpNode", std::to_string(_target_node_id));
    _build_row_id_chunk_timer = ADD_TIMER(runtime_profile, "BuildRowIdChunkTime");
    _gen_fetch_tasks_timer = ADD_TIMER(runtime_profile, "GenFetchTasksTime");
    _serialize_timer = ADD_TIMER(runtime_profile, "SerializeTime");
    _deserialize_timer = ADD_TIMER(runtime_profile, "DeserializeTime");
    _build_output_chunk_timer = ADD_TIMER(runtime_profile, "BuildOutputChunkTime");
    _rpc_count = ADD_COUNTER(runtime_profile, "RpcCount", TUnit::UNIT);
    _network_timer = ADD_TIMER(runtime_profile, "NetworkTime");
    _local_request_count = ADD_COUNTER(runtime_profile, "LocalRequestCount", TUnit::UNIT);
    _local_request_timer = ADD_TIMER(runtime_profile, "LocalRequestTime");

    return Status::OK();
}

bool FetchProcessor::need_input() const {
    if (_is_sink_complete) {
        return false;
    }

    std::shared_lock l(_queue_mu);
    if (_queue.size() < config::max_batch_num_per_fetch_operator) {
        return true;
    }
    return false;
}

bool FetchProcessor::has_output() const {
    if (!_get_io_task_status().ok()) {
        // trigger pull_chunk to return error
        return true;
    }
    std::shared_lock l(_queue_mu);
    if (!_queue.empty()) {
        auto& unit = _queue.front();
        if (unit->all_fetch_done()) {
            return true;
        }
    }

    return false;
}

bool FetchProcessor::is_finished() const {
    if (_is_sink_complete) {
        std::shared_lock l(_queue_mu);
        if (_queue.empty()) {
            // sink is complete and all data has been consumed
            return true;
        }
    }
    return false;
}

Status FetchProcessor::set_sink_finishing(RuntimeState* state) {
    std::unique_lock l(_queue_mu);
    VLOG_ROW << "[GLM] FetchProcessor::set_sink_finishing, " << (void*)this << ", queue size: " << _queue.size()
             << ", current unit: " << _current_unit->debug_string();
    if (!_current_unit->input_chunks.empty()) {
        VLOG_ROW << "[GLM] fetch res data after set_sink_finishing, " << (void*)this;
        RETURN_IF_ERROR(_fetch_data(state, _current_unit));
        _queue.push(_current_unit);
        _current_unit = std::make_shared<BatchUnit>();
    }
    _is_sink_complete = true;
    // for sink side, we don't need to wait for all data to be consumed

    return Status::OK();
}

Status FetchProcessor::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    std::unique_lock l(_queue_mu);
    // LOG(INFO) << "push chunk, chunk: " << chunk->debug_columns();
    _current_unit->input_chunks.push_back(chunk);
    if (_current_unit->input_chunks.size() < config::fetch_max_buffer_chunk_num) {
        return Status::OK();
    }
    RETURN_IF_ERROR(_fetch_data(state, _current_unit));

    _queue.push(_current_unit);
    _current_unit = std::make_shared<BatchUnit>();

    return Status::OK();
}

Status FetchProcessor::_fetch_data(RuntimeState* state, BatchUnitPtr& unit) {
    DCHECK(!unit->input_chunks.empty()) << "input chunk should not be empty";
    // generate request chunk
    ASSIGN_OR_RETURN(auto request_chunk, _build_request_chunk(state, unit));
    DLOG(INFO) << "request chunk: " << request_chunk->debug_columns();
    // generate fetch tasks
    RETURN_IF_ERROR(_gen_fetch_tasks(state, request_chunk, unit));
    DLOG(INFO) << "gen fetch tasks done, task num: " << unit->fetch_tasks.size();
    RETURN_IF_ERROR(_submit_fetch_tasks(state, unit));
    return Status::OK();
}

StatusOr<ChunkPtr> FetchProcessor::_build_request_chunk(RuntimeState* state, const BatchUnitPtr& unit) {
    SCOPED_TIMER(_build_row_id_chunk_timer);

    auto& input_chunks = unit->input_chunks;

    auto chunk = std::make_shared<Chunk>();

    auto position_column = UInt32Column::create();
    size_t total_rows = 0;
    std::for_each(input_chunks.begin(), input_chunks.end(),
                  [&total_rows](const ChunkPtr& chunk) { total_rows += chunk->num_rows(); });

    position_column->resize_uninitialized(total_rows);
    auto& position_data = position_column->get_data();
    for (size_t i = 0;i < total_rows; i++) {
        position_data[i] = i;
    }
    
    auto concat_column_func = [&] (SlotId slot_id) {
        auto col = input_chunks[0]->get_column_by_slot_id(slot_id)->clone_empty();
        col->reserve(total_rows);
        for (auto& input_chunk : input_chunks) {
            auto src_column = input_chunk->get_column_by_slot_id(slot_id);
            col->append(*src_column);
        }
        chunk->append_column(std::move(col), slot_id);
    };

    // concat each related column into one
    for (const auto& [_, row_pos_desc] : _row_pos_descs) {
        // source id column
        concat_column_func(row_pos_desc->get_row_source_slot_id());
        // ref columns
        for (const auto& slot_id : row_pos_desc->get_fetch_ref_slot_ids()) {
            concat_column_func(slot_id);
        }
    }
    chunk->append_column(std::move(position_column), kPositionColumnSlotId);
    chunk->check_or_die();

    return chunk;
}

StatusOr<FetchTaskPtr> FetchProcessor::_create_fetch_task(TupleId request_tuple_id, const RowPositionDescriptor* row_pos_desc, BatchUnitPtr unit, int32_t source_id, const ChunkPtr& request_chunk) {
    auto row_position_type = row_pos_desc->type();
    switch (row_position_type) {
        case RowPositionDescriptor::ICEBERG_V3: {
            auto task_ctx = std::make_shared<FetchTaskContext>();
            task_ctx->processor = this;
            task_ctx->unit = std::move(unit);
            task_ctx->request_tuple_id = request_tuple_id;
            task_ctx->source_node_id = source_id;
            task_ctx->request_chunk = std::move(request_chunk);
            return std::make_shared<IcebergFetchTask>(std::move(task_ctx));
        }
        default:
            return Status::InternalError(fmt::format("Unknown row position type: {}", row_position_type));
    }
}

Status FetchProcessor::_gen_fetch_tasks(RuntimeState* state, const ChunkPtr& request_chunk, BatchUnitPtr& unit) {
    SCOPED_TIMER(_gen_fetch_tasks_timer);
    DLOG(INFO) << "gen fetch tasks, request chunk: " << request_chunk->debug_columns();
    auto* fetch_tasks = &(unit->fetch_tasks);
    auto* null_position_columns = &(unit->missing_positions);
    for (const auto& [tuple_id, row_pos_desc] : _row_pos_descs) {
        DLOG(INFO) << "process tuple_id: " << tuple_id << ", row_pos_desc: " << row_pos_desc->debug_string();
        auto source_slot_id = row_pos_desc->get_row_source_slot_id();
        auto tmp_chunk = std::make_shared<Chunk>();

        ColumnPtr col = request_chunk->get_column_by_slot_id(source_slot_id);
        DLOG(INFO) << "source_slot_id: " << source_slot_id << ", col: " << col->debug_string();

        Int32Column::Ptr source_id_column;
        ColumnPtr position_column;

        // 1. find all null position, remove null rows
        if (col->is_nullable() && col->has_null()) {
            auto nullable_column = NullableColumn::static_pointer_cast(col);
            size_t null_rows = nullable_column->null_count();
            size_t not_null_rows = nullable_column->size() - null_rows;
            if (not_null_rows == 0) {
                // all rows are null
                null_position_columns->emplace(source_slot_id,
                                               request_chunk->get_column_by_slot_id(kPositionColumnSlotId)->clone());
                DLOG(INFO) << "all rows are null, tuple_id: " << tuple_id << ", row_pos_desc: " << row_pos_desc->debug_string();
                continue;
            }
            std::vector<uint32_t> not_null_indices;
            not_null_indices.reserve(not_null_rows);
            ColumnPtr null_row_positions = UInt32Column::create();
            null_row_positions->reserve(null_rows);
            auto& null_row_positions_data = UInt32Column::static_pointer_cast(null_row_positions)->get_data();

            const auto& null_data = nullable_column->null_column_data();
            for (size_t i = 0; i < nullable_column->size(); i++) {
                if (null_data[i] == 0) {
                    not_null_indices.push_back(i);
                } else {
                    null_row_positions_data.push_back(i);
                }
            }
            DCHECK_EQ(not_null_rows, not_null_indices.size()) << "not null rows not match, expected: " << not_null_rows
                                                              << ", actual: " << not_null_indices.size();

            null_position_columns->emplace(source_slot_id, std::move(null_row_positions));

            // only keep non-null row
            source_id_column = Int32Column::create();
            position_column = UInt32Column::create();
            const auto& data_column = nullable_column->data_column();
            source_id_column->append_selective(*data_column, not_null_indices.data(), 0, not_null_rows);
            const auto& pos_column = request_chunk->get_column_by_slot_id(kPositionColumnSlotId);
            position_column->append_selective(*pos_column, not_null_indices.data(), 0, not_null_rows);
            for (const auto& slot_id : row_pos_desc->get_fetch_ref_slot_ids()) {
                auto src_col = request_chunk->get_column_by_slot_id(slot_id);
                auto dst_col = src_col->clone_empty();
                dst_col->append_selective(*src_col, not_null_indices.data(), 0, not_null_rows);
                tmp_chunk->append_column(std::move(dst_col), slot_id);
            }
            
            // append selective all
        } else {
            source_id_column = Int32Column::static_pointer_cast(ColumnHelper::get_data_column(col));
            position_column = request_chunk->get_column_by_slot_id(kPositionColumnSlotId);
            for (const auto& slot_id : row_pos_desc->get_fetch_ref_slot_ids()) {
                auto src_col = request_chunk->get_column_by_slot_id(slot_id);
                tmp_chunk->append_column(std::move(src_col), slot_id);
            }
        }

        tmp_chunk->append_column(source_id_column, source_slot_id);
        tmp_chunk->append_column(position_column, kPositionColumnSlotId);
        tmp_chunk->check_or_die();
        DLOG(INFO) << "tuple_id: " << tuple_id << ", tmp_chunk: " << tmp_chunk->debug_columns();

        // 2. partition by source_id
        ASSIGN_OR_RETURN(auto sorted_chunk, _sort_chunk(state, tmp_chunk, {source_id_column}));

        source_id_column = Int32Column::static_pointer_cast(
            sorted_chunk->get_column_by_slot_id(source_slot_id));
        position_column = sorted_chunk->get_column_by_slot_id(kPositionColumnSlotId);

        const auto& source_ids = Int32Column::static_pointer_cast(source_id_column)->get_data();

        auto iter = source_ids.begin();
        while (iter != source_ids.end()) {
            int32_t cur_source_id = *iter;
            auto range = std::equal_range(iter, source_ids.end(), cur_source_id);
            size_t start = std::distance(source_ids.begin(), range.first);
            size_t end = std::distance(source_ids.begin(), range.second);
            size_t num_rows = end - start;

            // build new request chunk
            auto new_request_chunk = std::make_shared<Chunk>();
            
            // @TODO we don't need source_id column in request chunk
            auto new_source_id_column = Int32Column::create();
            new_source_id_column->reserve(num_rows);
            new_source_id_column->append(*source_id_column, start, num_rows);
            new_request_chunk->append_column(std::move(new_source_id_column), source_slot_id);

            auto new_position_column = UInt32Column::create();
            new_position_column->reserve(num_rows);
            new_position_column->append(*position_column, start, num_rows);
            new_request_chunk->append_column(std::move(new_position_column), kPositionColumnSlotId);

            // @TODO
            // copy all related column
            // @TODO should we use lookup_ref_slot_ids?
            for (const auto& slot_id : row_pos_desc->get_fetch_ref_slot_ids()) {
                auto src_col = sorted_chunk->get_column_by_slot_id(slot_id);
                auto new_col = src_col->clone_empty();
                new_col->reserve(num_rows);
                new_col->append(*src_col, start, num_rows);
                new_request_chunk->append_column(std::move(new_col), slot_id);
            }

            ASSIGN_OR_RETURN(auto fetch_task,
                _create_fetch_task(tuple_id, row_pos_desc, unit, cur_source_id, new_request_chunk));
            // add new fetch task
            // fetch_tasks->emplace_back(std::move(fetch_task));
            if (!fetch_tasks->contains(tuple_id)) {
                fetch_tasks->emplace(tuple_id, std::make_shared<std::vector<FetchTaskPtr>>());
            }
            fetch_tasks->at(tuple_id)->emplace_back(std::move(fetch_task));
            unit->total_request_num += 1;
            DLOG(INFO) << "add fetch task, task num: " << unit->total_request_num;

            iter = range.second;
        }
    }        
    return Status::OK();
}

Status FetchProcessor::_submit_fetch_tasks(RuntimeState* state, const BatchUnitPtr& unit) {
    for (const auto& [tuple_id, fetch_tasks] : unit->fetch_tasks) {
        for (const auto& fetch_task : *fetch_tasks) {
            RETURN_IF_ERROR(fetch_task->submit(state));
        }
    }
    return Status::OK();
}


StatusOr<ChunkPtr> FetchProcessor::_sort_chunk(RuntimeState* state, const ChunkPtr& chunk,
                                               const Columns& order_by_columns) {
    // @TODO(silverbullet233): reuse sort descs
    SortDescs sort_descs;
    sort_descs.descs.reserve(order_by_columns.size());
    for (size_t i = 0; i < order_by_columns.size(); i++) {
        sort_descs.descs.emplace_back(true, true);
    }
    _permutation.resize(0);

    RETURN_IF_ERROR(sort_and_tie_columns(state->cancelled_ref(), order_by_columns, sort_descs, &_permutation));
    auto sorted_chunk = chunk->clone_empty_with_slot(chunk->num_rows());
    materialize_by_permutation(sorted_chunk.get(), {chunk}, _permutation);

    return sorted_chunk;
}

StatusOr<ChunkPtr> FetchProcessor::_get_output_chunk(RuntimeState* state) {
    std::unique_lock l(_queue_mu);
    if (!_queue.empty()) {
        auto& unit = _queue.front();
        DCHECK(unit->all_fetch_done()) << "all fetch should be done";
        if (!unit->build_output_done) {
            RETURN_IF_ERROR(_build_output_chunk(state, unit));
        }
        DCHECK_LT(unit->next_output_idx, unit->input_chunks.size())
                << "next_output_idx should be less than input chunk num";
        auto chunk = unit->input_chunks[unit->next_output_idx++];
        if (unit->next_output_idx == unit->input_chunks.size()) {
            // no output, move to next
            _queue.pop();
        }

        return chunk;
    }
    return nullptr;
}

StatusOr<ChunkPtr> FetchProcessor::pull_chunk(RuntimeState* state) {
    RETURN_IF_ERROR(_get_io_task_status());

    ASSIGN_OR_RETURN(auto chunk, _get_output_chunk(state));

    return chunk;
}

Status FetchProcessor::_build_output_chunk(RuntimeState* state, const BatchUnitPtr& unit) {
    DLOG(INFO) << "build output chunk, unit: " << unit->debug_string();
    SCOPED_TIMER(_build_output_chunk_timer);
    const auto& all_fetch_tasks = unit->fetch_tasks;
    const auto& input_chunks = unit->input_chunks;
    const auto& missing_positions = unit->missing_positions;

    for (const auto& [tuple_id, row_pos_desc] : _row_pos_descs) {
        DLOG(INFO) << "build output chunk, tuple_id: " << tuple_id << ", row_pos_desc: " << row_pos_desc->debug_string();
        auto chunk = std::make_shared<Chunk>();
        const auto& tuple_desc = state->desc_tbl().get_tuple_descriptor(tuple_id);
        DLOG(INFO) << "tuple_desc: " << tuple_desc->debug_string();
        ColumnPtr position_column = UInt32Column::create();
        std::vector<SlotDescriptor*> slots;
        {
            for (const auto& slot : tuple_desc->slots()) {
                bool ignore = std::any_of(row_pos_desc->get_lookup_ref_slot_ids().begin(), 
                                          row_pos_desc->get_lookup_ref_slot_ids().end(),
                                          [slot](SlotId slot_id) { return slot_id == slot->id(); }) ||
                             std::any_of(row_pos_desc->get_fetch_ref_slot_ids().begin(), 
                                          row_pos_desc->get_fetch_ref_slot_ids().end(),
                                          [slot](SlotId slot_id) { return slot_id == slot->id(); });
                if (!ignore) {
                    slots.emplace_back(slot);
                }
            }
        }

        for (const auto& slot : slots) {
            // @TODO should ignore ref slots...
            auto column = ColumnHelper::create_column(slot->type(), slot->is_nullable());
            chunk->append_column(std::move(column), slot->id());
        }
        const auto& fetch_tasks = all_fetch_tasks.at(tuple_id);
        for (const auto& fetch_task : *fetch_tasks) {
            auto ctx = fetch_task->get_ctx();
            auto partial_pos_col = ctx->request_chunk->get_column_by_slot_id(kPositionColumnSlotId);
            position_column->append(*partial_pos_col);


            for (const auto& slot : slots) {
                // @TODO
                DCHECK(ctx->response_columns.contains(slot->id())) << "response columns should contains slot: " << slot->debug_string();
                auto partial_column = ctx->response_columns.at(slot->id());
                chunk->get_column_by_slot_id(slot->id())->append(*partial_column);
            }
        }
        // chunk->append_column(std::move(position_column), kPositionColumnSlotId);
        chunk->check_or_die();
        DLOG(INFO) << "chunk: " << chunk->debug_columns();
        if (missing_positions.contains(tuple_id)) {
            auto& null_positions = missing_positions.at(tuple_id);
            position_column->append(*null_positions, 0, null_positions->size());
            for (const auto& slot : tuple_desc->slots()) {
                auto dst_column = chunk->get_column_by_slot_id(slot->id());
                DCHECK(dst_column->is_nullable()) << "slot: " << slot->debug_string() << " should be nullable";
                dst_column->append_nulls(null_positions->size());
            }
            DCHECK_EQ(chunk->num_rows(), position_column->size())
                    << "chunk num rows: " << chunk->num_rows() << ", position column size: " << position_column->size();
        }

        // re-sort data by position
        ASSIGN_OR_RETURN(auto sorted_chunk, _sort_chunk(state, chunk, {position_column}));
        DLOG(INFO) << "sorted chunk: " << sorted_chunk->debug_columns();

        for (const auto& slot : slots) {
            size_t offset = 0;
            auto src_column = sorted_chunk->get_column_by_slot_id(slot->id());
            DLOG(INFO) << "slot id: " << slot->id() << ", src column: " << src_column->debug_string();
            for (const auto& input_chunk : input_chunks) {
                size_t num_rows = input_chunk->num_rows();
                auto dst_column = src_column->clone_empty();
                dst_column->reserve(num_rows);
                dst_column->append(*src_column, offset, num_rows);
                input_chunk->append_column(std::move(dst_column), slot->id());
                input_chunk->check_or_die();
                offset += num_rows;
            }
        }
    }
    unit->build_output_done = true;

    return Status::OK();
}

FetchProcessorFactory::FetchProcessorFactory(int32_t target_node_id, phmap::flat_hash_map<TupleId, RowPositionDescriptor*> row_pos_descs,
                                             phmap::flat_hash_map<SlotId, SlotDescriptor*> slot_id_to_desc,
                                             std::shared_ptr<StarRocksNodesInfo> nodes_info,
                                             std::shared_ptr<LookUpDispatcher> local_dispatcher)
        : _target_node_id(target_node_id),
          _row_pos_descs(std::move(row_pos_descs)),
          _slot_id_to_desc(std::move(slot_id_to_desc)),
          _nodes_info(std::move(nodes_info)),
          _local_dispatcher(std::move(local_dispatcher)) {}

FetchProcessorPtr FetchProcessorFactory::get_or_create(int32_t driver_sequence) {
    if (!_processor_map.contains(driver_sequence)) {
        _processor_map.try_emplace(driver_sequence,
                                   std::make_shared<FetchProcessor>(_target_node_id, _row_pos_descs, _slot_id_to_desc,
                                                                    _nodes_info, _local_dispatcher));
    }
    return _processor_map.at(driver_sequence);
}
} // namespace starrocks::pipeline