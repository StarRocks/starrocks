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

#include <memory>
#include <mutex>

#include "agent/master_info.h"
#include "column/chunk.h"
#include "column/fixed_length_column.h"
#include "common/config.h"
#include "common/logging.h"
#include "exec/sorting/sorting.h"
#include "exec/tablet_info.h"
#include "fmt/format.h"
#include "runtime/exec_env.h"
#include "runtime/lookup_stream_mgr.h"
#include "serde/column_array_serde.h"
#include "util/brpc_stub_cache.h"
#include "util/defer_op.h"
#include "util/disposable_closure.h"
#include "util/runtime_profile.h"
#include "util/uuid_generator.h"

namespace starrocks::pipeline {

std::string FetchProcessor::BatchUnit::debug_string() const {
    return fmt::format(
            "BatchUnit {}, input_chunks: {}, total_request_num: {}, finished_request_num: {}, "
            "next_output_idx: {}, build_output_done: {}",
            (void*)this, input_chunks.size(), total_request_num, finished_request_num.load(), next_output_idx,
            build_output_done);
}

Status FetchProcessor::prepare(RuntimeState* state, RuntimeProfile* runtime_profile) {
    // @TODO we can move this to fetch_node prepare
    if (auto opt = get_backend_id(); opt.has_value()) {
        _local_be_id = opt.value();
    } else {
        return Status::InternalError("can't get local backend id");
    }

    runtime_profile->add_info_string("LookUpNode", std::to_string(_target_node_id));
    _build_row_id_chunk_timer = ADD_TIMER(runtime_profile, "BuildRowIdChunkTime");
    _gen_request_chunk_timer = ADD_TIMER(runtime_profile, "GenRequestChunkTime");
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
    // gen row id chunk
    ASSIGN_OR_RETURN(auto row_id_chunk, _build_row_id_chunk(state, unit));
    // gen request chunk
    phmap::flat_hash_map<uint32_t, RequestColumnsPtr> request_chunks;
    RETURN_IF_ERROR(_gen_request_chunks(state, row_id_chunk, &request_chunks, &(unit->missing_positions)));
    RETURN_IF_ERROR(_send_fetch_request(state, unit, request_chunks));
    return Status::OK();
}

StatusOr<ChunkPtr> FetchProcessor::_build_row_id_chunk(RuntimeState* state, const BatchUnitPtr& unit) {
    SCOPED_TIMER(_build_row_id_chunk_timer);

    auto& input_chunks = unit->input_chunks;

    auto chunk = std::make_shared<Chunk>();

    auto position_column = UInt32Column::create();
    size_t total_rows = 0;
    std::for_each(input_chunks.begin(), input_chunks.end(),
                  [&total_rows](const ChunkPtr& chunk) { total_rows += chunk->num_rows(); });

    position_column->resize_uninitialized(total_rows);
    auto& position_data = position_column->get_data();
    for (size_t i = 0, idx = 0; i < input_chunks.size(); i++) {
        const auto& chunk = input_chunks[i];
        for (size_t j = 0; j < chunk->num_rows(); j++, idx++) {
            position_data[idx] = idx;
        }
    }

    // concat row_id_columns into one
    for (const auto& [_, slot_id] : _row_id_slots) {
        ColumnPtr row_id_column = input_chunks[0]->get_column_by_slot_id(slot_id)->clone_empty();
        row_id_column->reserve(total_rows);

        for (size_t i = 0; i < input_chunks.size(); i++) {
            auto src_column = input_chunks[i]->get_column_by_slot_id(slot_id);
            row_id_column->append(*src_column);
        }

        chunk->append_column(std::move(row_id_column), slot_id);
    }
    chunk->append_column(std::move(position_column), kPositionColumnSlotId);
    chunk->check_or_die();

    return chunk;
}

Status FetchProcessor::_gen_request_chunks(RuntimeState* state, const ChunkPtr& row_id_chunk,
                                           phmap::flat_hash_map<uint32_t, RequestColumnsPtr>* request_chunks,
                                           phmap::flat_hash_map<uint32_t, ColumnPtr>* null_position_columns) {
    SCOPED_TIMER(_gen_request_chunk_timer);

    for (const auto& [tuple_id, slot_id] : _row_id_slots) {
        auto tmp_chunk = std::make_shared<Chunk>();
        auto col = row_id_chunk->get_column_by_slot_id(slot_id);
        RowIdColumn::Ptr row_id_column;
        ColumnPtr position_column;
        if (col->is_nullable() && col->has_null()) {
            auto nullable_column = NullableColumn::static_pointer_cast(col);
            size_t null_rows = nullable_column->null_count();
            size_t not_null_rows = nullable_column->size() - null_rows;
            if (not_null_rows == 0) {
                null_position_columns->emplace(slot_id,
                                               row_id_chunk->get_column_by_slot_id(kPositionColumnSlotId)->clone());
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

            null_position_columns->emplace(slot_id, std::move(null_row_positions));

            // only keep non-null row
            row_id_column = RowIdColumn::create();
            position_column = UInt32Column::create();
            const auto& data_column = nullable_column->data_column();
            row_id_column->append_selective(*data_column, not_null_indices.data(), 0, not_null_rows);
            const auto& pos_column = row_id_chunk->get_column_by_slot_id(kPositionColumnSlotId);
            position_column->append_selective(*pos_column, not_null_indices.data(), 0, not_null_rows);
        } else {
            row_id_column = RowIdColumn::static_pointer_cast(ColumnHelper::get_data_column(col));
            position_column = row_id_chunk->get_column_by_slot_id(kPositionColumnSlotId);
        }

        tmp_chunk->append_column(row_id_column, slot_id);
        tmp_chunk->append_column(position_column, kPositionColumnSlotId);
        tmp_chunk->check_or_die();

        ASSIGN_OR_RETURN(auto sorted_chunk, _sort_chunk(state, tmp_chunk, {row_id_column->be_ids_column()}));

        row_id_column = RowIdColumn::static_pointer_cast(sorted_chunk->get_column_by_slot_id(slot_id));
        position_column = sorted_chunk->get_column_by_slot_id(kPositionColumnSlotId);

        const auto& be_ids = UInt32Column::static_pointer_cast(row_id_column->be_ids_column())->get_data();

        auto iter = be_ids.begin();
        while (iter != be_ids.end()) {
            uint32_t cur_be_id = *iter;
            auto range = std::equal_range(iter, be_ids.end(), cur_be_id);
            size_t start = std::distance(be_ids.begin(), range.first);
            size_t end = std::distance(be_ids.begin(), range.second);
            size_t num_rows = end - start;

            // build request chunk for this BE
            auto new_row_id_column = RowIdColumn::create();
            new_row_id_column->reserve(num_rows);
            new_row_id_column->append(*row_id_column, start, num_rows);
            auto new_position_column = UInt32Column::create();
            new_position_column->reserve(num_rows);
            new_position_column->append(*position_column, start, num_rows);

            auto [request_columns_iter, _] = request_chunks->try_emplace(cur_be_id, std::make_shared<RequestColumns>());
            auto& request_columns = request_columns_iter->second;
            request_columns->emplace(slot_id,
                                     std::make_pair(std::move(new_row_id_column), std::move(new_position_column)));
            iter = range.second;
        }
    }

    return Status::OK();
}

Status FetchProcessor::_send_fetch_request(RuntimeState* state, const BatchUnitPtr& unit,
                                           const phmap::flat_hash_map<uint32_t, RequestColumnsPtr>& request_chunks) {
    unit->fetch_ctxs.clear();
    unit->total_request_num = request_chunks.size();
    unit->finished_request_num = 0;

    for (const auto& [be_id, request_columns] : request_chunks) {
        auto fetch_ctx = std::make_shared<FetchContext>();
        fetch_ctx->be_id = be_id;
        fetch_ctx->request_columns = request_columns;
        fetch_ctx->send_ts = MonotonicNanos();
        unit->fetch_ctxs[be_id] = fetch_ctx;
        if (be_id == _local_be_id && config::enable_fetch_local_pass_through) {
            RETURN_IF_ERROR(_send_local_request(state, unit, fetch_ctx));
        } else {
            RETURN_IF_ERROR(_send_remote_request(state, unit, fetch_ctx));
        }
    }

    return Status::OK();
}

Status FetchProcessor::_send_local_request(RuntimeState* state, const BatchUnitPtr& unit,
                                           const FetchContextPtr& fetch_ctx) {
    LocalLookUpRequest request;
    request.fetch_ctx = fetch_ctx;
    request.callback = [this, unit, fetch_ctx](const Status& status) {
        DeferOp defer([&]() {
            if (++unit->finished_request_num == unit->total_request_num) {
                VLOG_ROW << "all request finished, notify fetch processor, total_request_num: "
                         << unit->total_request_num << ", " << (void*)this;
            }
        });
        if (!status.ok()) {
            LOG(WARNING) << "local fetch request failed, error: " << status.to_string();
            _set_io_task_status(status);
            return;
        }
        COUNTER_UPDATE(_local_request_count, 1);
        COUNTER_UPDATE(_local_request_timer, MonotonicNanos() - fetch_ctx->send_ts);
    };
    return _local_dispatcher->add_request(request);
}

Status FetchProcessor::_send_remote_request(RuntimeState* state, const BatchUnitPtr& unit,
                                            const FetchContextPtr& fetch_ctx) {
    const auto be_id = fetch_ctx->be_id;
    const auto& request_columns = fetch_ctx->request_columns;

    auto* closure = new DisposableClosure<PLookUpResponse, FetchContextPtr>(fetch_ctx);
    closure->addSuccessHandler([this, unit, closure](const FetchContextPtr& ctx, const PLookUpResponse& resp) noexcept {
        VLOG_ROW << "receive a response, finished request num: " << unit->finished_request_num
                 << ", total request num: " << unit->total_request_num << ", " << (void*)this
                 << ", latency: " << (MonotonicNanos() - ctx->send_ts) * 1.0 / 1000000 << "ms";
        DeferOp defer([&]() {
            if (++unit->finished_request_num == unit->total_request_num) {
                VLOG_ROW << "all request finished, notify fetch processor, total_request_num: "
                         << unit->total_request_num << ", " << (void*)this;
            }
        });

        COUNTER_UPDATE(_rpc_count, 1);
        COUNTER_UPDATE(_network_timer, MonotonicNanos() - ctx->send_ts);

        if (resp.status().status_code() != TStatusCode::OK) {
            auto msg = fmt::format("fetch request failed, status: {}", resp.status().DebugString());
            LOG(WARNING) << msg;
            _set_io_task_status(Status::InternalError(msg));
            return;
        }

        if (closure->cntl.response_attachment().size() > 0) {
            SCOPED_TIMER(_deserialize_timer);
            butil::IOBuf& io_buf = closure->cntl.response_attachment();
            raw::RawString buffer;
            for (size_t i = 0; i < resp.columns_size(); i++) {
                const auto& pcolumn = resp.columns(i);
                if (UNLIKELY(io_buf.size() < pcolumn.data_size())) {
                    auto msg = fmt::format("io_buf size {} is less than column data size {}", io_buf.size(),
                                           pcolumn.data_size());
                    LOG(WARNING) << msg;
                    _set_io_task_status(Status::InternalError(msg));
                    return;
                }
                buffer.resize(pcolumn.data_size());
                size_t size = io_buf.cutn(buffer.data(), pcolumn.data_size());
                if (UNLIKELY(size != pcolumn.data_size())) {
                    auto msg = fmt::format("iobuf read {} != expected {}", size, pcolumn.data_size());
                    LOG(WARNING) << msg;
                    _set_io_task_status(Status::InternalError(msg));
                    return;
                }
                int32_t slot_id = pcolumn.slot_id();
                const SlotDescriptor* slot_desc = _slot_id_to_desc.at(slot_id);
                auto column = ColumnHelper::create_column(slot_desc->type(), slot_desc->is_nullable());

                const uint8_t* buff = reinterpret_cast<const uint8_t*>(buffer.data());
                auto ret = serde::ColumnArraySerde::deserialize(buff, column.get());
                if (ret == nullptr) {
                    auto msg = fmt::format("deserialize column error, slot_id: {}", slot_id);
                    LOG(WARNING) << msg;
                    _set_io_task_status(Status::InternalError(msg));
                    return;
                }
                ctx->response_columns.insert({slot_id, std::move(column)});
            }
        }
    });
    closure->addFailedHandler([this, unit](const FetchContextPtr& ctx, std::string_view rpc_error_msg) noexcept {
        DeferOp defer([&]() {
            if (++unit->finished_request_num == unit->total_request_num) {
                VLOG_ROW << "all request finished, notify fetch processor, " << (void*)this;
            }
        });
        LOG(WARNING) << "fetch request failed, error: " << rpc_error_msg;
        _set_io_task_status(Status::InternalError(rpc_error_msg));
    });

    closure->cntl.Reset();
    closure->cntl.set_timeout_ms(state->query_options().query_timeout * 1000);

    PLookUpRequest request;
    PUniqueId p_query_id;
    p_query_id.set_hi(state->query_id().hi);
    p_query_id.set_lo(state->query_id().lo);
    *request.mutable_query_id() = std::move(p_query_id);
    request.set_lookup_node_id(_target_node_id);

    {
        SCOPED_TIMER(_serialize_timer);
        size_t max_serialize_size = 0;
        for (const auto& [slot_id, columns] : *request_columns) {
            const auto& row_id_column = columns.first;
            max_serialize_size += serde::ColumnArraySerde::max_serialized_size(*row_id_column);
        }
        _serialize_buffer.clear();
        _serialize_buffer.resize(max_serialize_size);

        uint8_t* buff = reinterpret_cast<uint8_t*>(_serialize_buffer.data());
        uint8_t* begin = buff;

        for (const auto& [slot_id, columns] : *request_columns) {
            auto p_row_id_column = request.add_row_id_columns();
            p_row_id_column->set_slot_id(slot_id);
            const auto& row_id_column = columns.first;
            uint8_t* start = buff;
            buff = serde::ColumnArraySerde::serialize(*row_id_column, buff);
            p_row_id_column->set_data_size(buff - start);
        }
        size_t actual_serialize_size = buff - begin;
        closure->cntl.request_attachment().append(_serialize_buffer.data(), actual_serialize_size);
    }
    VLOG_ROW << "[GLM] send fetch request, be_id: " << be_id << ", " << (void*)this
             << ", unit: " << unit->debug_string();
    const auto* node_info = _nodes_info->find_node(be_id);
    auto stub = state->exec_env()->brpc_stub_cache()->get_stub(node_info->host, node_info->brpc_port);
    stub->lookup(&closure->cntl, &request, &closure->result, closure);
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
        DCHECK_EQ(unit->finished_request_num, unit->total_request_num) << "all request should be finished";
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
    SCOPED_TIMER(_build_output_chunk_timer);
    const auto& fetch_ctxs = unit->fetch_ctxs;
    const auto& input_chunks = unit->input_chunks;
    const auto& missing_positions = unit->missing_positions;

    for (const auto& [tuple_id, row_id_slot] : _row_id_slots) {
        // build a tmp chunk contains all columns under this tuple and re-sort by position
        auto chunk = std::make_shared<Chunk>();
        const auto& tuple_desc = state->desc_tbl().get_tuple_descriptor(tuple_id);
        ColumnPtr position_column = UInt32Column::create();
        for (const auto& slot : tuple_desc->slots()) {
            auto column = ColumnHelper::create_column(slot->type(), slot->is_nullable());
            chunk->append_column(std::move(column), slot->id());
        }

        // @TODO if all null, we won't find row id col too, how to solve it
        for (const auto& [be_id, fetch_ctx] : fetch_ctxs) {
            if (auto iter = fetch_ctx->request_columns->find(row_id_slot); iter != fetch_ctx->request_columns->end()) {
                const auto& [row_id_col, pos_col] = iter->second;
                position_column->append(*pos_col);
                for (const auto& slot : tuple_desc->slots()) {
                    DCHECK(fetch_ctx->response_columns.contains(slot->id()))
                            << "response columns should contains slot: " << slot->debug_string();

                    auto partial_column = fetch_ctx->response_columns.at(slot->id());
                    ;
                    chunk->get_column_by_slot_id(slot->id())->append(*partial_column);
                }
            }
        }
        chunk->check_or_die();
        if (missing_positions.contains(row_id_slot)) {
            // for null rows in row_id_column, we should add null rows to related columns
            auto& null_positions = missing_positions.at(row_id_slot);
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
        // assign columns to output chunk
        for (const auto& slot : tuple_desc->slots()) {
            size_t offset = 0;
            auto src_column = sorted_chunk->get_column_by_slot_id(slot->id());
            for (auto& input_chunk : input_chunks) {
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
    unit->fetch_ctxs.clear();
    unit->build_output_done = true;
    return Status::OK();
}

FetchProcessorFactory::FetchProcessorFactory(int32_t target_node_id, phmap::flat_hash_map<TupleId, SlotId> row_id_slots,
                                             phmap::flat_hash_map<SlotId, SlotDescriptor*> slot_id_to_desc,
                                             std::shared_ptr<StarRocksNodesInfo> nodes_info,
                                             std::shared_ptr<LookUpDispatcher> local_dispatcher)
        : _target_node_id(target_node_id),
          _row_id_slots(std::move(row_id_slots)),
          _slot_id_to_desc(std::move(slot_id_to_desc)),
          _nodes_info(std::move(nodes_info)),
          _local_dispatcher(std::move(local_dispatcher)) {}

FetchProcessorPtr FetchProcessorFactory::get_or_create(int32_t driver_sequence) {
    if (!_processor_map.contains(driver_sequence)) {
        _processor_map.try_emplace(driver_sequence,
                                   std::make_shared<FetchProcessor>(_target_node_id, _row_id_slots, _slot_id_to_desc,
                                                                    _nodes_info, _local_dispatcher));
    }
    return _processor_map.at(driver_sequence);
}
} // namespace starrocks::pipeline