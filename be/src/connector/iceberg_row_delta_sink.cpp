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

#include "connector/iceberg_row_delta_sink.h"

#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "exec/pipeline/fragment_context.h"

namespace starrocks::connector {

IcebergRowDeltaSink::IcebergRowDeltaSink(std::unique_ptr<ConnectorChunkSink> delete_sink,
                                         std::unique_ptr<ConnectorChunkSink> data_sink, int32_t op_code_index,
                                         SinkMemoryManager* sink_mem_mgr, RuntimeState* state)
        : ConnectorChunkSink({}, {}, std::make_unique<NopPartitionChunkWriterFactory>(), state, true),
          _delete_sink(std::move(delete_sink)),
          _data_sink(std::move(data_sink)),
          _op_code_index(op_code_index),
          _sink_mem_mgr(sink_mem_mgr) {}

Status IcebergRowDeltaSink::init() {
    RETURN_IF_ERROR(ConnectorChunkSink::init());

    _delete_sink->set_io_poller(_io_poller);
    _data_sink->set_io_poller(_io_poller);

    // Each sub-sink needs its own SinkOperatorMemoryManager because
    // ConnectorChunkSink::init() binds the manager to the sink's _writers list.
    if (_sink_mem_mgr != nullptr) {
        _delete_sink->set_operator_mem_mgr(_sink_mem_mgr->create_child_manager());
        _data_sink->set_operator_mem_mgr(_sink_mem_mgr->create_child_manager());
    }

    if (_profile != nullptr) {
        _delete_sink->set_profile(_profile);
        _data_sink->set_profile(_profile);
    }

    RETURN_IF_ERROR(_delete_sink->init());
    RETURN_IF_ERROR(_data_sink->init());

    // This composite sink owns no writers of its own (NopPartitionChunkWriterFactory),
    // so the operator-level kill-victim / backpressure path would see an empty
    // candidate list. Register sub-sink writer lists with the outer manager so
    // memory pressure logic can flush real writers held by the sub-sinks.
    if (_op_mem_mgr != nullptr) {
        _op_mem_mgr->add_candidates(_delete_sink->writers());
        _op_mem_mgr->add_candidates(_data_sink->writers());
    }

    return Status::OK();
}

void IcebergRowDeltaSink::callback_on_commit(const CommitResult& result) {
    // Sub-sinks handle their own callbacks.
}

void IcebergRowDeltaSink::rollback() {
    // The outer sink owns no writers; rollback actions are held by the sub-sinks.
    // On cancel, propagate so uncommitted data / position-delete files get cleaned.
    _delete_sink->rollback();
    _data_sink->rollback();
    ConnectorChunkSink::rollback();
}

// Filter the original chunk to keep only the specified rows, preserving all
// columns and slot_ids. This ensures sub-sinks receive chunks that are
// structurally identical to what the pipeline would produce for a standalone
// DELETE or INSERT operation — same slot_ids, same column order, same types.
static ChunkPtr filter_chunk_by_rows(const ChunkPtr& chunk, const std::vector<uint32_t>& row_indices) {
    ChunkPtr filtered = chunk->clone_empty();
    filtered->append_selective(*chunk, row_indices.data(), 0, row_indices.size());
    return filtered;
}

Status IcebergRowDeltaSink::add(const ChunkPtr& chunk) {
    int num_rows = chunk->num_rows();
    if (num_rows == 0) {
        return Status::OK();
    }

    auto op_code_column = chunk->get_column_by_index(_op_code_index);
    const auto* op_codes = ColumnHelper::get_data_column(op_code_column.get());
    const auto* op_code_data = down_cast<const FixedLengthColumn<int8_t>*>(op_codes)->get_data().data();

    _delete_rows.clear();
    _data_rows.clear();

    for (int i = 0; i < num_rows; ++i) {
        int8_t op = op_code_data[i];
        switch (op) {
        case OP_NO_OP:
            break;
        case OP_DELETE:
            _delete_rows.push_back(i);
            break;
        case OP_UPDATE:
            _delete_rows.push_back(i);
            _data_rows.push_back(i);
            break;
        case OP_INSERT:
            _data_rows.push_back(i);
            break;
        default:
            return Status::InternalError(fmt::format("Unknown op_code: {}", op));
        }
    }

    // Fast path: when all rows have the same op_code (common for pure UPDATE where
    // every row is OP_UPDATE), pass the original chunk directly without copying.
    bool all_to_delete = (_delete_rows.size() == num_rows);
    bool all_to_data = (_data_rows.size() == num_rows);

    if (!_delete_rows.empty()) {
        RETURN_IF_ERROR(_delete_sink->add(all_to_delete ? chunk : filter_chunk_by_rows(chunk, _delete_rows)));
    }

    if (!_data_rows.empty()) {
        RETURN_IF_ERROR(_data_sink->add(all_to_data ? chunk : filter_chunk_by_rows(chunk, _data_rows)));
    }

    return Status::OK();
}

Status IcebergRowDeltaSink::finish() {
    RETURN_IF_ERROR(_delete_sink->finish());
    RETURN_IF_ERROR(_data_sink->finish());
    return Status::OK();
}

bool IcebergRowDeltaSink::is_finished() {
    return _delete_sink->is_finished() && _data_sink->is_finished();
}

StatusOr<std::unique_ptr<ConnectorChunkSink>> IcebergRowDeltaSinkProvider::create_chunk_sink(
        std::shared_ptr<ConnectorChunkSinkContext> context, int32_t driver_id) {
    auto ctx = std::dynamic_pointer_cast<IcebergRowDeltaSinkContext>(context);
    if (ctx == nullptr) {
        return Status::InternalError("IcebergRowDeltaSinkProvider: context is not IcebergRowDeltaSinkContext");
    }

    auto runtime_state = ctx->data_sink_ctx->fragment_context->runtime_state();

    IcebergDeleteSinkProvider delete_provider;
    ASSIGN_OR_RETURN(auto delete_sink, delete_provider.create_chunk_sink(ctx->delete_sink_ctx, driver_id));

    IcebergChunkSinkProvider data_provider;
    ASSIGN_OR_RETURN(auto data_sink, data_provider.create_chunk_sink(ctx->data_sink_ctx, driver_id));

    return std::make_unique<IcebergRowDeltaSink>(std::move(delete_sink), std::move(data_sink), ctx->op_code_index,
                                                 ctx->sink_mem_mgr, runtime_state);
}

} // namespace starrocks::connector
