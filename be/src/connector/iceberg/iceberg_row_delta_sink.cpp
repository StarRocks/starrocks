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

#include "connector/iceberg/iceberg_row_delta_sink.h"

#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "common/logging.h"
#include "connector/common/partition_chunk_writer_memory_manager.h"

namespace starrocks::connector {

IcebergRowDeltaSink::IcebergRowDeltaSink(std::unique_ptr<ConnectorSink> delete_sink,
                                         std::unique_ptr<ConnectorSink> data_sink, int32_t op_code_index,
                                         RuntimeState* state)
        : PartitionedConnectorChunkSink({}, {}, std::make_unique<NopPartitionChunkWriterFactory>(), state, true),
          _delete_sink(std::move(delete_sink)),
          _data_sink(std::move(data_sink)),
          _op_code_index(op_code_index) {}

IcebergRowDeltaSinkProvider::IcebergRowDeltaSinkProvider(std::shared_ptr<IcebergRowDeltaSinkContext> ctx)
        : _ctx(std::move(ctx)) {}

Status IcebergRowDeltaSink::init(formats::AsyncFlushStreamPoller* poller, RuntimeProfile* profile,
                                 SinkMemoryManager* sink_mem_mgr) {
    _io_poller = poller;
    _profile = profile;
    DCHECK(sink_mem_mgr != nullptr);
    init_profile();
    RETURN_IF_ERROR(ColumnEvaluator::init(_partition_column_evaluators));
    RETURN_IF_ERROR(_partition_chunk_writer_factory->init());

    RETURN_IF_ERROR(_delete_sink->init(poller, _profile, sink_mem_mgr));
    RETURN_IF_ERROR(_data_sink->init(poller, _profile, sink_mem_mgr));

    auto op_mem_mgr = std::make_unique<PartitionChunkWriterMemoryManager>();
    RETURN_IF_ERROR(op_mem_mgr->init(&_writers, _io_poller));

    // This composite sink owns no writers of its own (NopPartitionChunkWriterFactory),
    // so the operator-level kill-victim / backpressure path would see an empty
    // candidate list. Register sub-sink writer lists with the outer manager so
    // memory pressure logic can flush real writers held by the sub-sinks.
    _delete_sink->register_memory_candidates(op_mem_mgr.get());
    _data_sink->register_memory_candidates(op_mem_mgr.get());

    _partition_writer_mem_mgr = op_mem_mgr.get();
    _op_mem_mgr = sink_mem_mgr->register_child_manager(std::move(op_mem_mgr));

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
    PartitionedConnectorChunkSink::rollback();
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

    if (_op_code_index < 0) {
        RETURN_IF_ERROR(_delete_sink->add(chunk));
        RETURN_IF_ERROR(_data_sink->add(chunk));
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

    // Fast path: when all mixed-mode rows have the same op_code, pass the
    // original chunk directly without copying.
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

StatusOr<std::unique_ptr<ConnectorSink>> IcebergRowDeltaSinkProvider::create_sink(int32_t driver_id) {
    auto ctx = _ctx;
    if (ctx == nullptr) {
        return Status::InternalError("IcebergRowDeltaSinkProvider: context is not IcebergRowDeltaSinkContext");
    }

    DCHECK(ctx->data_sink_ctx != nullptr);
    DCHECK(ctx->data_sink_ctx->runtime_state != nullptr);
    auto* runtime_state = ctx->data_sink_ctx->runtime_state;

    IcebergDeleteSinkProvider delete_provider(ctx->delete_sink_ctx);
    ASSIGN_OR_RETURN(auto delete_sink, delete_provider.create_sink(driver_id));

    IcebergChunkSinkProvider data_provider(ctx->data_sink_ctx);
    ASSIGN_OR_RETURN(auto data_sink, data_provider.create_sink(driver_id));

    auto sink = std::make_unique<IcebergRowDeltaSink>(std::move(delete_sink), std::move(data_sink), ctx->op_code_index,
                                                      runtime_state);
    return sink;
}

} // namespace starrocks::connector
