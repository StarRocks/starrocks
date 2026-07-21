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

#include "connector/iceberg/iceberg_dv_sink.h"

#include <fmt/format.h>

#include "base/uid_util.h"
#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "connector/common/partition_chunk_writer_memory_manager.h"
#include "connector/iceberg/iceberg_utils.h"
#include "connector_primitive/sink_memory_manager.h"
#include "formats/column_evaluator.h"
#include "formats/io/async_flush_stream_poller.h"
#include "fs/fs.h"
#include "fs/fs_factory.h"
#include "gen_cpp/Types_types.h"
#include "runtime/runtime_state.h"

namespace starrocks::connector {

IcebergDvSinkProvider::IcebergDvSinkProvider(std::shared_ptr<IcebergDvSinkContext> ctx) : _ctx(std::move(ctx)) {}

StatusOr<std::unique_ptr<ConnectorSink>> IcebergDvSinkProvider::create_sink(int32_t driver_id) {
    auto ctx = _ctx;
    if (ctx == nullptr) {
        return Status::InternalError("IcebergDvSinkProvider: context is not IcebergDvSinkContext");
    }
    if (ctx->column_slot_map.find("_file") == ctx->column_slot_map.end()) {
        return Status::InternalError("Could not find _file column in column_slot_map");
    }
    if (ctx->column_slot_map.find("_pos") == ctx->column_slot_map.end()) {
        return Status::InternalError("Could not find _pos column in column_slot_map");
    }

    auto* runtime_state = ctx->runtime_state;
    DCHECK(runtime_state != nullptr);
    ASSIGN_OR_RETURN(auto fs,
                     FileSystemFactory::CreateUniqueFromString(ctx->path, FSOptions(&ctx->cloud_configuration)));
    std::shared_ptr<FileSystem> shared_fs = std::move(fs);

    // Puffin DV lives at the table data root (no partition subdir): suffix "puffin".
    auto location_provider =
            std::make_shared<LocationProvider>(ctx->path, print_id(runtime_state->query_id()),
                                               runtime_state->be_number(), driver_id, "puffin", ctx->writer_tag);

    return std::make_unique<IcebergDvSink>(shared_fs, location_provider, ctx->column_slot_map,
                                           ctx->partition_column_names, ctx->transform_exprs,
                                           ColumnEvaluator::clone(ctx->partition_evaluators), runtime_state);
}

IcebergDvSink::IcebergDvSink(std::shared_ptr<FileSystem> fs, std::shared_ptr<LocationProvider> location_provider,
                             std::unordered_map<std::string, TExprNode> column_slot_map,
                             std::vector<std::string> partition_column_names, std::vector<std::string> transform_exprs,
                             std::vector<std::unique_ptr<ColumnEvaluator>>&& partition_evaluators, RuntimeState* state)
        : PartitionedConnectorChunkSink(std::move(partition_column_names), std::move(partition_evaluators),
                                        /*partition_chunk_writer_factory=*/nullptr, state,
                                        /*support_null_partition=*/true),
          _fs(std::move(fs)),
          _location_provider(std::move(location_provider)),
          _column_slot_map(std::move(column_slot_map)),
          _transform_exprs(std::move(transform_exprs)) {}

Status IcebergDvSink::init(formats::AsyncFlushStreamPoller* poller, RuntimeProfile* profile,
                           SinkMemoryManager* sink_mem_mgr) {
    // Mirror PartitionedConnectorChunkSink::init, minus the PartitionChunkWriterFactory init:
    // the DV sink owns no partition writers (factory is null) and finalizes a single Puffin at
    // finish(). The op_mem_mgr wiring is still required: SinkMemoryManager polls the child
    // manager's releasable_memory() -> _io_poller on every need_input(), so _io_poller and the
    // child manager must be set here or that path null-derefs. With an empty _writers list the
    // memory manager applies no backpressure (DV bitmaps cannot be flushed incrementally).
    _io_poller = poller;
    _profile = profile;
    DCHECK(sink_mem_mgr != nullptr);
    auto op_mem_mgr = std::make_unique<PartitionChunkWriterMemoryManager>();
    init_profile();
    RETURN_IF_ERROR(ColumnEvaluator::init(_partition_column_evaluators));
    RETURN_IF_ERROR(op_mem_mgr->init(&_writers, _io_poller));
    _partition_writer_mem_mgr = op_mem_mgr.get();
    _op_mem_mgr = sink_mem_mgr->register_child_manager(std::move(op_mem_mgr));
    return Status::OK();
}

Status IcebergDvSink::add(const ChunkPtr& chunk) {
    const int num_rows = chunk->num_rows();
    if (num_rows == 0) {
        return Status::OK();
    }

    auto file_it = _column_slot_map.find("_file");
    auto pos_it = _column_slot_map.find("_pos");
    if (file_it == _column_slot_map.end() || pos_it == _column_slot_map.end()) {
        return Status::InternalError("Could not find _file/_pos in column_slot_map");
    }
    const SlotId file_slot = file_it->second.slot_ref.slot_id;
    const SlotId pos_slot = pos_it->second.slot_ref.slot_id;

    ColumnPtr file_col = chunk->get_column_by_slot_id(file_slot);
    ColumnPtr pos_col = chunk->get_column_by_slot_id(pos_slot);
    if (file_col == nullptr || pos_col == nullptr) {
        return Status::InternalError("Could not find _file/_pos column in chunk");
    }
    BinaryColumn* file_data = ColumnHelper::get_binary_column(chunk->get_column_raw_ptr_by_slot_id(file_slot));
    ColumnViewer<TYPE_BIGINT> pos_view(pos_col);

    // Stream rows straight into the writer: IcebergDvWriter::add takes a string_view and only
    // materializes the key for files it has not seen yet, so this loop is allocation-free per
    // row. Because DV shuffles by _file, one sink instance can hold data files that belong to
    // different partitions, so each file's partition is captured once from its first row (a
    // data file belongs to exactly one partition, so any of its rows yields the same value).
    const bool partitioned = !_partition_column_names.empty();
    for (int i = 0; i < num_rows; ++i) {
        if (file_col->is_null(i)) {
            return Status::InternalError("_file is NULL value");
        }
        if (pos_col->is_null(i)) {
            return Status::InternalError("_pos is NULL value");
        }
        const Slice file_path = file_data->get_slice(i);
        _dv_writer.add(file_path, static_cast<uint64_t>(pos_view.value(i)));
        if (partitioned && _partition_by_file.find(std::string_view(file_path)) == _partition_by_file.end()) {
            // One-row representative chunk so iceberg_make_partition_name (which reads row 0)
            // evaluates this file's partition columns.
            ChunkPtr rep = chunk->clone_empty();
            uint32_t idx = i;
            rep->append_selective(*chunk, &idx, 0, 1);
            std::vector<int8_t> null_list;
            ASSIGN_OR_RETURN(std::string part_name,
                             IcebergUtils::iceberg_make_partition_name(_partition_column_names,
                                                                       _partition_column_evaluators, _transform_exprs,
                                                                       rep.get(), _support_null_partition, null_list));
            std::string fingerprint(null_list.size(), '0');
            for (size_t k = 0; k < null_list.size(); ++k) {
                fingerprint[k] = null_list[k] ? '1' : '0';
            }
            _partition_by_file.emplace(
                    file_path.to_string(),
                    PartitionInfo{_location_provider->root_location(part_name), std::move(fingerprint)});
        }
    }
    return Status::OK();
}

Status IcebergDvSink::finish() {
    _finished = true;
    if (_dv_writer.empty()) {
        return Status::OK(); // nothing deleted -> no orphan zero-blob Puffin
    }

    const std::string location = _location_provider->get(); // <data_root>/{prefix}_{idx}.puffin
    ASSIGN_OR_RETURN(auto file, _fs->new_writable_file(location));
    // Register cleanup right after the file exists: if the query is cancelled after finish()
    // but before the FE commit (or the write below fails), ConnectorSinkOperator::close()
    // invokes rollback() and this deletes the uncommitted Puffin, matching the Parquet sinks.
    push_rollback_action([fs = _fs, location]() {
        WARN_IF_ERROR(ignore_not_found(fs->delete_file(location)), "fail to delete file " + location);
    });
    ASSIGN_OR_RETURN(std::vector<formats::IcebergDvCommitEntry> entries, _dv_writer.finish(file.get()));
    // Total Puffin size including the footer PuffinWriter appended after the last blob; the
    // last blob's end offset would understate it. Read before close() while the file is open.
    const int64_t file_size = static_cast<int64_t>(file->size());
    RETURN_IF_ERROR(file->close());

    int64_t total_deleted_rows = 0;
    for (const auto& e : entries) {
        TIcebergDataFile df;
        df.__set_path(location);
        df.__set_format("puffin");
        df.__set_file_content(TIcebergFileContent::POSITION_DELETES);
        df.__set_referenced_data_file(e.referenced_data_file);
        df.__set_content_offset(e.content_offset);
        df.__set_content_size_in_bytes(e.content_size_in_bytes);
        df.__set_record_count(e.record_count);
        df.__set_file_size_in_bytes(file_size);
        auto pit = _partition_by_file.find(e.referenced_data_file);
        if (pit != _partition_by_file.end()) {
            df.__set_partition_path(pit->second.partition_path);
            df.__set_partition_null_fingerprint(pit->second.null_fingerprint);
        }
        TSinkCommitInfo commit_info;
        commit_info.__set_iceberg_data_file(df);
        _state->add_sink_commit_info(commit_info);
        total_deleted_rows += e.record_count;
    }
    // Report deleted rows into the load counters (what the V2 delete sink does per commit
    // callback), so DML row accounting reflects the DV deletes. record_count is the bitmap
    // cardinality, i.e. distinct deleted positions.
    _state->update_num_rows_load_sink(total_deleted_rows);
    return Status::OK();
}

} // namespace starrocks::connector
