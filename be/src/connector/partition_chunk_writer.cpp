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

#include "connector/partition_chunk_writer.h"

#include "column/chunk.h"
#include "common/status.h"
#include "connector/async_flush_stream_poller.h"
#include "connector/connector_sink_executor.h"
#include "connector/sink_memory_manager.h"
#include "exec/pipeline/fragment_context.h"
#include "formats/file_writer.h"
#include "runtime/runtime_state.h"
#include "storage/chunk_helper.h"
#include "storage/convert_helper.h"
#include "storage/load_spill_block_manager.h"
#include "storage/storage_engine.h"
#include "storage/types.h"
#include "util/monotime.h"

namespace starrocks::connector {

PartitionChunkWriter::PartitionChunkWriter(std::string partition, std::vector<int8_t> partition_field_null_list,
                                           const std::shared_ptr<PartitionChunkWriterContext>& ctx)
        : _partition(std::move(partition)),
          _partition_field_null_list(std::move(partition_field_null_list)),
          _file_writer_factory(ctx->file_writer_factory),
          _location_provider(ctx->location_provider),
          _max_file_size(ctx->max_file_size),
          _is_default_partition(ctx->is_default_partition) {
    _commit_extra_data.resize(_partition_field_null_list.size(), '0');
    std::transform(_partition_field_null_list.begin(), _partition_field_null_list.end(), _commit_extra_data.begin(),
                   [](int8_t b) { return b + '0'; });
}

Status PartitionChunkWriter::create_file_writer_if_needed() {
    if (!_file_writer) {
        std::string path = _is_default_partition ? _location_provider->get() : _location_provider->get(_partition);
        ASSIGN_OR_RETURN(auto new_writer_and_stream, _file_writer_factory->create(path));
        _file_writer = std::move(new_writer_and_stream.writer);
        _out_stream = std::move(new_writer_and_stream.stream);
        RETURN_IF_ERROR(_file_writer->init());
        _io_poller->enqueue(_out_stream);
    }
    return Status::OK();
}

void PartitionChunkWriter::commit_file() {
    if (!_file_writer) {
        return;
    }
    auto result = _file_writer->commit();
    _commit_callback(result.set_extra_data(_commit_extra_data));
    _file_writer = nullptr;
    VLOG(3) << "commit to remote file, filename: " << _out_stream->filename()
            << ", size: " << result.file_statistics.file_size;
    _out_stream = nullptr;
}

Status BufferPartitionChunkWriter::init() {
    return Status::OK();
}

Status BufferPartitionChunkWriter::write(Chunk* chunk) {
    RETURN_IF_ERROR(create_file_writer_if_needed());
    if (_file_writer->get_written_bytes() >= _max_file_size) {
        commit_file();
    }
    return _file_writer->write(chunk);
}

Status BufferPartitionChunkWriter::flush() {
    commit_file();
    return Status::OK();
}

Status BufferPartitionChunkWriter::finish() {
    commit_file();
    return Status::OK();
}

SpillPartitionChunkWriter::SpillPartitionChunkWriter(std::string partition,
                                                     std::vector<int8_t> partition_field_null_list,
                                                     const std::shared_ptr<SpillPartitionChunkWriterContext>& ctx)
        : PartitionChunkWriter(std::move(partition), std::move(partition_field_null_list), ctx),
          _fs(ctx->fs),
          _fragment_context(ctx->fragment_context),
          _column_evaluators(ctx->column_evaluators),
          _sort_ordering(ctx->sort_ordering) {
    _chunk_spill_token = ExecEnv::GetInstance()->connector_sink_spill_executor()->create_token();
    _block_merge_token = StorageEngine::instance()->load_spill_block_merge_executor()->create_token();
    _tuple_desc = ctx->tuple_desc;
    _writer_id = generate_uuid();
}

SpillPartitionChunkWriter::~SpillPartitionChunkWriter() {
    if (_chunk_spill_token) {
        _chunk_spill_token->shutdown();
    }
    if (_block_merge_token) {
        _block_merge_token->shutdown();
    }
}

Status SpillPartitionChunkWriter::init() {
    std::string root_location = _location_provider->root_location();
    _load_spill_block_mgr =
            std::make_unique<LoadSpillBlockManager>(_fragment_context->query_id(), _writer_id, root_location, _fs);
    RETURN_IF_ERROR(_load_spill_block_mgr->init());
    _load_chunk_spiller = std::make_unique<LoadChunkSpiller>(_load_spill_block_mgr.get(),
                                                             _fragment_context->runtime_state()->runtime_profile());
    if (_column_evaluators) {
        RETURN_IF_ERROR(ColumnEvaluator::init(*_column_evaluators));
    }
    return Status::OK();
}

Status SpillPartitionChunkWriter::write(Chunk* chunk) {
    RETURN_IF_ERROR(create_file_writer_if_needed());

    _chunks.push_back(chunk->clone_unique());
    _chunk_bytes_usage += chunk->bytes_usage();
    if (!_base_chunk) {
        _base_chunk = _chunks.back();
    }

    int64_t max_flush_batch_size = _file_writer->get_flush_batch_size();
    if (_sort_ordering || max_flush_batch_size == 0) {
        max_flush_batch_size = _max_file_size;
    }
    if (_chunk_bytes_usage >= max_flush_batch_size) {
        return _flush_to_file();
    } else if (_mem_insufficent()) {
        return _spill();
    }
    return Status::OK();
}

Status SpillPartitionChunkWriter::flush() {
    RETURN_IF(!_file_writer, Status::OK());
    return _spill();
}

Status SpillPartitionChunkWriter::finish() {
    _chunk_spill_token->wait();
    // If no chunks have been spilled, flush data to remote file directly.
    if (_load_chunk_spiller->empty()) {
        VLOG(2) << "flush to remote directly when finish, query_id: " << print_id(_fragment_context->query_id())
                << ", writer_id: " << print_id(_writer_id);
        RETURN_IF_ERROR(_flush_to_file());
        commit_file();
        return Status::OK();
    }

    auto cb = [this](const Status& st) {
        LOG_IF(ERROR, !st.ok()) << "fail to merge spill blocks, query_id: " << print_id(_fragment_context->query_id())
                                << ", writer_id: " << print_id(_writer_id);
        _handle_err(st);
        commit_file();
    };
    auto merge_task = std::make_shared<MergeBlockTask>(this, cb);
    return _block_merge_token->submit(merge_task);
}

const int64_t SpillPartitionChunkWriter::kWaitMilliseconds = 10;

bool SpillPartitionChunkWriter::is_finished() {
    bool finished = _chunk_spill_token->wait_for(MonoDelta::FromMilliseconds(kWaitMilliseconds)) &&
                    _block_merge_token->wait_for(MonoDelta::FromMilliseconds(kWaitMilliseconds));
    return finished;
}

Status SpillPartitionChunkWriter::merge_blocks() {
    RETURN_IF_ERROR(flush());
    _chunk_spill_token->wait();

    auto write_func = [this](Chunk* chunk) { return _flush_chunk(chunk, false); };
    auto flush_func = [this]() {
        // Commit file after each merge function to ensure the data written to one file is ordered,
        // because data generated by different merge function may be unordered.
        if (_sort_ordering) {
            commit_file();
        }
        return Status::OK();
    };
    Status st = _load_chunk_spiller->merge_write(_max_file_size, _sort_ordering != nullptr, false /* do_agg */,
                                                 write_func, flush_func);
    VLOG(2) << "finish merge blocks, query_id: " << _fragment_context->query_id() << ", status: " << st.message();
    return st;
}

Status SpillPartitionChunkWriter::_sort() {
    RETURN_IF(!_result_chunk, Status::OK());

    auto chunk = _result_chunk->clone_empty_with_schema(0);
    _result_chunk->swap_chunk(*chunk);
    SmallPermutation perm = create_small_permutation(static_cast<uint32_t>(chunk->num_rows()));
    Columns columns;
    for (auto sort_key_idx : _sort_ordering->sort_key_idxes) {
        columns.push_back(chunk->get_column_by_index(sort_key_idx));
    }

    RETURN_IF_ERROR(stable_sort_and_tie_columns(false, columns, _sort_ordering->sort_descs, &perm));
    std::vector<uint32_t> selective;
    permutate_to_selective(perm, &selective);
    _result_chunk->rolling_append_selective(*chunk, selective.data(), 0, chunk->num_rows());
    return Status::OK();
}

Status SpillPartitionChunkWriter::_spill() {
    RETURN_IF(_chunks.empty(), Status::OK());

    RETURN_IF_ERROR(_merge_chunks());
    if (_sort_ordering) {
        RETURN_IF_ERROR(_sort());
    }

    auto callback = [this](const ChunkPtr& chunk, const StatusOr<size_t>& res) {
        if (!res.ok()) {
            LOG(ERROR) << "fail to spill connector partition chunk sink, write it to remote file directly. msg: "
                       << res.status().message();
            Status st = _flush_chunk(chunk.get(), true);
            _handle_err(st);
        } else {
            VLOG(3) << "spill chunk data, filename: " << out_stream()->filename() << ", size: " << chunk->bytes_usage()
                    << ", rows: " << chunk->num_rows() << ", partition: " << _partition
                    << ", writer_id: " << _writer_id;
        }
        _spilling_bytes_usage.fetch_sub(chunk->bytes_usage(), std::memory_order_relaxed);
    };
    auto spill_task = std::make_shared<ChunkSpillTask>(_load_chunk_spiller.get(), _result_chunk, callback);
    RETURN_IF_ERROR(_chunk_spill_token->submit(spill_task));
    _spilling_bytes_usage.fetch_add(_result_chunk->bytes_usage(), std::memory_order_relaxed);
    _chunk_bytes_usage = 0;
    return Status::OK();
}

Status SpillPartitionChunkWriter::_flush_to_file() {
    RETURN_IF(_chunks.empty(), Status::OK());

    if (!_sort_ordering) {
        for (auto& chunk : _chunks) {
            RETURN_IF_ERROR(_flush_chunk(chunk.get(), false));
        }
    } else {
        RETURN_IF_ERROR(_merge_chunks());
        RETURN_IF_ERROR(_sort());
        RETURN_IF_ERROR(_flush_chunk(_result_chunk.get(), true));
        commit_file();
    }
    _chunks.clear();
    _chunk_bytes_usage = 0;

    return Status::OK();
};

Status SpillPartitionChunkWriter::_flush_chunk(Chunk* chunk, bool split) {
    if (chunk->get_slot_id_to_index_map().empty()) {
        auto& slot_map = _base_chunk->get_slot_id_to_index_map();
        for (auto& it : slot_map) {
            chunk->set_slot_id_to_index(it.first, _col_index_map[it.second]);
        }
    }

    if (!split) {
        return _write_chunk(chunk);
    }
    size_t chunk_size = config::vector_chunk_size;
    for (size_t offset = 0; offset < chunk->num_rows(); offset += chunk_size) {
        auto sub_chunk = chunk->clone_empty(chunk_size);
        size_t num_rows = std::min(chunk_size, chunk->num_rows() - offset);
        sub_chunk->append(*chunk, offset, num_rows);
        RETURN_IF_ERROR(_write_chunk(sub_chunk.get()));
    }
    return Status::OK();
}

Status SpillPartitionChunkWriter::_write_chunk(Chunk* chunk) {
    if (!_sort_ordering && _file_writer->get_written_bytes() >= _max_file_size) {
        commit_file();
    }
    RETURN_IF_ERROR(create_file_writer_if_needed());
    RETURN_IF_ERROR(_file_writer->write(chunk));
    return Status::OK();
}

Status SpillPartitionChunkWriter::_merge_chunks() {
    if (_chunks.empty()) {
        return Status::OK();
    }

    // Create a target chunk with schema to make it can use some
    // module functions of native table directly.
    size_t num_rows = std::accumulate(_chunks.begin(), _chunks.end(), 0,
                                      [](int sum, const ChunkPtr& chunk) { return sum + chunk->num_rows(); });
    _result_chunk = _create_schema_chunk(_chunks.front(), num_rows);

    std::unordered_map<Column*, size_t> col_ptr_index_map;
    auto& columns = _chunks.front()->columns();
    for (size_t i = 0; i < columns.size(); ++i) {
        col_ptr_index_map[columns[i]->get_ptr()] = i;
    }
    for (auto& chunk : _chunks) {
        for (size_t i = 0; i < _result_chunk->num_columns(); ++i) {
            auto* dst_col = _result_chunk->get_column_by_index(i).get();
            ColumnPtr src_col;
            if (_column_evaluators) {
                ASSIGN_OR_RETURN(src_col, (*_column_evaluators)[i]->evaluate(chunk.get()));
            } else {
                src_col = chunk->get_column_by_index(i);
            }
            dst_col->append(*src_col);
            if (chunk == _chunks.front()) {
                auto it = col_ptr_index_map.find(src_col.get());
                if (it != col_ptr_index_map.end()) {
                    _col_index_map[it->second] = i;
                } else {
                    return Status::InternalError("unknown column index: " + std::to_string(i));
                }
            }
        }

        chunk.reset();
    }

    _chunks.clear();
    return Status::OK();
}

bool SpillPartitionChunkWriter::_mem_insufficent() {
    // Return false because we will triger spill by sink memory manager.
    return false;
}

void SpillPartitionChunkWriter::_handle_err(const Status& st) {
    if (!st.ok()) {
        _error_handler(st);
    }
}

SchemaPtr SpillPartitionChunkWriter::_make_schema() {
    Fields fields;
    for (auto& slot : _tuple_desc->slots()) {
        TypeDescriptor type_desc = slot->type();
        TypeInfoPtr type_info = get_type_info(type_desc.type, type_desc.precision, type_desc.scale);
        auto field = std::make_shared<Field>(slot->id(), slot->col_name(), type_info, slot->is_nullable());
        fields.push_back(field);
    }

    SchemaPtr schema;
    if (_sort_ordering) {
        schema = std::make_shared<Schema>(std::move(fields), KeysType::DUP_KEYS, _sort_ordering->sort_key_idxes,
                                          std::make_shared<SortDescs>(_sort_ordering->sort_descs));
    } else {
        schema = std::make_shared<Schema>(std::move(fields), KeysType::DUP_KEYS, std::vector<uint32_t>(), nullptr);
    }
    return schema;
}

ChunkPtr SpillPartitionChunkWriter::_create_schema_chunk(const ChunkPtr& base_chunk, size_t num_rows) {
    if (!_schema) {
        const SchemaPtr& schema = base_chunk->schema();
        if (schema) {
            _schema = schema;
            if (_sort_ordering) {
                _schema->set_sort_key_idxes(_sort_ordering->sort_key_idxes);
                _schema->set_sort_descs(std::make_shared<SortDescs>(_sort_ordering->sort_descs));
            }
        } else {
            _schema = _make_schema();
        }
    }
    auto chunk = ChunkHelper::new_chunk(*_schema, num_rows);
    return chunk;
}

} // namespace starrocks::connector
