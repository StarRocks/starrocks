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

#include "exec/pipeline/exchange/local_exchange.h"

#include <memory>

#include "column/chunk.h"
#include "exec/pipeline/exchange/shuffler.h"
#include "exprs/expr_context.h"
#include "util/runtime_profile.h"

namespace starrocks::pipeline {

Status PartitionExchanger::Partitioner::partition_chunk(const ChunkPtr& chunk,
                                                        std::vector<uint32_t>& partition_row_indexes) {
    int32_t num_rows = chunk->num_rows();
    int32_t num_partitions = _source->get_sources().size();

    if (_shuffler == nullptr) {
        _shuffler = std::make_unique<Shuffler>(_source->runtime_state()->func_version() <= 3, false, _part_type,
                                               _source->get_sources().size(), 1);
    }

    for (size_t i = 0; i < _partitions_columns.size(); ++i) {
        ASSIGN_OR_RETURN(_partitions_columns[i], _partition_expr_ctxs[i]->evaluate(chunk.get()));
        DCHECK(_partitions_columns[i] != nullptr);
    }

    // Compute hash for each partition column
    if (_part_type == TPartitionType::HASH_PARTITIONED) {
        _hash_values.assign(num_rows, HashUtil::FNV_SEED);
        for (const ColumnPtr& column : _partitions_columns) {
            column->fnv_hash(&_hash_values[0], 0, num_rows);
        }
    } else {
        // The data distribution was calculated using CRC32_HASH,
        // and bucket shuffle need to use the same hash function when sending data
        _hash_values.assign(num_rows, 0);
        for (const ColumnPtr& column : _partitions_columns) {
            column->crc32_hash(&_hash_values[0], 0, num_rows);
        }
    }

    _shuffle_channel_id.resize(num_rows);

    _shuffler->local_exchange_shuffle(_shuffle_channel_id, _hash_values, num_rows);

    _partition_row_indexes_start_points.assign(num_partitions + 1, 0);
    _partition_memory_usage.assign(num_partitions, 0);
    for (size_t i = 0; i < num_rows; ++i) {
        _partition_row_indexes_start_points[_shuffle_channel_id[i]]++;
        _partition_memory_usage[_shuffle_channel_id[i]] += chunk->bytes_usage(i, 1);
    }
    // We make the last item equal with number of rows of this chunk.
    for (int32_t i = 1; i <= num_partitions; ++i) {
        _partition_row_indexes_start_points[i] += _partition_row_indexes_start_points[i - 1];
    }

    for (int32_t i = num_rows - 1; i >= 0; --i) {
        partition_row_indexes[_partition_row_indexes_start_points[_shuffle_channel_id[i]] - 1] = i;
        _partition_row_indexes_start_points[_shuffle_channel_id[i]]--;
    }

    return Status::OK();
}

PartitionExchanger::PartitionExchanger(const std::shared_ptr<ChunkBufferMemoryManager>& memory_manager,
                                       LocalExchangeSourceOperatorFactory* source, const TPartitionType::type part_type,
                                       const std::vector<ExprContext*>& partition_expr_ctxs)
        : LocalExchanger(strings::Substitute("Partition($0)", to_string(part_type)), memory_manager, source),
          _part_type(part_type),
          _partition_exprs(partition_expr_ctxs) {}

void PartitionExchanger::incr_sinker() {
    LocalExchanger::incr_sinker();
    _partitioners.emplace_back(_source, _part_type, _partition_exprs);
}

Status PartitionExchanger::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(LocalExchanger::prepare(state));
    RETURN_IF_ERROR(Expr::prepare(_partition_exprs, state));
    RETURN_IF_ERROR(Expr::open(_partition_exprs, state));
    return Status::OK();
}

void PartitionExchanger::close(RuntimeState* state) {
    Expr::close(_partition_exprs, state);
    LocalExchanger::close(state);
}

Status PartitionExchanger::accept(const ChunkPtr& chunk, const int32_t sink_driver_sequence) {
    size_t num_rows = chunk->num_rows();
    if (num_rows == 0) {
        return Status::OK();
    }

    auto& partitioner = _partitioners[sink_driver_sequence];

    // Create a new partition_row_indexes here instead of reusing it in the partitioner.
    // The chunk and partition_row_indexes are cached in the queue of source operator,
    // and used later in pull_chunk() of source operator. If we reuse partition_row_indexes in partitioner,
    // it will be overwritten by the next time calling partitioner.partition_chunk().
    std::shared_ptr<std::vector<uint32_t>> partition_row_indexes = std::make_shared<std::vector<uint32_t>>(num_rows);
    RETURN_IF_ERROR(partitioner.partition_chunk(chunk, *partition_row_indexes));

    for (size_t i = 0; i < _source->get_sources().size(); ++i) {
        size_t from = partitioner.partition_begin_offset(i);
        size_t size = partitioner.partition_end_offset(i) - from;
        if (size == 0) {
            // No data for this partition.
            continue;
        }

        RETURN_IF_ERROR(_source->get_sources()[i]->add_chunk(chunk, partition_row_indexes, from, size,
                                                             partitioner.partition_memory_usage(i)));
    }
    return Status::OK();
}

Status BroadcastExchanger::accept(const ChunkPtr& chunk, const int32_t sink_driver_sequence) {
    for (auto* source : _source->get_sources()) {
        source->add_chunk(chunk);
    }
    return Status::OK();
}

Status PassthroughExchanger::accept(const ChunkPtr& chunk, const int32_t sink_driver_sequence) {
    size_t sources_num = _source->get_sources().size();
    if (sources_num == 1) {
        _source->get_sources()[0]->add_chunk(chunk);
    } else {
        _source->get_sources()[(_next_accept_source++) % sources_num]->add_chunk(chunk);
    }

    return Status::OK();
}

bool LocalExchanger::need_input() const {
    return !_memory_manager->is_full() && !is_all_sources_finished();
}
} // namespace starrocks::pipeline
