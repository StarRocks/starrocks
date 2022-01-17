// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exec/pipeline/exchange/local_exchange.h"

#include "column/chunk.h"
#include "exprs/expr_context.h"

namespace starrocks::pipeline {

Status PartitionExchanger::Partitioner::partition_chunk(const vectorized::ChunkPtr& chunk,
                                                        std::vector<uint32_t>& partition_row_indexes) {
    int32_t num_rows = chunk->num_rows();
    int32_t num_partitions = _source->get_sources().size();

    for (size_t i = 0; i < _partitions_columns.size(); ++i) {
        _partitions_columns[i] = _partition_expr_ctxs[i]->evaluate(chunk.get());
        DCHECK(_partitions_columns[i] != nullptr);
    }

    // Compute hash for each partition column
    if (_part_type == TPartitionType::HASH_PARTITIONED) {
        _hash_values.assign(num_rows, HashUtil::FNV_SEED);
        for (const vectorized::ColumnPtr& column : _partitions_columns) {
            column->fnv_hash(&_hash_values[0], 0, num_rows);
        }
    } else {
        // The data distribution was calculated using CRC32_HASH,
        // and bucket shuffle need to use the same hash function when sending data
        _hash_values.assign(num_rows, 0);
        for (const vectorized::ColumnPtr& column : _partitions_columns) {
            column->crc32_hash(&_hash_values[0], 0, num_rows);
        }
    }

    // Compute row indexes for each channel.
    _partition_row_indexes_start_points.assign(num_partitions + 1, 0);
    for (int32_t i = 0; i < num_rows; ++i) {
        size_t partition_index = _hash_values[i] % num_partitions;
        _partition_row_indexes_start_points[partition_index]++;
        _hash_values[i] = partition_index;
    }
    // We make the last item equal with number of rows of this chunk.
    for (int32_t i = 1; i <= num_partitions; ++i) {
        _partition_row_indexes_start_points[i] += _partition_row_indexes_start_points[i - 1];
    }

    for (int32_t i = num_rows - 1; i >= 0; --i) {
        partition_row_indexes[_partition_row_indexes_start_points[_hash_values[i]] - 1] = i;
        _partition_row_indexes_start_points[_hash_values[i]]--;
    }

    return Status::OK();
}

PartitionExchanger::PartitionExchanger(const std::shared_ptr<LocalExchangeMemoryManager>& memory_manager,
                                       LocalExchangeSourceOperatorFactory* source, const TPartitionType::type part_type,
                                       const std::vector<ExprContext*>& partition_expr_ctxs, const size_t num_sinks)
        : LocalExchanger("Partition", memory_manager, source) {
    _partitioners.reserve(num_sinks);
    for (size_t i = 0; i < num_sinks; i++) {
        _partitioners.emplace_back(source, part_type, partition_expr_ctxs);
    }
}

Status PartitionExchanger::accept(const vectorized::ChunkPtr& chunk, const int32_t sink_driver_sequence) {
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
    partitioner.partition_chunk(chunk, *partition_row_indexes);

    for (size_t i = 0; i < _source->get_sources().size(); ++i) {
        size_t from = partitioner.partition_begin_offset(i);
        size_t size = partitioner.partition_end_offset(i) - from;
        if (size == 0) {
            // No data for this partition.
            continue;
        }

        RETURN_IF_ERROR(_source->get_sources()[i]->add_chunk(chunk, partition_row_indexes, from, size));
    }
    return Status::OK();
}

Status BroadcastExchanger::accept(const vectorized::ChunkPtr& chunk, const int32_t sink_driver_sequence) {
    for (auto* source : _source->get_sources()) {
        source->add_chunk(chunk);
    }
    return Status::OK();
}

Status PassthroughExchanger::accept(const vectorized::ChunkPtr& chunk, const int32_t sink_driver_sequence) {
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
