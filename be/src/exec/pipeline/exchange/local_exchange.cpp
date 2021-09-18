// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/pipeline/exchange/local_exchange.h"

#include "column/chunk.h"
#include "exprs/expr_context.h"

namespace starrocks::pipeline {

PartitionExchanger::PartitionExchanger(const std::shared_ptr<LocalExchangeMemoryManager>& memory_manager,
                                       LocalExchangeSourceOperatorFactory* source, bool is_shuffle,
                                       const std::vector<ExprContext*>& partition_expr_ctxs)
        : LocalExchanger(memory_manager),
          _source(source),
          _is_shuffle(is_shuffle),
          _partition_expr_ctxs(partition_expr_ctxs) {
    _partitions_columns.resize(partition_expr_ctxs.size());
}

Status PartitionExchanger::accept(const vectorized::ChunkPtr& chunk) {
    uint16_t num_rows = chunk->num_rows();
    if (num_rows == 0) {
        return Status::OK();
    }

    _memory_manager->update_row_count(chunk->num_rows());

    // hash-partition batch's rows across channels
    int num_channels = _source->get_sources().size();
    {
        // SCOPED_TIMER(_shuffle_hash_timer);
        for (size_t i = 0; i < _partitions_columns.size(); ++i) {
            _partitions_columns[i] = _partition_expr_ctxs[i]->evaluate(chunk.get());
            DCHECK(_partitions_columns[i] != nullptr);
        }

        if (_is_shuffle) {
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

        // compute row indexes for each channel
        _channel_row_idx_start_points.assign(num_channels + 1, 0);
        for (uint16_t i = 0; i < num_rows; ++i) {
            uint16_t channel_index = _hash_values[i] % num_channels;
            _channel_row_idx_start_points[channel_index]++;
            _hash_values[i] = channel_index;
        }
        // NOTE:
        // we make the last item equal with number of rows of this chunk
        for (int i = 1; i <= num_channels; ++i) {
            _channel_row_idx_start_points[i] += _channel_row_idx_start_points[i - 1];
        }

        for (int i = num_rows - 1; i >= 0; --i) {
            _row_indexes[_channel_row_idx_start_points[_hash_values[i]] - 1] = i;
            _channel_row_idx_start_points[_hash_values[i]]--;
        }
    }

    for (int i = 0; i < num_channels; ++i) {
        size_t from = _channel_row_idx_start_points[i];
        size_t size = _channel_row_idx_start_points[i + 1] - from;
        if (size == 0) {
            // no data for this channel continue;
            continue;
        }
        // TODO(kks): support bucket shuffle later
        // if (_channels[i]->get_fragment_instance_id().lo == -1) {
        //     // dest bucket is no used, continue
        //     continue;
        // }
        RETURN_IF_ERROR(_source->get_sources()[i]->add_chunk(chunk.get(), _row_indexes.data(), from, size));
    }
    return Status::OK();
}

Status BroadcastExchanger::accept(const vectorized::ChunkPtr& chunk) {
    _memory_manager->update_row_count(chunk->num_rows());
    for (auto* buffer : _source->get_sources()) {
        buffer->add_chunk(chunk);
    }
    return Status::OK();
}

Status PassthroughExchanger::accept(const vectorized::ChunkPtr& chunk) {
    _memory_manager->update_row_count(chunk->num_rows());
    _source->get_sources()[0]->add_chunk(chunk);
    return Status::OK();
}

bool LocalExchanger::need_input() const {
    return !_memory_manager->is_full();
}
} // namespace starrocks::pipeline