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
#include <unordered_map>

#include "column/chunk.h"
#include "connector/utils.h"
#include "exec/pipeline/exchange/shuffler.h"
#include "exprs/expr_context.h"
#include "gutil/hash/hash.h"
#include "util/runtime_profile.h"

namespace starrocks::pipeline {
Status Partitioner::partition_chunk(const ChunkPtr& chunk, int32_t num_partitions,
                                    std::vector<uint32_t>& partition_row_indexes) {
    size_t num_rows = chunk->num_rows();

    // step1: compute shuffle channel ids.
    RETURN_IF_ERROR(shuffle_channel_ids(chunk, num_partitions));

    // step2: shuffle chunk into dest partitions.
    {
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
    }
    return Status::OK();
}

Status Partitioner::send_chunk(const ChunkPtr& chunk,
                               const std::shared_ptr<std::vector<uint32_t>>& partition_row_indexes) {
    size_t num_partitions = _source->get_sources().size();
    for (size_t i = 0; i < num_partitions; ++i) {
        size_t from = partition_begin_offset(i);
        size_t size = partition_end_offset(i) - from;
        if (size == 0) {
            // No data for this partition.
            continue;
        }

        RETURN_IF_ERROR(_source->get_sources()[i]->add_chunk(chunk, partition_row_indexes, from, size,
                                                             partition_memory_usage(i)));
    }
    return Status::OK();
}

Status ShufflePartitioner::shuffle_channel_ids(const ChunkPtr& chunk, int32_t num_partitions) {
    size_t num_rows = chunk->num_rows();
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
    return Status::OK();
}

Status RandomPartitioner::shuffle_channel_ids(const ChunkPtr& chunk, int32_t num_partitions) {
    size_t num_rows = chunk->num_rows();
    _shuffle_channel_id.resize(num_rows, 0);
    {
        if (num_rows <= num_partitions) {
            std::iota(_shuffle_channel_id.begin(), _shuffle_channel_id.end(), 0);
        } else {
            size_t i = 0;
            for (; i < num_rows - num_partitions; i += num_partitions) {
                std::iota(_shuffle_channel_id.begin() + i, _shuffle_channel_id.begin() + i + num_partitions, 0);
            }
            if (i < num_rows - 1) {
                std::iota(_shuffle_channel_id.begin() + i, _shuffle_channel_id.end(), 0);
            }
        }
    }
    return Status::OK();
}

PartitionExchanger::PartitionExchanger(const std::shared_ptr<ChunkBufferMemoryManager>& memory_manager,
                                       LocalExchangeSourceOperatorFactory* source, const TPartitionType::type part_type,
                                       std::vector<ExprContext*> partition_expr_ctxs)
        : LocalExchanger(strings::Substitute("Partition($0)", to_string(part_type)), memory_manager, source),
          _part_type(part_type),
          _partition_exprs(std::move(partition_expr_ctxs)) {}

void PartitionExchanger::incr_sinker() {
    LocalExchanger::incr_sinker();
    _partitioners.emplace_back(std::make_unique<ShufflePartitioner>(_source, _part_type, _partition_exprs));
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

    size_t num_partitions = _source->get_sources().size();
    auto& partitioner = _partitioners[sink_driver_sequence];

    // Create a new partition_row_indexes here instead of reusing it in the partitioner.
    // The chunk and partition_row_indexes are cached in the queue of source operator,
    // and used later in pull_chunk() of source operator. If we reuse partition_row_indexes in partitioner,
    // it will be overwritten by the next time calling partitioner.partition_chunk().
    std::shared_ptr<std::vector<uint32_t>> partition_row_indexes = std::make_shared<std::vector<uint32_t>>(num_rows);
    RETURN_IF_ERROR(partitioner->partition_chunk(chunk, num_partitions, *partition_row_indexes));
    RETURN_IF_ERROR(partitioner->send_chunk(chunk, std::move(partition_row_indexes)));
    return Status::OK();
}

OrderedPartitionExchanger::OrderedPartitionExchanger(const std::shared_ptr<ChunkBufferMemoryManager>& memory_manager,
                                                     LocalExchangeSourceOperatorFactory* source,
                                                     std::vector<ExprContext*> partition_expr_ctxs)
        : LocalExchanger("OrderedPartition", memory_manager, source),
          _partition_exprs(std::move(partition_expr_ctxs)) {}

Status OrderedPartitionExchanger::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(LocalExchanger::prepare(state));
    RETURN_IF_ERROR(Expr::prepare(_partition_exprs, state));
    RETURN_IF_ERROR(Expr::open(_partition_exprs, state));
    return Status::OK();
}

void OrderedPartitionExchanger::close(RuntimeState* state) {
    Expr::close(_partition_exprs, state);
    LocalExchanger::close(state);
}

Status OrderedPartitionExchanger::accept(const ChunkPtr& chunk, const int32_t sink_driver_sequence) {
    DCHECK_EQ(sink_driver_sequence, 0);

    Columns partition_columns(_partition_exprs.size());
    for (size_t i = 0; i < partition_columns.size(); ++i) {
        ASSIGN_OR_RETURN(partition_columns[i], _partition_exprs[i]->evaluate(chunk.get()));
        DCHECK(partition_columns[i] != nullptr);
    }

    if (_channel_row_nums.empty()) {
        _channel_row_nums.resize(source_dop());
        _channel_row_nums.assign(source_dop(), 0);
    }

    std::vector<std::pair<size_t, ChunkPtr>> chunks;

    size_t min_channel_id = _find_min_channel_id();
    if (_previous_chunk == nullptr || _previous_channel_id == min_channel_id) {
        chunks.emplace_back(min_channel_id, chunk);
    } else {
        auto is_equal = [](const Columns& columns1, size_t offset1, const Columns& columns2, size_t offset2) {
            for (size_t i = 0; i < columns1.size(); ++i) {
                auto cmp = columns1[i]->compare_at(offset1, offset2, *columns2[i], 1);
                if (cmp != 0) {
                    return false;
                }
            }
            return true;
        };
        // Check if the joint of two consecutive chunks are the same
        bool is_joint_equal =
                is_equal(_previous_partition_columns, _previous_chunk->num_rows() - 1, partition_columns, 0);

        if (!is_joint_equal) {
            // The first row of current chunk is the start of a new partition, so
            // send the chunk to the channel with the minimum number of rows.
            chunks.emplace_back(min_channel_id, chunk);
        } else {
            bool is_current_of_same_partition =
                    is_equal(partition_columns, 0, partition_columns, chunk->num_rows() - 1);
            if (is_current_of_same_partition) {
                chunks.emplace_back(_previous_channel_id, chunk);
            } else {
                // Found partition end that belongs to the first row of current chunk, and split the chunk into two parts:
                // 1. The first part is the rows of the same partition as the last row of previous chunk, and send it to previous channel
                // 2. The second part is the rows of the different partition, and send it to the channel with the minimum number of rows.

                int64_t end = chunk->num_rows();
                for (auto& column : partition_columns) {
                    end = ColumnHelper::find_first_not_equal(column.get(), 0, 0, end);
                }
                // First part: [0, end)
                ChunkPtr first_part = chunk->clone_empty();
                first_part->append(*chunk, 0, end);
                chunks.emplace_back(_previous_channel_id, first_part);

                // Second part: [end, chunk->num_rows())
                ChunkPtr second_part = chunk->clone_empty();
                second_part->append(*chunk, end, chunk->num_rows() - end);
                chunks.emplace_back(min_channel_id, second_part);
            }
        }
    }

    for (auto& kv : chunks) {
        _channel_row_nums[kv.first] += kv.second->num_rows();
        _source->get_sources()[kv.first]->add_chunk(kv.second);
    }

    _previous_channel_id = chunks.back().first;
    _previous_chunk = chunk;
    _previous_partition_columns = std::move(partition_columns);

    return Status::OK();
}

size_t OrderedPartitionExchanger::_find_min_channel_id() {
    return std::distance(_channel_row_nums.begin(),
                         std::min_element(_channel_row_nums.begin(), _channel_row_nums.end()));
}

KeyPartitionExchanger::KeyPartitionExchanger(const std::shared_ptr<ChunkBufferMemoryManager>& memory_manager,
                                             LocalExchangeSourceOperatorFactory* source,
                                             std::vector<ExprContext*> partition_expr_ctxs, const size_t num_sinks)
        : LocalExchanger(strings::Substitute("KeyPartition"), memory_manager, source),
          _source(source),
          _partition_expr_ctxs(std::move(partition_expr_ctxs)) {}

Status KeyPartitionExchanger::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(LocalExchanger::prepare(state));
    RETURN_IF_ERROR(Expr::prepare(_partition_expr_ctxs, state));
    RETURN_IF_ERROR(Expr::open(_partition_expr_ctxs, state));
    return Status::OK();
}

void KeyPartitionExchanger::close(RuntimeState* state) {
    Expr::close(_partition_expr_ctxs, state);
    LocalExchanger::close(state);
}

Status KeyPartitionExchanger::accept(const ChunkPtr& chunk, const int32_t sink_driver_sequence) {
    size_t num_rows = chunk->num_rows();
    size_t source_op_cnt = _source->get_sources().size();

    if (num_rows == 0) {
        return Status::OK();
    }

    Columns partition_columns;
    for (size_t i = 0; i < _partition_expr_ctxs.size(); i++) {
        ASSIGN_OR_RETURN(auto partition_column, _partition_expr_ctxs[i]->evaluate(chunk.get()));
        partition_columns.push_back(std::move(partition_column));
    }

    std::unordered_map<std::vector<std::string>, std::vector<uint32_t>> key2indices;
    for (int i = 0; i < num_rows; i++) {
        std::vector<std::string> partition_key;
        for (int j = 0; j < partition_columns.size(); j++) {
            auto type = _partition_expr_ctxs[j]->root()->type();
            ASSIGN_OR_RETURN(auto partition_value, connector::HiveUtils::column_value(type, partition_columns[j], i));
            partition_key.push_back(std::move(partition_value));
        }
        key2indices[partition_key].push_back(i);
    }

    std::vector<uint32_t> hash_values(chunk->num_rows(), HashUtil::FNV_SEED);
    for (auto& column : partition_columns) {
        column->fnv_hash(hash_values.data(), 0, num_rows);
    }

    for (auto& [key, indices] : key2indices) {
        uint32_t shuffle_channel_id = hash_values[indices[0]] % source_op_cnt;
        auto partial_chunk = chunk->clone_empty_with_slot();
        partial_chunk->append_selective(*chunk, indices.data(), 0, indices.size());
        RETURN_IF_ERROR(_source->get_sources()[shuffle_channel_id]->add_chunk(key, std::move(partial_chunk)));
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

Status DirectThroughExchanger::accept(const ChunkPtr& chunk, const int32_t sink_driver_sequence) {
    size_t sources_num = _source->get_sources().size();
    _source->get_sources()[(sink_driver_sequence) % sources_num]->add_chunk(chunk);

    return Status::OK();
}

Status ConnectorSinkPassthroughExchanger::accept(const ChunkPtr& chunk, const int32_t sink_driver_sequence) {
    size_t sources_num = _source->get_sources().size();
    if (sources_num == 1) {
        _source->get_sources()[0]->add_chunk(chunk);
    } else {
        // Scale up writers when current buffer memory utilization is more than 50% of the maximum and data processed
        // is greater than current writer count * connector_sink_scaling_min_size. This also mean that we won't scale
        // local writers if the writing speed can cope up with incoming data. In another word, buffer utilization is
        // below 50%.
        if (_writer_count < sources_num && _memory_manager->is_half_full() &&
            _data_processed > _writer_count * config::writer_scaling_min_size_mb * 1024 * 1024) {
            _writer_count++;
        }
        // set to default value in case of _source vector out of bound in multi thread
        if (_writer_count > sources_num) {
            _writer_count = sources_num;
        }
        _source->get_sources()[(_next_accept_source++) % _writer_count.load()]->add_chunk(chunk);
        if (_writer_count < sources_num) {
            _data_processed += chunk->bytes_usage();
        }
    }

    return Status::OK();
}

bool LocalExchanger::need_input() const {
    return !_memory_manager->is_full() && !is_all_sources_finished();
}

void RandomPassthroughExchanger::incr_sinker() {
    LocalExchanger::incr_sinker();
    _random_partitioners.emplace_back(std::make_unique<RandomPartitioner>(_source));
}

Status RandomPassthroughExchanger::accept(const ChunkPtr& chunk, const int32_t sink_driver_sequence) {
    size_t num_rows = chunk->num_rows();
    if (num_rows == 0) {
        return Status::OK();
    }

    size_t num_partitions = _source->get_sources().size();

    auto& partitioner = _random_partitioners[sink_driver_sequence];
    std::shared_ptr<std::vector<uint32_t>> partition_row_indexes = std::make_shared<std::vector<uint32_t>>(num_rows);
    RETURN_IF_ERROR(partitioner->partition_chunk(chunk, num_partitions, *partition_row_indexes));
    RETURN_IF_ERROR(partitioner->send_chunk(chunk, std::move(partition_row_indexes)));
    return Status::OK();
}

void AdaptivePassthroughExchanger::incr_sinker() {
    LocalExchanger::incr_sinker();
    _random_partitioners.emplace_back(std::make_unique<RandomPartitioner>(_source));
}

Status AdaptivePassthroughExchanger::accept(const ChunkPtr& chunk, const int32_t sink_driver_sequence) {
    size_t num_rows = chunk->num_rows();
    if (num_rows == 0) {
        return Status::OK();
    }

    size_t num_partitions = _source->get_sources().size();

    // NOTE: When total chunk num is greater than num_partitions, passthrough chunk directyly, otherwise
    // random shuffle each rows for the input chunk to seperate it evenly.
    if (_is_pass_through_by_chunk) {
        if (num_partitions == 1) {
            _source->get_sources()[0]->add_chunk(chunk);
        } else {
            _source->get_sources()[(_next_accept_source++) % num_partitions]->add_chunk(chunk);
        }
    } else {
        // The first `num_partitions / 2` will pass through by rows to split more meanly, and if there are more
        // chunks, then pass through by chunks directly.
        if (_source_total_chunk_num++ > num_partitions / 2) {
            _is_pass_through_by_chunk = true;
        }

        auto& partitioner = _random_partitioners[sink_driver_sequence];
        std::shared_ptr<std::vector<uint32_t>> partition_row_indexes =
                std::make_shared<std::vector<uint32_t>>(num_rows);
        RETURN_IF_ERROR(partitioner->partition_chunk(chunk, num_partitions, *partition_row_indexes));
        RETURN_IF_ERROR(partitioner->send_chunk(chunk, std::move(partition_row_indexes)));
    }
    return Status::OK();
}

} // namespace starrocks::pipeline
