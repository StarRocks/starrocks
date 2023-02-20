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

#pragma once

#include <cstdint>
#include <vector>

#include "gen_cpp/Partitions_types.h"
#include "util/hash_util.hpp"

namespace starrocks::pipeline {
class Shuffler {
public:
    Shuffler(bool compatibility, bool is_two_level_shuffle, TPartitionType::type partition_type, size_t num_channels,
             int32_t num_shuffles_per_channel)
            : _part_type(partition_type),
              _num_channels(num_channels),
              _num_shuffles_per_channel(num_shuffles_per_channel) {
        if (_part_type == TPartitionType::HASH_PARTITIONED && !compatibility) {
            if (is_two_level_shuffle) {
                _exchange_shuffle = &Shuffler::exchange_shuffle<true, ReduceOp>;
            } else {
                _exchange_shuffle = &Shuffler::exchange_shuffle<false, ReduceOp>;
            }
        } else {
            if (is_two_level_shuffle) {
                _exchange_shuffle = &Shuffler::exchange_shuffle<true, ModuloOp>;
            } else {
                _exchange_shuffle = &Shuffler::exchange_shuffle<false, ModuloOp>;
            }
        }

        if (_part_type == TPartitionType::HASH_PARTITIONED && !compatibility) {
            _local_exchange_shuffle = &Shuffler::local_exchange_shuffle<ReduceOp>;
        } else {
            _local_exchange_shuffle = &Shuffler::local_exchange_shuffle<ModuloOp>;
        }
    }

    void exchange_shuffle(std::vector<uint32_t>& shuffle_channel_ids, const std::vector<uint32_t>& hash_values,
                          size_t num_rows) {
        (this->*_exchange_shuffle)(shuffle_channel_ids, hash_values, num_rows);
    }

    void local_exchange_shuffle(std::vector<uint32_t>& shuffle_channel_ids, std::vector<uint32_t>& hash_values,
                                size_t num_rows) {
        (this->*_local_exchange_shuffle)(shuffle_channel_ids, hash_values, num_rows);
    }

private:
    void (Shuffler::*_exchange_shuffle)(std::vector<uint32_t>& shuffle_channel_ids,
                                        const std::vector<uint32_t>& hash_values, size_t num_rows) = nullptr;

    void (Shuffler::*_local_exchange_shuffle)(std::vector<uint32_t>& shuffle_channel_ids,
                                              std::vector<uint32_t>& hash_values, size_t num_rows) = nullptr;

    template <bool two_level_shuffle, typename ReduceOp>
    void exchange_shuffle(std::vector<uint32_t>& shuffle_channel_ids, const std::vector<uint32_t>& hash_values,
                          size_t num_rows) {
        for (size_t i = 0; i < num_rows; ++i) {
            size_t channel_id = ReduceOp()(hash_values[i], _num_channels);
            size_t shuffle_id;
            if constexpr (!two_level_shuffle) {
                shuffle_id = channel_id;
            } else {
                uint32_t driver_sequence = ReduceOp()(HashUtil::xorshift32(hash_values[i]), _num_shuffles_per_channel);
                shuffle_id = channel_id * _num_shuffles_per_channel + driver_sequence;
            }
            shuffle_channel_ids[i] = shuffle_id;
        }
    }

    // When the local exchange is the successor of ExchangeSourceOperator,
    // the data flow is `exchange sink -> exchange source -> local exchange`.
    // `exchange sink -> exchange source` (phase 1) determines which fragment instance to deliver each row,
    // while `exchange source -> local exchange` (phase 2) determines which pipeline driver to deliver each row.
    // To avoid hash two times and data skew due to hash one time, phase 1 hashes one time
    // and phase 2 applies xorshift32 on the hash value.
    // Note that xorshift32 rehash must be applied for both local shuffle here and exchange sink.
    template <typename ReduceOp>
    void local_exchange_shuffle(std::vector<uint32_t>& shuffle_channel_ids, std::vector<uint32_t>& hash_values,
                                size_t num_rows) {
        for (int32_t i = 0; i < num_rows; ++i) {
            uint32_t driver_sequence = ReduceOp()(HashUtil::xorshift32(hash_values[i]), _num_channels);
            shuffle_channel_ids[i] = driver_sequence;
        }
    }

    const TPartitionType::type _part_type = TPartitionType::UNPARTITIONED;
    const size_t _num_channels;
    const size_t _num_shuffles_per_channel;
};
} // namespace starrocks::pipeline
