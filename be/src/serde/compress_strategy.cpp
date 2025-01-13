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

#include "serde/compress_strategy.h"

#include <cmath>
#include <random>

#include "common/config.h"

namespace starrocks::serde {

CompressStrategy::CompressStrategy() : _gen(std::random_device()()) {}

void CompressStrategy::feedback(uint64_t uncompressed_bytes, uint64_t compressed_bytes, uint64_t serialization_time_ns,
                                uint64_t compression_time_ns) {
    if (uncompressed_bytes == 0 || compressed_bytes == 0 || compression_time_ns == 0) {
        return;
    }
    double compress_speed = uncompressed_bytes / compression_time_ns * (1e9 / 1024 / 1024); // MB/s
    double compress_ratio = uncompressed_bytes / compressed_bytes;
    double reward_ratio = (compress_ratio / config::lz4_expected_compression_ratio) *
                          (compress_speed / config::lz4_expected_compression_speed_mbps);
    if (reward_ratio > 1.0) {
        _alpha += 1;
    } else {
        _beta += 1;
    }
}

bool CompressStrategy::decide() {
    std::gamma_distribution<double> gamma_alpha(_alpha, 1.0);
    std::gamma_distribution<double> gamma_beta(_beta, 1.0);

    double sample_alpha = gamma_alpha(_gen);
    double sample_beta = gamma_beta(_gen);

    double theta = sample_alpha / (sample_alpha + sample_beta);
    return theta > 0.5;
}

} // namespace starrocks::serde
