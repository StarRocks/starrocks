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

#include "common/config.h"

namespace starrocks::serde {

CompressStrategy::CompressStrategy() : _rd(), _gen(_rd()), _dis(0.0, 1.0) {}

void CompressStrategy::feedback(uint64_t uncompressed_bytes, uint64_t compressed_bytes, uint64_t serialization_time_ns,
                                uint64_t compression_time_ns) {
    if (uncompressed_bytes == 0 || compressed_bytes == 0) {
        return;
    }
    // TODO: consider the compression_time as reward factor
    double compress_ratio = (uncompressed_bytes + 1) / (compressed_bytes + 1);
    double reward_ratio = compress_ratio / config::lz4_expected_compression_ratio;
    if (reward_ratio > 1.0) {
        _alpha += reward_ratio * reward_ratio;
    } else {
        _beta += 1 / (reward_ratio * reward_ratio);
    }
}

bool CompressStrategy::decide() {
    double theta = _dis(_gen);
    double probability = _alpha / (_alpha + _beta);
    return theta < probability;
}

} // namespace starrocks::serde
