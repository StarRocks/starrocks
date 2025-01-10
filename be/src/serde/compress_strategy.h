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
#include <random>

namespace starrocks::serde {

// Compression strategy based on Thompson Sampling
class CompressStrategy {
public:
    CompressStrategy();
    ~CompressStrategy() = default;

    // Give the feedback based on previous compression
    void feedback(uint64_t uncompressed_bytes, uint64_t compressed_bytes, uint64_t serialization_time_ns,
                  uint64_t compression_time_ns);

    // Make the decision for the next compression
    bool decide();

private:
    std::mt19937 _gen;

    // Thompson sampling parameters, biased for TRUE value
    double _alpha = 3.0; // Success count
    double _beta = 1.0;  // Failure count
};

} // namespace starrocks::serde