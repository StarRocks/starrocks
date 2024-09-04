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

namespace starrocks::serde {

// Adaptive compression strategy:
// Sample a few chunks to test the compression ratio, if the result is positive
// keep compressing more chunks. Otherwise stop compressing.
// Decision formula:
//  - if compression_ratio >= decision_threshold, choose to do compression
//  - compression_ratio = uncompressed_bytes/compressed_bytes
//  - decision_threshold = baseline / good_decision_ratio / processed_data_factor
//  - baseline = 3.0 by default
//  - good_decision_ratio = positive_decision_count / total_decision_count
//  - processed_data_factor = 1 + 3 * steps/(steps+140)
// The idea is:
// 1. If keeping making bad decision, let's raise the threshould
// 2. If processing more data, let's reduce the threshold
class CompressStrategy {
public:
    CompressStrategy() = default;
    ~CompressStrategy() = default;

    // Give the feedback based on previous compression
    void feedback(uint64_t uncompressed_bytes, uint64_t compressed_bytes, uint64_t serialization_time_ns,
                  uint64_t compression_time_ns);

    // Make the decision for the next compression
    bool make_decision() const;

    // Do sample every kSampleDataSize of uncompressed data
    bool do_sample() const;

private:
    static constexpr int kSampleDataSize = 4 << 20;       // sample every 4MB data
    static constexpr int kAccumulateDataStep = 128 << 20; // every 128MB is a step
    // When steps=10, factor=1.2; When steps=100, factor=2.25
    static constexpr int kAccumulateDataStepAlpha = 140;
    static constexpr double kAccumulateFactorMax = 3.0;
    static constexpr double kCompressionThreshold = 3.0;
    static constexpr double kCompressionLowerBound = 2.0;
    static constexpr double kCompressionUpperBound = 10.0;

    int _compress_count = 0;
    int _positive_count = 0;
    double _decision_bound = kCompressionThreshold;
    uint64_t _sampled_data_bytes = 0;
    uint64_t _uncompressed_bytes = 0;
    uint64_t _compressed_bytes = 0;
};
} // namespace starrocks::serde