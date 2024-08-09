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

#include "common/logging.h"

namespace starrocks::serde {

void CompressStrategy::feedback(uint64_t uncompressed_bytes, uint64_t compressed_bytes, uint64_t serialization_time_ns,
                                uint64_t compression_time_ns) {
    _sampled_data_bytes += uncompressed_bytes;
    if (uncompressed_bytes != compressed_bytes) {
        _uncompressed_bytes += uncompressed_bytes;
        _compressed_bytes += compressed_bytes;
        _serialization_time_ns += serialization_time_ns;
        _compression_time_ns += compression_time_ns;

        double ratio = uncompressed_bytes / (compressed_bytes + 1);
        bool positive = ratio >= _decision_bound;
        if (positive) {
            _positive_count++;
        }
        _compress_count++;
    }

    if (_sampled_data_bytes >= kSampleDataSize) {
        _sampled_data_bytes = 0;

        double new_decision_bound = kCompressionLowerBound;
        double good_decision_ratio = 1.0 * (_positive_count + 1) / (_compress_count + 1);
        new_decision_bound *= good_decision_ratio;
        new_decision_bound = std::max(new_decision_bound, kCompressionLowerBound);
        new_decision_bound = std::min(new_decision_bound, kCompressionUpperBound);

        int accumulate_steps = _uncompressed_bytes / kAccumulateDataStep;
        double processed_data_factor = std::pow(kAccumulateDataStepFactor, accumulate_steps);
        new_decision_bound /= processed_data_factor;
        new_decision_bound = std::max(new_decision_bound, kCompressionLowerBound);
        new_decision_bound = std::min(new_decision_bound, kCompressionUpperBound);
        _decision_bound = new_decision_bound;

        VLOG_ROW << "update compress decision_bound to " << _decision_bound;
    }
}

bool CompressStrategy::make_decision() const {
    double ratio = 1.0 * _uncompressed_bytes / (_compressed_bytes + 1);
    return ratio >= _decision_bound;
}

bool CompressStrategy::do_sample() const {
    return _compressed_bytes == 0 || _sampled_data_bytes >= kSampleDataSize;
}

} // namespace starrocks::serde
