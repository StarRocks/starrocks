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
#include <map>
#include <mutex>

namespace starrocks {

class TKafkaScanRangeReport;

// What the Kafka scan ranges (TBrokerScanRange.kafka_scan_range) of one fragment instance consumed in a routine load
// round of the coordinated mode. Each scan range merges its result when its consumption ends, before the fragment
// can finish, and the final exec status report of the instance carries the merged result to the FE, which commits
// the round's transaction with these offsets.
class KafkaScanRangeReport {
public:
    // partition_cmt_offset maps a partition to the offset of its last consumed message. reached_end tells whether
    // the consumption ended with all its partitions at their end.
    void merge(const std::map<int32_t, int64_t>& partition_cmt_offset,
               const std::map<int32_t, int64_t>& partition_cmt_offset_timestamp, int64_t received_bytes,
               int64_t received_rows, bool reached_end);

    void to_thrift(TKafkaScanRangeReport* report) const;

private:
    mutable std::mutex _lock;
    std::map<int32_t, int64_t> _partition_cmt_offset;
    std::map<int32_t, int64_t> _partition_cmt_offset_timestamp;
    int64_t _received_bytes = 0;
    int64_t _received_rows = 0;
    // whether every scan range ended with all its partitions at their end
    bool _reached_end = true;
};

} // namespace starrocks
