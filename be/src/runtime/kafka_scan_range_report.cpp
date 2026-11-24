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

#include "runtime/kafka_scan_range_report.h"

#include "gen_cpp/FrontendService_types.h"

namespace starrocks {

void KafkaScanRangeReport::merge(const std::map<int32_t, int64_t>& partition_cmt_offset,
                                 const std::map<int32_t, int64_t>& partition_cmt_offset_timestamp,
                                 int64_t received_bytes, int64_t received_rows, bool reached_end) {
    std::lock_guard<std::mutex> l(_lock);
    for (const auto& [partition, offset] : partition_cmt_offset) {
        _partition_cmt_offset[partition] = offset;
    }
    for (const auto& [partition, timestamp] : partition_cmt_offset_timestamp) {
        _partition_cmt_offset_timestamp[partition] = timestamp;
    }
    _received_bytes += received_bytes;
    _received_rows += received_rows;
    _reached_end = _reached_end && reached_end;
}

void KafkaScanRangeReport::to_thrift(TKafkaScanRangeReport* report) const {
    std::lock_guard<std::mutex> l(_lock);
    report->__set_partition_cmt_offset(_partition_cmt_offset);
    report->__set_partition_cmt_offset_timestamp(_partition_cmt_offset_timestamp);
    report->__set_received_bytes(_received_bytes);
    report->__set_received_rows(_received_rows);
    report->__set_reached_end(_reached_end);
}

} // namespace starrocks
