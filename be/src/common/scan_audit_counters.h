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

#include <algorithm>
#include <cstdint>

namespace starrocks {

// Read counters of one scan node, reported per plan node id to the audit log.
//   physical_rows: rows read from storage after pruning, before deletes are applied
//   logical_rows:  physical_rows after deletes are applied
//   output_rows:   rows the scan node outputs after all predicates
//   remote_bytes:  physical bytes read from remote storage (object storage, HDFS, remote database, local disk)
//   cache_bytes:   physical bytes read from the DataCache
// A reader sets the bit of every counter it tracks in `mask`; a counter whose bit is unset is not applicable to
// it and is left out of the audit log.
struct ScanAuditCounters {
    enum Bit : uint32_t {
        PHYSICAL_ROWS = 1u << 0,
        LOGICAL_ROWS = 1u << 1,
        OUTPUT_ROWS = 1u << 2,
        REMOTE_BYTES = 1u << 3,
        CACHE_BYTES = 1u << 4,
    };
    static constexpr uint32_t ROWS_AND_BYTES = PHYSICAL_ROWS | LOGICAL_ROWS | REMOTE_BYTES | CACHE_BYTES;

    uint32_t mask = 0;
    int64_t physical_rows = 0;
    int64_t logical_rows = 0;
    int64_t output_rows = 0;
    int64_t remote_bytes = 0;
    int64_t cache_bytes = 0;

    bool empty() const {
        return mask == 0 && physical_rows == 0 && logical_rows == 0 && output_rows == 0 && remote_bytes == 0 &&
               cache_bytes == 0;
    }

    void add(const ScanAuditCounters& other) {
        mask |= other.mask;
        physical_rows += other.physical_rows;
        logical_rows += other.logical_rows;
        output_rows += other.output_rows;
        remote_bytes += other.remote_bytes;
        cache_bytes += other.cache_bytes;
    }

    // Growth of the cumulative counters `now` over `prev`. A reader that restarts its counters can make one go
    // back; such a counter grows by zero instead of going negative.
    static ScanAuditCounters delta(const ScanAuditCounters& now, const ScanAuditCounters& prev) {
        ScanAuditCounters d;
        d.mask = now.mask;
        d.physical_rows = std::max<int64_t>(0, now.physical_rows - prev.physical_rows);
        d.logical_rows = std::max<int64_t>(0, now.logical_rows - prev.logical_rows);
        d.output_rows = std::max<int64_t>(0, now.output_rows - prev.output_rows);
        d.remote_bytes = std::max<int64_t>(0, now.remote_bytes - prev.remote_bytes);
        d.cache_bytes = std::max<int64_t>(0, now.cache_bytes - prev.cache_bytes);
        return d;
    }
};

} // namespace starrocks
