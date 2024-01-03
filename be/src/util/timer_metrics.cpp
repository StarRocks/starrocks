// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "timer_metrics.h"

#include <queue>

#include "starrocks_metrics.h"

namespace starrocks {

// MaxHeap
struct Compare {
    bool operator()(const std::pair<TTabletId, int64_t>& lhs, const std::pair<TTabletId, int64_t>& rhs) const {
        return lhs.second > rhs.second;
    }
};

std::string TimerMetrics::printScanTabletBytes() {
    std::priority_queue<std::pair<TTabletId, int64_t>, std::vector<std::pair<TTabletId, int64_t>>, Compare>
            scan_bytes_pq;

    std::stringstream res;
    res << "# TYPE tablet_scan_bytes counter"
        << "\n";
    // Scan bytes
    for (const auto& pair : StarRocksMetrics::instance()->tablet_scan_bytes) {
        scan_bytes_pq.push(pair);
        if (scan_bytes_pq.size() > 10) {
            scan_bytes_pq.pop();
        }
    }
    while (!scan_bytes_pq.empty()) {
        res << formatOutput("tablet_scan_bytes", "tablet_id", scan_bytes_pq.top().first, scan_bytes_pq.top().second);
        scan_bytes_pq.pop();
    }

    return res.str();
}

std::string TimerMetrics::printScanTabletNum() {
    std::priority_queue<std::pair<TTabletId, int64_t>, std::vector<std::pair<TTabletId, int64_t>>, Compare> scan_num_pq;

    std::stringstream res;
    res << "# TYPE tablet_scan_num counter"
        << "\n";
    // Scan nums
    for (const auto& pair : StarRocksMetrics::instance()->tablet_scan_nums) {
        scan_num_pq.push(pair);
        if (scan_num_pq.size() > 10) {
            scan_num_pq.pop();
        }
    }
    while (!scan_num_pq.empty()) {
        res << formatOutput("tablet_scan_num", "tablet_id", scan_num_pq.top().first, scan_num_pq.top().second);
        scan_num_pq.pop();
    }

    return res.str();
}

std::string TimerMetrics::formatOutput(const std::string& name, const std::string& labelName, long label, long value) {
    std::stringstream output;
    //name{labelName="label"} value
    output << name;
    if (!labelName.empty()) {
        output << "{" << labelName << "=\"" << label << "\"}";
    }
    output << " " << value << "\n";
    return output.str();
}

std::string TimerMetrics::formatOutput(const std::string& name, const std::string& labelName, const std::string& label,
                                       long value) {
    std::stringstream output;
    //name{labelName="label"} value
    output << name;
    if (!labelName.empty()) {
        output << "{" << labelName << "=\"" << label << "\"}";
    }
    output << " " << value << "\n";
    return output.str();
}

std::string TimerMetrics::doTimerMetrics() {
    std::stringstream ss;
    if (!StarRocksMetrics::instance()->tablet_scan_bytes.empty()) {
        ss << printScanTabletBytes();
        StarRocksMetrics::instance()->tablet_scan_bytes.clear();
    }
    if (!StarRocksMetrics::instance()->tablet_scan_nums.empty()) {
        ss << printScanTabletNum();
        StarRocksMetrics::instance()->tablet_scan_nums.clear();
    }
    return ss.str();
}
} // namespace starrocks