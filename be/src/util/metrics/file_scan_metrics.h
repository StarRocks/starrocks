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

#include <map>
#include <memory>
#include <string>

#include "base/metrics.h"

namespace starrocks {

struct ScannerCounter;

class FileScanMetrics {
public:
    FileScanMetrics(MetricRegistry* registry);
    ~FileScanMetrics() = default;

    void update(const std::string& file_format, const std::string& scan_type, const ScannerCounter& counter);

private:
    struct SingleFileScanMetrics {
        std::unique_ptr<IntCounter> file_read;
        std::unique_ptr<IntCounter> bytes_read;
        std::unique_ptr<IntCounter> raw_rows_read;
        std::unique_ptr<IntCounter> valid_rows_read;
        std::unique_ptr<IntCounter> rows_return;
    };

    void _register_metrics(MetricRegistry* registry, const std::string& file_format, const std::string& scan_type);

    // Key is <file_format, scan_type>
    std::map<std::pair<std::string, std::string>, std::unique_ptr<SingleFileScanMetrics>> _metrics_map;
};

} // namespace starrocks
