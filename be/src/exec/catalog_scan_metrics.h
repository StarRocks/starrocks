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
#include <shared_mutex>
#include <string>

#include "base/metrics.h"

namespace starrocks {

class CatalogScanMetrics {
public:
    CatalogScanMetrics() = default;
    explicit CatalogScanMetrics(MetricRegistry* registry) { install(registry); }
    ~CatalogScanMetrics() = default;

    static CatalogScanMetrics* instance();

    void install(MetricRegistry* registry);
    void update_scan_bytes(const std::string& catalog_type, int64_t bytes);
    void update_scan_rows(const std::string& catalog_type, int64_t rows);
    void update_files_scan_bytes_read(const std::string& catalog_type, int64_t bytes);
    void update_files_scan_rows_return(const std::string& catalog_type, int64_t rows);

private:
    struct SingleCatalogMetrics {
        std::unique_ptr<IntCounter> scan_bytes;
        std::unique_ptr<IntCounter> scan_rows;
        std::unique_ptr<IntCounter> files_scan_bytes_read;
        std::unique_ptr<IntCounter> files_scan_rows_return;
    };

    SingleCatalogMetrics* _get_or_create_metrics(const std::string& catalog_type);

    MetricRegistry* _registry = nullptr;
    std::shared_mutex _mutex;
    std::map<std::string, std::unique_ptr<SingleCatalogMetrics>> _metrics_map;
};

} // namespace starrocks
