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

#include "util/metrics/catalog_scan_metrics.h"

namespace starrocks {

CatalogScanMetrics* CatalogScanMetrics::instance() {
    // Process-lifetime singleton: registered Metric objects keep back-pointers
    // to MetricRegistry, so avoid exit-time destruction after registry teardown.
    static auto* instance = new CatalogScanMetrics();
    return instance;
}

void CatalogScanMetrics::install(MetricRegistry* registry) {
    std::unique_lock lock(_mutex);
    if (_registry != nullptr) {
        DCHECK_EQ(_registry, registry);
        return;
    }
    _registry = registry;
}

CatalogScanMetrics::SingleCatalogMetrics* CatalogScanMetrics::_get_or_create_metrics(const std::string& catalog_type) {
    {
        std::shared_lock lock(_mutex);
        if (_registry == nullptr) {
            return nullptr;
        }
        auto it = _metrics_map.find(catalog_type);
        if (it != _metrics_map.end()) {
            return it->second.get();
        }
    }

    std::unique_lock lock(_mutex);
    // Double-check after acquiring exclusive lock.
    auto it = _metrics_map.find(catalog_type);
    if (it != _metrics_map.end()) {
        return it->second.get();
    }

    auto metrics = std::make_unique<SingleCatalogMetrics>();
    MetricLabels labels;
    labels.add("catalog_type", catalog_type);

    metrics->scan_bytes = std::make_unique<IntCounter>(MetricUnit::BYTES);
    _registry->register_metric("catalog_query_scan_bytes", labels, metrics->scan_bytes.get());

    metrics->scan_rows = std::make_unique<IntCounter>(MetricUnit::ROWS);
    _registry->register_metric("catalog_query_scan_rows", labels, metrics->scan_rows.get());

    metrics->files_scan_bytes_read = std::make_unique<IntCounter>(MetricUnit::BYTES);
    _registry->register_metric("catalog_files_scan_num_bytes_read", labels, metrics->files_scan_bytes_read.get());

    metrics->files_scan_rows_return = std::make_unique<IntCounter>(MetricUnit::ROWS);
    _registry->register_metric("catalog_files_scan_num_rows_return", labels, metrics->files_scan_rows_return.get());

    auto* ptr = metrics.get();
    _metrics_map.emplace(catalog_type, std::move(metrics));
    return ptr;
}

void CatalogScanMetrics::update_scan_bytes(const std::string& catalog_type, int64_t bytes) {
    if (auto* metrics = _get_or_create_metrics(catalog_type); metrics != nullptr) {
        metrics->scan_bytes->increment(bytes);
    }
}

void CatalogScanMetrics::update_scan_rows(const std::string& catalog_type, int64_t rows) {
    if (auto* metrics = _get_or_create_metrics(catalog_type); metrics != nullptr) {
        metrics->scan_rows->increment(rows);
    }
}

void CatalogScanMetrics::update_files_scan_bytes_read(const std::string& catalog_type, int64_t bytes) {
    if (auto* metrics = _get_or_create_metrics(catalog_type); metrics != nullptr) {
        metrics->files_scan_bytes_read->increment(bytes);
    }
}

void CatalogScanMetrics::update_files_scan_rows_return(const std::string& catalog_type, int64_t rows) {
    if (auto* metrics = _get_or_create_metrics(catalog_type); metrics != nullptr) {
        metrics->files_scan_rows_return->increment(rows);
    }
}

} // namespace starrocks
