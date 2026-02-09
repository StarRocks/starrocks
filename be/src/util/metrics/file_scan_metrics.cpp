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

#include "util/metrics/file_scan_metrics.h"

#include "exec/file_scanner/file_scanner.h"

namespace starrocks {

// NOTE:
// * avro_stream -> AvroScanner for routine load, purposely ignored here.
// * avro -> AvroCppScanner, Avro Files Scanner
static const std::vector<std::string> FILE_FORMATS = {"avro", "csv", "json", "orc", "parquet"};
static const std::vector<std::string> SCAN_TYPES = {"insert", "query", "load"};

FileScanMetrics::FileScanMetrics(MetricRegistry* registry) {
    for (const auto& file_format : FILE_FORMATS) {
        for (const auto& scan_type : SCAN_TYPES) {
            _register_metrics(registry, file_format, scan_type);
        }
    }
}

void FileScanMetrics::_register_metrics(MetricRegistry* registry, const std::string& file_format,
                                        const std::string& scan_type) {
    auto metrics = std::make_unique<SingleFileScanMetrics>();

    MetricLabels labels;
    labels.add("file_format", file_format);
    labels.add("scan_type", scan_type);

    metrics->file_read = std::make_unique<IntCounter>(MetricUnit::NOUNIT);
    registry->register_metric("files_scan_num_files_read", labels, metrics->file_read.get());

    metrics->bytes_read = std::make_unique<IntCounter>(MetricUnit::BYTES);
    registry->register_metric("files_scan_num_bytes_read", labels, metrics->bytes_read.get());

    metrics->raw_rows_read = std::make_unique<IntCounter>(MetricUnit::ROWS);
    registry->register_metric("files_scan_num_raw_rows_read", labels, metrics->raw_rows_read.get());

    metrics->valid_rows_read = std::make_unique<IntCounter>(MetricUnit::ROWS);
    registry->register_metric("files_scan_num_valid_rows_read", labels, metrics->valid_rows_read.get());

    metrics->rows_return = std::make_unique<IntCounter>(MetricUnit::ROWS);
    registry->register_metric("files_scan_num_rows_return", labels, metrics->rows_return.get());

    _metrics_map.emplace(std::make_pair(file_format, scan_type), std::move(metrics));
}

void FileScanMetrics::update(const std::string& file_format, const std::string& scan_type,
                             const ScannerCounter& counter) {
    auto it = _metrics_map.find({file_format, scan_type});
    if (it != _metrics_map.end()) {
        auto* metrics = it->second.get();
        metrics->file_read->increment(counter.num_files_read);
        metrics->bytes_read->increment(counter.num_bytes_read);

        // raw_rows = filtered_rows_read + num_rows_filtered
        int64_t raw_rows = counter.filtered_rows_read + counter.num_rows_filtered;
        metrics->raw_rows_read->increment(raw_rows);

        // valid_rows = unselected + returned
        int64_t valid_rows = counter.num_rows_unselected + counter.num_rows_read;
        metrics->valid_rows_read->increment(valid_rows);

        metrics->rows_return->increment(counter.num_rows_read);
    }
}

} // namespace starrocks
