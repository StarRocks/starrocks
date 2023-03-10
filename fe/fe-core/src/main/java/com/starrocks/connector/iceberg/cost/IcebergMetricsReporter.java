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

package com.starrocks.connector.iceberg.cost;

import com.google.common.collect.Lists;
import org.apache.iceberg.metrics.MetricsReport;
import org.apache.iceberg.metrics.MetricsReporter;
import org.apache.iceberg.metrics.ScanReport;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class IcebergMetricsReporter implements MetricsReporter {
    private final List<MetricsReport> reports = Lists.newArrayList();
    private static final Logger LOG = LogManager.getLogger(IcebergMetricsReporter.class);

    @Override
    public void report(MetricsReport report) {
        reports.add(report);
        LOG.debug(String.format("Received metrics report: %s", report));
    }

    public IcebergScanReportWithCounter lastReport() {
        if (reports.isEmpty()) {
            return null;
        }
        return new IcebergScanReportWithCounter(reports.size(), (ScanReport) reports.get(reports.size() - 1));
    }

    public static class IcebergScanReportWithCounter {
        private final int count;
        private final ScanReport scanReport;
        IcebergScanReportWithCounter(int count, ScanReport scanReport) {
            this.count = count;
            this.scanReport = scanReport;
        }

        public int getCount() {
            return count;
        }

        public ScanReport getScanReport() {
            return scanReport;
        }
    }
}
