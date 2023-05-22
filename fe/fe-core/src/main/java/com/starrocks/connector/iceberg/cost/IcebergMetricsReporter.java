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
import java.util.Optional;

public class IcebergMetricsReporter implements MetricsReporter {
    private static final Logger LOG = LogManager.getLogger(IcebergMetricsReporter.class);

    protected static ThreadLocal<IcebergMetricsReporter> threadLocalReporter = new ThreadLocal<>();
    private final List<MetricsReport> reports = Lists.newArrayList();

    public static IcebergMetricsReporter get() {
        return threadLocalReporter.get();
    }

    public static void remove() {
        threadLocalReporter.remove();
    }

    public void setThreadLocalReporter() {
        threadLocalReporter.set(this);
    }

    @Override
    public void report(MetricsReport report) {
        IcebergMetricsReporter reporter = get();
        if (reporter == null) {
            return;
        }

        reporter.reports.add(report);
        LOG.debug(String.format("Received metrics report: %s", report));
    }

    public static Optional<IcebergScanReportWithCounter> lastReport() {
        IcebergMetricsReporter reporter = get();
        if (reporter == null || reporter.reports.isEmpty()) {
            return Optional.empty();
        }

        int reportCount = reporter.reports.size();
        return Optional.of(new IcebergScanReportWithCounter(reportCount, (ScanReport) reporter.reports.get(reportCount - 1)));
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
