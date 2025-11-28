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

import org.apache.iceberg.metrics.MetricsReport;
import org.apache.iceberg.metrics.ScanReport;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
public class IcebergScanMetricsTest {

    @Test
    void report_shouldDelegateScanReports_andReturnLatest() {
        IcebergMetricsReporter reporter = new IcebergMetricsReporter();

        Assertions.assertNull(reporter.getScanReport(), "expect null before any report");

        ScanReport scan1 = Mockito.mock(ScanReport.class);
        reporter.report(scan1);
        Assertions.assertSame(scan1, reporter.getScanReport(), "should return the last ScanReport");

        ScanReport scan2 = Mockito.mock(ScanReport.class);
        reporter.report(scan2);
        Assertions.assertSame(scan2, reporter.getScanReport(), "should overwrite with the latest ScanReport");
    }

    @Test
    void report_shouldIgnoreNonScanReports() {
        IcebergMetricsReporter reporter = new IcebergMetricsReporter();

        ScanReport baseline = Mockito.mock(ScanReport.class);
        reporter.report(baseline);
        Assertions.assertSame(baseline, reporter.getScanReport());

        MetricsReport nonScan = Mockito.mock(MetricsReport.class);
        reporter.report(nonScan);

        Assertions.assertSame(baseline, reporter.getScanReport(), "non-Scan report should be ignored");
    }
}
