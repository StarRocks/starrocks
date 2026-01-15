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

import org.apache.iceberg.metrics.InMemoryMetricsReporter;
import org.apache.iceberg.metrics.MetricsReport;
import org.apache.iceberg.metrics.MetricsReporter;
import org.apache.iceberg.metrics.ScanReport;

public class IcebergMetricsReporter implements MetricsReporter {
    //for scan metrics, will be collected during scan plan.
    private InMemoryMetricsReporter scanMetricsReporter = new InMemoryMetricsReporter();

    @Override
    public void report(MetricsReport report) {
        if (report instanceof ScanReport) {
            scanMetricsReporter.report(report);
        } else {
            // Can implement other kinds of reporter & report if needed
        }
    }

    public ScanReport getScanReport() {
        return scanMetricsReporter.scanReport();
    }

}
