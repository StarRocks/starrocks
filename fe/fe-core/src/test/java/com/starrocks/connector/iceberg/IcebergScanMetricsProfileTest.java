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

package com.starrocks.connector.iceberg;

import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.metrics.CounterResult;
import org.apache.iceberg.metrics.ImmutableScanMetricsResult;
import org.apache.iceberg.metrics.ImmutableScanReport;
import org.apache.iceberg.metrics.MetricsContext;
import org.apache.iceberg.metrics.ScanReport;
import org.apache.iceberg.metrics.TimerResult;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class IcebergScanMetricsProfileTest {

    @Test
    public void testScanMetricsFields() {
        ScanReport report = ImmutableScanReport.builder()
                .tableName("iceberg.db1.t1")
                .snapshotId(42)
                .filter(Expressions.alwaysTrue())
                .schemaId(0)
                .projectedFieldNames(List.of("c1", "c3"))
                .scanMetrics(ImmutableScanMetricsResult.builder()
                        .totalPlanningDuration(TimerResult.of(TimeUnit.NANOSECONDS, Duration.ofMillis(16254), 1))
                        .resultDataFiles(CounterResult.of(MetricsContext.Unit.COUNT, 1))
                        .totalDataManifests(CounterResult.of(MetricsContext.Unit.COUNT, 4))
                        .scannedDataManifests(CounterResult.of(MetricsContext.Unit.COUNT, 3))
                        .totalFileSizeInBytes(CounterResult.of(MetricsContext.Unit.BYTES, 2048))
                        .build())
                .build();

        Map<String, String> fields = IcebergMetadata.scanMetricsFields(report);
        // in reading order, unreported counters left out
        Assertions.assertEquals(List.of("Table", "SnapshotId", "Filter", "ProjectedFieldNames", "TotalPlanningDuration",
                "ResultDataFiles", "TotalDataManifests", "ScannedDataManifests", "TotalFileSize"),
                List.copyOf(fields.keySet()));
        Assertions.assertEquals("iceberg.db1.t1", fields.get("Table"));
        Assertions.assertEquals("42", fields.get("SnapshotId"));
        Assertions.assertEquals("[c1, c3]", fields.get("ProjectedFieldNames"));
        Assertions.assertEquals("16s254ms", fields.get("TotalPlanningDuration"));
        Assertions.assertEquals("3", fields.get("ScannedDataManifests"));
        Assertions.assertEquals("2.000KB", fields.get("TotalFileSize"));
    }

    @Test
    public void testScanProfileGroup() {
        Assertions.assertEquals("t[node=3]", IcebergMetadata.scanProfileGroup("cat", "db1", "t", 3));
        // without a scan node, t of two databases must not share a group
        Assertions.assertEquals("t[cat/db1]", IcebergMetadata.scanProfileGroup("cat", "db1", "t", -1));
        Assertions.assertNotEquals(IcebergMetadata.scanProfileGroup("cat", "db1", "t", -1),
                IcebergMetadata.scanProfileGroup("cat", "db2", "t", -1));
    }
}
