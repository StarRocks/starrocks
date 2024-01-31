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

import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ExpressionUtil;
import org.apache.iceberg.metrics.MetricsReport;
import org.apache.iceberg.metrics.MetricsReporter;
import org.apache.iceberg.metrics.ScanReport;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class IcebergMetricsReporter implements MetricsReporter {
    private final Map<ScanMetricsFilter, ScanReport> reports = new ConcurrentHashMap<>();

    @Override
    public void report(MetricsReport report) {
        if (report instanceof ScanReport) {
            ScanReport scanReport = (ScanReport) report;
            String tableName = scanReport.tableName();
            long snapshotId = scanReport.snapshotId();
            Expression predicate = scanReport.filter();
            ScanMetricsFilter filter = new ScanMetricsFilter(tableName, predicate, snapshotId);
            reports.put(filter, scanReport);
        }
    }

    public Optional<ScanReport> getReporter(String catalogName, String dbName, String tableName,
                                                              long snapshotId, Expression icebergPredicate) {
        if (reports.isEmpty()) {
            return Optional.empty();
        }

        ScanMetricsFilter filter = ScanMetricsFilter.from(catalogName, dbName, tableName, snapshotId, icebergPredicate);

        ScanReport report = reports.get(filter);
        return Optional.ofNullable(report);
    }

    public void clear() {
        reports.clear();
    }

    private static class ScanMetricsFilter {
        String icebergTableName;
        Expression predicate;
        long snapshotId;

        static ScanMetricsFilter from(String catalogName, String dbName, String tableName,
                                      long snapshotId, Expression icebergPredicate) {
            String icebergTableName = catalogName + '.' + dbName + "." + tableName;
            Expression sanitizeExpr = ExpressionUtil.sanitize(icebergPredicate);
            return new ScanMetricsFilter(icebergTableName, sanitizeExpr, snapshotId);
        }

        public ScanMetricsFilter(String tableName, Expression predicate, long snapshotId) {
            this.icebergTableName = tableName;
            this.predicate = predicate;
            this.snapshotId = snapshotId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ScanMetricsFilter filter = (ScanMetricsFilter) o;
            return snapshotId == filter.snapshotId &&
                    Objects.equals(icebergTableName, filter.icebergTableName) &&
                    ExpressionUtil.toSanitizedString(predicate).equalsIgnoreCase(
                            ExpressionUtil.toSanitizedString(filter.predicate));
        }

        @Override
        public int hashCode() {
            return Objects.hash(icebergTableName, snapshotId);
        }
    }
}
