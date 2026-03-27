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

import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.Table;
import com.starrocks.connector.metadata.iceberg.IcebergFilesTable;
import com.starrocks.connector.metadata.iceberg.LogicalIcebergMetadataTable;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.ast.QueryPeriod;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.ast.expression.StringLiteral;
import org.apache.iceberg.SnapshotRef;

import java.util.EnumSet;
import java.util.Optional;

public final class IcebergTimeTravelQueryAnalyzer {
    public enum TimeTravelType {
        BRANCH("branch"),
        TAG("tag"),
        SNAPSHOT("snapshot"),
        TIMESTAMP("timestamp");

        private final String metricLabel;

        TimeTravelType(String metricLabel) {
            this.metricLabel = metricLabel;
        }

        public String getMetricLabel() {
            return metricLabel;
        }
    }

    private IcebergTimeTravelQueryAnalyzer() {
    }

    public static EnumSet<TimeTravelType> collectTimeTravelTypes(StatementBase parsedStmt) {
        EnumSet<TimeTravelType> timeTravelTypes = EnumSet.noneOf(TimeTravelType.class);
        if (parsedStmt == null) {
            return timeTravelTypes;
        }

        AnalyzerUtils.collectTableRelations(parsedStmt).forEach(tableRelation ->
                resolveTimeTravelType(tableRelation.getTable(), tableRelation.getQueryPeriod())
                        .ifPresent(timeTravelTypes::add));
        return timeTravelTypes;
    }

    private static Optional<TimeTravelType> resolveTimeTravelType(Table table, QueryPeriod queryPeriod) {
        if (queryPeriod == null || !isTimeTravelTable(table)) {
            return Optional.empty();
        }
        if (queryPeriod.getPeriodType() == QueryPeriod.PeriodType.TIMESTAMP) {
            return Optional.of(TimeTravelType.TIMESTAMP);
        }
        if (queryPeriod.getEnd().isEmpty()) {
            return Optional.empty();
        }

        Expr versionExpr = queryPeriod.getEnd().get();
        if (versionExpr instanceof IntLiteral) {
            return Optional.of(TimeTravelType.SNAPSHOT);
        }
        if (!(versionExpr instanceof StringLiteral) || !(table instanceof IcebergTable)) {
            return Optional.empty();
        }

        org.apache.iceberg.Table nativeTable = ((IcebergTable) table).getNativeTable();
        if (nativeTable == null) {
            return Optional.empty();
        }

        SnapshotRef snapshotRef = nativeTable.refs().get(((StringLiteral) versionExpr).getStringValue());
        if (snapshotRef == null) {
            return Optional.empty();
        }
        if (snapshotRef.isBranch()) {
            return Optional.of(TimeTravelType.BRANCH);
        }
        if (snapshotRef.isTag()) {
            return Optional.of(TimeTravelType.TAG);
        }
        return Optional.empty();
    }

    private static boolean isTimeTravelTable(Table table) {
        return table instanceof IcebergTable
                || table instanceof LogicalIcebergMetadataTable
                || table instanceof IcebergFilesTable;
    }
}
