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

package com.starrocks.statistic;

import com.google.common.collect.Lists;
import com.google.gson.JsonArray;
import com.starrocks.common.DdlException;
import com.starrocks.common.util.SqlUtils;
import com.starrocks.sql.ast.ColumnDef;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.OriginStatement;
import com.starrocks.sql.ast.QualifiedName;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.TableRef;
import com.starrocks.sql.ast.ValuesRelation;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.ExprUtils;
import com.starrocks.sql.ast.expression.NullLiteral;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.thrift.TStatisticData;
import org.apache.commons.lang.StringUtils;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public final class HistogramStatisticsUtils {
    private HistogramStatisticsUtils() {
    }

    static String buildMcvJson(Map<String, String> mostCommonValues) {
        if (mostCommonValues.isEmpty()) {
            return null;
        }

        JsonArray mcvArray = new JsonArray();
        for (Map.Entry<String, String> entry : mostCommonValues.entrySet()) {
            JsonArray mcvEntry = new JsonArray();
            mcvEntry.add(entry.getKey());
            mcvEntry.add(entry.getValue());
            mcvArray.add(mcvEntry);
        }
        return mcvArray.toString();
    }

    static TStatisticData getSingleHistogramResult(
            List<TStatisticData> results, String columnName, String resultDescription) throws DdlException {
        if (results.size() != 1) {
            throw new DdlException("Expected exactly one " + resultDescription + " result for column " + columnName +
                    ", but got " + results.size());
        }
        return results.get(0);
    }

    static Expr buildBucketsLiteral(String buckets) {
        return isEmptyBuckets(buckets) ? new NullLiteral() : new StringLiteral(buckets);
    }

    static String buildBucketsSql(String buckets) {
        return isEmptyBuckets(buckets) ? "NULL" : quoteSqlString(buckets);
    }

    static String normalizeBucketsForHll(String buckets) {
        return StringUtils.isEmpty(buckets) ? "[]" : buckets;
    }

    private static boolean isEmptyBuckets(String buckets) {
        return StringUtils.isEmpty(buckets) || "[]".equals(buckets);
    }

    static String quoteSqlString(String value) {
        return "'" + SqlUtils.escapeSqlString(value) + "'";
    }

    static long utf8Length(String value) {
        return value.getBytes(StandardCharsets.UTF_8).length;
    }

    static long batchInsertPrefixSize(String tableName) {
        return utf8Length(buildBatchInsertPrefix(tableName)) + 1;
    }

    static String buildBatchInsertPrefix(String tableName) {
        List<String> targetColumnNames = buildStatsTargetColumnNames(tableName);
        return "INSERT INTO " + StatsConstants.STATISTICS_DB_NAME + "." + tableName +
                "(" + String.join(", ", targetColumnNames) + ") VALUES ";
    }

    static StatementBase createInsertStmt(String tableName, List<List<Expr>> rowsBuffer, String sql) {
        List<List<Expr>> rows = new ArrayList<>();
        for (List<Expr> row : rowsBuffer) {
            rows.add(ExprUtils.cloneList(row));
        }

        List<String> targetColumnNames = buildStatsTargetColumnNames(tableName);
        QueryStatement queryStatement = new QueryStatement(new ValuesRelation(rows, targetColumnNames));
        TableRef tableRef = new TableRef(
                QualifiedName.of(Lists.newArrayList(StatsConstants.STATISTICS_DB_NAME, tableName)),
                null, NodePosition.ZERO);
        InsertStmt insertStmt = new InsertStmt(tableRef, queryStatement);
        insertStmt.setTargetColumnNames(targetColumnNames);
        insertStmt.setOrigStmt(new OriginStatement(sql, 0));
        return insertStmt;
    }

    static List<String> buildStatsTargetColumnNames(String tableName) {
        return StatisticUtils.buildStatsColumnDef(tableName).stream()
                .map(ColumnDef::getName)
                .collect(Collectors.toList());
    }
}
