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

import com.starrocks.qe.SqlModeHelper;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.PlanTestNoneDBBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;

import java.util.List;

abstract class HistogramStatisticsCollectJobTestBase extends PlanTestNoneDBBase {
    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestNoneDBBase.beforeClass();
        starRocksAssert.withDatabase("test").useDatabase("test");
        starRocksAssert.withTable("CREATE TABLE `t0_stats` (\n" +
                "  `v1` bigint NULL,\n" +
                "  `v2` bigint NULL,\n" +
                "  `v3` bigint NULL,\n" +
                "  `v4` date NULL,\n" +
                "  `v5` datetime NULL,\n" +
                "  `v6` bigint NULL,\n" +
                "  `v7` varchar(20) NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`v1`, `v2`, `v3`)\n" +
                "DISTRIBUTED BY HASH(`v1`) BUCKETS 3\n" +
                "PROPERTIES (\"replication_num\" = \"1\");");
    }

    protected static void assertSqlLiteralRoundTrips(String expected, String escaped) {
        StatementBase statement = SqlParser.parseSingleStatement(
                "select '" + escaped + "'", SqlModeHelper.MODE_DEFAULT);
        SelectRelation selectRelation = (SelectRelation) ((QueryStatement) statement).getQueryRelation();
        StringLiteral literal = (StringLiteral) selectRelation.getSelectList().getItems().get(0).getExpr();
        Assertions.assertEquals(expected, literal.getStringValue());
    }

    protected static void assertSqlStatements(List<String> expected, List<String> actual) {
        Assertions.assertEquals(
                expected.stream().map(HistogramStatisticsCollectJobTestBase::normalizeSql).toList(),
                actual.stream().map(HistogramStatisticsCollectJobTestBase::normalizeSql).toList());
    }

    private static String normalizeSql(String sql) {
        return sql.replaceAll("\\s+", " ")
                .replaceAll("\\( ", "(")
                .replaceAll(" \\)", ")")
                .trim();
    }
}
