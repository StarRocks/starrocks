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

package com.starrocks.sql.analyzer;

import com.starrocks.analysis.Expr;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.catalog.Type;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.optimizer.operator.ColumnFilterConverter;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class AnalyzerUtilsTest {

    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void beforeClass() throws Exception {
        FeConstants.runningUnitTest = true;
        Config.dynamic_partition_enable = false;
        UtFrameUtils.createMinStarRocksCluster();
        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);

        starRocksAssert.withDatabase("test").useDatabase("test")
                .withTable("CREATE TABLE `bill_detail` (\n" +
                        "  `bill_code` varchar(200) NOT NULL DEFAULT \"\" COMMENT \"\"\n" +
                        ") ENGINE=OLAP \n" +
                        "PRIMARY KEY(`bill_code`)\n" +
                        "COMMENT \"OLAP\"\n" +
                        "DISTRIBUTED BY HASH(`bill_code`) BUCKETS 10 \n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\"\n" +
                        ");");
    }

    @Test
    public void testGetFormatPartitionValue() {
        Assert.assertEquals("_11", AnalyzerUtils.getFormatPartitionValue("-11"));
        Assert.assertEquals("20200101", AnalyzerUtils.getFormatPartitionValue("2020-01-01"));
        Assert.assertEquals("676d5dde", AnalyzerUtils.getFormatPartitionValue("杭州"));
    }

    @Test
    public void testRewritePredicate() throws Exception {
        String sql = "select cast(substr(bill_code, 3, 13) as bigint) from bill_detail;";
        ConnectContext ctx = starRocksAssert.getCtx();
        QueryStatement queryStatement = (QueryStatement) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Expr expr = queryStatement.getQueryRelation().getOutputExpression().get(0);
        ColumnRefOperator columnRefOperator = new ColumnRefOperator(1, Type.VARCHAR, "bill_code", false);
        ConstantOperator constantOperator = new ConstantOperator("JT2921712368984", Type.VARCHAR);
        boolean success = ColumnFilterConverter.rewritePredicate(expr, columnRefOperator, constantOperator);
        Assert.assertTrue(success);
        Expr shouldReplaceExpr = expr.getChild(0).getChild(0);
        Assert.assertTrue(shouldReplaceExpr instanceof StringLiteral);
    }

}
