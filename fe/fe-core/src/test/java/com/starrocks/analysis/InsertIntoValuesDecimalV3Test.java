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


package com.starrocks.analysis;

import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.common.Config;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.ValuesRelation;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.List;

public class InsertIntoValuesDecimalV3Test {
    private static StarRocksAssert starRocksAssert;

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    private static ConnectContext ctx;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        String createTblStmtStr =
                "CREATE TABLE if not exists test_table (\n" +
                        "\tcol_int INT NOT NULL, \n" +
                        "\tcol_decimal DECIMAL128(20, 9) NOT NULL \n" +
                        ") ENGINE=OLAP\n" +
                        "DUPLICATE KEY(`col_int`) \n" +
                        "COMMENT \"OLAP\" \n" +
                        "DISTRIBUTED BY HASH(`col_int`) BUCKETS 1 \n" +
                        "PROPERTIES( \"replication_num\" = \"1\", \"in_memory\" = \"false\")";

        ctx = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(ctx);
        starRocksAssert.withDatabase("db1").useDatabase("db1");
        starRocksAssert.withTable(createTblStmtStr);

        starRocksAssert.withTable("CREATE TABLE `tarray` (\n" +
                "  `v1` bigint NULL COMMENT \"\",\n" +
                "  `v2` bigint NULL COMMENT \"\",\n" +
                "  `v3` ARRAY<bigint(20)>  NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`v1`, `v2`)\n" +
                "DISTRIBUTED BY HASH(`v1`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");
    }

    @Before
    public void setUp() {
        ctx.setQueryId(UUIDUtil.genUUID());
        ctx.setExecutionId(UUIDUtil.toTUniqueId(ctx.getQueryId()));
    }

    @Test
    public void testInsertIntoValuesInvolvingDecimalV3() throws Exception {
        Config.enable_decimal_v3 = true;
        String sql1 = "INSERT INTO db1.test_table\n" +
                "  (col_int, col_decimal)\n" +
                "VALUES\n" +
                "  (\"3\", \"9180620.681794072\"),\n" +
                "  (4, 9180620.681794072),\n" +
                "  (5, 9180620),\n" +
                "  (6, 0.681794072),\n" +
                "  (\"99\", \"1724.069658963\");";
        InsertStmt stmt = (InsertStmt) UtFrameUtils.parseStmtWithNewParser(sql1, ctx);
        QueryStatement selectStmt = stmt.getQueryStatement();
        ExecPlan execPlan = new StatementPlanner().plan(stmt, ctx);
        for (List<Expr> exprs : ((ValuesRelation) selectStmt.getQueryRelation()).getRows()) {
            Assert.assertEquals(
                    exprs.get(1).getType(),
                    ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 20, 9));
        }
    }

    @Test
    public void testInsertArray() throws Exception {
        String sql = "insert into tarray values (1, 2, []) ";
        InsertStmt stmt = (InsertStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        ExecPlan execPlan = new StatementPlanner().plan(stmt, ctx);
        String plan = execPlan.getExplainString(TExplainLevel.NORMAL);

        Assert.assertTrue(plan.contains("constant exprs: \n" +
                "         1 | 2 | []"));
    }
}

