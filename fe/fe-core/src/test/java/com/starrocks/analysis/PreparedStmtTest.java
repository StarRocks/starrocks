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

import com.starrocks.common.exception.AnalysisException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.PrepareStmtContext;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.sql.ast.PrepareStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.validate.ValidateException;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

public class PreparedStmtTest{
    private static ConnectContext ctx;
    private static StarRocksAssert starRocksAssert;
    private static String createTable = "CREATE TABLE `prepare_stmt` (\n" +
            "  `c0` varchar(24) NOT NULL COMMENT \"\",\n" +
            "  `c1` decimal128(24, 5) NOT NULL COMMENT \"\",\n" +
            "  `c2` decimal128(24, 2) NOT NULL COMMENT \"\"\n" +
            ") ENGINE=OLAP \n" +
            "DUPLICATE KEY(`c0`)\n" +
            "COMMENT \"OLAP\"\n" +
            "DISTRIBUTED BY HASH(`c0`) BUCKETS 1 \n" +
            "PROPERTIES (\n" +
            "\"replication_num\" = \"1\",\n" +
            "\"in_memory\" = \"false\",\n" +
            "\"storage_format\" = \"DEFAULT\",\n" +
            "\"enable_persistent_index\" = \"false\",\n" +
            "\"replicated_storage\" = \"true\",\n" +
            "\"compression\" = \"LZ4\"\n" +
            "); ";


    @BeforeClass
    public static void setUp() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        ctx = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(ctx);
        starRocksAssert.withDatabase("demo").useDatabase("demo");
        starRocksAssert.withTable(createTable);
    }

    @Test
    public void testParser() throws Exception {
        String sql1= "PREPARE stmt2 FROM select * from demo.prepare_stmt where k1 = ? and k2 = ?;";
        String sql2 = "PREPARE stmt3 FROM 'select * from demo.prepare_stmt';";
        String sql3 = "execute stmt3;";
        String sql4 = "execute stmt2 using @i;";

        PrepareStmt stmt1 = (PrepareStmt) UtFrameUtils.parseStmtWithNewParser(sql1, ctx);
        PrepareStmt stmt2 = (PrepareStmt) UtFrameUtils.parseStmtWithNewParser(sql2, ctx);
        Assert.assertEquals(2, stmt1.getParameters().size());
        Assert.assertEquals(0, stmt2.getParameters().size());
        Assert.assertThrows(StarRocksPlannerException.class, () -> UtFrameUtils.parseStmtWithNewParser(sql3, ctx));

        ctx.putPreparedStmt("stmt2", new PrepareStmtContext(stmt2, ctx, null));
        Assert.assertThrows(AnalysisException.class, () -> UtFrameUtils.parseStmtWithNewParser(sql4, ctx));
    }

    @Test
    public void testPrepareEnable() {
        ctx.getSessionVariable().setEnablePrepareStmt(false);
        String prepareSql = "PREPARE stmt1 FROM insert into demo.prepare_stmt values (?, ?, ?, ?);";
        String executeSql = "execute stmt1 using @i, @i;";
        Assert.assertThrows(StarRocksPlannerException.class, () -> starRocksAssert.query(prepareSql).explainQuery());
        Assert.assertThrows(StarRocksPlannerException.class, () -> starRocksAssert.query(executeSql).explainQuery());
        ctx.getSessionVariable().setEnablePrepareStmt(true);
        assertDoesNotThrow(() -> starRocksAssert.query(prepareSql));

        // TODO support forward leader for fe
        StatementBase statement = SqlParser.parse(prepareSql, ctx.getSessionVariable()).get(0);
        StmtExecutor executor = new StmtExecutor(ctx, statement);
        Assert.assertFalse(executor.isForwardToLeader());
    }

    @Test
    public void testPrepareStatementParser() {
        String sql = "PREPARE stmt1 FROM insert into demo.prepare_stmt values (?, ?, ?, ?);";
        assertThrows("Invalid statement type for prepared statement", ValidateException.class,
                () -> UtFrameUtils.parseStmtWithNewParser(sql, ctx));
    }
}
