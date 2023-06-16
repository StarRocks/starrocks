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

package com.starrocks.sql.ast;

import com.starrocks.catalog.Column;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowExecutor;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

public class DescribeStmtTest {

    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;


    @BeforeClass
    public static void beforeClass() throws Exception {
        FeConstants.runningUnitTest = true;
        Config.enable_experimental_mv = true;
        Config.alter_scheduler_interval_millisecond = 100;
        Config.dynamic_partition_enable = true;
        Config.dynamic_partition_check_interval_seconds = 1;
        Config.enable_strict_storage_medium_check = false;
        UtFrameUtils.createMinStarRocksCluster();
        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);

        starRocksAssert.withDatabase("test").useDatabase("test")
                .withTable("CREATE TABLE sales_records(\n" +
                        "    record_id INT,\n" +
                        "    seller_id INT,\n" +
                        "    store_id INT,\n" +
                        "    sale_date DATE,\n" +
                        "    sale_amt BIGINT\n" +
                        ") DISTRIBUTED BY HASH(record_id)\n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\",\n" +
                        "\"in_memory\" = \"false\"\n" +
                        ");")
                .withMaterializedView("CREATE MATERIALIZED VIEW store_amt AS\n" +
                        "SELECT store_id, SUM(sale_amt)\n" +
                        "FROM sales_records\n" +
                        "GROUP BY store_id;")
                .withMaterializedView("CREATE MATERIALIZED VIEW store_amt_async\n" +
                        "DISTRIBUTED BY HASH(`store_id`) BUCKETS 10 \n" +
                        "REFRESH ASYNC\n" +
                        " AS\n" +
                        "SELECT store_id, SUM(sale_amt) as sale_amt\n" +
                        "FROM sales_records\n" +
                        "GROUP BY store_id;");
    }

    @AfterClass
    public static void tearDown() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        String dropSQL = "drop table sales_records";
        try {
            DropTableStmt dropTableStmt = (DropTableStmt) UtFrameUtils.parseStmtWithNewParser(dropSQL, ctx);
            GlobalStateMgr.getCurrentState().dropTable(dropTableStmt);
        } catch (Exception ex) {

        }
    }

    @Test
    public void testDescTable() throws Exception {
        String destTableSql = "desc sales_records";
        DescribeStmt describeStmt = (DescribeStmt) UtFrameUtils.parseStmtWithNewParser(destTableSql,
                starRocksAssert.getCtx());
        ShowExecutor showExecutor = new ShowExecutor(connectContext, describeStmt);
        ShowResultSet execute = showExecutor.execute();
        List<Column> columns = execute.getMetaData().getColumns();
        Assert.assertEquals(6, columns.size());
        Assert.assertEquals("Field", columns.get(0).getName());
        Assert.assertEquals("Type", columns.get(1).getName());
        Assert.assertEquals("Null", columns.get(2).getName());
        Assert.assertEquals("Key", columns.get(3).getName());
        Assert.assertEquals("Default", columns.get(4).getName());
        Assert.assertEquals("Extra", columns.get(5).getName());

        List<List<String>> resultRows = execute.getResultRows();
        Assert.assertEquals("record_id", resultRows.get(0).get(0));
        Assert.assertEquals("int", resultRows.get(0).get(1));
        Assert.assertEquals("YES", resultRows.get(0).get(2));

        Assert.assertEquals("sale_date", resultRows.get(3).get(0));
        Assert.assertEquals("date", resultRows.get(3).get(1));
        Assert.assertEquals("YES", resultRows.get(3).get(2));

    }

    @Test
    public void testDescTableAll() throws Exception {
        String destTableSql = "desc sales_records all";
        DescribeStmt describeStmt = (DescribeStmt) UtFrameUtils.parseStmtWithNewParser(destTableSql,
                starRocksAssert.getCtx());
        ShowExecutor showExecutor = new ShowExecutor(connectContext, describeStmt);
        ShowResultSet execute = showExecutor.execute();
        List<Column> columns = execute.getMetaData().getColumns();
        Assert.assertEquals(8, columns.size());
        Assert.assertEquals("IndexName", columns.get(0).getName());
        Assert.assertEquals("IndexKeysType", columns.get(1).getName());
        Assert.assertEquals("Field", columns.get(2).getName());
        Assert.assertEquals("Type", columns.get(3).getName());
        Assert.assertEquals("Null", columns.get(4).getName());
        Assert.assertEquals("Key", columns.get(5).getName());
        Assert.assertEquals("Default", columns.get(6).getName());
        Assert.assertEquals("Extra", columns.get(7).getName());

        List<List<String>> resultRows = execute.getResultRows();
        Assert.assertEquals("record_id", resultRows.get(0).get(2));
        Assert.assertEquals("int", resultRows.get(0).get(3));
        Assert.assertEquals("YES", resultRows.get(0).get(4));

        Assert.assertEquals("sale_date", resultRows.get(3).get(2));
        Assert.assertEquals("date", resultRows.get(3).get(3));
        Assert.assertEquals("YES", resultRows.get(3).get(4));
    }

    @Test
    public void testDescSyncMv() throws Exception {
        String destTableSql = "desc store_amt";
        DescribeStmt describeStmt = (DescribeStmt) UtFrameUtils.parseStmtWithNewParser(destTableSql,
                starRocksAssert.getCtx());
        ShowExecutor showExecutor = new ShowExecutor(connectContext, describeStmt);
        ShowResultSet execute = showExecutor.execute();
        List<Column> columns = execute.getMetaData().getColumns();
        Assert.assertEquals(6, columns.size());
        Assert.assertEquals("Field", columns.get(0).getName());
        Assert.assertEquals("Type", columns.get(1).getName());
        Assert.assertEquals("Null", columns.get(2).getName());
        Assert.assertEquals("Key", columns.get(3).getName());
        Assert.assertEquals("Default", columns.get(4).getName());
        Assert.assertEquals("Extra", columns.get(5).getName());

        List<List<String>> resultRows = execute.getResultRows();
        Assert.assertEquals("store_id", resultRows.get(0).get(0));
        Assert.assertEquals("int", resultRows.get(0).get(1));
        Assert.assertEquals("YES", resultRows.get(0).get(2));

        Assert.assertEquals("mv_sum_sale_amt", resultRows.get(1).get(0));
        Assert.assertEquals("bigint", resultRows.get(1).get(1));
        Assert.assertEquals("YES", resultRows.get(1).get(2));
    }

    @Test
    public void testDescSyncMvAll() throws Exception {
        String destTableSql = "desc store_amt all";
        DescribeStmt describeStmt = (DescribeStmt) UtFrameUtils.parseStmtWithNewParser(destTableSql,
                starRocksAssert.getCtx());
        ShowExecutor showExecutor = new ShowExecutor(connectContext, describeStmt);
        ShowResultSet execute = showExecutor.execute();
        List<Column> columns = execute.getMetaData().getColumns();
        Assert.assertEquals(6, columns.size());
        Assert.assertEquals("Field", columns.get(0).getName());
        Assert.assertEquals("Type", columns.get(1).getName());
        Assert.assertEquals("Null", columns.get(2).getName());
        Assert.assertEquals("Key", columns.get(3).getName());
        Assert.assertEquals("Default", columns.get(4).getName());
        Assert.assertEquals("Extra", columns.get(5).getName());

        List<List<String>> resultRows = execute.getResultRows();
        Assert.assertEquals("store_id", resultRows.get(0).get(0));
        Assert.assertEquals("int", resultRows.get(0).get(1));
        Assert.assertEquals("YES", resultRows.get(0).get(2));

        Assert.assertEquals("mv_sum_sale_amt", resultRows.get(1).get(0));
        Assert.assertEquals("bigint", resultRows.get(1).get(1));
        Assert.assertEquals("YES", resultRows.get(1).get(2));
    }

    @Test
    public void testDescAsyncMv() throws Exception {
        String destTableSql = "desc store_amt_async";
        DescribeStmt describeStmt = (DescribeStmt) UtFrameUtils.parseStmtWithNewParser(destTableSql,
                starRocksAssert.getCtx());
        ShowExecutor showExecutor = new ShowExecutor(connectContext, describeStmt);
        ShowResultSet execute = showExecutor.execute();
        List<Column> columns = execute.getMetaData().getColumns();
        Assert.assertEquals(6, columns.size());
        Assert.assertEquals("Field", columns.get(0).getName());
        Assert.assertEquals("Type", columns.get(1).getName());
        Assert.assertEquals("Null", columns.get(2).getName());
        Assert.assertEquals("Key", columns.get(3).getName());
        Assert.assertEquals("Default", columns.get(4).getName());
        Assert.assertEquals("Extra", columns.get(5).getName());

        List<List<String>> resultRows = execute.getResultRows();
        Assert.assertEquals("store_id", resultRows.get(0).get(0));
        Assert.assertEquals("int", resultRows.get(0).get(1));
        Assert.assertEquals("YES", resultRows.get(0).get(2));

        Assert.assertEquals("sale_amt", resultRows.get(1).get(0));
        Assert.assertEquals("bigint", resultRows.get(1).get(1));
        Assert.assertEquals("YES", resultRows.get(1).get(2));
    }

    @Test
    public void testDescAsyncMvAll() throws Exception {
        String destTableSql = "desc store_amt_async all";
        DescribeStmt describeStmt = (DescribeStmt) UtFrameUtils.parseStmtWithNewParser(destTableSql,
                starRocksAssert.getCtx());
        ShowExecutor showExecutor = new ShowExecutor(connectContext, describeStmt);
        ShowResultSet execute = showExecutor.execute();
        List<Column> columns = execute.getMetaData().getColumns();
        Assert.assertEquals(8, columns.size());
        Assert.assertEquals("IndexName", columns.get(0).getName());
        Assert.assertEquals("IndexKeysType", columns.get(1).getName());
        Assert.assertEquals("Field", columns.get(2).getName());
        Assert.assertEquals("Type", columns.get(3).getName());
        Assert.assertEquals("Null", columns.get(4).getName());
        Assert.assertEquals("Key", columns.get(5).getName());
        Assert.assertEquals("Default", columns.get(6).getName());
        Assert.assertEquals("Extra", columns.get(7).getName());

        List<List<String>> resultRows = execute.getResultRows();
        Assert.assertEquals("store_id", resultRows.get(0).get(2));
        Assert.assertEquals("int", resultRows.get(0).get(3));
        Assert.assertEquals("YES", resultRows.get(0).get(4));

        Assert.assertEquals("sale_amt", resultRows.get(1).get(2));
        Assert.assertEquals("bigint", resultRows.get(1).get(3));
        Assert.assertEquals("YES", resultRows.get(1).get(4));
    }
}
