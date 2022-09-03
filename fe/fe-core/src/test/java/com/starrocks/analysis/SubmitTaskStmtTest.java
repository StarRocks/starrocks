// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.analysis;

import com.starrocks.common.FeConstants;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.SubmitTaskStmt;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Map;

public class SubmitTaskStmtTest {

    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void beforeClass() throws Exception {
        FeConstants.runningUnitTest = true;
        UtFrameUtils.createMinStarRocksCluster();

        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);

        starRocksAssert.withDatabase("test").useDatabase("test")
                .withTable("CREATE TABLE test.tbl1\n" +
                        "(\n" +
                        "    k1 date,\n" +
                        "    k2 int,\n" +
                        "    v1 int sum\n" +
                        ")\n" +
                        "PARTITION BY RANGE(k1)\n" +
                        "(\n" +
                        "    PARTITION p1 values less than('2020-02-01'),\n" +
                        "    PARTITION p2 values less than('2020-03-01')\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                        "PROPERTIES('replication_num' = '1');");
    }

    @Test
    public void BasicSubmitStmtTest() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        ctx.setExecutionId(UUIDUtil.toTUniqueId(UUIDUtil.genUUID()));
        String submitSQL = "submit task as create table temp as select count(*) as cnt from tbl1";
        SubmitTaskStmt submitTaskStmt = (SubmitTaskStmt) UtFrameUtils.parseStmtWithNewParser(submitSQL, ctx);

        Assert.assertEquals(submitTaskStmt.getDbName(), "test");
        Assert.assertNull(submitTaskStmt.getTaskName());
        Assert.assertEquals(submitTaskStmt.getProperties().size(), 0);

        String submitSQL2 = "submit /*+ SET_VAR(query_timeout = 1) */ task as " +
                "create table temp as select count(*) as cnt from tbl1";

        SubmitTaskStmt submitTaskStmt2 = (SubmitTaskStmt) UtFrameUtils.parseStmtWithNewParser(submitSQL2, ctx);
        Assert.assertEquals(submitTaskStmt2.getDbName(), "test");
        Assert.assertNull(submitTaskStmt2.getTaskName());
        Assert.assertEquals(submitTaskStmt2.getProperties().size(), 1);
        Map<String, String> properties = submitTaskStmt2.getProperties();
        for (String key : properties.keySet()) {
            Assert.assertEquals(key, "query_timeout");
            Assert.assertEquals(properties.get(key), "1");
        }

        String submitSQL3 = "submit task task_name as create table temp as select count(*) as cnt from tbl1";
        SubmitTaskStmt submitTaskStmt3 = (SubmitTaskStmt) UtFrameUtils.parseStmtWithNewParser(submitSQL3, ctx);

        Assert.assertEquals(submitTaskStmt3.getDbName(), "test");
        Assert.assertEquals(submitTaskStmt3.getTaskName(), "task_name");
        Assert.assertEquals(submitTaskStmt3.getProperties().size(), 0);

        String submitSQL4 = "submit task test.task_name as create table temp as select count(*) as cnt from tbl1";
        SubmitTaskStmt submitTaskStmt4 = (SubmitTaskStmt) UtFrameUtils.parseStmtWithNewParser(submitSQL4, ctx);

        Assert.assertEquals(submitTaskStmt4.getDbName(), "test");
        Assert.assertEquals(submitTaskStmt4.getTaskName(), "task_name");
        Assert.assertEquals(submitTaskStmt4.getProperties().size(), 0);
    }

}
