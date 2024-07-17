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

package com.starrocks.sql.optimizer.rewrite;

import com.google.common.collect.Lists;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Type;
import com.starrocks.common.Config;
import com.starrocks.common.ErrorReportException;
import com.starrocks.common.FeConstants;
import com.starrocks.leader.ReportHandler;
import com.starrocks.load.pipe.filelist.RepoExecutor;
import com.starrocks.memory.MemoryUsageTracker;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.sql.optimizer.function.MetaFunctions;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.thrift.TResultBatch;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.List;

public class MetaFunctionsTest {

    static {
        MemoryUsageTracker.registerMemoryTracker("Report", new ReportHandler());
    }

    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void beforeClass() throws Exception {
        FeConstants.runningUnitTest = true;
        Config.alter_scheduler_interval_millisecond = 100;
        Config.dynamic_partition_enable = true;
        Config.dynamic_partition_check_interval_seconds = 1;
        Config.enable_strict_storage_medium_check = false;
        UtFrameUtils.createMinStarRocksCluster();
        UtFrameUtils.addMockBackend(10002);
        UtFrameUtils.addMockBackend(10003);
        // create connect context
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
                        "PROPERTIES('replication_num' = '1');")
                .withTable("CREATE EXTERNAL TABLE mysql_external_table\n" +
                        "(\n" +
                        "    k1 DATE,\n" +
                        "    k2 INT,\n" +
                        "    k3 SMALLINT,\n" +
                        "    k4 VARCHAR(2048),\n" +
                        "    k5 DATETIME\n" +
                        ")\n" +
                        "ENGINE=mysql\n" +
                        "PROPERTIES\n" +
                        "(\n" +
                        "    \"host\" = \"127.0.0.1\",\n" +
                        "    \"port\" = \"3306\",\n" +
                        "    \"user\" = \"mysql_user\",\n" +
                        "    \"password\" = \"mysql_passwd\",\n" +
                        "    \"database\" = \"mysql_db_test\",\n" +
                        "    \"table\" = \"mysql_table_test\"\n" +
                        ");");
    }

    @Test
    public void testInspectMemory() {
        MetaFunctions.inspectMemory(new ConstantOperator("report", Type.VARCHAR));
    }

    @Test(expected = SemanticException.class)
    public void testInspectMemoryFailed() {
        MetaFunctions.inspectMemory(new ConstantOperator("abc", Type.VARCHAR));
    }

    @Test
    public void testInspectMemoryDetail() {
        MemoryUsageTracker.registerMemoryTracker("Report", new ReportHandler());
        try {
            MetaFunctions.inspectMemoryDetail(
                    new ConstantOperator("abc", Type.VARCHAR),
                    new ConstantOperator("def", Type.VARCHAR));
            Assert.fail();
        } catch (Exception ex) {
        }
        try {
            MetaFunctions.inspectMemoryDetail(
                    new ConstantOperator("report", Type.VARCHAR),
                    new ConstantOperator("def", Type.VARCHAR));
            Assert.fail();
        } catch (Exception ex) {
        }
        try {
            MetaFunctions.inspectMemoryDetail(
                    new ConstantOperator("report", Type.VARCHAR),
                    new ConstantOperator("reportHandler.abc", Type.VARCHAR));
            Assert.fail();
        } catch (Exception ex) {
        }
        MetaFunctions.inspectMemoryDetail(
                new ConstantOperator("report", Type.VARCHAR),
                new ConstantOperator("reportHandler", Type.VARCHAR));
        MetaFunctions.inspectMemoryDetail(
                new ConstantOperator("report", Type.VARCHAR),
                new ConstantOperator("reportHandler.reportQueue", Type.VARCHAR));
    }

    private UserIdentity testUser = UserIdentity.createAnalyzedUserIdentWithIp("test_user", "%");

    @Test(expected = ErrorReportException.class)
    public void testInspectTableAccessDeniedException() {
        connectContext.setCurrentUserIdentity(testUser);
        connectContext.setCurrentRoleIds(testUser);
        MetaFunctions.inspectTable(new TableName("test", "tbl1"));
    }

    @Test(expected = ErrorReportException.class)
    public void testInspectExternalTableAccessDeniedException() {
        connectContext.setCurrentUserIdentity(testUser);
        connectContext.setCurrentRoleIds(testUser);
        MetaFunctions.inspectTable(new TableName("test", "mysql_external_table"));
    }

    private String lookupString(String tableName, String key, String column) {
        ConstantOperator res = MetaFunctions.lookupString(
                ConstantOperator.createVarchar(tableName),
                ConstantOperator.createVarchar(key),
                ConstantOperator.createVarchar(column)
        );
        if (res.isNull()) {
            return null;
        }
        return res.toString();
    }

    @Test
    public void testLookupString() throws Exception {
        // exceptions:
        // 1. table not found
        // 2. column not found
        // 3. key not exists
        {
            Exception e = Assert.assertThrows(SemanticException.class, () ->
                    lookupString("t1", "v1", "c1")
            );
            Assert.assertEquals("Getting analyzing error. Detail message: lookup failed: " +
                    "Getting analyzing error. Detail message: dict table t1 is not found..", e.getMessage());
        }
        {
            starRocksAssert.withTable("create table t1(c1 string, c2 bigint) duplicate key(c1) " +
                    "properties('replication_num'='1')");
            Exception e = Assert.assertThrows(SemanticException.class, () ->
                    lookupString("t1", "v1", "c1")
            );
            Assert.assertEquals("Getting analyzing error. Detail message: lookup failed: " +
                            "Getting analyzing error. Detail message: dict table test.t1 should be primary key table..",
                    e.getMessage());
            starRocksAssert.dropTable("t1");
        }
        {
            starRocksAssert.withTable("create table t1(c1 string, c2 bigint auto_increment) primary key(c1) " +
                    "properties('replication_num'='1')");
            Assert.assertNull(lookupString("t1", "v1", "c1"));

            // normal
            new MockUp<RepoExecutor>() {
                @Mock
                public List<TResultBatch> executeDQL(String sql) {
                    MetaFunctions.LookupRecord record = new MetaFunctions.LookupRecord();
                    record.data = Lists.newArrayList("v1");
                    String json = GsonUtils.GSON.toJson(record);

                    TResultBatch resultBatch = new TResultBatch();
                    ByteBuffer buffer = ByteBuffer.wrap(json.getBytes());
                    resultBatch.setRows(Lists.newArrayList(buffer));
                    return Lists.newArrayList(resultBatch);
                }
            };
            Assert.assertEquals("v1", lookupString("t1", "v1", "c1"));

            // record not found
            new MockUp<RepoExecutor>() {
                @Mock
                public List<TResultBatch> executeDQL(String sql) {
                    throw new RuntimeException("query failed if record not exist in dict table");
                }
            };
            Assert.assertNull(lookupString("t1", "v1", "c1"));
        }

    }
}
