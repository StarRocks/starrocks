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
import com.starrocks.catalog.Type;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.ErrorReportException;
import com.starrocks.leader.ReportHandler;
import com.starrocks.memory.MemoryUsageTracker;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.SimpleExecutor;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.expression.TableName;
import com.starrocks.sql.optimizer.function.MetaFunctions;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVTestBase;
import com.starrocks.thrift.TResultBatch;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer.MethodName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertThrows;

@TestMethodOrder(MethodName.class)
public class MetaFunctionsTest extends MVTestBase {

    static {
        MemoryUsageTracker.registerMemoryTracker("Report", new ReportHandler());
    }

    @BeforeAll
    public static void beforeClass() throws Exception {
        MVTestBase.beforeClass();
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

    @Test
    public void testInspectMemoryFailed() {
        assertThrows(SemanticException.class, () -> MetaFunctions.inspectMemory(new ConstantOperator("abc", Type.VARCHAR)));
    }

    @Test
    public void testInspectMemoryDetail() {
        MemoryUsageTracker.registerMemoryTracker("Report", new ReportHandler());
        try {
            MetaFunctions.inspectMemoryDetail(
                    new ConstantOperator("abc", Type.VARCHAR),
                    new ConstantOperator("def", Type.VARCHAR));
            Assertions.fail();
        } catch (Exception ex) {
        }
        try {
            MetaFunctions.inspectMemoryDetail(
                    new ConstantOperator("report", Type.VARCHAR),
                    new ConstantOperator("def", Type.VARCHAR));
            Assertions.fail();
        } catch (Exception ex) {
        }
        try {
            MetaFunctions.inspectMemoryDetail(
                    new ConstantOperator("report", Type.VARCHAR),
                    new ConstantOperator("reportHandler.abc", Type.VARCHAR));
            Assertions.fail();
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

    @Test
    public void testInspectTableAccessDeniedException() {
        UserIdentity currentUserIdentity = connectContext.getCurrentUserIdentity();
        Set<Long> currentRoleIds = connectContext.getCurrentRoleIds();
        assertThrows(ErrorReportException.class, () -> {
            connectContext.setCurrentUserIdentity(testUser);
            connectContext.setCurrentRoleIds(testUser);
            connectContext.setThreadLocalInfo();
            MetaFunctions.inspectTable(new TableName("test", "tbl1"));
        });
        connectContext.setCurrentUserIdentity(currentUserIdentity);
        connectContext.setCurrentRoleIds(currentRoleIds);
    }

    @Test
    public void testInspectExternalTableAccessDeniedException() {
        UserIdentity currentUserIdentity = connectContext.getCurrentUserIdentity();
        Set<Long> currentRoleIds = connectContext.getCurrentRoleIds();
        assertThrows(ErrorReportException.class, () -> {
            connectContext.setCurrentUserIdentity(testUser);
            connectContext.setCurrentRoleIds(testUser);
            connectContext.setThreadLocalInfo();
            MetaFunctions.inspectTable(new TableName("test", "mysql_external_table"));
        });
        connectContext.setCurrentUserIdentity(currentUserIdentity);
        connectContext.setCurrentRoleIds(currentRoleIds);
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
            Exception e = Assertions.assertThrows(SemanticException.class, () ->
                    lookupString("t1", "v1", "c1")
            );
            Assertions.assertEquals("Getting analyzing error. Detail message: Unknown table 'test.t1'.",
                    e.getMessage());
        }
        {
            starRocksAssert.withTable("create table t1(c1 string, c2 bigint) duplicate key(c1) " +
                    "properties('replication_num'='1')");
            Exception e = Assertions.assertThrows(SemanticException.class, () ->
                    lookupString("t1", "v1", "c1")
            );
            Assertions.assertEquals("Getting analyzing error. Detail message: " +
                            "Invalid parameter must be PRIMARY_KEY.", e.getMessage());
            starRocksAssert.dropTable("t1");
        }
        {
            starRocksAssert.withTable("create table t1(c1 string, c2 bigint auto_increment) primary key(c1) " +
                    "properties('replication_num'='1')");
            Assertions.assertNull(lookupString("t1", "v1", "c1"));

            // normal
            new MockUp<SimpleExecutor>() {
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
            Assertions.assertEquals("v1", lookupString("t1", "v1", "c1"));

            // record not found
            new MockUp<SimpleExecutor>() {
                @Mock
                public List<TResultBatch> executeDQL(String sql) {
                    throw new RuntimeException("query failed if record not exist in dict table");
                }
            };
            Assertions.assertNull(lookupString("t1", "v1", "c1"));
        }
    }

    @Test
    public void inspectMVRefreshInfoReturnsValidJsonForMaterializedView() throws Exception {
        starRocksAssert.withRefreshedMaterializedView("create materialized view mv1 distributed by random " +
                "as select k1, sum(v1) from test.tbl1 group by k1");
        ConstantOperator result = MetaFunctions.inspectMVRefreshInfo(ConstantOperator.createVarchar("test.mv1"));
        Assertions.assertNotNull(result);
        Assertions.assertTrue(result.getVarchar().contains("tableToUpdatePartitions"));
        starRocksAssert.dropMaterializedView("mv1");
    }

    @Test
    public void inspectMVRefreshInfoThrowsExceptionForNonMaterializedView() {
        assertThrows(SemanticException.class, () -> {
            starRocksAssert.withTable("create table tbl2(k1 int, v1 int) properties('replication_num'='1')");
            MetaFunctions.inspectMVRefreshInfo(ConstantOperator.createVarchar("test.tbl2"));
            starRocksAssert.dropTable("tbl2");
        });
    }

    @Test
    public void inspectTablePartitionInfoReturnsValidJsonForOlapTable() throws Exception {
        starRocksAssert.withTable("create table tbl3(k1 int, v1 int) partition by range(k1) " +
                "(partition p1 values less than('10'), partition p2 values less than('20')) " +
                "properties('replication_num'='1')");
        ConstantOperator result = MetaFunctions.inspectTablePartitionInfo(ConstantOperator.createVarchar("test.tbl3"));
        Assertions.assertNotNull(result);
        Assertions.assertTrue(result.getVarchar().contains("p1"));
        Assertions.assertTrue(result.getVarchar().contains("p2"));
        starRocksAssert.dropTable("tbl3");
    }

    @Test
    public void inspectTablePartitionInfoThrowsExceptionForInvalidTable() {
        assertThrows(SemanticException.class,
                () -> MetaFunctions.inspectTablePartitionInfo(ConstantOperator.createVarchar("test.invalid_table")));
    }

    @Test
    public void inspectMVRefreshInfoHandlesEmptyBaseTables() throws Exception {
        starRocksAssert.withMaterializedView("create materialized view mv_empty distributed by random " +
                "   as select k1 from test.tbl1 group by k1");
        ConstantOperator result = MetaFunctions.inspectMVRefreshInfo(ConstantOperator.createVarchar("test.mv_empty"));
        Assertions.assertNotNull(result);
        Assertions.assertTrue(result.getVarchar().contains("{}")); // Ensure empty base tables are handled
        starRocksAssert.dropMaterializedView("mv_empty");
    }

    @Test
    public void inspectMVRefreshInfoThrowsExceptionForNullInput() {
        assertThrows(SemanticException.class, () -> MetaFunctions.inspectMVRefreshInfo(null));
    }

    @Test
    public void inspectTablePartitionInfoHandlesEmptyPartitions() throws Exception {
        starRocksAssert.withTable("create table empty_partition_table(k1 int, v1 int) properties('replication_num'='1')");
        ConstantOperator result = MetaFunctions.inspectTablePartitionInfo(
                ConstantOperator.createVarchar("test.empty_partition_table"));
        Assertions.assertNotNull(result);
        Assertions.assertTrue(result.getVarchar().contains("empty_partition_table"));
        starRocksAssert.dropTable("empty_partition_table");
    }

    @Test
    public void inspectTablePartitionInfoThrowsExceptionForNullInput() {
        assertThrows(SemanticException.class, () -> MetaFunctions.inspectTablePartitionInfo(null));
    }

    @Test
    public void inspectTablePartitionInfoThrowsExceptionForNonExistentTable() {
        assertThrows(SemanticException.class,
                () -> MetaFunctions.inspectTablePartitionInfo(ConstantOperator.createVarchar("test.non_existent_table")));
    }

    @Test
    public void testInvalidateGlobalDict() throws Exception {
        // Test with non-existent table
        assertThrows(SemanticException.class, () -> {
            MetaFunctions.invalidateGlobalDict(
                    ConstantOperator.createVarchar("test.non_existent_table"),
                    ConstantOperator.createVarchar("k1"));
        });

        // Test with non-OLAP table
        assertThrows(SemanticException.class, () -> {
            MetaFunctions.invalidateGlobalDict(
                    ConstantOperator.createVarchar("test.mysql_external_table"),
                    ConstantOperator.createVarchar("k1"));
        });

        // Test with valid table and column (should return message about no global dict found)
        ConstantOperator result = MetaFunctions.invalidateGlobalDict(
                ConstantOperator.createVarchar("test.tbl1"),
                ConstantOperator.createVarchar("k1"));
        Assertions.assertNotNull(result);
        Assertions.assertTrue(result.getVarchar().contains("No global dictionary found for column: k1"));
    }

    @Test
    public void testInvalidateMinMax() throws Exception {
        // Test with non-existent table
        assertThrows(SemanticException.class, () -> {
            MetaFunctions.invalidateMinMax(
                    ConstantOperator.createVarchar("test.non_existent_table"),
                    ConstantOperator.createVarchar("k1"));
        });

        // Test with non-OLAP table
        assertThrows(SemanticException.class, () -> {
            MetaFunctions.invalidateMinMax(
                    ConstantOperator.createVarchar("test.mysql_external_table"),
                    ConstantOperator.createVarchar("k1"));
        });

        // Test with valid table and column
        ConstantOperator result = MetaFunctions.invalidateMinMax(
                ConstantOperator.createVarchar("test.tbl1"),
                ConstantOperator.createVarchar("k1"));
        Assertions.assertTrue(result.getVarchar().contains("invalidated column minmax"), result.getVarchar());
    }

    @Test
    public void testInspectMinMax() throws Exception {
        // Test with non-existent table
        assertThrows(SemanticException.class, () -> {
            MetaFunctions.inspectMinMax(
                    ConstantOperator.createVarchar("test.non_existent_table"),
                    ConstantOperator.createVarchar("k1"));
        });

        // Test with non-OLAP table
        assertThrows(SemanticException.class, () -> {
            MetaFunctions.inspectMinMax(
                    ConstantOperator.createVarchar("test.mysql_external_table"),
                    ConstantOperator.createVarchar("k1"));
        });

        // Test with valid table and column (should return null since no MinMax stats exist)
        ConstantOperator result = MetaFunctions.inspectMinMax(
                ConstantOperator.createVarchar("test.tbl1"),
                ConstantOperator.createVarchar("k1"));
        Assertions.assertNotNull(result);
        Assertions.assertTrue(result.isNull());
    }
}
