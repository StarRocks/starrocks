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
import com.starrocks.common.ErrorReportException;
import com.starrocks.leader.ReportHandler;
import com.starrocks.memory.MemoryUsageTracker;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SimpleExecutor;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.UserIdentity;
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
        try {
            assertThrows(ErrorReportException.class, () -> {
                connectContext.setCurrentUserIdentity(testUser);
                connectContext.setCurrentRoleIds(testUser);
                connectContext.setThreadLocalInfo();
                MetaFunctions.inspectTable(new TableName("test", "tbl1"));
            });
        } finally {
            connectContext.setCurrentUserIdentity(currentUserIdentity);
            connectContext.setCurrentRoleIds(currentRoleIds);
        }
    }

    @Test
    public void testInspectExternalTableAccessDeniedException() {
        UserIdentity currentUserIdentity = connectContext.getCurrentUserIdentity();
        Set<Long> currentRoleIds = connectContext.getCurrentRoleIds();
        try {
            assertThrows(ErrorReportException.class, () -> {
                connectContext.setCurrentUserIdentity(testUser);
                connectContext.setCurrentRoleIds(testUser);
                connectContext.setThreadLocalInfo();
                MetaFunctions.inspectTable(new TableName("test", "mysql_external_table"));
            });
        } finally {
            connectContext.setCurrentUserIdentity(currentUserIdentity);
            connectContext.setCurrentRoleIds(currentRoleIds);
        }
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
                public List<TResultBatch> executeDQLAsCaller(String sql, int queryTimeoutSeconds,
                                                              ConnectContext caller) {
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
                public List<TResultBatch> executeDQLAsCaller(String sql, int queryTimeoutSeconds,
                                                              ConnectContext caller) {
                    throw new RuntimeException("query failed if record not exist in dict table");
                }
            };
            Assertions.assertNull(lookupString("t1", "v1", "c1"));
            starRocksAssert.dropTable("t1");
        }
    }

    @Test
    public void testLookupStringEscapesArguments() throws Exception {
        starRocksAssert.withTable("create table t_esc(c1 string, v string) primary key(c1) " +
                "properties('replication_num'='1')");
        String[] captured = new String[1];
        new MockUp<SimpleExecutor>() {
            @Mock
            public List<TResultBatch> executeDQLAsCaller(String sql, int queryTimeoutSeconds,
                                                         ConnectContext caller) {
                captured[0] = sql;
                return Lists.newArrayList();
            }
        };
        try {
            // A single quote in the key must be escaped (doubled), not break out of the literal.
            Assertions.assertNull(lookupString("t_esc", "a'b", "v"));
            Assertions.assertTrue(captured[0].contains("'a''b'"), captured[0]);
            // Identifiers are backquoted.
            Assertions.assertTrue(captured[0].contains("`v`"), captured[0]);
            Assertions.assertTrue(captured[0].contains("`c1`"), captured[0]);
        } finally {
            starRocksAssert.dropTable("t_esc");
        }
    }

    @Test
    public void testLookupStringUnknownReturnColumn() throws Exception {
        starRocksAssert.withTable("create table t_col(c1 string, v string) primary key(c1) " +
                "properties('replication_num'='1')");
        try {
            SemanticException e = assertThrows(SemanticException.class,
                    () -> lookupString("t_col", "k", "no_such_col"));
            Assertions.assertTrue(e.getMessage().contains("Unknown column 'no_such_col'"), e.getMessage());
        } finally {
            starRocksAssert.dropTable("t_col");
        }
    }

    @Test
    public void testLookupStringRunsAsCaller() throws Exception {
        starRocksAssert.withTable("create table t_lookup_caller(c1 string, c2 string) primary key(c1) " +
                "properties('replication_num'='1')");
        UserIdentity currentUserIdentity = connectContext.getCurrentUserIdentity();
        Set<Long> currentRoleIds = connectContext.getCurrentRoleIds();
        Set<String> currentGroups = connectContext.getGroups();
        String currentRemoteIP = connectContext.getRemoteIP();
        UserIdentity[] innerUser = new UserIdentity[1];
        Set<String>[] innerGroups = new Set[1];
        String[] innerRemoteIP = new String[1];
        new MockUp<SimpleExecutor>() {
            @Mock
            public List<TResultBatch> executeDQL(String sql, ConnectContext context) {
                innerUser[0] = context.getCurrentUserIdentity();
                innerGroups[0] = context.getGroups();
                innerRemoteIP[0] = context.getRemoteIP();
                return Lists.newArrayList();
            }
        };
        Set<String> callerGroups = Set.of("g_ranger");
        String callerRemoteIP = "10.1.2.3";
        try {
            connectContext.setCurrentUserIdentity(testUser);
            connectContext.setCurrentRoleIds(testUser);
            connectContext.setGroups(callerGroups);
            connectContext.setRemoteIP(callerRemoteIP);
            connectContext.setThreadLocalInfo();
            Assertions.assertNull(lookupString("t_lookup_caller", "k", "c2"));
            // The internal lookup query must run as the caller, never as ROOT
            Assertions.assertEquals(testUser, innerUser[0]);
            // Groups must be propagated: Ranger authorization and row/column policies read
            // context.getGroups(), so dropping them would silently bypass group-based policies.
            Assertions.assertEquals(callerGroups, innerGroups[0]);
            // Remote IP must be propagated: USER()/SESSION_USER() are folded from qualifiedUser
            // plus remoteIP, and Ranger row-filter/masking expressions are analyzed here.
            Assertions.assertEquals(callerRemoteIP, innerRemoteIP[0]);
            Assertions.assertSame(connectContext, ConnectContext.get());
        } finally {
            connectContext.setCurrentUserIdentity(currentUserIdentity);
            connectContext.setCurrentRoleIds(currentRoleIds);
            connectContext.setGroups(currentGroups);
            connectContext.setRemoteIP(currentRemoteIP);
            starRocksAssert.dropTable("t_lookup_caller");
        }
    }

    @Test
    public void testLookupStringAccessDenied() throws Exception {
        starRocksAssert.withTable("create table t_lookup_denied(c1 string, c2 string) primary key(c1) " +
                "properties('replication_num'='1')");
        UserIdentity currentUserIdentity = connectContext.getCurrentUserIdentity();
        Set<Long> currentRoleIds = connectContext.getCurrentRoleIds();
        try {
            connectContext.setCurrentUserIdentity(testUser);
            connectContext.setCurrentRoleIds(testUser);
            connectContext.setThreadLocalInfo();
            SemanticException e = assertThrows(SemanticException.class,
                    () -> lookupString("t_lookup_denied", "k", "c2"));
            Assertions.assertTrue(e.getMessage().contains("Access denied"), e.getMessage());
        } finally {
            connectContext.setCurrentUserIdentity(currentUserIdentity);
            connectContext.setCurrentRoleIds(currentRoleIds);
            starRocksAssert.dropTable("t_lookup_denied");
        }
    }

    @Test
    public void testLookupStringIgnoresCallerBypass() throws Exception {
        // A lookup triggered inside an existing bypassAuthorizerCheck scope must still be
        // authorized: the internal query must not inherit the caller's bypass flag, otherwise
        // the SELECT / Ranger checks would be skipped again.
        starRocksAssert.withTable("create table t_lookup_bypass(c1 string, c2 string) primary key(c1) " +
                "properties('replication_num'='1')");
        UserIdentity currentUserIdentity = connectContext.getCurrentUserIdentity();
        Set<Long> currentRoleIds = connectContext.getCurrentRoleIds();
        boolean currentBypass = connectContext.isBypassAuthorizerCheck();
        try {
            connectContext.setCurrentUserIdentity(testUser);
            connectContext.setCurrentRoleIds(testUser);
            connectContext.setBypassAuthorizerCheck(true);
            connectContext.setThreadLocalInfo();
            SemanticException e = assertThrows(SemanticException.class,
                    () -> lookupString("t_lookup_bypass", "k", "c2"));
            Assertions.assertTrue(e.getMessage().contains("Access denied"), e.getMessage());
        } finally {
            connectContext.setCurrentUserIdentity(currentUserIdentity);
            connectContext.setCurrentRoleIds(currentRoleIds);
            connectContext.setBypassAuthorizerCheck(currentBypass);
            starRocksAssert.dropTable("t_lookup_bypass");
        }
    }

    @Test
    public void inspectMVRefreshInfoReturnsValidJsonForMaterializedView() throws Exception {
        starRocksAssert.withRefreshedMaterializedView("create materialized view mv1 distributed by random " +
                "as select k1, sum(v1) from test.tbl1 group by k1");
        ConstantOperator result = MetaFunctions.inspectMVRefreshInfo(ConstantOperator.createVarchar("test.mv1"));
        Assertions.assertNotNull(result);
        Assertions.assertTrue(result.getVarchar().contains("mvToRefreshPartitions"));
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
        Assertions.assertTrue(result.getVarchar().contains("mvToRefreshPartitions"));
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
}
