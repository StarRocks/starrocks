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

package com.starrocks.catalog;

import com.starrocks.common.Config;
import com.starrocks.persist.CreateTableInfo;
import com.starrocks.persist.ModifyTablePropertyOperationLog;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.AlterClause;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.CreateTableLikeStmt;
import com.starrocks.sql.ast.MergeTabletClause;
import com.starrocks.sql.ast.SplitTabletClause;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RangeTableReshardTargetSizeTest {
    private static final long GIB = 1024L * 1024L * 1024L;
    private static final String KEY = "tablet_reshard_target_size";
    private static final String NON_RANGE_MSG =
            "tablet_reshard_target_size is only supported for range-distributed tables";
    private static final String VALUE_MSG = "tablet_reshard_target_size of a table must be a positive integer";
    private static final String ALTER_VALUE_MSG =
            VALUE_MSG + ", or \"\" to fall back to the FE configuration tablet_reshard_target_size";
    // The bucket_size message for any table that is not RANDOM-distributed.
    private static final String BUCKET_SIZE_MSG = "Setting bucket size is not allowed: only supported for tables "
            + "with RANDOM distribution and when 'enable_automatic_bucket' is enabled.";

    private static StarRocksAssert starRocksAssert;
    private static boolean savedEnableRangeDistribution;

    @BeforeAll
    public static void setUp() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        UtFrameUtils.setUpForPersistTest();
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(ctx);
        starRocksAssert.withDatabase("rts_test").useDatabase("rts_test");
        savedEnableRangeDistribution = Config.enable_range_distribution;
        Config.enable_range_distribution = true;
    }

    @AfterAll
    public static void tearDown() {
        Config.enable_range_distribution = savedEnableRangeDistribution;
        UtFrameUtils.tearDownForPersisTest();
    }

    private static String rangeDdl(String name, String targetSize) {
        return "create table " + name + " (k1 int, v1 int) duplicate key(k1) properties('replication_num'='1'"
                + (targetSize == null ? "" : ", '" + KEY + "'='" + targetSize + "'") + ")";
    }

    private static String alterSql(String name, String key, String value) {
        return "alter table " + name + " set (\"" + key + "\" = \"" + value + "\")";
    }

    private static OlapTable table(String name) {
        return (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("rts_test").getTable(name);
    }

    private static String showCreate(String name) throws Exception {
        return starRocksAssert.showCreateTable("show create table " + name);
    }

    private static String allMessages(Executable e) {
        Throwable t = assertThrows(Throwable.class, e);
        StringBuilder all = new StringBuilder();
        for (Throwable c = t; c != null; c = c.getCause()) {
            all.append(c.getMessage()).append('\n');
        }
        return all.toString();
    }

    private static void assertThrowsContaining(String expected, Executable e) {
        String all = allMessages(e);
        assertTrue(all.contains(expected), "expected '" + expected + "' in: " + all);
    }

    private static void assertUnset(OlapTable t) {
        assertFalse(t.getTableProperty().getProperties().containsKey(KEY));
        assertEquals(0L, t.getTableProperty().getTabletReshardTargetSize());
        assertEquals(0L, t.getTabletReshardTargetSizeOverride(Config.tablet_reshard_target_size));
        assertEquals(Config.tablet_reshard_target_size, t.getTabletReshardTargetSize());
    }

    @Test
    public void testCreateRangeTableWithTargetSize() throws Exception {
        starRocksAssert.withTable(rangeDdl("t_create", "1073741824"));
        OlapTable t = table("t_create");
        assertTrue(t.isRangeDistribution());
        assertEquals(GIB, t.getTableProperty().getTabletReshardTargetSize());
        assertEquals(GIB, t.getTabletReshardTargetSizeOverride(Config.tablet_reshard_target_size));
        assertEquals(GIB, t.getTabletReshardTargetSize());
        assertEquals("1073741824", t.getTableProperty().getProperties().get(KEY));
        assertFalse(t.getTableProperty().getProperties().containsKey("bucket_size"));
        String ddl = showCreate("t_create");
        assertTrue(ddl.contains("\"" + KEY + "\" = \"1073741824\""), ddl);
        assertFalse(ddl.contains("DISTRIBUTED BY"), ddl);
        assertFalse(ddl.contains("bucket_size"), ddl);
    }

    @Test
    public void testRangeTableWithoutTargetSizeFollowsCluster() throws Exception {
        starRocksAssert.withTable(rangeDdl("t_default", null));
        OlapTable t = table("t_default");
        assertUnset(t);
        assertFalse(showCreate("t_default").contains(KEY));
    }

    @Test
    public void testCreateRejectsNonRangeTables() {
        assertThrowsContaining(NON_RANGE_MSG, () -> starRocksAssert.withTable(
                "create table t_hash (k1 int, v1 int) duplicate key(k1) distributed by hash(k1) buckets 1 "
                        + "properties('replication_num'='1', '" + KEY + "'='1073741824')"));
        assertThrowsContaining(NON_RANGE_MSG, () -> starRocksAssert.withTable(
                "create table t_random_rts (k1 int, v1 int) duplicate key(k1) distributed by random "
                        + "properties('replication_num'='1', '" + KEY + "'='1073741824')"));
    }

    @Test
    public void testCreateRejectsInvalidValues() {
        for (String v : new String[] {"0", "-1", "abc", ""}) {
            String all = allMessages(() -> starRocksAssert.withTable(rangeDdl("t_invalid", v)));
            assertTrue(all.contains(VALUE_MSG), "value '" + v + "': " + all);
            assertFalse(all.contains("fall back"), "value '" + v + "': " + all);
        }
    }

    @Test
    public void testCreateStillRejectsUnknownProperties() {
        assertThrowsContaining("Unknown properties", () -> starRocksAssert.withTable(
                "create table t_unknown (k1 int, v1 int) duplicate key(k1) properties('replication_num'='1', '"
                        + KEY + "'='1073741824', 'no_such_property'='1')"));
    }

    @Test
    public void testAlterSetAndClear() throws Exception {
        starRocksAssert.withTable(rangeDdl("t_alter", null));
        OlapTable t = table("t_alter");
        starRocksAssert.alterTable(alterSql("t_alter", KEY, "2147483648"));
        assertEquals(2 * GIB, t.getTableProperty().getTabletReshardTargetSize());
        assertEquals(2 * GIB, t.getTabletReshardTargetSize());
        assertTrue(showCreate("t_alter").contains("\"" + KEY + "\" = \"2147483648\""));

        starRocksAssert.alterTable(alterSql("t_alter", KEY, ""));
        assertUnset(t);
        assertFalse(showCreate("t_alter").contains(KEY));
    }

    @Test
    public void testAlterRejectsNonRangeTables() throws Exception {
        starRocksAssert.withTable("create table t_hash_alter (k1 int, v1 int) duplicate key(k1) "
                + "distributed by hash(k1) buckets 1 properties('replication_num'='1')");
        starRocksAssert.withTable("create table t_random_alter (k1 int, v1 int) duplicate key(k1) "
                + "distributed by random properties('replication_num'='1')");
        for (String name : new String[] {"t_hash_alter", "t_random_alter"}) {
            assertThrowsContaining(NON_RANGE_MSG,
                    () -> starRocksAssert.alterTable(alterSql(name, KEY, "1073741824")));
            assertFalse(table(name).getTableProperty().getProperties().containsKey(KEY));
        }
    }

    @Test
    public void testAlterRejectsInvalidValues() throws Exception {
        starRocksAssert.withTable(rangeDdl("t_bad", "1073741824"));
        OlapTable t = table("t_bad");
        for (String v : new String[] {"0", "-5", "abc"}) {
            assertThrowsContaining(ALTER_VALUE_MSG, () -> starRocksAssert.alterTable(alterSql("t_bad", KEY, v)));
            assertEquals(GIB, t.getTabletReshardTargetSize());
            assertEquals("1073741824", t.getTableProperty().getProperties().get(KEY));
        }
    }

    @Test
    public void testBucketSizeKeepsBaseBehavior() throws Exception {
        assertThrowsContaining(BUCKET_SIZE_MSG, () -> starRocksAssert.withTable(
                "create table t_range_bs (k1 int, v1 int) duplicate key(k1) "
                        + "properties('replication_num'='1', 'bucket_size'='1073741824')"));
        assertThrowsContaining(BUCKET_SIZE_MSG, () -> starRocksAssert.withTable(
                "create table t_hash_bs (k1 int, v1 int) duplicate key(k1) distributed by hash(k1) buckets 1 "
                        + "properties('replication_num'='1', 'bucket_size'='1073741824')"));

        starRocksAssert.withTable(rangeDdl("t_range_bs_alter", null));
        assertThrowsContaining(BUCKET_SIZE_MSG,
                () -> starRocksAssert.alterTable(alterSql("t_range_bs_alter", "bucket_size", "1073741824")));
        OlapTable t = table("t_range_bs_alter");
        assertFalse(t.getTableProperty().getProperties().containsKey("bucket_size"));
        assertUnset(t);
    }

    @Test
    public void testRandomTableUnchanged() throws Exception {
        starRocksAssert.withTable("create table t_random (k1 int, v1 int) duplicate key(k1) distributed by random "
                + "properties('replication_num'='1', 'bucket_size'='2147483648')");
        OlapTable t = table("t_random");
        assertEquals(2 * GIB, t.getAutomaticBucketSize());
        assertUnset(t);
        String ddl = showCreate("t_random");
        assertTrue(ddl.contains("\"bucket_size\" = \"2147483648\""), ddl);
        assertFalse(ddl.contains(KEY), ddl);
    }

    @Test
    public void testClusterOffSwitchWinsOverTableTarget() throws Exception {
        starRocksAssert.withTable(rangeDdl("t_off", "1073741824"));
        OlapTable t = table("t_off");
        long saved = Config.tablet_reshard_target_size;
        try {
            Config.tablet_reshard_target_size = 0;
            assertEquals(0L, t.getTabletReshardTargetSizeOverride(0));
            assertEquals(0L, t.getTabletReshardTargetSize());
            assertTrue(showCreate("t_off").contains("\"" + KEY + "\" = \"1073741824\""));
            Config.tablet_reshard_target_size = -3;
            assertEquals(-3L, t.getTabletReshardTargetSize());
            Config.tablet_reshard_target_size = 10 * GIB;
            assertEquals(GIB, t.getTabletReshardTargetSize());
        } finally {
            Config.tablet_reshard_target_size = saved;
        }
    }

    private static long clauseTarget(String sql) throws Exception {
        AlterTableStmt stmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        AlterClause clause = stmt.getAlterClauseList().get(0);
        if (clause instanceof SplitTabletClause) {
            return ((SplitTabletClause) clause).getTabletReshardTargetSize();
        }
        return ((MergeTabletClause) clause).getTabletReshardTargetSize();
    }

    @Test
    public void testManualSplitAndMergeDefaultToTableTarget() throws Exception {
        starRocksAssert.withTable(rangeDdl("t_manual_target", "1073741824"));
        starRocksAssert.withTable(rangeDdl("t_manual_plain", null));
        for (String op : new String[] {"SPLIT", "MERGE"}) {
            assertEquals(GIB, clauseTarget("ALTER TABLE t_manual_target " + op + " TABLET"));
            assertEquals(2 * GIB, clauseTarget("ALTER TABLE t_manual_target " + op
                    + " TABLET PROPERTIES(\"" + KEY + "\"=\"2147483648\")"));
            assertEquals(Config.tablet_reshard_target_size,
                    clauseTarget("ALTER TABLE t_manual_plain " + op + " TABLET"));
        }
        long saved = Config.tablet_reshard_target_size;
        try {
            Config.tablet_reshard_target_size = 0L;
            assertEquals(0L, clauseTarget("ALTER TABLE t_manual_target SPLIT TABLET"));
            assertEquals(0L, clauseTarget("ALTER TABLE t_manual_target MERGE TABLET"));
        } finally {
            Config.tablet_reshard_target_size = saved;
        }
    }

    @Test
    public void testAlterJournalAndReplay() throws Exception {
        starRocksAssert.withTable(rangeDdl("t_journal", null));
        OlapTable t = table("t_journal");
        UtFrameUtils.PseudoJournalReplayer.resetFollowerJournalQueue();

        starRocksAssert.alterTable(alterSql("t_journal", KEY, "2147483648"));
        ModifyTablePropertyOperationLog setLog = (ModifyTablePropertyOperationLog)
                UtFrameUtils.PseudoJournalReplayer.replayNextJournal(OperationType.OP_ALTER_TABLE_PROPERTIES);
        assertEquals(Map.of(KEY, "2147483648"), setLog.getProperties());

        starRocksAssert.alterTable(alterSql("t_journal", KEY, ""));
        ModifyTablePropertyOperationLog clearLog = (ModifyTablePropertyOperationLog)
                UtFrameUtils.PseudoJournalReplayer.replayNextJournal(OperationType.OP_ALTER_TABLE_PROPERTIES);
        assertEquals(Map.of(KEY, ""), clearLog.getProperties());

        // Replay both onto a table that holds a different value, as a follower would.
        t.setTabletReshardTargetSize(GIB);
        GlobalStateMgr.getCurrentState().getLocalMetastore()
                .replayModifyTableProperty(OperationType.OP_ALTER_TABLE_PROPERTIES, setLog);
        assertEquals(2 * GIB, t.getTabletReshardTargetSize());
        assertEquals("2147483648", t.getTableProperty().getProperties().get(KEY));
        GlobalStateMgr.getCurrentState().getLocalMetastore()
                .replayModifyTableProperty(OperationType.OP_ALTER_TABLE_PROPERTIES, clearLog);
        assertUnset(t);
    }

    // ALTER stores and journals the parsed value, as CREATE TABLE does, so the map, the journal and SHOW CREATE
    // TABLE agree on one spelling.
    @Test
    public void testAlterNormalizesValue() throws Exception {
        starRocksAssert.withTable(rangeDdl("t_normalize", null));
        OlapTable t = table("t_normalize");
        for (String v : new String[] {"+1073741824", "01073741824"}) {
            UtFrameUtils.PseudoJournalReplayer.resetFollowerJournalQueue();
            starRocksAssert.alterTable(alterSql("t_normalize", KEY, v));
            ModifyTablePropertyOperationLog log = (ModifyTablePropertyOperationLog)
                    UtFrameUtils.PseudoJournalReplayer.replayNextJournal(OperationType.OP_ALTER_TABLE_PROPERTIES);
            assertEquals(Map.of(KEY, "1073741824"), log.getProperties(), "value '" + v + "'");
            assertEquals("1073741824", t.getTableProperty().getProperties().get(KEY), "value '" + v + "'");
            assertEquals(GIB, t.getTabletReshardTargetSize());
            assertTrue(showCreate("t_normalize").contains("\"" + KEY + "\" = \"1073741824\""), "value '" + v + "'");
        }
    }

    private static TableProperty imageRoundTrip(TableProperty property) {
        return GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(property), TableProperty.class);
    }

    @Test
    public void testImageRoundTrip() throws Exception {
        starRocksAssert.withTable(rangeDdl("t_image", "1073741824"));
        OlapTable t = table("t_image");
        TableProperty loaded = imageRoundTrip(t.getTableProperty());
        assertEquals(GIB, loaded.getTabletReshardTargetSize());
        assertEquals("1073741824", loaded.getProperties().get(KEY));

        starRocksAssert.alterTable(alterSql("t_image", KEY, ""));
        loaded = imageRoundTrip(t.getTableProperty());
        assertEquals(0L, loaded.getTabletReshardTargetSize());
        assertFalse(loaded.getProperties().containsKey(KEY));

        // An FE without this property replays a clear as a plain map write and may checkpoint the "".
        Map<String, String> leftover = new HashMap<>();
        leftover.put(KEY, "");
        loaded = imageRoundTrip(new TableProperty(leftover));
        assertEquals(0L, loaded.getTabletReshardTargetSize());
        assertFalse(loaded.getProperties().containsKey(KEY));
    }

    @Test
    public void testShowCreateTableRoundTrip() throws Exception {
        starRocksAssert.withTable(rangeDdl("t_show", "1073741824"));
        starRocksAssert.withTable(showCreate("t_show").replace("`t_show`", "`t_show_copy`"));
        OlapTable copy = table("t_show_copy");
        assertTrue(copy.isRangeDistribution());
        assertEquals(GIB, copy.getTabletReshardTargetSize());
        assertEquals("1073741824", copy.getTableProperty().getProperties().get(KEY));
    }

    private static void createTableLike(String sql) throws Exception {
        CreateTableLikeStmt stmt = (CreateTableLikeStmt) UtFrameUtils.parseStmtWithNewParser(sql,
                starRocksAssert.getCtx());
        GlobalStateMgr.getCurrentState().getLocalMetastore().createTable(stmt.getCreateTableStmt());
    }

    @Test
    public void testCreateTableLike() throws Exception {
        starRocksAssert.withTable(rangeDdl("t_like_src", "1073741824"));
        createTableLike("create table t_like_dst like t_like_src");
        OlapTable dst = table("t_like_dst");
        assertTrue(dst.isRangeDistribution());
        assertEquals(GIB, dst.getTabletReshardTargetSize());

        // Where range distribution is not the default the copy is not range-distributed, so the property is
        // rejected.
        Config.enable_range_distribution = false;
        try {
            assertThrowsContaining(NON_RANGE_MSG, () -> createTableLike("create table t_like_off like t_like_src"));
        } finally {
            Config.enable_range_distribution = true;
        }
    }

    // Materialized views keep rejecting the property as an unknown one.
    @Test
    public void testMaterializedViewRejectsTargetSize() throws Exception {
        starRocksAssert.withTable(rangeDdl("t_mv_base", null));
        assertThrowsContaining("Analyze materialized properties failed because unknown properties: {" + KEY,
                () -> starRocksAssert.withMaterializedView("create materialized view mv_with_target refresh manual "
                        + "properties('replication_num'='1', '" + KEY + "'='1073741824') as select k1, v1 from t_mv_base"));
        starRocksAssert.withMaterializedView("create materialized view mv_plain refresh manual "
                + "properties('replication_num'='1') as select k1, v1 from t_mv_base");
        assertThrowsContaining("Modify failed because unknown properties: {" + KEY, () -> starRocksAssert
                .alterMvProperties("alter materialized view mv_plain set ('" + KEY + "'='1073741824')"));
    }

    private static OlapTable createdTableInJournal(String sql) throws Exception {
        UtFrameUtils.PseudoJournalReplayer.resetFollowerJournalQueue();
        starRocksAssert.withTable(sql);
        CreateTableInfo info = (CreateTableInfo)
                UtFrameUtils.PseudoJournalReplayer.replayNextJournal(OperationType.OP_CREATE_TABLE_V2);
        return (OlapTable) info.getTable();
    }

    @Test
    public void testCreateJournalCarriesTargetSize() throws Exception {
        OlapTable unpartitioned = createdTableInJournal(rangeDdl("t_journal_create", "1073741824"));
        assertEquals("1073741824", unpartitioned.getTableProperty().getProperties().get(KEY));
        assertEquals(GIB, unpartitioned.getTableProperty().getTabletReshardTargetSize());

        OlapTable partitioned = createdTableInJournal("create table t_journal_create_parts (k1 int, v1 int)\n"
                + "duplicate key(k1)\n"
                + "partition by range(k1) (partition p1 values less than ('100'),\n"
                + "                        partition p2 values less than ('200'))\n"
                + "properties('replication_num'='1', '" + KEY + "'='2147483648')");
        assertEquals("2147483648", partitioned.getTableProperty().getProperties().get(KEY));
        assertEquals(2 * GIB, partitioned.getTableProperty().getTabletReshardTargetSize());
        assertEquals(2 * GIB, table("t_journal_create_parts").getTabletReshardTargetSize());
    }
}
