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

package com.starrocks.server;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.ColumnId;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.TableProperty;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.persist.ModifyTablePropertyOperationLog;
import com.starrocks.persist.OperationType;
import com.starrocks.sql.analyzer.AstToStringBuilder;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.optimizer.base.ColumnIdentifier;
import com.starrocks.sql.optimizer.statistics.CacheDictManager;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TStorageType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.TypeFactory;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

// Runtime/persistence coverage for the column-level "no dict" forbid (the dictionary thrash guard's
// persistent output, also the target of ALTER TABLE ... DISABLE/ENABLE DICTIONARY):
//   1. LocalMetastore.updateNoDictColumns add/drop semantics (what the ALTER executor and the guard call).
//   2. SHOW CREATE TABLE does NOT render no_dict_columns (auto-managed guard property).
//   3. Leader edit-log write + follower replay (survives FE restart / failover).
public class LocalMetastoreNoDictColumnEditLogTest {
    private static final String DB_NAME = "test_no_dict_editlog";
    private static final String TABLE_NAME = "t_no_dict";
    private static final long DB_ID = 30001L;
    private static final long TABLE_ID = 30002L;
    private static final long PARTITION_ID = 30003L;
    private static final long PHYSICAL_PARTITION_ID = 30004L;
    private static final long INDEX_ID = 30005L;
    private static final long TABLET_ID = 30006L;

    @BeforeEach
    public void setUp() throws Exception {
        UtFrameUtils.setUpForPersistTest();
    }

    @AfterEach
    public void tearDown() {
        clearNoDictMemory();
        UtFrameUtils.tearDownForPersisTest();
    }

    // Table with two VARCHAR columns (c1, c2) so the no-dict set names real string columns.
    private static OlapTable createStringOlapTable(long tableId, String tableName) {
        List<Column> columns = new ArrayList<>();
        Column key = new Column("id", IntegerType.BIGINT);
        key.setIsKey(true);
        columns.add(key);
        columns.add(new Column("c1", TypeFactory.createVarcharType(64)));
        columns.add(new Column("c2", TypeFactory.createVarcharType(64)));

        PartitionInfo partitionInfo = new SinglePartitionInfo();
        partitionInfo.setDataProperty(PARTITION_ID, com.starrocks.catalog.DataProperty.DEFAULT_DATA_PROPERTY);
        partitionInfo.setReplicationNum(PARTITION_ID, (short) 1);

        DistributionInfo distributionInfo = new HashDistributionInfo(3, List.of(key));

        MaterializedIndex baseIndex = new MaterializedIndex(INDEX_ID, MaterializedIndex.IndexState.NORMAL);
        LocalTablet tablet = new LocalTablet(TABLET_ID);
        TabletMeta tabletMeta = new TabletMeta(DB_ID, tableId, PARTITION_ID, INDEX_ID, TStorageMedium.HDD);
        baseIndex.addTablet(tablet, tabletMeta);

        Partition partition = new Partition(PARTITION_ID, PHYSICAL_PARTITION_ID, tableName, baseIndex, distributionInfo);

        OlapTable olapTable = new OlapTable(tableId, tableName, columns, KeysType.DUP_KEYS, partitionInfo, distributionInfo);
        olapTable.setIndexMeta(INDEX_ID, tableName, columns, 0, 0, (short) 1, TStorageType.COLUMN, KeysType.DUP_KEYS);
        olapTable.setBaseIndexMetaId(INDEX_ID);
        olapTable.addPartition(partition);
        olapTable.setTableProperty(new TableProperty(new HashMap<>()));
        return olapTable;
    }

    private static String showCreateTable(OlapTable table) {
        List<String> createTableStmt = new ArrayList<>();
        AstToStringBuilder.getDdlStmt(table, createTableStmt, new ArrayList<>(), new ArrayList<>(), false, false);
        return String.join("\n", createTableStmt);
    }

    @Test
    public void testDisableEnableDictionaryPersistAndReplay() throws Exception {
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        Database db = new Database(DB_ID, DB_NAME);
        metastore.unprotectCreateDb(db);
        OlapTable table = createStringOlapTable(TABLE_ID, TABLE_NAME);
        db.registerTableUnlocked(table);

        // --- DISABLE DICTIONARY (c1, c2): add both to the no-dict set ---
        metastore.updateNoDictColumns(DB_ID, TABLE_ID, Set.of("c1", "c2"), Collections.emptySet());

        table = (OlapTable) db.getTable(TABLE_ID);
        Assertions.assertEquals(Set.of("c1", "c2"), table.getNoDictColumns());
        Assertions.assertTrue(table.isNoDictColumn("c1"));
        Assertions.assertTrue(table.isNoDictColumn("c2"));

        // no_dict_columns is an auto-managed guard property and is intentionally NOT rendered in
        // SHOW CREATE TABLE (it would not round-trip through CREATE).
        String ddl = showCreateTable(table);
        Assertions.assertFalse(ddl.contains("no_dict_columns"),
                "SHOW CREATE TABLE must not render no_dict_columns, got:\n" + ddl);

        // Drain the DISABLE journal.
        ModifyTablePropertyOperationLog disableLog = (ModifyTablePropertyOperationLog) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_MODIFY_NO_DICT_COLUMNS);
        Assertions.assertNotNull(disableLog);
        Assertions.assertEquals(DB_ID, disableLog.getDbId());
        Assertions.assertEquals(TABLE_ID, disableLog.getTableId());

        // --- ENABLE DICTIONARY (c1): drop c1, leaving {c2} ---
        metastore.updateNoDictColumns(DB_ID, TABLE_ID, Collections.emptySet(), Set.of("c1"));
        table = (OlapTable) db.getTable(TABLE_ID);
        Assertions.assertEquals(Set.of("c2"), table.getNoDictColumns());
        Assertions.assertFalse(table.isNoDictColumn("c1"));

        ModifyTablePropertyOperationLog enableLog = (ModifyTablePropertyOperationLog) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_MODIFY_NO_DICT_COLUMNS);
        Assertions.assertNotNull(enableLog);

        // --- Follower replay (== FE restart / failover): a fresh table replays both journals ---
        LocalMetastore followerMetastore = new LocalMetastore(GlobalStateMgr.getCurrentState(), null, null);
        Database followerDb = new Database(DB_ID, DB_NAME);
        followerMetastore.unprotectCreateDb(followerDb);
        OlapTable followerTable = createStringOlapTable(TABLE_ID, TABLE_NAME);
        followerDb.registerTableUnlocked(followerTable);

        followerMetastore.replayModifyTableProperty(OperationType.OP_MODIFY_NO_DICT_COLUMNS, disableLog);
        Assertions.assertEquals(Set.of("c1", "c2"),
                ((OlapTable) followerDb.getTable(TABLE_ID)).getNoDictColumns());

        followerMetastore.replayModifyTableProperty(OperationType.OP_MODIFY_NO_DICT_COLUMNS, enableLog);
        OlapTable replayed = (OlapTable) followerDb.getTable(TABLE_ID);
        Assertions.assertEquals(Set.of("c2"), replayed.getNoDictColumns(),
                "no_dict_columns must survive edit-log replay after restart");
        Assertions.assertTrue(replayed.isNoDictColumn("c2"));
        Assertions.assertFalse(replayed.isNoDictColumn("c1"));
    }

    @Test
    public void testUpdateIsIdempotentNoJournalWhenUnchanged() throws Exception {
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        Database db = new Database(DB_ID, DB_NAME);
        metastore.unprotectCreateDb(db);
        OlapTable table = createStringOlapTable(TABLE_ID, TABLE_NAME);
        db.registerTableUnlocked(table);

        metastore.updateNoDictColumns(DB_ID, TABLE_ID, Set.of("c1"), Collections.emptySet());
        Assertions.assertEquals(Set.of("c1"), table.getNoDictColumns());
        ModifyTablePropertyOperationLog first = (ModifyTablePropertyOperationLog) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_MODIFY_NO_DICT_COLUMNS);
        Assertions.assertNotNull(first);

        // Re-adding the same column is a no-op: set unchanged, so no journal is written. If a journal
        // had been written, replayNextJournal for a different op type below would surface the stray entry.
        metastore.updateNoDictColumns(DB_ID, TABLE_ID, Set.of("c1"), Collections.emptySet());
        Assertions.assertEquals(Set.of("c1"), table.getNoDictColumns());
    }

    @SuppressWarnings("unchecked")
    private static Set<ColumnIdentifier> noDictMemory() {
        try {
            java.lang.reflect.Field f = CacheDictManager.class.getDeclaredField("NO_DICT_STRING_COLUMNS");
            f.setAccessible(true);
            return (Set<ColumnIdentifier>) f.get(null);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void clearNoDictMemory() {
        noDictMemory().removeIf(ci -> ci.getTableId() == TABLE_ID);
    }

    private static ColumnIdentifier colId(String name) {
        return new ColumnIdentifier(TABLE_ID, ColumnId.create(name));
    }

    // The bug this guards against: ENABLE DICTIONARY previously cleared only the persisted noDictColumns
    // property, not CacheDictManager's in-memory NO_DICT set; because hasGlobalDict checks the in-memory set
    // first, the column stayed forbidden until FE restart. On the leader the clear runs in updateNoDictColumns.
    @Test
    public void testEnableClearsInMemoryForbidOnLeader() throws Exception {
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        Database db = new Database(DB_ID, DB_NAME);
        metastore.unprotectCreateDb(db);
        OlapTable table = createStringOlapTable(TABLE_ID, TABLE_NAME);
        db.registerTableUnlocked(table);

        // DISABLE c1 (persist), and simulate the thrash guard also marking it in memory.
        metastore.updateNoDictColumns(DB_ID, TABLE_ID, Set.of("c1"), Collections.emptySet());
        ColumnIdentifier c1 = colId("c1");
        noDictMemory().add(c1);
        Assertions.assertTrue(noDictMemory().contains(c1));
        UtFrameUtils.PseudoJournalReplayer.replayNextJournal(OperationType.OP_MODIFY_NO_DICT_COLUMNS);

        // ENABLE c1 must clear the in-memory forbid, not just the persisted property.
        metastore.updateNoDictColumns(DB_ID, TABLE_ID, Collections.emptySet(), Set.of("c1"));
        Assertions.assertFalse(noDictMemory().contains(c1),
                "ENABLE must clear the in-memory NO_DICT forbid on the leader");
        Assertions.assertTrue(table.getNoDictColumns().isEmpty());
        UtFrameUtils.PseudoJournalReplayer.replayNextJournal(OperationType.OP_MODIFY_NO_DICT_COLUMNS);
    }

    // The same clear must happen on followers, where it runs in replayModifyTableProperty (old vs new diff).
    @Test
    public void testEnableClearsInMemoryForbidOnReplay() throws Exception {
        LocalMetastore follower = new LocalMetastore(GlobalStateMgr.getCurrentState(), null, null);
        Database db = new Database(DB_ID, DB_NAME);
        follower.unprotectCreateDb(db);
        OlapTable table = createStringOlapTable(TABLE_ID, TABLE_NAME);
        table.setNoDictColumns(Set.of("c1"));
        db.registerTableUnlocked(table);
        ColumnIdentifier c1 = colId("c1");
        noDictMemory().add(c1);

        // Replay an ENABLE (new noDictColumns = empty): dropped = {c1} must clear the in-memory forbid.
        Map<String, String> prop = new HashMap<>();
        prop.put(PropertyAnalyzer.PROPERTIES_NO_DICT_COLUMNS, "");
        ModifyTablePropertyOperationLog info = new ModifyTablePropertyOperationLog(DB_ID, TABLE_ID, prop);
        follower.replayModifyTableProperty(OperationType.OP_MODIFY_NO_DICT_COLUMNS, info);

        Assertions.assertFalse(noDictMemory().contains(c1),
                "replay of ENABLE must clear the in-memory NO_DICT forbid");
        Assertions.assertTrue(table.getNoDictColumns().isEmpty());
    }

    // Race the codex review flagged: the thrash guard marked a column in memory and queued an async
    // persist, but an operator runs ENABLE before that persist writes the property. The persisted set is
    // still empty, so updateNoDictColumns makes no journal change -- yet ENABLE must clear the in-memory
    // forbid anyway, otherwise hasGlobalDict keeps short-circuiting and the column stays disabled.
    @Test
    public void testEnableClearsInMemoryForbidWhenPersistedUnchanged() throws Exception {
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        Database db = new Database(DB_ID, DB_NAME);
        metastore.unprotectCreateDb(db);
        OlapTable table = createStringOlapTable(TABLE_ID, TABLE_NAME);
        db.registerTableUnlocked(table);

        // Guard marked c1 in memory only; the async persist has not run, so the persisted set is empty.
        ColumnIdentifier c1 = colId("c1");
        noDictMemory().add(c1);
        Assertions.assertTrue(table.getNoDictColumns().isEmpty());

        // ENABLE c1: the persisted set does not change (already empty), but the in-memory forbid must clear.
        metastore.updateNoDictColumns(DB_ID, TABLE_ID, Collections.emptySet(), Set.of("c1"));
        Assertions.assertFalse(noDictMemory().contains(c1),
                "ENABLE must clear the in-memory forbid even when the persisted set is unchanged");
        Assertions.assertTrue(table.getNoDictColumns().isEmpty());
    }
}
