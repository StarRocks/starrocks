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

import com.starrocks.catalog.OlapTable.OlapTableState;
import com.starrocks.common.DdlException;
import com.starrocks.lake.LakeMaterializedView;
import com.starrocks.lake.LakeTable;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.RecoverInfo;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.sql.ast.RecoverDbStmt;
import com.starrocks.thrift.TStorageType;
import com.starrocks.utframe.UtFrameUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

public class CatalogRecycleBinRecoveryStateTest {
    private static final long DB_ID = 84001L;
    private static final long TABLE_ID = 84101L;
    private CatalogRecycleBin recycleBin;
    private Database db;
    private final List<LogEvent> warnings = new ArrayList<>();
    private final org.apache.logging.log4j.core.Logger logger =
            (org.apache.logging.log4j.core.Logger) LogManager.getLogger(CatalogRecycleBin.class);
    private Level oldLevel;
    private AbstractAppender appender;

    @BeforeEach
    public void setUp() throws Exception {
        UtFrameUtils.setUpForPersistTest();
        recycleBin = new CatalogRecycleBin();
        db = new Database(DB_ID, "recovery_state_db");
        warnings.clear();
        oldLevel = logger.getLevel();
        appender = new AbstractAppender("recovery-state-warnings", null, null) {
            @Override
            public void append(LogEvent event) {
                if (event.getLevel() == Level.WARN) {
                    warnings.add(event.toImmutable());
                }
            }
        };
        appender.start();
        logger.addAppender(appender);
        logger.setLevel(Level.WARN);
    }

    @AfterEach
    public void tearDown() {
        logger.removeAppender(appender);
        logger.setLevel(oldLevel);
        appender.stop();
        UtFrameUtils.tearDownForPersisTest();
    }

    private static Stream<Arguments> unsafeNativeTables() {
        return Stream.of(
                Arguments.of("olap_hash", OlapTableState.SCHEMA_CHANGE),
                Arguments.of("olap_hash", OlapTableState.ROLLUP),
                Arguments.of("lake_hash", OlapTableState.SCHEMA_CHANGE),
                Arguments.of("lake_hash", OlapTableState.ROLLUP),
                Arguments.of("mv", OlapTableState.SCHEMA_CHANGE),
                Arguments.of("lake_mv", OlapTableState.ROLLUP),
                Arguments.of("lake_hash", OlapTableState.TABLET_RESHARD),
                Arguments.of("olap_hash", OlapTableState.WAITING_STABLE),
                Arguments.of("lake_hash", OlapTableState.OPTIMIZE),
                Arguments.of("olap_hash", OlapTableState.RESTORE),
                Arguments.of("olap_hash", OlapTableState.RESTORE_WITH_LOAD),
                Arguments.of("lake_hash", OlapTableState.UPDATING_META));
    }

    private OlapTable nativeTable(String kind, long id) {
        Column column = new Column("k", Type.INT);
        column.setIsKey(true);
        List<Column> columns = List.of(column);
        PartitionInfo partitionInfo = new SinglePartitionInfo();
        DistributionInfo distribution = new HashDistributionInfo(1, columns);
        OlapTable table;
        switch (kind) {
            case "lake_hash":
                table = new LakeTable(id, kind + id, columns, KeysType.DUP_KEYS, partitionInfo, distribution);
                break;
            case "mv":
                table = new MaterializedView(id, DB_ID, kind + id, columns, KeysType.DUP_KEYS,
                        partitionInfo, distribution, new MaterializedView.MvRefreshScheme());
                break;
            case "lake_mv":
                table = new LakeMaterializedView(id, DB_ID, kind + id, columns, KeysType.DUP_KEYS,
                        partitionInfo, distribution, new MaterializedView.MvRefreshScheme());
                break;
            default:
                table = new OlapTable(id, kind + id, columns, KeysType.DUP_KEYS, partitionInfo, distribution);
                break;
        }
        table.setIndexMeta(id + 1000, table.getName(), columns, 0, 0, (short) 1,
                TStorageType.COLUMN, KeysType.DUP_KEYS);
        table.setBaseIndexId(id + 1000);
        MaterializedIndex baseIndex = new MaterializedIndex(id + 1000, MaterializedIndex.IndexState.NORMAL);
        Partition partition = new Partition(id + 2000, id + 3000, "p", baseIndex, distribution);
        partition.getDefaultPhysicalPartition().setNextVersion(3);
        table.addPartition(partition);
        return table;
    }

    private void assertNoRecoveryJournal(short operation) {
        Exception exception = Assertions.assertThrows(Exception.class,
                () -> UtFrameUtils.PseudoJournalReplayer.replayNextJournal(operation));
        Assertions.assertTrue(exception.getMessage().contains("queue is empty"), exception.getMessage());
    }

    private List<LogEvent> recoveryWarnings() {
        return warnings.stream().filter(event -> event.getMessage().getFormattedMessage()
                .contains("Attempting to recover native table")).toList();
    }

    private void assertRecoveryWarning(OlapTable table) {
        List<LogEvent> events = recoveryWarnings();
        Assertions.assertEquals(1, events.size());
        Assertions.assertEquals(Level.WARN, events.get(0).getLevel());
        String message = events.get(0).getMessage().getFormattedMessage();
        for (String context : List.of(db.getFullName(), table.getName(), "dbId=" + DB_ID,
                "tableId=" + table.getId(), table.getState().name(), "not repaired", "may", "publication", "DDL")) {
            Assertions.assertTrue(message.contains(context), message);
        }
    }

    @ParameterizedTest(name = "{0} {1}")
    @MethodSource("unsafeNativeTables")
    public void testRecoverTableWarnsAndPreservesLegacyBehavior(String kind, OlapTableState state) throws Exception {
        OlapTable table = nativeTable(kind, TABLE_ID);
        table.setState(state);
        PhysicalPartition partition = table.getPartitions().iterator().next().getDefaultPhysicalPartition();
        long visibleVersion = partition.getVisibleVersion();
        recycleBin.recycleTable(DB_ID, table, true);
        recycleBin.enableEraseLater.add(TABLE_ID);
        UtFrameUtils.PseudoJournalReplayer.resetFollowerJournalQueue();

        Assertions.assertTrue(recycleBin.recoverTable(db, table.getName()));

        Assertions.assertSame(table, db.getTable(table.getName()));
        Assertions.assertSame(table, db.getTable(TABLE_ID));
        Assertions.assertNull(recycleBin.getTable(DB_ID, TABLE_ID));
        Assertions.assertNull(recycleBin.getRecycleTableInfo(TABLE_ID));
        Assertions.assertFalse(recycleBin.idToRecycleTime.containsKey(TABLE_ID));
        Assertions.assertFalse(recycleBin.enableEraseLater.contains(TABLE_ID));
        Assertions.assertEquals(state, table.getState());
        Assertions.assertEquals(visibleVersion, partition.getVisibleVersion());
        Assertions.assertEquals(3, partition.getNextVersion());
        RecoverInfo journal = (RecoverInfo) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_RECOVER_TABLE_V2);
        Assertions.assertEquals(DB_ID, journal.getDbId());
        Assertions.assertEquals(TABLE_ID, journal.getTableId());
        assertNoRecoveryJournal(OperationType.OP_RECOVER_TABLE_V2);
        assertRecoveryWarning(table);
    }

    @Test
    public void testRecoverNormalNativeAndExternalTables() throws Exception {
        long id = TABLE_ID;
        for (String kind : List.of("olap_hash", "lake_hash", "mv", "lake_mv")) {
            OlapTable table = nativeTable(kind, id++);
            Assertions.assertEquals(OlapTableState.NORMAL, table.getState());
            recycleBin.recycleTable(DB_ID, table, true);
            Assertions.assertTrue(recycleBin.recoverTable(db, table.getName()));
            Assertions.assertSame(table, db.getTable(table.getId()));
            Assertions.assertNull(recycleBin.getTable(DB_ID, table.getId()));
        }

        ExternalOlapTable externalOlap = new ExternalOlapTable();
        externalOlap.setId(id++);
        externalOlap.setName("external_olap");
        externalOlap.setState(OlapTableState.SCHEMA_CHANGE);
        Table external = new Table(id, "external_mysql", Table.TableType.MYSQL, List.of());
        for (Table table : List.of(externalOlap, external)) {
            Assertions.assertFalse(table.isNativeTableOrMaterializedView());
            recycleBin.recycleTable(DB_ID, table, true);
            Assertions.assertTrue(recycleBin.recoverTable(db, table.getName()));
            Assertions.assertSame(table, db.getTable(table.getId()));
            Assertions.assertNull(recycleBin.getTable(DB_ID, table.getId()));
        }
        Assertions.assertEquals(OlapTableState.SCHEMA_CHANGE, externalOlap.getState());
        Assertions.assertTrue(recoveryWarnings().isEmpty());
    }

    @Test
    public void testRecoverDatabaseWarnsAndRestoresSelectedTables() throws Exception {
        OlapTable healthy = nativeTable("lake_hash", TABLE_ID);
        OlapTable unsafe = nativeTable("olap_hash", TABLE_ID + 1);
        unsafe.setState(OlapTableState.ROLLUP);
        recycleBin.recycleTable(DB_ID, healthy, true);
        recycleBin.recycleTable(DB_ID, unsafe, true);
        recycleBin.recycleDatabase(db, Set.of(healthy.getName(), unsafe.getName()), true);
        recycleBin.enableEraseLater.addAll(Set.of(DB_ID, healthy.getId(), unsafe.getId()));
        UtFrameUtils.PseudoJournalReplayer.resetFollowerJournalQueue();
        LocalMetastore metastore = new LocalMetastore(GlobalStateMgr.getCurrentState(), recycleBin, null);

        metastore.recoverDatabase(new RecoverDbStmt(db.getFullName()));

        Assertions.assertSame(db, metastore.getDb(DB_ID));
        Assertions.assertEquals(2, db.getTables().size());
        for (OlapTable table : List.of(healthy, unsafe)) {
            Assertions.assertSame(table, db.getTable(table.getName()));
            Assertions.assertNull(recycleBin.getTable(DB_ID, table.getId()));
            Assertions.assertNull(recycleBin.getRecycleTableInfo(table.getId()));
            Assertions.assertFalse(recycleBin.idToRecycleTime.containsKey(table.getId()));
            Assertions.assertFalse(recycleBin.enableEraseLater.contains(table.getId()));
            PhysicalPartition partition = table.getPartitions().iterator().next().getDefaultPhysicalPartition();
            Assertions.assertEquals(1, partition.getVisibleVersion());
            Assertions.assertEquals(3, partition.getNextVersion());
        }
        Assertions.assertEquals(OlapTableState.NORMAL, healthy.getState());
        Assertions.assertEquals(OlapTableState.ROLLUP, unsafe.getState());
        Assertions.assertNull(recycleBin.getDatabase(DB_ID));
        Assertions.assertFalse(recycleBin.idToRecycleTime.containsKey(DB_ID));
        Assertions.assertFalse(recycleBin.enableEraseLater.contains(DB_ID));
        RecoverInfo journal = (RecoverInfo) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_RECOVER_DB_V2);
        Assertions.assertEquals(DB_ID, journal.getDbId());
        assertNoRecoveryJournal(OperationType.OP_RECOVER_DB_V2);
        assertRecoveryWarning(unsafe);
    }

    @Test
    public void testRecoverDatabaseWarningDoesNotHideMissingTable() throws Exception {
        OlapTable unsafe = nativeTable("lake_hash", TABLE_ID);
        unsafe.setState(OlapTableState.SCHEMA_CHANGE);
        recycleBin.recycleTable(DB_ID, unsafe, true);
        recycleBin.recycleDatabase(db, Set.of(unsafe.getName(), "missing_table"), true);
        recycleBin.enableEraseLater.addAll(Set.of(DB_ID, TABLE_ID));
        UtFrameUtils.PseudoJournalReplayer.resetFollowerJournalQueue();
        LocalMetastore metastore = new LocalMetastore(GlobalStateMgr.getCurrentState(), recycleBin, null);

        DdlException exception = Assertions.assertThrows(DdlException.class,
                () -> metastore.recoverDatabase(new RecoverDbStmt(db.getFullName())));

        Assertions.assertTrue(exception.getMessage().contains("some tables have been deleted: missing_table"),
                exception.getMessage());
        // Preserve the historical partial consumption before the missing-name failure.
        Assertions.assertNull(metastore.getDb(DB_ID));
        Assertions.assertSame(db, recycleBin.getDatabase(DB_ID));
        Assertions.assertSame(unsafe, db.getTable(TABLE_ID));
        Assertions.assertNull(recycleBin.getTable(DB_ID, TABLE_ID));
        Assertions.assertNull(recycleBin.getRecycleTableInfo(TABLE_ID));
        Assertions.assertFalse(recycleBin.idToRecycleTime.containsKey(TABLE_ID));
        Assertions.assertFalse(recycleBin.enableEraseLater.contains(TABLE_ID));
        Assertions.assertTrue(recycleBin.idToRecycleTime.containsKey(DB_ID));
        Assertions.assertTrue(recycleBin.enableEraseLater.contains(DB_ID));
        Assertions.assertEquals(OlapTableState.SCHEMA_CHANGE, unsafe.getState());
        PhysicalPartition partition = unsafe.getPartitions().iterator().next().getDefaultPhysicalPartition();
        Assertions.assertEquals(1, partition.getVisibleVersion());
        Assertions.assertEquals(3, partition.getNextVersion());
        assertNoRecoveryJournal(OperationType.OP_RECOVER_DB_V2);
        assertRecoveryWarning(unsafe);
    }

    @Test
    public void testRecoverDatabaseIgnoresUnselectedAndNonrecoverableTables() throws Exception {
        OlapTable old = nativeTable("lake_hash", TABLE_ID);
        old.setName("selected");
        old.setState(OlapTableState.SCHEMA_CHANGE);
        recycleBin.recycleTable(DB_ID, old, true);
        OlapTable selected = nativeTable("lake_hash", TABLE_ID + 1);
        selected.setName("selected");
        recycleBin.recycleTable(DB_ID, selected, true);
        Assertions.assertFalse(recycleBin.isTableRecoverable(DB_ID, old.getId()));
        OlapTable unrelated = nativeTable("olap_hash", TABLE_ID + 2);
        unrelated.setState(OlapTableState.ROLLUP);
        recycleBin.recycleTable(DB_ID, unrelated, true);
        recycleBin.recycleDatabase(db, Set.of("selected"), true);

        Assertions.assertSame(db, recycleBin.recoverDatabase(db.getFullName()));

        Assertions.assertEquals(1, db.getTables().size());
        Assertions.assertSame(selected, db.getTable("selected"));
        Assertions.assertNull(recycleBin.getTable(DB_ID, selected.getId()));
        Assertions.assertSame(old, recycleBin.getTable(DB_ID, old.getId()));
        Assertions.assertFalse(recycleBin.isTableRecoverable(DB_ID, old.getId()));
        Assertions.assertSame(unrelated, recycleBin.getTable(DB_ID, unrelated.getId()));
        Assertions.assertTrue(recycleBin.isTableRecoverable(DB_ID, unrelated.getId()));
        Assertions.assertNull(recycleBin.getDatabase(DB_ID));
        Assertions.assertTrue(recoveryWarnings().isEmpty());
    }

    @Test
    public void testLegacyRecoveryReplayAllowsNonNormalTables() {
        LocalMetastore metastore = new LocalMetastore(GlobalStateMgr.getCurrentState(), recycleBin, null);
        metastore.unprotectCreateDb(db);
        OlapTable table = nativeTable("lake_hash", TABLE_ID);
        table.setState(OlapTableState.SCHEMA_CHANGE);
        recycleBin.recycleTable(DB_ID, table, true);

        metastore.replayRecoverTable(new RecoverInfo(DB_ID, TABLE_ID, -1L));

        Assertions.assertSame(table, db.getTable(TABLE_ID));
        Assertions.assertEquals(OlapTableState.SCHEMA_CHANGE, table.getState());
        Assertions.assertEquals(3, table.getPartitions().iterator().next().getDefaultPhysicalPartition().getNextVersion());
        Assertions.assertNull(recycleBin.getTable(DB_ID, TABLE_ID));

        Database recoveredDb = new Database(DB_ID + 1, "legacy_recovery_db");
        OlapTable dbTable = nativeTable("lake_hash", TABLE_ID + 1);
        dbTable.setState(OlapTableState.TABLET_RESHARD);
        recycleBin.recycleTable(recoveredDb.getId(), dbTable, true);
        recycleBin.recycleDatabase(recoveredDb, Set.of(dbTable.getName()), true);

        metastore.replayRecoverDatabase(new RecoverInfo(recoveredDb.getId(), -1L, -1L));

        Assertions.assertSame(recoveredDb, metastore.getDb(recoveredDb.getId()));
        Assertions.assertSame(dbTable, recoveredDb.getTable(dbTable.getId()));
        Assertions.assertEquals(OlapTableState.TABLET_RESHARD, dbTable.getState());
        Assertions.assertEquals(3, dbTable.getPartitions().iterator().next().getDefaultPhysicalPartition().getNextVersion());
        Assertions.assertNull(recycleBin.getTable(recoveredDb.getId(), dbTable.getId()));
        Assertions.assertNull(recycleBin.getDatabase(recoveredDb.getId()));
        Assertions.assertTrue(recoveryWarnings().isEmpty());
    }
}
