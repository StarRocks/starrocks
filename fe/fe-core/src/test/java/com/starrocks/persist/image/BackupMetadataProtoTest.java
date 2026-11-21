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

package com.starrocks.persist.image;

import com.starrocks.backup.AbstractJob;
import com.starrocks.backup.BackupJobInfo;
import com.starrocks.backup.BackupMeta;
import com.starrocks.backup.BlobStorage;
import com.starrocks.backup.RestoreJob;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionNames;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.RandomDistributionInfo;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableName;
import com.starrocks.common.Pair;
import com.starrocks.lake.LakeTable;
import com.starrocks.persist.TableRefPersist;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.persist.proto.BackupJobInfoPB;
import com.starrocks.persist.proto.BackupMetaPB;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.thrift.TStorageType;
import com.starrocks.type.IntegerType;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.starrocks.common.jmockit.Deencapsulation.getField;
import static com.starrocks.common.jmockit.Deencapsulation.setField;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BackupMetadataProtoTest {
    private static final String DB = "image_v3_backup_metadata";

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        StarRocksAssert starRocks = new StarRocksAssert(UtFrameUtils.createDefaultCtx());
        starRocks.withDatabase(DB).useDatabase(DB);
        starRocks.withTable("CREATE TABLE base (k1 int, v1 bigint) DUPLICATE KEY(k1)"
                + " PARTITION BY RANGE(k1) (PARTITION p1 VALUES LESS THAN ('10'),"
                + " PARTITION p2 VALUES LESS THAN ('20')) DISTRIBUTED BY HASH(k1) BUCKETS 1"
                + " PROPERTIES ('replication_num' = '1')");
        starRocks.alterTable("ALTER TABLE base ADD TEMPORARY PARTITION tp1 VALUES [('20'), ('30'))");
        starRocks.withView("CREATE VIEW view1 AS SELECT k1, v1 FROM base");
        starRocks.withMaterializedView("CREATE MATERIALIZED VIEW mv1 DISTRIBUTED BY HASH(k1) BUCKETS 1"
                + " REFRESH DEFERRED MANUAL PROPERTIES ('replication_num' = '1') AS SELECT k1, sum(v1) FROM base GROUP BY k1");
    }

    static LakeTable lakeTable() {
        List<Column> columns = List.of(new Column("k1", IntegerType.BIGINT, true, null, null, false, null, "", 1));
        LakeTable table = new LakeTable(70001, "lake_backup_table", columns, KeysType.DUP_KEYS,
                new SinglePartitionInfo(), new RandomDistributionInfo(2), null);
        MaterializedIndex index = new MaterializedIndex(70002, 70002, MaterializedIndex.IndexState.NORMAL,
                PhysicalPartition.INVALID_SHARD_GROUP_ID);
        table.setMaxColUniqueId(1);
        table.setBaseIndexMetaId(index.getMetaId());
        table.setIndexMeta(index.getMetaId(), table.getName(), columns, 0, 10, (short) 1, TStorageType.COLUMN, KeysType.DUP_KEYS);
        Partition partition = new Partition(70003, 70004, "lake_part", index, new RandomDistributionInfo(2));
        partition.getDefaultPhysicalPartition().setVisibleVersion(17, 12345);
        partition.getDefaultPhysicalPartition().setNextVersion(18);
        table.addPartition(partition);
        return table;
    }

    private static List<Table> tables() {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB);
        List<Table> tables = new ArrayList<>();
        tables.add(db.getTable("base"));
        tables.add(db.getTable("view1"));
        tables.add(db.getTable("mv1"));
        tables.add(lakeTable());
        return tables;
    }

    private static void assertTable(Table source, Table restored) {
        assertEquals(source.getClass(), restored.getClass());
        assertNotSame(source, restored);
        GsonEquivalence.assertEquivalent(source, restored, CatalogEquivalenceRules.LIVE_TABLE_FIELDS,
                CatalogEquivalenceRules.UNORDERED_PATHS);
        if (source instanceof OlapTable) {
            OlapTable expected = (OlapTable) source;
            OlapTable actual = (OlapTable) restored;
            assertEquals(expected.getPartitions().size(), actual.getPartitions().size());
            assertEquals(expected.getTempPartitions().size(), actual.getTempPartitions().size());
            for (Partition partition : expected.getPartitions()) {
                assertEquals(partition.getDefaultPhysicalPartition().getVisibleVersion(),
                        actual.getPartition(partition.getId()).getDefaultPhysicalPartition().getVisibleVersion());
            }
        }
    }

    @Test
    public void testCompleteBackupTablesAndDerivedIdMap() throws Exception {
        List<Table> tables = tables();
        BackupMeta source = new BackupMeta(tables);
        source.setFunctions(List.of(BackupJobProtoTest.function()));
        source.setCatalogs(List.of(BackupJobProtoTest.catalog()));
        BackupMeta loaded = BackupMeta.fromProto(BackupMetaPB.parseFrom(source.toProto().toByteArray()));
        BackupMeta v2 = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(source), BackupMeta.class);
        for (Table table : tables) {
            for (BackupMeta restored : List.of(loaded, v2)) {
                assertSame(restored.getTable(table.getName()), restored.getTable(table.getId()));
                assertTable(table, restored.getTable(table.getName()));
            }
        }
        GsonEquivalence.assertEquivalent(source.getFunctions(), loaded.getFunctions(),
                CatalogEquivalenceRules.TYPE_LABELS_ADDED_ON_LOAD, Set.of());
        GsonEquivalence.assertEquivalent(source.getCatalogs(), loaded.getCatalogs());
        // Backup-owned tables are separate objects; conversion must not register or replace live tables.
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB);
        assertSame(tables.get(0), db.getTable("base"));
        assertNull(db.getTable("lake_backup_table"));
        assertEquals(1, ((OlapTable) loaded.getTable("base")).getTempPartitions().size());
    }

    @Test
    public void testBackupMetaKeepsOriginalKeyAfterRestoreRenamesTable() throws Exception {
        Table table = lakeTable();
        String originalName = table.getName();
        BackupMeta source = new BackupMeta(List.of(table));
        // RestoreJob keeps the original map key while changing the shared table object's name and ID.
        table.setName("restored_alias");
        table.setId(80001);
        RestoreJob job = (RestoreJob) BackupJobProtoTest.jobs(System.currentTimeMillis()).get(2);
        setField(job, "backupMeta", source);
        setField(job, "restoredTbls", List.of(table));
        RestoreJob restoredJob = (RestoreJob) AbstractJob.fromProto(job.toProto());
        BackupMeta loaded = getField(restoredJob, "backupMeta");
        BackupMeta v2 = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(source), BackupMeta.class);
        for (BackupMeta restored : List.of(loaded, v2)) {
            assertEquals(Set.of(originalName), restored.getTables().keySet());
            assertEquals("restored_alias", restored.getTable(originalName).getName());
            assertSame(restored.getTable(originalName), restored.getTable(80001L));
            assertNull(restored.getTable("restored_alias"));
            assertTable(table, restored.getTable(originalName));
        }
        List<Table> restoredTables = getField(restoredJob, "restoredTbls");
        assertTable(table, restoredTables.get(0));
    }

    @Test
    public void testRestoreJobOwnsCompleteTablesAndNamedPartitions() {
        List<Table> tables = tables();
        RestoreJob source = (RestoreJob) BackupJobProtoTest.jobs(System.currentTimeMillis()).get(2);
        setField(source, "restoredTbls", tables);
        Partition partition = ((OlapTable) tables.get(0)).getPartitions().iterator().next();
        setField(source, "restoredPartitions", List.of(Pair.create("restored_base", partition)));
        RestoreJob restored = (RestoreJob) AbstractJob.fromProto(source.toProto());
        List<Table> actualTables = getField(restored, "restoredTbls");
        assertEquals(tables.size(), actualTables.size());
        for (int i = 0; i < tables.size(); i++) {
            assertTable(tables.get(i), actualTables.get(i));
        }
        List<Pair<String, Partition>> partitions = getField(restored, "restoredPartitions");
        assertEquals("restored_base", partitions.get(0).first);
        assertEquals(partition.getId(), partitions.get(0).second.getId());
        assertNotSame(partition, partitions.get(0).second);
    }

    @Test
    public void testJobInfoTreeAndRetentionFields() throws Exception {
        BackupJobInfo source = BackupJobProtoTest.jobInfo();
        BackupJobInfo restored = BackupJobInfo.fromProto(BackupJobInfoPB.parseFrom(source.toProto().toByteArray()));
        GsonEquivalence.assertEquivalent(source, restored);
        BackupJobInfo v2 = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(source), BackupJobInfo.class);
        GsonEquivalence.assertEquivalent(source, v2);
        BackupJobInfo.BackupTableInfo table = restored.tables.get("table");
        assertEquals(998L, table.autoIncrementId.longValue());
        assertEquals(707, table.partitions.get("p1").subPartitions.get(704L).indexes.get("base").tablets.get(0).id);
        assertEquals("restored_table", restored.getAliasByOriginNameIfSet("table"));
        assertEquals(991234568L, restored.expireTime.longValue());
        // The maps preserve explicit keys, even for incomplete historical records.
        source.tables = Map.of("stored-key", source.tables.get("table"));
        assertTrue(BackupJobInfo.fromProto(source.toProto()).tables.containsKey("stored-key"));
    }

    @Test
    public void testNullEmptyAndMissingHistoricalFields() {
        BackupJobInfo source = new BackupJobInfo();
        source.clusterId = null;
        source.finishTime = null;
        source.expireTime = null;
        BackupJobInfo restored = BackupJobInfo.fromProto(source.toProto());
        assertNull(restored.clusterId);
        assertNull(restored.finishTime);
        assertNull(restored.expireTime);
        assertTrue(restored.tables.isEmpty());
        GsonEquivalence.assertEquivalent(source, restored);
        source.tables = null;
        source.tblAlias = null;
        restored = BackupJobInfo.fromProto(source.toProto());
        assertNull(restored.tables);
        assertNull(restored.tblAlias);
        BackupMeta meta = new BackupMeta(List.of());
        meta.setFunctions(null);
        meta.setCatalogs(List.of());
        BackupMeta loaded = BackupMeta.fromProto(meta.toProto());
        assertNull(loaded.getFunctions());
        assertTrue(loaded.getCatalogs().isEmpty());
        assertFalse(loaded.toProto().hasFunctions());
        assertTrue(loaded.toProto().hasCatalogs());
    }

    @Test
    public void testTableReferenceAliasesAndPartitionNames() {
        List<String> names = new ArrayList<>(List.of("p2", "p1"));
        TableRefPersist source = new TableRefPersist(new TableName("db", "table"), "alias",
                new PartitionNames(true, names));
        setField(source, "aliases", new String[] {"alias", "second_alias"});
        TableRefPersist restored = TableRefPersist.fromProto(source.toProto());
        assertEquals("db", restored.getName().getDb());
        assertEquals("table", restored.getName().getTbl());
        assertArrayEquals(new String[] {"alias", "second_alias"}, (String[]) getField(restored, "aliases"));
        assertEquals(List.of("p2", "p1"), restored.getPartitionNames().getPartitionNames());
        assertTrue(restored.getPartitionNames().isTemp());
        names.clear();
        assertEquals(List.of("p2", "p1"), restored.getPartitionNames().getPartitionNames());
        TableRefPersist empty = TableRefPersist.fromProto(new TableRefPersist(new TableName("db", "table"), null).toProto());
        assertNull(getField(empty, "aliases"));
        assertNull(empty.getPartitionNames());
        PartitionNames nullable = new PartitionNames(false, null);
        assertNull(PartitionNames.fromProto(nullable.toProto()).getPartitionNames());
        PartitionNames noPartitions = new PartitionNames(false, List.of());
        assertTrue(PartitionNames.fromProto(noPartitions.toProto()).getPartitionNames().isEmpty());
    }

    @Test
    public void testBlobStoragePreservesBrokerAndDirectAccessForms() {
        for (boolean hasBroker : List.of(true, false)) {
            BlobStorage source = new BlobStorage("broker", Map.of("endpoint", "host", "key", "value"), hasBroker);
            BlobStorage restored = BlobStorage.fromProto(source.toProto());
            GsonEquivalence.assertEquivalent(source, restored);
            assertEquals(hasBroker, (boolean) getField(restored, "hasBroker"));
            if (!hasBroker) {
                assertEquals("", source.toProto().getBrokerPersistInfo().getName());
                assertEquals(Map.of("endpoint", "host", "key", "value"),
                        source.toProto().getBrokerPersistInfo().getProperties().getValuesMap());
            }
        }
        BlobStorage source = new BlobStorage("broker", Map.of());
        setField(source, "properties", null);
        assertNull(getField(BlobStorage.fromProto(source.toProto()), "properties"));
        setField(source, "properties", Map.of());
        assertEquals(Map.of(), getField(BlobStorage.fromProto(source.toProto()), "properties"));
    }
}
