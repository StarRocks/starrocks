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

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.starrocks.backup.AbstractJob;
import com.starrocks.backup.BackupJob;
import com.starrocks.backup.BackupJobInfo;
import com.starrocks.backup.BackupMeta;
import com.starrocks.backup.RestoreFileMapping;
import com.starrocks.backup.RestoreJob;
import com.starrocks.backup.SnapshotInfo;
import com.starrocks.backup.Status;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.ColocateTableIndex;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionName;
import com.starrocks.catalog.PartitionNames;
import com.starrocks.catalog.ScalarFunction;
import com.starrocks.catalog.TableName;
import com.starrocks.lake.backup.LakeBackupJob;
import com.starrocks.lake.backup.LakeRestoreJob;
import com.starrocks.lake.backup.LakeTableSnapshotInfo;
import com.starrocks.persist.ColocatePersistInfo;
import com.starrocks.persist.TableRefPersist;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.persist.gson.SubtypeNotFoundException;
import com.starrocks.persist.proto.AbstractJobPB;
import com.starrocks.persist.proto.BackupJobCorePB;
import com.starrocks.persist.proto.BackupJobPB;
import com.starrocks.persist.proto.BackupStatusPB;
import com.starrocks.persist.proto.SnapshotInfoPB;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.starrocks.common.jmockit.Deencapsulation.getField;
import static com.starrocks.common.jmockit.Deencapsulation.setField;
import static com.starrocks.persist.image.ProtoInstanceAssertions.assertInstances;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class BackupJobProtoTest {
    static void assertJobEquivalent(AbstractJob source, AbstractJob loaded) {
        // Both image formats materialize registered ScalarType objects for live IntegerType/VarcharType values.
        GsonEquivalence.assertEquivalent(source, loaded, CatalogEquivalenceRules.TYPE_LABELS_ADDED_ON_LOAD, Set.of());
    }

    static BackupJobInfo jobInfo() {
        BackupJobInfo info = new BackupJobInfo();
        info.name = "snapshot";
        info.dbName = "remote_db";
        info.dbId = 701;
        info.backupTime = 1234567;
        info.clusterId = 19;
        info.finishTime = 1234568L;
        info.ttl = "3d";
        info.expireTime = 991234568L;
        info.metaVersion = 100;
        info.starrocksMetaVersion = 101;
        info.tblAlias.put("table", "restored_table");
        BackupJobInfo.BackupTableInfo table = new BackupJobInfo.BackupTableInfo();
        table.name = "table";
        table.id = 702;
        table.autoIncrementId = 998L;
        BackupJobInfo.BackupPartitionInfo partition = new BackupJobInfo.BackupPartitionInfo();
        partition.name = "p1";
        partition.id = 703;
        partition.version = 9;
        BackupJobInfo.BackupPhysicalPartitionInfo physical = new BackupJobInfo.BackupPhysicalPartitionInfo();
        physical.id = 704;
        physical.version = 10;
        BackupJobInfo.BackupIndexInfo index = new BackupJobInfo.BackupIndexInfo();
        index.id = 705;
        index.name = "base";
        index.schemaHash = 1001;
        for (long id : List.of(707L, 706L)) {
            BackupJobInfo.BackupTabletInfo tablet = new BackupJobInfo.BackupTabletInfo();
            tablet.id = id;
            index.tablets.add(tablet);
        }
        physical.indexes.put(index.name, index);
        partition.indexes.put(index.name, index);
        partition.subPartitions.put(physical.id, physical);
        table.partitions.put(partition.name, partition);
        info.tables.put(table.name, table);
        return info;
    }

    static Function function() {
        return new ScalarFunction(800, new FunctionName("backup_db", "saved_fn"),
                List.of(IntegerType.BIGINT), IntegerType.BIGINT, false);
    }

    static Catalog catalog() {
        return new Catalog(900, "saved_catalog", Map.of("type", "hive", "hive.metastore.uris", "thrift://host:9083"),
                "backup catalog");
    }

    static List<AbstractJob> jobs(long now) {
        BackupMeta meta = new BackupMeta(List.of());
        meta.setFunctions(List.of(function()));
        meta.setCatalogs(List.of(catalog()));
        List<TableRefPersist> refs = List.of(new TableRefPersist(new TableName("backup_db", "table"), "alias",
                new PartitionNames(true, List.of("p2", "p1"))));
        List<AbstractJob> jobs = List.of(
                new BackupJob("backup", 1, "db1", refs, 901, null, 77),
                new LakeBackupJob("lake_backup", 2, "db2", refs, 902, null, 77),
                new RestoreJob("restore", "ts1", 3, "db3", jobInfo(), true, 2, 903, null, 77, meta, null),
                new LakeRestoreJob("lake_restore", "ts2", 4, "db4", jobInfo(), false, 3, 904, null, 77, meta, null));
        long id = 100;
        for (AbstractJob job : jobs) {
            setField(job, "jobId", id++);
            setField(job, "createTime", now - 1000);
            setField(job, "finishedTime", now);
            setField(job, "status", new Status(Status.ErrCode.BAD_FILE, "persisted failure"));
            setField(job, "taskErrMsg", new HashMap<>(Map.of(18L, "task failed", 19L, "retry")));
            SnapshotInfo normal = new SnapshotInfo(1, 2, 3, 4, 5, 6, 7, "/snapshot", List.of("b.dat", "a.hdr"));
            SnapshotInfo lake = new LakeTableSnapshotInfo(8, 9, 10, 11, 12, 13, 14, 15);
            if (job instanceof BackupJob) {
                setField(job, "state", BackupJob.BackupJobState.UPLOADING);
                setField(job, "snapshotFinishedTime", now - 400);
                setField(job, "snapshotUploadFinishedTime", now - 200);
                setField(job, "snapshotInfos", Map.of(5L, normal, 12L, lake));
                setField(job, "backupMeta", meta);
                setField(job, "localMetaInfoFilePath", "/backup/meta");
                setField(job, "localJobInfoFilePath", "/backup/job");
                setField(job, "backupFunctions", List.of(function()));
                setField(job, "backupCatalogs", List.of(catalog()));
                setField(job, "ttl", "7d");
            } else {
                setField(job, "state", RestoreJob.RestoreJobState.COMMITTING);
                setField(job, "metaPreparedTime", now - 700);
                setField(job, "snapshotFinishedTime", now - 500);
                setField(job, "downloadFinishedTime", now - 300);
                RestoreFileMapping mapping = new RestoreFileMapping();
                mapping.putMapping(new RestoreFileMapping.IdChain(1L, 2L, 3L, 4L, 5L),
                        new RestoreFileMapping.IdChain(11L, 12L, 13L, 14L, 15L), true);
                Map<Long, Boolean> overwrite = getField(mapping, "overwriteMap");
                overwrite.put(999L, false);
                setField(job, "fileMapping", mapping);
                Table<Long, Long, Long> versions = HashBasedTable.create();
                versions.put(20L, 30L, 50L);
                versions.put(20L, 31L, 60L);
                setField(job, "restoredVersionInfo", versions);
                Table<Long, Long, SnapshotInfo> snapshots = HashBasedTable.create();
                snapshots.put(5L, 6L, normal);
                snapshots.put(12L, 13L, lake);
                setField(job, "snapshotInfos", snapshots);
                setField(job, "colocatePersistInfos", List.of(ColocatePersistInfo.createForAddTable(
                        new ColocateTableIndex.GroupId(12, 13), 14, List.of(List.of(3L, 2L), List.of(5L)))));
            }
        }
        return jobs;
    }

    @Test
    public void testAllJobVariantsAndFields() throws Exception {
        for (AbstractJob source : jobs(123456789)) {
            AbstractJob loaded = AbstractJob.fromProto(AbstractJobPB.parseFrom(assertInstances(source.toProto()).toByteArray()));
            assertEquals(source.getClass(), loaded.getClass());
            assertEquals(source.getType(), loaded.getType());
            assertJobEquivalent(source, loaded);
            AbstractJob v2 = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(source, AbstractJob.class), AbstractJob.class);
            assertJobEquivalent(source, v2);
            assertNull(getField(loaded, "globalStateMgr"));
            assertNull(getField(loaded, "repo"));
            if (loaded instanceof BackupJob) {
                Map<Long, SnapshotInfo> snapshots = getField(loaded, "snapshotInfos");
                assertEquals(2, snapshots.size());
                assertEquals(LakeTableSnapshotInfo.class, snapshots.get(12L).getClass());
                assertNull(snapshots.get(12L).getFiles());
                assertEquals(15, ((LakeTableSnapshotInfo) snapshots.get(12L)).getVersion());
            } else {
                RestoreFileMapping mapping = getField(loaded, "fileMapping");
                Map<Long, Boolean> overwrite = getField(mapping, "overwriteMap");
                assertEquals(false, overwrite.get(999L));
                assertEquals(new RestoreFileMapping.IdChain(11L, 12L, 13L, 14L, 15L),
                        mapping.get(new RestoreFileMapping.IdChain(1L, 2L, 3L, 4L, 5L)));
            }
        }
    }

    @Test
    public void testEveryJobStateAndStatusCode() {
        for (AbstractJob job : jobs(1000)) {
            if (job instanceof BackupJob) {
                for (BackupJob.BackupJobState state : BackupJob.BackupJobState.values()) {
                    setField(job, "state", state);
                    assertEquals(state, getField(AbstractJob.fromProto(job.toProto()), "state"));
                }
            } else {
                for (RestoreJob.RestoreJobState state : RestoreJob.RestoreJobState.values()) {
                    setField(job, "state", state);
                    assertEquals(state, getField(AbstractJob.fromProto(job.toProto()), "state"));
                }
            }
        }
        for (Status.ErrCode code : Status.ErrCode.values()) {
            Status status = new Status(code, "reason");
            GsonEquivalence.assertEquivalent(status, Status.fromProto(status.toProto()));
        }
    }

    @Test
    public void testNullAndEmptyOptionalMembers() {
        BackupJob job = (BackupJob) jobs(1000).get(0);
        for (String name : List.of("tableRefs", "snapshotInfos", "backupMeta", "localMetaInfoFilePath",
                "localJobInfoFilePath", "backupFunctions", "backupCatalogs", "ttl")) {
            setField(job, name, null);
        }
        BackupJob loaded = (BackupJob) AbstractJob.fromProto(job.toProto());
        GsonEquivalence.assertEquivalent(job, loaded);
        setField(job, "tableRefs", new ArrayList<>());
        setField(job, "snapshotInfos", new HashMap<>());
        setField(job, "backupFunctions", new ArrayList<>());
        setField(job, "backupCatalogs", new ArrayList<>());
        GsonEquivalence.assertEquivalent(job, AbstractJob.fromProto(job.toProto()));
        RestoreJob restore = (RestoreJob) jobs(1000).get(2);
        for (String name : List.of("backupTimestamp", "jobInfo", "backupMeta", "restoredPartitions",
                "restoredTbls", "restoredVersionInfo", "snapshotInfos", "colocatePersistInfos")) {
            setField(restore, name, null);
        }
        GsonEquivalence.assertEquivalent(restore, AbstractJob.fromProto(restore.toProto()));
    }

    @Test
    public void testDefaultsAndMalformedKinds() {
        AbstractJob minimal = AbstractJob.fromProto(AbstractJobPB.newBuilder().setKind(AbstractJobPB.Kind.BACKUP_JOB)
                .setBackupJob(BackupJobPB.getDefaultInstance()).build());
        assertEquals(-1L, (long) getField(minimal, "jobId"));
        assertEquals(-1L, (long) getField(minimal, "createTime"));
        assertEquals(-1L, (long) getField(minimal, "snapshotFinishedTime"));
        for (AbstractJobPB.Kind kind : AbstractJobPB.Kind.values()) {
            if (kind == AbstractJobPB.Kind.UNRECOGNIZED) {
                continue;
            }
            assertThrows(IllegalArgumentException.class, () -> AbstractJob.fromProto(
                    AbstractJobPB.newBuilder().setKind(kind).build()));
        }
        assertThrows(SubtypeNotFoundException.class, () -> AbstractJob.fromProto(
                AbstractJobPB.newBuilder().setKindValue(999).build()));
        assertThrows(IllegalArgumentException.class, () -> AbstractJob.fromProto(AbstractJobPB.newBuilder()
                .setKind(AbstractJobPB.Kind.BACKUP_JOB).setBackupJob(BackupJobPB.newBuilder()
                        .setCore(BackupJobCorePB.newBuilder().setStateValue(999))).build()));
        assertThrows(IllegalArgumentException.class, () -> SnapshotInfo.fromProto(SnapshotInfoPB.getDefaultInstance()));
        assertThrows(IllegalArgumentException.class, () -> SnapshotInfo.fromProto(SnapshotInfoPB.newBuilder()
                .setKind(SnapshotInfoPB.Kind.LAKE_TABLE_SNAPSHOT_INFO).build()));
        assertThrows(IllegalArgumentException.class,
                () -> Status.fromProto(BackupStatusPB.newBuilder().setErrCodeValue(999).build()));
    }
}
