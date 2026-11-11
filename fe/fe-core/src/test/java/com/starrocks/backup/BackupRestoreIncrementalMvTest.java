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

package com.starrocks.backup;

import com.starrocks.backup.BackupJobInfo.BackupTableInfo;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.scheduler.mv.ivm.MVIVMIcebergTestBase;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.BackupStmt;
import com.starrocks.sql.ast.RestoreStmt;
import com.starrocks.sql.ast.TableRef;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mockito;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class BackupRestoreIncrementalMvTest extends MVIVMIcebergTestBase {
    private static final String DB = "backup_ivm_db";
    private static final String TARGET_DB = "backup_ivm_target_db";
    private static final String REPO = "backup_ivm_repo";
    private static final String SNAPSHOT = "backup_ivm_snapshot";
    private static final String RESTORE_PROPERTIES = " PROPERTIES (\"backup_timestamp\" = \"2026-10-23-00-00-00\")";
    private static final String MV_DDL = "CREATE MATERIALIZED VIEW %s.%s REFRESH DEFERRED MANUAL "
            + "PROPERTIES (\"refresh_mode\" = \"%s\") "
            + "AS SELECT id, count(data) AS cnt FROM `iceberg0`.`unpartitioned_db`.`t0` GROUP BY id";

    @BeforeAll
    public static void beforeClass() throws Exception {
        MVIVMIcebergTestBase.beforeClass();
        GlobalStateMgr.getCurrentState().getBackupHandler().getRepoMgr().replayAddRepo(mockRepository(null));
        starRocksAssert.withDatabase(DB).withDatabase(TARGET_DB);
        starRocksAssert.withMaterializedView(String.format(MV_DDL, DB, "ivm_mv", "incremental"));
        starRocksAssert.withMaterializedView(String.format(MV_DDL, DB, "auto_mv", "incremental"));
        starRocksAssert.withMaterializedView(String.format(MV_DDL, DB, "pct_mv", "pct"));
        // CREATE rejects AUTO on some branches; set it directly so the test is the same everywhere.
        localMv("auto_mv").setCurrentRefreshMode(MaterializedView.RefreshMode.AUTO);
    }

    @Override
    public void advanceTableVersionTo(long toVersion) {
    }

    @ParameterizedTest
    @CsvSource({"ivm_mv, INCREMENTAL", "auto_mv, AUTO"})
    public void testBackupRejectsNamedIncrementalMv(String mvName, String refreshMode) {
        for (String on : List.of("ON (MATERIALIZED VIEW %s)", "ON (%s)")) {
            String sql = "BACKUP SNAPSHOT " + DB + ".snap TO " + REPO + " " + String.format(on, mvName);
            Exception e = Assertions.assertThrows(Exception.class, () -> parse(sql));
            Assertions.assertTrue(e.getMessage().contains("Materialized view " + mvName + " can not be backed up "
                    + "because its refresh_mode is " + refreshMode + ". Recreate it after restoring its base tables."),
                    e.getMessage());
        }
    }

    @Test
    public void testBackupAcceptsNamedPctMv() throws Exception {
        BackupStmt stmt = (BackupStmt) parse("BACKUP SNAPSHOT " + DB + ".snap TO " + REPO
                + " ON (MATERIALIZED VIEW pct_mv)");
        Assertions.assertEquals(Set.of("pct_mv"), tableNames(stmt.getTableRefs()));
    }

    @Test
    public void testDatabaseBackupLeavesOutIncrementalMvs() throws Exception {
        boolean saved = Config.enable_backup_materialized_view;
        Config.enable_backup_materialized_view = true;
        try {
            for (String on : List.of("", " ON (ALL MATERIALIZED VIEWS)")) {
                BackupStmt stmt = (BackupStmt) parse("BACKUP SNAPSHOT " + DB + ".snap TO " + REPO + on);
                Assertions.assertEquals(Set.of("pct_mv"), tableNames(stmt.getTableRefs()), on);
            }
        } finally {
            Config.enable_backup_materialized_view = saved;
        }
    }

    @ParameterizedTest
    @CsvSource({"ivm_mv, INCREMENTAL", "auto_mv, AUTO"})
    public void testRestoreRejectsNamedIncrementalMv(String mvName, String refreshMode) throws Exception {
        Table pctMv = snapshotCopy("pct_mv");
        pctMv.setName("pct_mv0");
        SnapshotRestoreHandler handler = new SnapshotRestoreHandler(pctMv, snapshotCopy(mvName));
        Assertions.assertEquals("pct_mv0", handler.snapshotInfo.tables.keySet().iterator().next(),
                "guard: the partition-refresh view must be checked first, else the context assertions are vacuous");
        DdlException e = Assertions.assertThrows(DdlException.class, () -> handler.restore(
                "RESTORE SNAPSHOT " + TARGET_DB + "." + SNAPSHOT + " FROM " + REPO
                        + " ON (MATERIALIZED VIEW pct_mv0, MATERIALIZED VIEW " + mvName + ")" + RESTORE_PROPERTIES));
        Assertions.assertEquals("Materialized view " + mvName + " in snapshot " + SNAPSHOT + " can not be restored "
                + "because its refresh_mode is " + refreshMode + ". Recreate it after restoring its base tables.",
                e.getMessage());
        Assertions.assertNull(handler.submittedJob);
        Assertions.assertTrue(handler.mvRestoreContext.getMvIdToTableNameMap().isEmpty());
        Assertions.assertTrue(handler.mvRestoreContext.getMvBaseTableToBackupTableInfo().isEmpty());
    }

    @Test
    public void testDatabaseRestoreLeavesOutIncrementalMvs() throws Exception {
        for (String on : List.of("", " ON (ALL MATERIALIZED VIEWS)")) {
            SnapshotRestoreHandler handler = new SnapshotRestoreHandler(
                    snapshotCopy("ivm_mv"), snapshotCopy("auto_mv"), snapshotCopy("pct_mv"));
            handler.restore("RESTORE SNAPSHOT " + TARGET_DB + "." + SNAPSHOT + " FROM " + REPO + on
                    + RESTORE_PROPERTIES);
            Assertions.assertNotNull(handler.submittedJob, on);
            Assertions.assertEquals(Set.of("pct_mv"), handler.submittedJob.getJobInfo().tables.keySet(), on);
        }
    }

    @Test
    public void testRestoreOverExistingIncrementalMv() throws Exception {
        SnapshotRestoreHandler named = new SnapshotRestoreHandler(snapshotCopy("pct_mv"));
        DdlException e = Assertions.assertThrows(DdlException.class, () -> named.restore(
                "RESTORE SNAPSHOT " + DB + "." + SNAPSHOT + " FROM " + REPO
                        + " ON (MATERIALIZED VIEW pct_mv AS ivm_mv)" + RESTORE_PROPERTIES));
        Assertions.assertEquals("Can not restore pct_mv over materialized view ivm_mv because its refresh_mode is "
                + "INCREMENTAL. Drop it, or restore under another name with AS.", e.getMessage());
        Assertions.assertNull(named.submittedJob);

        Table sameNameAsIvmMv = snapshotCopy("pct_mv");
        sameNameAsIvmMv.setName("ivm_mv");
        SnapshotRestoreHandler implicit = new SnapshotRestoreHandler(sameNameAsIvmMv, snapshotCopy("pct_mv"));
        implicit.restore("RESTORE SNAPSHOT " + DB + "." + SNAPSHOT + " FROM " + REPO + RESTORE_PROPERTIES);
        Assertions.assertNotNull(implicit.submittedJob);
        Assertions.assertEquals(Set.of("pct_mv"), implicit.submittedJob.getJobInfo().tables.keySet());
    }

    @Test
    public void testRestoreCreatesTargetDbOnlyWhenAccepted() throws Exception {
        String newDb = "backup_ivm_new_db";
        SnapshotRestoreHandler rejected = new SnapshotRestoreHandler(snapshotCopy("ivm_mv"));
        Assertions.assertThrows(DdlException.class, () -> rejected.restore("RESTORE SNAPSHOT " + newDb + "." + SNAPSHOT
                + " FROM " + REPO + " ON (MATERIALIZED VIEW ivm_mv)" + RESTORE_PROPERTIES));
        Assertions.assertNull(GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(newDb));

        SnapshotRestoreHandler accepted = new SnapshotRestoreHandler(snapshotCopy("ivm_mv"), snapshotCopy("pct_mv"));
        accepted.restore("RESTORE SNAPSHOT " + newDb + "." + SNAPSHOT + " FROM " + REPO + RESTORE_PROPERTIES);
        Assertions.assertNotNull(GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(newDb));
        Assertions.assertEquals(Set.of("pct_mv"), accepted.submittedJob.getJobInfo().tables.keySet());
    }

    @Test
    public void testRestoreAcceptsNamedPctMv() throws Exception {
        SnapshotRestoreHandler handler = new SnapshotRestoreHandler(snapshotCopy("ivm_mv"), snapshotCopy("pct_mv"));
        handler.restore("RESTORE SNAPSHOT " + TARGET_DB + "." + SNAPSHOT + " FROM " + REPO
                + " ON (MATERIALIZED VIEW pct_mv)" + RESTORE_PROPERTIES);
        Assertions.assertNotNull(handler.submittedJob);
        Assertions.assertEquals(Set.of("pct_mv"), handler.submittedJob.getJobInfo().tables.keySet());
    }

    private static MaterializedView localMv(String name) {
        return (MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(DB, name);
    }

    private static Table snapshotCopy(String mvName) {
        return localMv(mvName).selectiveCopy(null, true, MaterializedIndex.IndexExtState.VISIBLE);
    }

    private static Object parse(String sql) throws Exception {
        return UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
    }

    private static Set<String> tableNames(List<TableRef> tableRefs) {
        return tableRefs.stream().map(TableRef::getTableName).collect(Collectors.toSet());
    }

    private static Repository mockRepository(BackupJobInfo snapshotInfo) {
        Repository repository = Mockito.mock(Repository.class);
        Mockito.when(repository.getName()).thenReturn(REPO);
        Mockito.when(repository.getId()).thenReturn(90001L);
        Mockito.doAnswer(invocation -> {
            List<BackupJobInfo> infos = invocation.getArgument(2);
            infos.add(snapshotInfo);
            return Status.OK;
        }).when(repository).getSnapshotInfoFile(Mockito.any(), Mockito.any(), Mockito.any());
        return repository;
    }

    private static final class SnapshotRestoreHandler extends BackupHandler {
        private final BackupMeta snapshotMeta;
        private final BackupJobInfo snapshotInfo = new BackupJobInfo();
        private RestoreJob submittedJob;

        SnapshotRestoreHandler(Table... snapshotTables) {
            super(GlobalStateMgr.getCurrentState());
            this.snapshotMeta = new BackupMeta(List.of(snapshotTables));
            snapshotInfo.name = SNAPSHOT;
            snapshotInfo.dbName = DB;
            snapshotInfo.dbId = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB).getId();
            for (Table table : snapshotTables) {
                BackupTableInfo tableInfo = new BackupTableInfo();
                tableInfo.id = table.getId();
                tableInfo.name = table.getName();
                snapshotInfo.tables.put(tableInfo.name, tableInfo);
            }
            getRepoMgr().replayAddRepo(mockRepository(snapshotInfo));
        }

        void restore(String sql) throws Exception {
            process(connectContext, (RestoreStmt) parse(sql));
        }

        @Override
        protected BackupMeta downloadAndDeserializeMetaInfo(BackupJobInfo jobInfo, Repository repo, RestoreStmt stmt) {
            return snapshotMeta;
        }

        @Override
        protected void addRestoreJob(RestoreJob restoreJob) {
            submittedJob = restoreJob;
        }
    }
}
