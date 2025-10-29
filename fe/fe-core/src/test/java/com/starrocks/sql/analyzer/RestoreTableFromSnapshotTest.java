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

package com.starrocks.sql.analyzer;

import com.starrocks.alter.AlterTest;
import com.starrocks.common.StarRocksException;
import com.starrocks.lake.snapshot.ClusterSnapshotJob.ClusterSnapshotJobState;
import com.starrocks.lake.snapshot.ClusterSnapshotMgrEPack;
import com.starrocks.lake.snapshot.ManualClusterSnapshotJob;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.StorageVolumeMgr;
import com.starrocks.sql.ast.DdlStmt;
import com.starrocks.sql.ast.RestoreTableFromSnapshotStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.TableRef;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class RestoreTableFromSnapshotTest {

    @BeforeAll
    public static void beforeClass() throws Exception {
        if (AnalyzeTestUtil.getConnectContext() == null) {
            AlterTest.beforeClass();
            AnalyzeTestUtil.init();
        }
    }

    @Test
    public void testRestoreTableFromSnapshotRequiresSharedDataMode() {
        analyzeFail("RESTORE TABLE test.t0 FROM SNAPSHOT snapshot_missing TO TABLE test.t0_copy",
                "Table snapshot recovery only supports shared data mode");
    }

    @Test
    public void testRestoreTableFromSnapshotSuccess() {
        MockUp<RunMode> mock = new MockUp<RunMode>() {
            @Mock
            public RunMode getCurrentRunMode() {
                return RunMode.SHARED_DATA;
            }

            @Mock
            public boolean isSharedDataMode() {
                return true;
            }
        };
        String snapshotName = "snapshot_for_copy_ut";
        ClusterSnapshotMgrEPack mgr =
                (ClusterSnapshotMgrEPack) GlobalStateMgr.getCurrentState().getClusterSnapshotMgr();
        long jobId = GlobalStateMgr.getCurrentState().getNextId();
        ManualClusterSnapshotJob job = new ManualClusterSnapshotJob(jobId, snapshotName,
                StorageVolumeMgr.BUILTIN_STORAGE_VOLUME, System.currentTimeMillis());
        job.setState(ClusterSnapshotJobState.FINISHED);
        mgr.addManualClusterSnapshotJob(job);
        try {
            analyzeSuccess(String.format(
                    "RESTORE TABLE test.t0 FROM SNAPSHOT %s TO TABLE test.t0_copy " +
                            "WITH PROPERTIES (\"copy_mode\"=\"logical\")",
                    snapshotName));
        } finally {
            mgr.getManualClusterSnapshotJobs().remove(jobId);
        }
    }

    @Test
    public void testRestoreTableFromSnapshotMissingSnapshot() {
        MockUp<RunMode> mock = new MockUp<RunMode>() {
            @Mock
            public RunMode getCurrentRunMode() {
                return RunMode.SHARED_DATA;
            }

            @Mock
            public boolean isSharedDataMode() {
                return true;
            }
        };
        try {
            analyzeFail("RESTORE TABLE test.t0 FROM SNAPSHOT snapshot_not_ready TO TABLE test.t0_copy",
                    "Cluster snapshot 'snapshot_not_ready' is not available for restore");
        } finally {
            // no-op
        }
    }

    @Test
    public void testParseRestoreTableFromSnapshotStatement() throws Exception {
        String sql = "RESTORE TABLE test.t0 FROM SNAPSHOT snapshot_parse TO TABLE test.t0_copy " +
                "WITH PROPERTIES (\"copy_mode\"=\"logical\", \"keep_partition\"=\"false\")";
        ConnectContext ctx = AnalyzeTestUtil.getConnectContext();
        StatementBase stmt = UtFrameUtils.parseStmtWithNewParserNotIncludeAnalyzer(sql, ctx);
        Assertions.assertTrue(stmt instanceof RestoreTableFromSnapshotStmt);
        RestoreTableFromSnapshotStmt restoreStmt = (RestoreTableFromSnapshotStmt) stmt;
        TableRef sourceRef = restoreStmt.getSourceTable();
        TableRef targetRef = restoreStmt.getTargetTable();
        Assertions.assertEquals("test", sourceRef.getDbName());
        Assertions.assertEquals("t0", sourceRef.getTableName());
        Assertions.assertEquals("snapshot_parse", restoreStmt.getSnapshotName());
        Assertions.assertEquals("test", targetRef.getDbName());
        Assertions.assertEquals("t0_copy", targetRef.getTableName());
        Assertions.assertEquals("logical", restoreStmt.getProperties().get("copy_mode"));
        Assertions.assertEquals("false", restoreStmt.getProperties().get("keep_partition"));
    }

    @Test
    public void testRestoreTableFromSnapshotDdlExecutorInvokesManager() throws Exception {
        String snapshotName = "snapshot_for_exec";
        ClusterSnapshotMgrEPack mgr =
                (ClusterSnapshotMgrEPack) GlobalStateMgr.getCurrentState().getClusterSnapshotMgr();
        long jobId = GlobalStateMgr.getCurrentState().getNextId();
        ManualClusterSnapshotJob job = new ManualClusterSnapshotJob(jobId, snapshotName,
                StorageVolumeMgr.BUILTIN_STORAGE_VOLUME, System.currentTimeMillis());
        job.setState(ClusterSnapshotJobState.FINISHED);
        mgr.addManualClusterSnapshotJob(job);

        MockUp<RunMode> runModeMock = new MockUp<RunMode>() {
            @Mock
            public RunMode getCurrentRunMode() {
                return RunMode.SHARED_DATA;
            }

            @Mock
            public boolean isSharedDataMode() {
                return true;
            }
        };

        ConnectContext ctx = AnalyzeTestUtil.getConnectContext();
        String sql = String.format("RESTORE TABLE test.t0 FROM SNAPSHOT %s TO TABLE test.t0_copy", snapshotName);
        StatementBase parsed = UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Assertions.assertTrue(parsed instanceof RestoreTableFromSnapshotStmt);

        AtomicBoolean invoked = new AtomicBoolean(false);
        MockUp<ClusterSnapshotMgrEPack> submitMock = new MockUp<ClusterSnapshotMgrEPack>() {
            @Mock
            public void submitTableSnapshotRestore(RestoreTableFromSnapshotStmt stmt, ConnectContext context)
                    throws StarRocksException {
                invoked.set(true);
                throw new StarRocksException("mock submit");
            }
        };

        try {
            StarRocksException ex = Assertions.assertThrows(StarRocksException.class,
                    () -> DDLStmtExecutor.execute((DdlStmt) parsed, ctx));
            Assertions.assertTrue(invoked.get());
            Assertions.assertEquals("mock submit", ex.getMessage());
        } finally {
            mgr.getManualClusterSnapshotJobs().remove(jobId);
        }
    }
}
