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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.backup.Status.ErrCode;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.persist.TableRefPersist;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.NodeMgr;
import com.starrocks.sql.ast.DropSnapshotStmt;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * TTL cleanup and DROP SNAPSHOT, both of which decide what to delete purely from what the repository
 * says about a snapshot.
 */
public class BackupSnapshotCleanupTest {

    private static final int THIS_CLUSTER = 1001;
    private static final int OTHER_CLUSTER = 2002;
    private static final String MD5 = "0123456789abcdef0123456789abcdef";
    private static final long NOW = 1700000000000L;

    private BackupHandler handler;
    private Repository repo;

    /** One snapshot as the repository holds it. */
    private static class RemoteSnapshot {
        String backupTimestamp = "2026-01-01-10-00-00-000";
        Integer clusterId = THIS_CLUSTER;
        String ttl = "7 DAY";
        Long expireTime = NOW - 1;
        boolean hasJobInfoFile = true;
    }

    private final Map<String, RemoteSnapshot> remote = Maps.newLinkedHashMap();
    private final List<String> deleted = Lists.newArrayList();
    private Set<String> deleteFailures = new HashSet<>();
    private boolean listFailure;
    private int savedRetryLimit;

    // What the in-lock re-verification sees. It normally reads the same snapshot the retention was
    // read from; overriding it is how a test puts a different backup under the label in between.
    private boolean overrideReverify;
    private String reverifyTimestamp;

    // How often the repository was asked for a job info file, which is what the retention cache is
    // there to keep down.
    private int listCount;
    private int readCount;

    @BeforeEach
    public void setUp() throws Exception {
        UtFrameUtils.setUpForPersistTest();
        savedRetryLimit = Config.backup_clean_retry_limit;
        remote.clear();
        deleted.clear();
        deleteFailures = new HashSet<>();
        listFailure = false;
        overrideReverify = false;
        reverifyTimestamp = null;
        listCount = 0;
        readCount = 0;

        new MockUp<NodeMgr>() {
            @Mock
            public int getClusterId() {
                return THIS_CLUSTER;
            }
        };

        new MockUp<Repository>() {
            @Mock
            public Status initRepository() {
                return Status.OK;
            }

            @Mock
            public Status listSnapshots(List<String> snapshotNames) {
                if (listFailure) {
                    return new Status(ErrCode.COMMON_ERROR, "simulated list failure");
                }
                snapshotNames.addAll(remote.keySet());
                return Status.OK;
            }

            @Mock
            public RemoteFile findJobInfoFile(String label) {
                listCount++;
                RemoteSnapshot snapshot = remote.get(label);
                if (snapshot == null || !snapshot.hasJobInfoFile) {
                    return null;
                }
                return new RemoteFile(Repository.PREFIX_JOB_INFO + snapshot.backupTimestamp + "." + MD5, true, 100);
            }

            @Mock
            public Status readJobInfoFile(String label, RemoteFile jobInfoFile, List<BackupJobInfo> infos) {
                readCount++;
                RemoteSnapshot snapshot = remote.get(label);
                BackupJobInfo info = new BackupJobInfo();
                info.clusterId = snapshot.clusterId;
                info.ttl = snapshot.ttl;
                info.expireTime = snapshot.expireTime;
                infos.add(info);
                return Status.OK;
            }

            @Mock
            public String getSnapshotTimestamp(String label) {
                if (overrideReverify) {
                    return reverifyTimestamp;
                }
                RemoteSnapshot snapshot = remote.get(label);
                return snapshot == null || !snapshot.hasJobInfoFile ? null : snapshot.backupTimestamp;
            }

            @Mock
            public Status deleteSnapshot(String label) {
                if (deleteFailures.contains(label)) {
                    return new Status(ErrCode.COMMON_ERROR, "simulated delete failure");
                }
                deleted.add(label);
                remote.remove(label);
                return Status.OK;
            }
        };

        handler = new BackupHandler(GlobalStateMgr.getCurrentState());
        repo = new Repository(10000, "repo", false, "bos://backup",
                new BlobStorage("broker", Maps.newHashMap()));
        Assertions.assertTrue(handler.getRepoMgr().addAndInitRepoIfNotExist(repo).ok());
    }

    @AfterEach
    public void tearDown() throws Exception {
        Config.backup_clean_retry_limit = savedRetryLimit;
        UtFrameUtils.tearDownForPersisTest();
    }

    private RemoteSnapshot addRemoteSnapshot(String label) {
        RemoteSnapshot snapshot = new RemoteSnapshot();
        remote.put(label, snapshot);
        return snapshot;
    }

    private void addUnfinishedRestoreJob(String label) {
        RestoreJob job = new RestoreJob(label, "2026-01-01-10-00-00-000", 1L, "db", null, false, 1, 10000,
                GlobalStateMgr.getCurrentState(), repo.getId(), null, null);
        handler.dbIdToBackupOrRestoreJob.put(1L, job);
    }

    private void addUnfinishedBackupJob(String label) {
        BackupJob job = new BackupJob(label, 2L, "db", new ArrayList<TableRefPersist>(), 10000,
                GlobalStateMgr.getCurrentState(), repo.getId());
        handler.dbIdToBackupOrRestoreJob.put(2L, job);
    }

    // ------------------------------------- automatic cleanup ----------------------------------------

    @Test
    public void testDeletesOwnExpiredSnapshot() {
        addRemoteSnapshot("expired");
        handler.cleanExpiredSnapshots();
        Assertions.assertEquals(Lists.newArrayList("expired"), deleted);
    }

    @Test
    public void testKeepsSnapshotThatHasNotExpired() {
        addRemoteSnapshot("live").expireTime = System.currentTimeMillis() + 3600_000L;
        handler.cleanExpiredSnapshots();
        Assertions.assertTrue(deleted.isEmpty());
    }

    @Test
    public void testLaterRoundsOnlyListForASnapshotAlreadyRead() {
        addRemoteSnapshot("live").expireTime = System.currentTimeMillis() + 3600_000L;
        handler.cleanExpiredSnapshots();
        int listsAfterFirstRound = listCount;
        int readsAfterFirstRound = readCount;

        handler.cleanExpiredSnapshots();
        handler.cleanExpiredSnapshots();
        // Every round asks which backup the label holds now, and that is what makes the retention
        // read in the first round safe to decide from without downloading the file again.
        Assertions.assertEquals(listsAfterFirstRound + 2, listCount);
        Assertions.assertEquals(readsAfterFirstRound, readCount);
        Assertions.assertTrue(deleted.isEmpty());
    }

    /**
     * A label remembered as kept forever, then replaced by a backup that does expire. Nothing this FE
     * did invalidated the entry, so only re-reading the backup timestamp each round finds the change.
     */
    @Test
    public void testDeletesASnapshotThatReplacedOneKeptForever() {
        RemoteSnapshot snapshot = addRemoteSnapshot("reused");
        snapshot.ttl = null;
        snapshot.expireTime = null;
        handler.cleanExpiredSnapshots();
        Assertions.assertTrue(deleted.isEmpty());

        snapshot.backupTimestamp = "2026-06-06-10-00-00-000";
        snapshot.ttl = "1 DAY";
        snapshot.expireTime = NOW - 1;

        handler.cleanExpiredSnapshots();
        Assertions.assertEquals(Lists.newArrayList("reused"), deleted);
    }

    @Test
    public void testRereadsARetentionThatWasSuperseded() {
        RemoteSnapshot snapshot = addRemoteSnapshot("reused");
        snapshot.backupTimestamp = "2026-01-01-10-00-00-000";

        // The label was dropped and backed up again after it was judged expired.
        overrideReverify = true;
        reverifyTimestamp = "2026-06-06-10-00-00-000";
        handler.cleanExpiredSnapshots();
        Assertions.assertTrue(deleted.isEmpty());

        // What the repository holds under the label now is that new backup, kept forever.
        overrideReverify = false;
        snapshot.backupTimestamp = "2026-06-06-10-00-00-000";
        snapshot.ttl = null;
        snapshot.expireTime = null;
        int readsSoFar = readCount;

        handler.cleanExpiredSnapshots();
        Assertions.assertTrue(readCount > readsSoFar, "the superseded retention must not be reused");
        Assertions.assertTrue(deleted.isEmpty());
    }

    @Test
    public void testKeepsSnapshotWithoutTtl() {
        RemoteSnapshot snapshot = addRemoteSnapshot("forever");
        snapshot.ttl = null;
        snapshot.expireTime = null;
        handler.cleanExpiredSnapshots();
        Assertions.assertTrue(deleted.isEmpty());
    }

    @Test
    public void testKeepsSnapshotOfAnotherCluster() {
        addRemoteSnapshot("theirs").clusterId = OTHER_CLUSTER;
        handler.cleanExpiredSnapshots();
        Assertions.assertTrue(deleted.isEmpty());
    }

    @Test
    public void testKeepsSnapshotWithoutClusterId() {
        addRemoteSnapshot("legacy").clusterId = null;
        handler.cleanExpiredSnapshots();
        Assertions.assertTrue(deleted.isEmpty());
    }

    @Test
    public void testKeepsSnapshotWithoutJobInfoFile() {
        addRemoteSnapshot("half_uploaded").hasJobInfoFile = false;
        handler.cleanExpiredSnapshots();
        Assertions.assertTrue(deleted.isEmpty());
    }

    @Test
    public void testSkipsReadOnlyRepository() throws Exception {
        addRemoteSnapshot("expired");
        Repository readOnlyRepo = new Repository(10001, "ro_repo", true, "bos://backup",
                new BlobStorage("broker", Maps.newHashMap()));
        BackupHandler readOnlyHandler = new BackupHandler(GlobalStateMgr.getCurrentState());
        Assertions.assertTrue(readOnlyHandler.getRepoMgr().addAndInitRepoIfNotExist(readOnlyRepo).ok());

        readOnlyHandler.cleanExpiredSnapshots();
        Assertions.assertTrue(deleted.isEmpty());
    }

    @Test
    public void testKeepsSnapshotAnUnfinishedBackupIsWriting() {
        // A backup that has already uploaded its job info file writes the timestamp the retention
        // was read from, so only the job itself says the snapshot is not finished.
        addRemoteSnapshot("in_use");
        addUnfinishedBackupJob("in_use");
        handler.cleanExpiredSnapshots();
        Assertions.assertTrue(deleted.isEmpty());
    }

    @Test
    public void testKeepsSnapshotAnUnfinishedRestoreIsReading() {
        addRemoteSnapshot("in_use");
        addUnfinishedRestoreJob("in_use");
        handler.cleanExpiredSnapshots();
        Assertions.assertTrue(deleted.isEmpty());
    }

    @Test
    public void testGivesUpAfterRepeatedFailures() {
        addRemoteSnapshot("stubborn");
        deleteFailures.add("stubborn");
        Config.backup_clean_retry_limit = 2;

        handler.cleanExpiredSnapshots();
        handler.cleanExpiredSnapshots();
        // The third round finds the retry count exhausted and stops trying.
        handler.cleanExpiredSnapshots();
        deleteFailures.clear();
        handler.cleanExpiredSnapshots();
        Assertions.assertTrue(deleted.isEmpty(), "cleanup must stay away after giving up");

        // Raising the limit lets it try again.
        Config.backup_clean_retry_limit = 5;
        handler.cleanExpiredSnapshots();
        Assertions.assertEquals(Lists.newArrayList("stubborn"), deleted);
    }

    // ------------------------------- the label was backed up again ----------------------------------

    @Test
    public void testDoesNotDeleteANewerBackupUnderTheSameLabel() {
        addRemoteSnapshot("reused").backupTimestamp = "2026-01-01-10-00-00-000";

        // The label was dropped and backed up again after it was judged expired, so what sits under
        // it now is fresh data.
        overrideReverify = true;
        reverifyTimestamp = "2026-06-06-10-00-00-000";

        handler.cleanExpiredSnapshots();
        Assertions.assertTrue(deleted.isEmpty(), "must not delete a backup other than the one judged expired");
    }

    @Test
    public void testDoesNotDeleteALabelThatIsBeingBackedUpAgain() {
        addRemoteSnapshot("reused");

        // A backup is running again under the same label, so it has written no job info file yet.
        overrideReverify = true;
        reverifyTimestamp = null;

        handler.cleanExpiredSnapshots();
        Assertions.assertTrue(deleted.isEmpty(), "must not delete a label whose backup has not finished");
    }

    @Test
    public void testCleanupLeavesARepositoryThatCannotBeListed() {
        addRemoteSnapshot("expired");
        listFailure = true;
        handler.cleanExpiredSnapshots();
        Assertions.assertTrue(deleted.isEmpty());
    }

    // ---------------------------------------- DROP SNAPSHOT -----------------------------------------

    @Test
    public void testDropOwnSnapshot() throws DdlException {
        addRemoteSnapshot("mine");
        handler.dropSnapshot(new DropSnapshotStmt("mine", "repo", false));
        Assertions.assertEquals(Lists.newArrayList("mine"), deleted);
    }

    @Test
    public void testDropThroughTheStatementExecutor() throws Exception {
        addRemoteSnapshot("mine");
        BackupHandler globalHandler = GlobalStateMgr.getCurrentState().getBackupHandler();
        Assertions.assertTrue(globalHandler.getRepoMgr().addAndInitRepoIfNotExist(repo).ok());

        ConnectContext context = new ConnectContext();
        context.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        DDLStmtExecutor.execute(new DropSnapshotStmt("mine", "repo", false), context);
        Assertions.assertEquals(Lists.newArrayList("mine"), deleted);
    }

    @Test
    public void testDropRefusesSnapshotOfAnotherCluster() {
        addRemoteSnapshot("theirs").clusterId = OTHER_CLUSTER;
        DdlException e = Assertions.assertThrows(DdlException.class,
                () -> handler.dropSnapshot(new DropSnapshotStmt("theirs", "repo", false)));
        Assertions.assertTrue(e.getMessage().contains("FORCE"), e.getMessage());
        Assertions.assertTrue(deleted.isEmpty());
    }

    @Test
    public void testDropRefusesSnapshotWithoutClusterId() {
        addRemoteSnapshot("legacy").clusterId = null;
        Assertions.assertThrows(DdlException.class,
                () -> handler.dropSnapshot(new DropSnapshotStmt("legacy", "repo", false)));
        Assertions.assertTrue(deleted.isEmpty());
    }

    @Test
    public void testDropRefusesSnapshotWithoutJobInfoFile() {
        addRemoteSnapshot("half_uploaded").hasJobInfoFile = false;
        Assertions.assertThrows(DdlException.class,
                () -> handler.dropSnapshot(new DropSnapshotStmt("half_uploaded", "repo", false)));
        Assertions.assertTrue(deleted.isEmpty());
    }

    @Test
    public void testForceDropsAnySnapshot() throws DdlException {
        addRemoteSnapshot("theirs").clusterId = OTHER_CLUSTER;
        addRemoteSnapshot("legacy").clusterId = null;
        addRemoteSnapshot("half_uploaded").hasJobInfoFile = false;

        handler.dropSnapshot(new DropSnapshotStmt("theirs", "repo", true));
        handler.dropSnapshot(new DropSnapshotStmt("legacy", "repo", true));
        handler.dropSnapshot(new DropSnapshotStmt("half_uploaded", "repo", true));
        Assertions.assertEquals(Lists.newArrayList("theirs", "legacy", "half_uploaded"), deleted);
    }

    @Test
    public void testDropRejectsUnknownRepository() {
        Assertions.assertThrows(DdlException.class,
                () -> handler.dropSnapshot(new DropSnapshotStmt("mine", "no_such_repo", false)));
    }

    @Test
    public void testDropRejectsReadOnlyRepository() throws Exception {
        addRemoteSnapshot("mine");
        Repository readOnlyRepo = new Repository(10001, "ro_repo", true, "bos://backup",
                new BlobStorage("broker", Maps.newHashMap()));
        Assertions.assertTrue(handler.getRepoMgr().addAndInitRepoIfNotExist(readOnlyRepo).ok());

        DdlException e = Assertions.assertThrows(DdlException.class,
                () -> handler.dropSnapshot(new DropSnapshotStmt("mine", "ro_repo", false)));
        Assertions.assertTrue(e.getMessage().contains("read only"), e.getMessage());
    }

    @Test
    public void testDropRejectsARepositoryThatCannotBeListed() {
        addRemoteSnapshot("mine");
        listFailure = true;
        DdlException e = Assertions.assertThrows(DdlException.class,
                () -> handler.dropSnapshot(new DropSnapshotStmt("mine", "repo", false)));
        Assertions.assertTrue(e.getMessage().contains("Failed to list snapshots"), e.getMessage());
        Assertions.assertTrue(deleted.isEmpty());
    }

    @Test
    public void testDropRejectsMissingSnapshot() {
        DdlException e = Assertions.assertThrows(DdlException.class,
                () -> handler.dropSnapshot(new DropSnapshotStmt("no_such_snapshot", "repo", false)));
        Assertions.assertTrue(e.getMessage().contains("does not exist"), e.getMessage());
    }

    @Test
    public void testDropRejectsSnapshotAnUnfinishedBackupIsWriting() {
        addRemoteSnapshot("in_use");
        addUnfinishedBackupJob("in_use");
        DdlException e = Assertions.assertThrows(DdlException.class,
                () -> handler.dropSnapshot(new DropSnapshotStmt("in_use", "repo", false)));
        Assertions.assertTrue(e.getMessage().contains("backup is writing"), e.getMessage());
        Assertions.assertTrue(deleted.isEmpty());
    }

    @Test
    public void testDropRejectsSnapshotAnUnfinishedRestoreIsReading() {
        addRemoteSnapshot("in_use");
        addUnfinishedRestoreJob("in_use");
        DdlException e = Assertions.assertThrows(DdlException.class,
                () -> handler.dropSnapshot(new DropSnapshotStmt("in_use", "repo", false)));
        Assertions.assertTrue(e.getMessage().contains("restore is reading"), e.getMessage());
        Assertions.assertTrue(deleted.isEmpty());
    }

    @Test
    public void testDropReportsDeleteFailureAndLeavesTheSnapshot() {
        addRemoteSnapshot("mine");
        deleteFailures.add("mine");

        Assertions.assertThrows(DdlException.class,
                () -> handler.dropSnapshot(new DropSnapshotStmt("mine", "repo", false)));
        Assertions.assertTrue(deleted.isEmpty());
        Assertions.assertTrue(remote.containsKey("mine"));

        // Nothing changed, so the statement can simply be repeated.
        deleteFailures.clear();
        Assertions.assertDoesNotThrow(() -> handler.dropSnapshot(new DropSnapshotStmt("mine", "repo", false)));
        Assertions.assertEquals(Lists.newArrayList("mine"), deleted);
    }
}
