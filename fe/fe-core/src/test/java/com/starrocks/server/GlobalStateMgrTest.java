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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/catalog/CatalogTest.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.server;

import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.rep.MasterStateException;
import com.sleepycat.je.rep.MemberNotFoundException;
import com.sleepycat.je.rep.ReplicaStateException;
import com.sleepycat.je.rep.UnknownMasterException;
import com.sleepycat.je.rep.util.ReplicationGroupAdmin;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.Pair;
import com.starrocks.ha.BDBHA;
import com.starrocks.ha.FrontendNodeType;
import com.starrocks.journal.JournalException;
import com.starrocks.journal.JournalInconsistentException;
import com.starrocks.journal.bdbje.BDBEnvironment;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.ImageWriter;
import com.starrocks.persist.OperationType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.ModifyFrontendAddressClause;
import com.starrocks.system.Frontend;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.File;
import java.lang.reflect.Field;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class GlobalStateMgrTest {
    private String testMetaDir;
    private String testPluginDir;

    @BeforeEach
    public void setUp() {
        testMetaDir = UUID.randomUUID().toString();
        testPluginDir = UUID.randomUUID().toString();
        Config.meta_dir = testMetaDir;
        Config.plugin_dir = testPluginDir;
    }

    @AfterEach
    public void tearDown() throws Exception {
        FileUtils.deleteQuietly(new File(testMetaDir));
        FileUtils.deleteQuietly(new File(testPluginDir));
    }

    @Test
    public void testSaveLoadHeader() throws Exception {
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();

        ImageWriter imageWriter = new ImageWriter("", 0);
        // test json-format header
        UtFrameUtils.PseudoImage image2 = new UtFrameUtils.PseudoImage();
        imageWriter.setOutputStream(image2.getDataOutputStream());
        globalStateMgr.saveHeader(imageWriter.getDataOutputStream());
        globalStateMgr.loadHeader(image2.getDataInputStream());
    }

    private GlobalStateMgr mockGlobalStateMgr() throws Exception {
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();

        NodeMgr nodeMgr = new NodeMgr();

        Field field1 = nodeMgr.getClass().getDeclaredField("frontends");
        field1.setAccessible(true);
        ConcurrentHashMap<String, Frontend> frontends = new ConcurrentHashMap<>();
        Frontend fe1 = new Frontend(FrontendNodeType.LEADER, "testName", "127.0.0.1", 1000);
        frontends.put("testName", fe1);
        field1.set(nodeMgr, frontends);

        Pair<String, Integer> selfNode = new Pair<>("test-address", 1000);
        Field field2 = nodeMgr.getClass().getDeclaredField("selfNode");
        field2.setAccessible(true);
        field2.set(nodeMgr, selfNode);

        Field field3 = nodeMgr.getClass().getDeclaredField("role");
        field3.setAccessible(true);
        field3.set(nodeMgr, FrontendNodeType.LEADER);

        Field field4 = globalStateMgr.getClass().getDeclaredField("nodeMgr");
        field4.setAccessible(true);
        field4.set(globalStateMgr, nodeMgr);

        return globalStateMgr;
    }

    @Mocked
    BDBEnvironment env;

    @Mocked
    ReplicationGroupAdmin replicationGroupAdmin;

    @Test
    public void testUpdateFrontend(@Mocked EditLog editLog) throws Exception {

        new Expectations() {
            {
                env.getReplicationGroupAdmin();
                result = replicationGroupAdmin;
            }
        };

        new MockUp<ReplicationGroupAdmin>() {
            @Mock
            public void updateAddress(String nodeName, String newHostName, int newPort)
                    throws EnvironmentFailureException,
                    MasterStateException,
                    MemberNotFoundException,
                    ReplicaStateException,
                    UnknownMasterException {
            }
        };

        GlobalStateMgr globalStateMgr = mockGlobalStateMgr();
        BDBHA ha = new BDBHA(env, "testNode");
        globalStateMgr.setHaProtocol(ha);
        globalStateMgr.setEditLog(editLog);
        List<Frontend> frontends = globalStateMgr.getNodeMgr().getFrontends(null);
        Frontend fe = frontends.get(0);
        ModifyFrontendAddressClause clause = new ModifyFrontendAddressClause(fe.getHost(), "sandbox-fqdn");
        globalStateMgr.getNodeMgr().modifyFrontendHost(clause);
    }

    @Test
    public void testUpdateFeNotFoundException() {
        assertThrows(DdlException.class, () -> {
            GlobalStateMgr globalStateMgr = mockGlobalStateMgr();
            ModifyFrontendAddressClause clause = new ModifyFrontendAddressClause("test", "sandbox-fqdn");
            // this case will occur [frontend does not exist] exception
            globalStateMgr.getNodeMgr().modifyFrontendHost(clause);
        });
    }

    @Test
    public void testUpdateModifyCurrentMasterException() {
        assertThrows(DdlException.class, () -> {
            GlobalStateMgr globalStateMgr = mockGlobalStateMgr();
            ModifyFrontendAddressClause clause = new ModifyFrontendAddressClause("test-address", "sandbox-fqdn");
            // this case will occur [can not modify current master node] exception
            globalStateMgr.getNodeMgr().modifyFrontendHost(clause);
        });
    }

    @Test
    public void testAddRepeatedFe() {
        assertThrows(DdlException.class, () -> {
            GlobalStateMgr globalStateMgr = mockGlobalStateMgr();
            globalStateMgr.getNodeMgr().addFrontend(FrontendNodeType.FOLLOWER, "127.0.0.1", 1000);
        });
    }

    @Test
    public void testCanSkipBadReplayedJournal() {
        boolean originVal = Config.metadata_journal_ignore_replay_failure;
        Config.metadata_journal_ignore_replay_failure = false;

        // when recover_on_load_journal_failed is false, the failure of every operation type can not be skipped.
        Assertions.assertFalse(GlobalStateMgr.getServingState().canSkipBadReplayedJournal(
                new JournalException(OperationType.OP_ADD_ANALYZE_STATUS, "failed")));
        Assertions.assertFalse(GlobalStateMgr.getServingState().canSkipBadReplayedJournal(
                new JournalException(OperationType.OP_CREATE_DB_V2, "failed")));
        Assertions.assertFalse(GlobalStateMgr.getServingState().canSkipBadReplayedJournal(
                new JournalInconsistentException(OperationType.OP_ADD_ANALYZE_STATUS, "failed")));
        Assertions.assertFalse(GlobalStateMgr.getServingState().canSkipBadReplayedJournal(
                new JournalInconsistentException(OperationType.OP_CREATE_DB_V2, "failed")));

        Config.metadata_journal_ignore_replay_failure = true;
        // when recover_on_load_journal_failed is false, the failure of recoverable operation type can be skipped.
        Assertions.assertTrue(GlobalStateMgr.getServingState().canSkipBadReplayedJournal(
                new JournalException(OperationType.OP_ADD_ANALYZE_STATUS, "failed")));
        Assertions.assertFalse(GlobalStateMgr.getServingState().canSkipBadReplayedJournal(
                new JournalException(OperationType.OP_CREATE_DB_V2, "failed")));
        Assertions.assertTrue(GlobalStateMgr.getServingState().canSkipBadReplayedJournal(
                new JournalInconsistentException(OperationType.OP_ADD_ANALYZE_STATUS, "failed")));
        Assertions.assertFalse(GlobalStateMgr.getServingState().canSkipBadReplayedJournal(
                new JournalInconsistentException(OperationType.OP_CREATE_DB_V2, "failed")));

        Config.metadata_journal_ignore_replay_failure = originVal;

        // when metadata_enable_recovery_mode is true, all types of failure can be skipped.
        originVal = Config.metadata_enable_recovery_mode;
        Config.metadata_enable_recovery_mode = true;
        Assertions.assertTrue(GlobalStateMgr.getServingState().canSkipBadReplayedJournal(
                new JournalException(OperationType.OP_ADD_ANALYZE_STATUS, "failed")));
        Assertions.assertTrue(GlobalStateMgr.getServingState().canSkipBadReplayedJournal(
                new JournalException(OperationType.OP_CREATE_DB_V2, "failed")));
        Assertions.assertTrue(GlobalStateMgr.getServingState().canSkipBadReplayedJournal(
                new JournalInconsistentException(OperationType.OP_ADD_ANALYZE_STATUS, "failed")));
        Assertions.assertTrue(GlobalStateMgr.getServingState().canSkipBadReplayedJournal(
                new JournalInconsistentException(OperationType.OP_CREATE_DB_V2, "failed")));
        Config.metadata_enable_recovery_mode = originVal;
    }

    @Test
    public void testLeaderLeaseActivation() {
        GlobalStateMgr globalStateMgr = new GlobalStateMgr(new NodeMgr());
        globalStateMgr.beginLeaderActivation();
        Assertions.assertEquals(GlobalStateMgr.LeaderRoleState.ACTIVATING, globalStateMgr.getLeaderRoleState());
        Assertions.assertFalse(globalStateMgr.isLeaderWorkAdmissionOpen());
        Assertions.assertEquals(LeaderLease.INVALID, globalStateMgr.captureLeaderLease());

        globalStateMgr.setFrontendNodeType(FrontendNodeType.LEADER);
        globalStateMgr.publishLeaderLease(101L);

        LeaderLease lease = globalStateMgr.captureLeaderLeaseOrThrow();
        Assertions.assertEquals(101L, lease.getHaEpoch());
        Assertions.assertEquals(1L, lease.getGeneration());
        Assertions.assertTrue(globalStateMgr.isLeaderLeaseValid(lease));
        Assertions.assertTrue(globalStateMgr.isLeaderWorkAdmissionOpen());
        Assertions.assertEquals(GlobalStateMgr.LeaderRoleState.ACTIVE, globalStateMgr.getLeaderRoleState());
        Assertions.assertNull(globalStateMgr.getPendingDemotionTargetType());
    }

    @Test
    public void testLeaderLeaseActivationAllowsEpochZero() {
        GlobalStateMgr globalStateMgr = new GlobalStateMgr(new NodeMgr());
        globalStateMgr.beginLeaderActivation();
        globalStateMgr.setFrontendNodeType(FrontendNodeType.LEADER);
        globalStateMgr.publishLeaderLease(0L);

        LeaderLease lease = globalStateMgr.captureLeaderLeaseOrThrow();
        Assertions.assertEquals(0L, lease.getHaEpoch());
        Assertions.assertTrue(lease.isValid());
        Assertions.assertTrue(globalStateMgr.isLeaderLeaseValid(lease));
    }

    @Test
    public void testLeaderLeaseInvalidatedByDemotionSkeleton() {
        GlobalStateMgr globalStateMgr = new GlobalStateMgr(new NodeMgr());
        globalStateMgr.beginLeaderActivation();
        globalStateMgr.setFrontendNodeType(FrontendNodeType.LEADER);
        globalStateMgr.publishLeaderLease(102L);
        LeaderLease lease = globalStateMgr.captureLeaderLeaseOrThrow();

        globalStateMgr.beginLeaderDemotion(FrontendNodeType.FOLLOWER);

        Assertions.assertFalse(globalStateMgr.isLeaderWorkAdmissionOpen());
        Assertions.assertTrue(globalStateMgr.isLeaderDemoting());
        Assertions.assertFalse(globalStateMgr.isLeaderLeaseValid(lease));
        Assertions.assertEquals(LeaderLease.INVALID, globalStateMgr.captureLeaderLease());
        Assertions.assertEquals(GlobalStateMgr.LeaderRoleState.DEMOTING, globalStateMgr.getLeaderRoleState());
        Assertions.assertEquals(FrontendNodeType.FOLLOWER, globalStateMgr.getPendingDemotionTargetType());
        Assertions.assertThrows(IllegalStateException.class, globalStateMgr::captureLeaderLeaseOrThrow);
    }

    @Test
    public void testLeaderGenerationBumpsAcrossDemotionAndReelection() {
        GlobalStateMgr globalStateMgr = new GlobalStateMgr(new NodeMgr());
        globalStateMgr.beginLeaderActivation();
        globalStateMgr.setFrontendNodeType(FrontendNodeType.LEADER);
        globalStateMgr.publishLeaderLease(103L);
        LeaderLease firstLease = globalStateMgr.captureLeaderLeaseOrThrow();

        globalStateMgr.beginLeaderDemotion(FrontendNodeType.FOLLOWER);
        globalStateMgr.setFrontendNodeType(FrontendNodeType.FOLLOWER);

        globalStateMgr.beginLeaderActivation();
        globalStateMgr.setFrontendNodeType(FrontendNodeType.LEADER);
        globalStateMgr.publishLeaderLease(104L);
        LeaderLease secondLease = globalStateMgr.captureLeaderLeaseOrThrow();

        Assertions.assertTrue(secondLease.getGeneration() > firstLease.getGeneration());
        Assertions.assertEquals(104L, secondLease.getHaEpoch());
        Assertions.assertFalse(globalStateMgr.isLeaderLeaseValid(firstLease));
    }

    @Test
    public void testLeaderLeaseRollbackAfterActivationFailure() {
        GlobalStateMgr globalStateMgr = new GlobalStateMgr(new NodeMgr());
        globalStateMgr.beginLeaderActivation();
        globalStateMgr.setFrontendNodeType(FrontendNodeType.LEADER);
        globalStateMgr.publishLeaderLease(105L);
        LeaderLease lease = globalStateMgr.captureLeaderLeaseOrThrow();

        globalStateMgr.rollbackLeaderActivation();

        Assertions.assertFalse(globalStateMgr.isLeaderWorkAdmissionOpen());
        Assertions.assertFalse(globalStateMgr.isLeaderLeaseValid(lease));
        Assertions.assertEquals(LeaderLease.INVALID, globalStateMgr.captureLeaderLease());
        Assertions.assertEquals(GlobalStateMgr.LeaderRoleState.INACTIVE, globalStateMgr.getLeaderRoleState());
        Assertions.assertNull(globalStateMgr.getPendingDemotionTargetType());
    }

    private static class MyGlobalStateMgr extends GlobalStateMgr {
        public static final String ERROR_MESSAGE = "Create Exception here.";
        private final boolean throwException;

        public MyGlobalStateMgr(boolean throwException) {
            super();
            this.throwException = throwException;
        }

        public MyGlobalStateMgr(boolean throwException, NodeMgr nodeMgr) {
            super(nodeMgr);
            this.throwException = throwException;
        }

        @Override
        public void initJournal() throws JournalException, InterruptedException {
            if (throwException) {
                throw new UnsupportedOperationException(ERROR_MESSAGE);
            }
            super.initJournal();
        }
    }

    @Test
    public void testRemoveRoleAndVersionFileAtFirstTimeStarting() {
        GlobalStateMgr globalStateMgr = new MyGlobalStateMgr(true);
        NodeMgr nodeMgr = globalStateMgr.getNodeMgr();
        Assertions.assertTrue(nodeMgr.isVersionAndRoleFilesNotExist());
        try {
            globalStateMgr.initialize(null);
        } catch (Exception e) {
            Assertions.assertTrue(e instanceof UnsupportedOperationException);
            Assertions.assertEquals(MyGlobalStateMgr.ERROR_MESSAGE, e.getMessage());
        }
        Assertions.assertTrue(nodeMgr.isVersionAndRoleFilesNotExist());
    }

    @Test
    public void testSuccessfullyInitializeGlobalStateMgr() {
        GlobalStateMgr globalStateMgr = new MyGlobalStateMgr(false);
        NodeMgr nodeMgr = globalStateMgr.getNodeMgr();
        Assertions.assertTrue(nodeMgr.isVersionAndRoleFilesNotExist());
        try {
            globalStateMgr.initialize(null);
        } catch (Exception e) {
            Assertions.fail("No exception is expected here.");
        }
        Assertions.assertFalse(nodeMgr.isVersionAndRoleFilesNotExist());
    }

    @Test
    public void testErrorOccursWhileRemovingClusterIdAndRoleWhenStartAtFirstTime() {
        final String removeFileErrorMessage = "Failed to delete role and version files.";

        NodeMgr nodeMgr = Mockito.spy(new NodeMgr());
        Mockito.doThrow(new RuntimeException(removeFileErrorMessage)).when(nodeMgr).removeClusterIdAndRole();

        GlobalStateMgr globalStateMgr = new MyGlobalStateMgr(true, nodeMgr);
        Assertions.assertTrue(nodeMgr.isVersionAndRoleFilesNotExist());
        try {
            globalStateMgr.initialize(null);
        } catch (Exception e) {
            Assertions.assertTrue(e instanceof UnsupportedOperationException);
            Assertions.assertEquals(MyGlobalStateMgr.ERROR_MESSAGE, e.getMessage());

            Throwable[] suppressedExceptions = e.getSuppressed();
            Assertions.assertEquals(1, suppressedExceptions.length);
            Assertions.assertTrue(suppressedExceptions[0] instanceof RuntimeException);
            Assertions.assertEquals(removeFileErrorMessage, suppressedExceptions[0].getMessage());
        }
    }

    @Test
    public void testReloadTables() throws Exception {
        ConnectContext ctx = UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT);
        UtFrameUtils.createMinStarRocksCluster();
        GlobalStateMgr currentState = GlobalStateMgr.getCurrentState();
        StarRocksAssert starRocksAssert = new StarRocksAssert();

        currentState.getLocalMetastore().createDb("db1");
        currentState.getLocalMetastore().createDb("db2");
        {
            String sql = "create table db1.t1(c1 int not null, c2 int) " +
                    "properties('replication_num'='1', 'unique_constraints'='c1') ";
            starRocksAssert.withTable(sql);
        }
        {
            String sql = "create table db2.t1(c1 int, c2 int) properties('replication_num'='1'," +
                    "'foreign_key_constraints'='(c1) REFERENCES db1.t1(c1)')";
            starRocksAssert.withTable(sql);
        }

        // move image file
        String imagePath = currentState.dumpImage();
        Path targetPath = Path.of(Config.meta_dir, GlobalStateMgr.IMAGE_DIR, "/v2",
                Path.of(imagePath).getFileName().toString());
        Files.move(Path.of(imagePath), targetPath);
        // Move all checksum files instead of a single file
        Path checksumDir = Path.of(Config.meta_dir);
        Path checksumTargetDir = Path.of(Config.meta_dir, GlobalStateMgr.IMAGE_DIR, "/v2");
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(checksumDir, "checksum.*")) {
            for (Path file : stream) {
                Path target = checksumTargetDir.resolve(file.getFileName());
                Files.move(file, target);
            }
        }

        GlobalStateMgr newState = new MyGlobalStateMgr(false);
        newState.loadImage();
        Table table = newState.getLocalMetastore().getTable("db1", "t1");
        Assertions.assertNotNull(table);
        table = newState.getLocalMetastore().getTable("db2", "t1");
        Assertions.assertEquals(1, table.getForeignKeyConstraints().size());
    }
}
