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
import com.starrocks.catalog.TableName;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.Config;
import com.starrocks.common.ConfigRefreshDaemon;
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
import com.starrocks.rpc.ThriftRPCRequestExecutor;
import com.starrocks.sql.ast.ModifyFrontendAddressClause;
import com.starrocks.system.Frontend;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TRefreshTableResponse;
import com.starrocks.thrift.TStatus;
import com.starrocks.thrift.TStatusCode;
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
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;

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

    @Test
    public void testStopOneInvokesAction() throws Exception {
        GlobalStateMgr mgr = new MyGlobalStateMgr(false);
        AtomicBoolean ran = new AtomicBoolean(false);
        Runnable action = () -> ran.set(true);

        Method stopOne = GlobalStateMgr.class.getDeclaredMethod("stopOne", String.class, Runnable.class);
        stopOne.setAccessible(true);
        stopOne.invoke(mgr, "fakeDaemon", action);

        Assertions.assertTrue(ran.get(), "stopOne must invoke the action");
    }

    @Test
    public void testStopOneSwallowsThrowable() throws Exception {
        // stopOne wraps each daemon's stopGracefully so that a single misbehaving daemon
        // does not abort the demotion drain mid-way and leave later daemons running.
        GlobalStateMgr mgr = new MyGlobalStateMgr(false);
        AtomicBoolean ran = new AtomicBoolean(false);
        Runnable action = () -> {
            ran.set(true);
            throw new RuntimeException("boom");
        };

        Method stopOne = GlobalStateMgr.class.getDeclaredMethod("stopOne", String.class, Runnable.class);
        stopOne.setAccessible(true);
        // No exception expected to escape.
        stopOne.invoke(mgr, "throwingDaemon", action);

        Assertions.assertTrue(ran.get(), "action should still run");
    }

    @Test
    public void testStopLeaderOnlyDaemonThreadsDrivesEveryWiredDaemon() throws Exception {
        // Sanity test for the demotion drain wiring: every daemon listed in
        // stopLeaderOnlyDaemonThreads must be reachable and its stopGracefully invocation must
        // be shielded by stopOne, so a misbehaving daemon cannot abort the drain. The lazily
        // initialized timePrinter / txnTimeoutChecker fields are still null here, exercising the
        // null-skip branches.
        GlobalStateMgr mgr = new MyGlobalStateMgr(false);

        Method stop = GlobalStateMgr.class.getDeclaredMethod("stopLeaderOnlyDaemonThreads");
        stop.setAccessible(true);
        // None of the daemons are running; every stopGracefully is effectively a state reset.
        // Any throwable from a daemon's onStopped is contained by stopOne, so the call must
        // complete without propagating.
        stop.invoke(mgr);
    }

    @Test
    public void testStopLeaderOnlyDaemonThreadsCoversLazilyInitializedDaemons() throws Exception {
        // timePrinter and txnTimeoutChecker are created lazily after the leader has activated;
        // exercise the non-null branch so the drain stops them too.
        GlobalStateMgr mgr = new MyGlobalStateMgr(false);
        mgr.createTxnTimeoutChecker();
        mgr.createTimePrinter();

        Method stop = GlobalStateMgr.class.getDeclaredMethod("stopLeaderOnlyDaemonThreads");
        stop.setAccessible(true);
        stop.invoke(mgr);
    }

    @Test
    public void testRefreshOtherFeExecutorsResizeAfterConfigRefresh() throws Exception {
        int originalThreadNum = Config.refresh_other_fe_rpc_executor_thread_num;
        int originalAsyncThreadNum = Config.refresh_other_fe_dispatch_executor_thread_num;
        Config.refresh_other_fe_rpc_executor_thread_num = 1;
        Config.refresh_other_fe_dispatch_executor_thread_num = 1;
        GlobalStateMgr globalStateMgr = createRefreshTestGlobalStateMgr(1, 1);
        try {
            ThreadPoolExecutor rpcExecutor = getRefreshOtherFeExecutor(globalStateMgr);
            ThreadPoolExecutor dispatchExecutor = getRefreshOtherFeAsyncExecutor(globalStateMgr);
            Assertions.assertEquals(1, rpcExecutor.getCorePoolSize());
            Assertions.assertEquals(1, dispatchExecutor.getCorePoolSize());

            Config.refresh_other_fe_rpc_executor_thread_num = 2;
            Config.refresh_other_fe_dispatch_executor_thread_num = 3;
            triggerConfigRefresh(globalStateMgr.getConfigRefreshDaemon());

            Assertions.assertEquals(2, rpcExecutor.getCorePoolSize());
            Assertions.assertEquals(2, rpcExecutor.getMaximumPoolSize());
            Assertions.assertEquals(3, dispatchExecutor.getCorePoolSize());
            Assertions.assertEquals(3, dispatchExecutor.getMaximumPoolSize());
        } finally {
            Config.refresh_other_fe_rpc_executor_thread_num = originalThreadNum;
            Config.refresh_other_fe_dispatch_executor_thread_num = originalAsyncThreadNum;
            shutdownRefreshOtherFeExecutors(globalStateMgr);
        }
    }

    @Test
    public void testRefreshOtherFeExecutorsIgnoreInvalidConfigRefresh() throws Exception {
        int originalThreadNum = Config.refresh_other_fe_rpc_executor_thread_num;
        int originalAsyncThreadNum = Config.refresh_other_fe_dispatch_executor_thread_num;
        Config.refresh_other_fe_rpc_executor_thread_num = 1;
        Config.refresh_other_fe_dispatch_executor_thread_num = 1;
        GlobalStateMgr globalStateMgr = createRefreshTestGlobalStateMgr(1, 1);
        try {
            ThreadPoolExecutor rpcExecutor = getRefreshOtherFeExecutor(globalStateMgr);
            ThreadPoolExecutor dispatchExecutor = getRefreshOtherFeAsyncExecutor(globalStateMgr);
            Config.refresh_other_fe_rpc_executor_thread_num = 0;
            Config.refresh_other_fe_dispatch_executor_thread_num = -1;

            triggerConfigRefresh(globalStateMgr.getConfigRefreshDaemon());

            Assertions.assertEquals(1, rpcExecutor.getCorePoolSize());
            Assertions.assertEquals(1, rpcExecutor.getMaximumPoolSize());
            Assertions.assertEquals(1, dispatchExecutor.getCorePoolSize());
            Assertions.assertEquals(1, dispatchExecutor.getMaximumPoolSize());
        } finally {
            Config.refresh_other_fe_rpc_executor_thread_num = originalThreadNum;
            Config.refresh_other_fe_dispatch_executor_thread_num = originalAsyncThreadNum;
            shutdownRefreshOtherFeExecutors(globalStateMgr);
        }
    }

    @Test
    public void testRefreshOthersFeTableAsyncSwallowsWorkerThrowable() throws Exception {
        GlobalStateMgr globalStateMgr = Mockito.spy(createRefreshTestGlobalStateMgr(1, 1));
        try {
            Mockito.doThrow(new RuntimeException("boom"))
                    .when(globalStateMgr)
                    .refreshOthersFeTable(Mockito.any(), Mockito.anyList(), Mockito.eq(false));

            Future<?> future = globalStateMgr.refreshOthersFeTableAsync(new TableName("c", "d", "t"), List.of("p1"));
            future.get();
            Assertions.assertTrue(future.isDone());
        } finally {
            shutdownRefreshOtherFeExecutors(globalStateMgr);
        }
    }

    @Test
    public void testRefreshOthersFeTableAggregatesRpcSubmitRejection() throws Exception {
        GlobalStateMgr globalStateMgr = createRefreshTestGlobalStateMgr(1, 1);
        CountDownLatch latch = new CountDownLatch(1);
        try (MockedStatic<GlobalStateMgr> globalStateMgrMock =
                Mockito.mockStatic(GlobalStateMgr.class, Mockito.CALLS_REAL_METHODS)) {
            globalStateMgrMock.when(GlobalStateMgr::getCurrentState).thenReturn(globalStateMgr);
            ThreadPoolExecutor executor = getRefreshOtherFeExecutor(globalStateMgr);
            executor.setRejectedExecutionHandler((r, e) -> {
                throw new RejectedExecutionException("forced rejection");
            });
            executor.submit(() -> awaitLatch(latch));
            int queueCapacity = executor.getQueue().remainingCapacity();
            for (int i = 0; i < queueCapacity; i++) {
                executor.submit(() -> awaitLatch(latch));
            }

            DdlException exception = assertThrows(DdlException.class,
                    () -> globalStateMgr.refreshOthersFeTable(new TableName("c", "d", "t"), List.of("p1"), true));
            Assertions.assertTrue(exception.getMessage().contains("forced rejection"));
        } finally {
            latch.countDown();
            shutdownRefreshOtherFeExecutors(globalStateMgr);
        }
    }

    @Test
    public void testRefreshOthersFeTableAsyncDoesNotThrowOnDispatchRejection() throws Exception {
        GlobalStateMgr globalStateMgr = createRefreshTestGlobalStateMgr(1, 1);
        CountDownLatch latch = new CountDownLatch(1);
        try {
            ThreadPoolExecutor executor = getRefreshOtherFeAsyncExecutor(globalStateMgr);
            executor.setRejectedExecutionHandler((r, e) -> {
                throw new RejectedExecutionException("forced rejection");
            });
            executor.submit(() -> awaitLatch(latch));
            int queueCapacity = executor.getQueue().remainingCapacity();
            for (int i = 0; i < queueCapacity; i++) {
                executor.submit(() -> awaitLatch(latch));
            }

            Future<?> future = globalStateMgr.refreshOthersFeTableAsync(new TableName("c", "d", "t"), List.of("p1"));
            Assertions.assertTrue(future.isDone());
            Assertions.assertThrows(ExecutionException.class, future::get);
        } finally {
            latch.countDown();
            shutdownRefreshOtherFeExecutors(globalStateMgr);
        }
    }

    @Test
    public void testSubmitRefreshOtherFeRpcReturnsStatus() throws Exception {
        GlobalStateMgr globalStateMgr = createRefreshTestGlobalStateMgr(1, 1);
        new MockUp<ThriftRPCRequestExecutor>() {
            @Mock
            public <RESULT, SERVER_CLIENT extends org.apache.thrift.TServiceClient> RESULT call(
                    com.starrocks.rpc.ThriftConnectionPool<SERVER_CLIENT> genericPool,
                    TNetworkAddress address,
                    int timeoutMs,
                    ThriftRPCRequestExecutor.MethodCallable<SERVER_CLIENT, RESULT> callable) {
                return (RESULT) new TRefreshTableResponse(new TStatus(TStatusCode.OK));
            }
        };
        try {
            Future<TStatus> future = submitRefreshOtherFeRpc(globalStateMgr,
                    createFrontend(FrontendNodeType.FOLLOWER, "fe2", "127.0.0.2", 9011, 9021),
                    new TableName("c", "d", "t"), List.of("p1"));
            Assertions.assertEquals(TStatusCode.OK, future.get().getStatus_code());
        } finally {
            shutdownRefreshOtherFeExecutors(globalStateMgr);
        }
    }

    @Test
    public void testRefreshOtherFeTableRpcReturnsInternalErrorOnException() throws Exception {
        GlobalStateMgr globalStateMgr = createRefreshTestGlobalStateMgr(1, 1);
        new MockUp<ThriftRPCRequestExecutor>() {
            @Mock
            public <RESULT, SERVER_CLIENT extends org.apache.thrift.TServiceClient> RESULT call(
                    com.starrocks.rpc.ThriftConnectionPool<SERVER_CLIENT> genericPool,
                    TNetworkAddress address,
                    int timeoutMs,
                    ThriftRPCRequestExecutor.MethodCallable<SERVER_CLIENT, RESULT> callable) {
                throw new RuntimeException("rpc failed");
            }
        };
        try {
            TStatus status = refreshOtherFeTableRpc(globalStateMgr, new TNetworkAddress("127.0.0.2", 9021),
                    new TableName("c", "d", "t"), List.of("p1"));
            Assertions.assertEquals(TStatusCode.INTERNAL_ERROR, status.getStatus_code());
            Assertions.assertTrue(status.getError_msgs().contains("rpc failed"));
        } finally {
            shutdownRefreshOtherFeExecutors(globalStateMgr);
        }
    }

    private GlobalStateMgr createRefreshTestGlobalStateMgr(int refreshThreadNum) throws Exception {
        int originalThreadNum = Config.refresh_other_fe_rpc_executor_thread_num;
        int originalAsyncThreadNum = Config.refresh_other_fe_dispatch_executor_thread_num;
        Config.refresh_other_fe_rpc_executor_thread_num = refreshThreadNum;
        Config.refresh_other_fe_dispatch_executor_thread_num = refreshThreadNum;
        try {
            return createRefreshTestGlobalStateMgr(refreshThreadNum, refreshThreadNum);
        } finally {
            Config.refresh_other_fe_rpc_executor_thread_num = originalThreadNum;
            Config.refresh_other_fe_dispatch_executor_thread_num = originalAsyncThreadNum;
        }
    }

    private GlobalStateMgr createRefreshTestGlobalStateMgr(int refreshThreadNum, int refreshAsyncThreadNum)
            throws Exception {
        int originalThreadNum = Config.refresh_other_fe_rpc_executor_thread_num;
        int originalAsyncThreadNum = Config.refresh_other_fe_dispatch_executor_thread_num;
        Config.refresh_other_fe_rpc_executor_thread_num = refreshThreadNum;
        Config.refresh_other_fe_dispatch_executor_thread_num = refreshAsyncThreadNum;
        try {
            NodeMgr nodeMgr = new NodeMgr();
            setFrontends(nodeMgr, List.of(
                    createFrontend(FrontendNodeType.LEADER, "fe1", "127.0.0.1", 9010, 9020),
                    createFrontend(FrontendNodeType.FOLLOWER, "fe2", "127.0.0.2", 9011, 9021),
                    createFrontend(FrontendNodeType.FOLLOWER, "fe3", "127.0.0.3", 9012, 9022)));
            setSelfNode(nodeMgr, new Pair<>("127.0.0.1", 9010));
            setRole(nodeMgr, FrontendNodeType.LEADER);
            return new GlobalStateMgr(nodeMgr);
        } finally {
            Config.refresh_other_fe_rpc_executor_thread_num = originalThreadNum;
            Config.refresh_other_fe_dispatch_executor_thread_num = originalAsyncThreadNum;
        }
    }

    private Frontend createFrontend(FrontendNodeType type, String name, String host, int editLogPort, int rpcPort) {
        Frontend frontend = new Frontend(type, name, host, editLogPort);
        frontend.setRpcPort(rpcPort);
        return frontend;
    }

    private void setFrontends(NodeMgr nodeMgr, List<Frontend> frontendList) throws Exception {
        Field field = nodeMgr.getClass().getDeclaredField("frontends");
        field.setAccessible(true);
        ConcurrentHashMap<String, Frontend> frontends = new ConcurrentHashMap<>();
        for (Frontend frontend : frontendList) {
            frontends.put(frontend.getNodeName(), frontend);
        }
        field.set(nodeMgr, frontends);
    }

    private void setSelfNode(NodeMgr nodeMgr, Pair<String, Integer> selfNode) throws Exception {
        Field field = nodeMgr.getClass().getDeclaredField("selfNode");
        field.setAccessible(true);
        field.set(nodeMgr, selfNode);
    }

    private void setRole(NodeMgr nodeMgr, FrontendNodeType role) throws Exception {
        Field field = nodeMgr.getClass().getDeclaredField("role");
        field.setAccessible(true);
        field.set(nodeMgr, role);
    }

    private ThreadPoolExecutor getRefreshOtherFeExecutor(GlobalStateMgr globalStateMgr) throws Exception {
        Field field = GlobalStateMgr.class.getDeclaredField("refreshOtherFeRpcExecutor");
        field.setAccessible(true);
        return (ThreadPoolExecutor) field.get(globalStateMgr);
    }

    private ThreadPoolExecutor getRefreshOtherFeAsyncExecutor(GlobalStateMgr globalStateMgr) throws Exception {
        Field field = GlobalStateMgr.class.getDeclaredField("refreshOtherFeDispatchExecutor");
        field.setAccessible(true);
        return (ThreadPoolExecutor) field.get(globalStateMgr);
    }

    private void shutdownRefreshOtherFeExecutors(GlobalStateMgr globalStateMgr) throws Exception {
        shutdownExecutor(globalStateMgr, "refreshOtherFeRpcExecutor");
        shutdownExecutor(globalStateMgr, "refreshOtherFeDispatchExecutor");
    }

    private void shutdownExecutor(GlobalStateMgr globalStateMgr, String fieldName) throws Exception {
        Field field = GlobalStateMgr.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        ExecutorService executor = (ExecutorService) field.get(globalStateMgr);
        executor.shutdownNow();
    }

    private void triggerConfigRefresh(ConfigRefreshDaemon configRefreshDaemon) throws Exception {
        Method method = ConfigRefreshDaemon.class.getDeclaredMethod("runAfterCatalogReady");
        method.setAccessible(true);
        method.invoke(configRefreshDaemon);
    }

    private void awaitLatch(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @SuppressWarnings("unchecked")
    private Future<TStatus> submitRefreshOtherFeRpc(GlobalStateMgr globalStateMgr, Frontend fe,
                                                    TableName tableName, List<String> partitions) throws Exception {
        Method method = GlobalStateMgr.class.getDeclaredMethod(
                "submitRefreshOtherFeRpc", Frontend.class, TableName.class, List.class);
        method.setAccessible(true);
        return (Future<TStatus>) method.invoke(globalStateMgr, fe, tableName, partitions);
    }

    private TStatus refreshOtherFeTableRpc(GlobalStateMgr globalStateMgr, TNetworkAddress thriftAddress,
                                           TableName tableName, List<String> partitions) throws Exception {
        Method method = GlobalStateMgr.class.getDeclaredMethod(
                "refreshOtherFeTableRpc", TNetworkAddress.class, TableName.class, List.class);
        method.setAccessible(true);
        return (TStatus) method.invoke(globalStateMgr, thriftAddress, tableName, partitions);
    }
}
