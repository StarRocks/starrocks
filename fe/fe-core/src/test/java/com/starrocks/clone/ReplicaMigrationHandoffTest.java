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

package com.starrocks.clone;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.FakeEditLog;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.LocalTablet.TabletHealthStatus;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.Replica.ReplicaState;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.common.Config;
import com.starrocks.persist.EditLog;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.server.NodeMgr;
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TFinishTaskRequest;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TTabletInfo;
import com.starrocks.transaction.GtidGenerator;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ReplicaMigrationHandoffTest {
    private static final long DB_ID = 1;
    private static final long TABLE_ID = 2;
    private static final long PARTITION_ID = 3;
    private static final long INDEX_ID = 4;
    private static final long TABLET_ID = 5;
    private static final long SOURCE_A = 10001;
    private static final long SOURCE_B = 10002;
    private static final long TARGET = 10003;
    private static final long SNAPSHOT = 100;
    private static final long VISIBLE = 120;
    private static final long LATEST = 127;
    private static final int SCHEMA_HASH = 17;

    @Mocked
    private GlobalStateMgr globalStateMgr;
    @Mocked
    private NodeMgr nodeMgr;
    @Mocked
    private EditLog editLog;
    @Mocked
    private LocalMetastore localMetastore;
    @Mocked
    private Database database;
    @Mocked
    private OlapTable table;

    private SystemInfoService infoService;
    private TabletInvertedIndex invertedIndex;
    private boolean previousSyncPublish;

    @BeforeEach
    public void setUp() {
        previousSyncPublish = Config.enable_sync_publish;
        Config.enable_sync_publish = true;
        infoService = new SystemInfoService();
        invertedIndex = new TabletInvertedIndex();
        GtidGenerator gtidGenerator = new GtidGenerator();
        new FakeEditLog();
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                minTimes = 0;
                GlobalStateMgr.isCheckpointThread();
                result = false;
                minTimes = 0;
                globalStateMgr.getNodeMgr();
                result = nodeMgr;
                minTimes = 0;
                nodeMgr.getClusterInfo();
                result = infoService;
                minTimes = 0;
                globalStateMgr.getTabletInvertedIndex();
                result = invertedIndex;
                minTimes = 0;
                globalStateMgr.getEditLog();
                result = editLog;
                minTimes = 0;
                globalStateMgr.getGtidGenerator();
                result = gtidGenerator;
                minTimes = 0;
                globalStateMgr.getLocalMetastore();
                result = localMetastore;
                minTimes = 0;
                localMetastore.getDbIncludeRecycleBin(DB_ID);
                result = database;
                minTimes = 0;
                localMetastore.getTableIncludeRecycleBin(database, TABLE_ID);
                result = table;
                minTimes = 0;
            }
        };
    }

    @AfterEach
    public void tearDown() {
        Config.enable_sync_publish = previousSyncPublish;
    }

    private final class Fixture {
        private final Replica sourceA = new Replica(101, SOURCE_A, SCHEMA_HASH, ReplicaState.NORMAL);
        private final Replica sourceB = new Replica(102, SOURCE_B, SCHEMA_HASH, ReplicaState.NORMAL);
        private final Replica target = new Replica(103, TARGET, SCHEMA_HASH, ReplicaState.NORMAL);
        private final LocalTablet tablet = new LocalTablet(TABLET_ID);
        private final PhysicalPartition partition = new PhysicalPartition(PARTITION_ID, TABLE_ID);
        private final TabletSchedCtx context;

        private Fixture(TabletSchedCtx.Type type, TabletHealthStatus status) {
            for (int i = 0; i < 3; i++) {
                Backend backend = new Backend(SOURCE_A + i, "192.0.2." + (i + 1), 9050);
                backend.setAlive(true);
                infoService.addBackend(backend);
            }
            sourceA.updateVersionInfo(VISIBLE, -1, VISIBLE);
            sourceB.updateVersionInfo(VISIBLE, -1, VISIBLE);
            target.updateVersionInfo(SNAPSHOT, VISIBLE, SNAPSHOT);
            sourceA.setPathHash(1);
            sourceB.setPathHash(2);
            target.setPathHash(3);
            target.setNeedFurtherRepair(true);
            invertedIndex.addTablet(TABLET_ID,
                    new TabletMeta(DB_ID, TABLE_ID, PARTITION_ID, INDEX_ID, TStorageMedium.SSD));
            tablet.addReplica(sourceA);
            tablet.addReplica(sourceB);
            tablet.addReplica(target);
            partition.setVisibleVersion(VISIBLE, System.currentTimeMillis());
            context = new TabletSchedCtx(type, DB_ID, TABLE_ID, PARTITION_ID, INDEX_ID, TABLET_ID,
                    System.currentTimeMillis(), infoService);
            context.setTablet(tablet);
            context.setReplicaNum(2);
            context.setDest(TARGET, 3);
            context.setTabletStatus(status);
            context.setVersionInfo(VISIBLE, VISIBLE + 1, 0, System.currentTimeMillis());
            new Expectations() {
                {
                    localMetastore.getPhysicalPartitionIncludeRecycleBin(table, PARTITION_ID);
                    result = partition;
                    minTimes = 0;
                }
            };
        }

        private String finish(long received, long readable) throws Exception {
            TTabletInfo reported = new TTabletInfo();
            reported.setTablet_id(TABLET_ID);
            reported.setSchema_hash(SCHEMA_HASH);
            reported.setVersion(received);
            reported.setMax_readable_version(readable);
            reported.setMin_readable_version(SNAPSHOT);
            reported.setData_size(100);
            reported.setRow_count(10);
            reported.setPath_hash(3);
            TFinishTaskRequest request = new TFinishTaskRequest();
            request.setFinish_tablet_infos(List.of(reported));
            Method callback = TabletSchedCtx.class.getDeclaredMethod("unprotectedFinishClone",
                    TFinishTaskRequest.class, PhysicalPartition.class, short.class, Multimap.class);
            callback.setAccessible(true);
            try {
                return (String) callback.invoke(context, request, partition, (short) 2, ArrayListMultimap.create());
            } catch (InvocationTargetException exception) {
                if (exception.getCause() instanceof Exception) {
                    throw (Exception) exception.getCause();
                }
                throw new AssertionError(exception.getCause());
            }
        }

        private TabletHealthStatus health() {
            return TabletChecker.getTabletHealthStatusWithPriority(tablet, infoService, partition.getVisibleVersion(),
                    2, List.of(SOURCE_A, SOURCE_B, TARGET), ArrayListMultimap.create()).first;
        }

        private void protectionStart(long timestamp) throws Exception {
            Field field = Replica.class.getDeclaredField("furtherRepairSetTime");
            field.setAccessible(true);
            field.setLong(target, timestamp);
        }

        private long protectionStart() throws Exception {
            Field field = Replica.class.getDeclaredField("furtherRepairSetTime");
            field.setAccessible(true);
            return field.getLong(target);
        }

        private boolean replacements(long visibleVersion) {
            return TabletScheduler.hasEnoughMigrationReplacementReplicas(context, sourceA, visibleVersion, infoService);
        }

        private void promoteMetadata() throws Exception {
            sourceA.updateVersionInfo(LATEST, -1, LATEST);
            sourceB.updateVersionInfo(LATEST, -1, LATEST);
            partition.setVisibleVersion(LATEST, System.currentTimeMillis());
            target.updateVersionInfo(SNAPSHOT, VISIBLE, LATEST);
            finish(LATEST, VISIBLE);
        }
    }

    private Fixture furtherRepair() {
        return new Fixture(TabletSchedCtx.Type.REPAIR, TabletHealthStatus.NEED_FURTHER_REPAIR);
    }

    private static final class RecordingScheduler extends TabletScheduler {
        private long selectedReplica = -1;

        private RecordingScheduler() {
            super(new TabletSchedulerStat());
        }

        @Override
        protected void deleteReplicaInternal(TabletSchedCtx context, Replica replica, String reason, boolean force) {
            selectedReplica = replica.getId();
        }
    }

    private boolean select(RecordingScheduler scheduler, Fixture fixture, String name, boolean force) throws Exception {
        Method method = TabletScheduler.class.getDeclaredMethod(name, TabletSchedCtx.class, boolean.class);
        method.setAccessible(true);
        return (boolean) method.invoke(scheduler, fixture.context, force);
    }

    @Test
    public void testReceivedButUnreadableCloneRemainsProtected() throws Exception {
        Fixture fixture = furtherRepair();
        fixture.finish(VISIBLE + 1, SNAPSHOT);
        assertEquals(SNAPSHOT, fixture.target.getVersion());
        assertTrue(fixture.target.needFurtherRepair());
        assertEquals(TabletHealthStatus.NEED_FURTHER_REPAIR, fixture.health());
    }

    @Test
    public void testCaughtUpTargetAllowsCleanupWithoutEndingProtection() throws Exception {
        Fixture fixture = furtherRepair();
        fixture.finish(VISIBLE, VISIBLE);
        assertTrue(fixture.target.needFurtherRepair());
        assertEquals(TabletHealthStatus.REDUNDANT, fixture.health());
    }

    @Test
    public void testNewFailedVersionAfterCatchupRestoresInPlaceRepair() throws Exception {
        Fixture fixture = furtherRepair();
        fixture.finish(VISIBLE, VISIBLE);
        fixture.target.updateLastFailedVersion(VISIBLE + 1);
        assertEquals(TabletHealthStatus.NEED_FURTHER_REPAIR, fixture.health());
    }

    @Test
    public void testFutureFailedVersionKeepsProtection() throws Exception {
        Fixture fixture = furtherRepair();
        fixture.target.updateVersionInfo(VISIBLE, VISIBLE + 4, VISIBLE);
        fixture.finish(LATEST, VISIBLE);
        assertTrue(fixture.target.getLastFailedVersion() > 0);
        assertEquals(TabletHealthStatus.NEED_FURTHER_REPAIR, fixture.health());
    }

    @Test
    public void testIncrementalCompletionDoesNotRenewProtection() throws Exception {
        Fixture fixture = furtherRepair();
        long started = System.currentTimeMillis() - 15000;
        fixture.protectionStart(started);
        fixture.finish(VISIBLE + 1, SNAPSHOT);
        fixture.finish(VISIBLE + 1, SNAPSHOT);
        assertEquals(started, fixture.protectionStart());
    }

    @Test
    public void testExpiredProtectionAllowsExistingCleanup() throws Exception {
        Fixture fixture = furtherRepair();
        fixture.protectionStart(System.currentTimeMillis() - 1200001);
        fixture.finish(VISIBLE + 1, SNAPSHOT);
        assertFalse(fixture.target.needFurtherRepair());
        assertEquals(TabletHealthStatus.REDUNDANT, fixture.health());
    }

    @Test
    public void testOrdinaryRepairKeepsExistingCompletionBehavior() throws Exception {
        Fixture fixture = new Fixture(TabletSchedCtx.Type.REPAIR, TabletHealthStatus.VERSION_INCOMPLETE);
        fixture.target.setNeedFurtherRepair(false);
        fixture.sourceB.updateVersionInfo(VISIBLE, VISIBLE + 4, VISIBLE);
        fixture.finish(VISIBLE, VISIBLE);
        assertFalse(fixture.target.needFurtherRepair());
        assertEquals(TabletHealthStatus.REDUNDANT, fixture.health());
    }

    @Test
    public void testFullBalanceCloneProtectsIncompleteTarget() throws Exception {
        Fixture fixture = new Fixture(TabletSchedCtx.Type.BALANCE, TabletHealthStatus.HEALTHY);
        fixture.target.setState(ReplicaState.CLONE);
        fixture.target.setNeedFurtherRepair(false);
        fixture.context.setVersionInfo(SNAPSHOT, VISIBLE + 1, 0, System.currentTimeMillis());
        fixture.finish(SNAPSHOT, SNAPSHOT);
        assertEquals(ReplicaState.NORMAL, fixture.target.getState());
        assertEquals(TabletHealthStatus.NEED_FURTHER_REPAIR, fixture.health());
    }

    @Test
    public void testFullBalanceClonePinsAnInitiallyCaughtUpTarget() throws Exception {
        Fixture fixture = new Fixture(TabletSchedCtx.Type.BALANCE, TabletHealthStatus.HEALTHY);
        fixture.target.setState(ReplicaState.CLONE);
        fixture.target.setNeedFurtherRepair(false);
        fixture.finish(VISIBLE, VISIBLE);
        assertTrue(fixture.target.needFurtherRepair());
        assertEquals(TabletHealthStatus.REDUNDANT, fixture.health());
    }

    @Test
    public void testAsyncPublishKeepsReceivedVersionSemantics() throws Exception {
        Config.enable_sync_publish = false;
        Fixture fixture = furtherRepair();
        fixture.finish(VISIBLE + 1, SNAPSHOT);
        assertEquals(VISIBLE + 1, fixture.target.getVersion());
        assertFalse(fixture.target.needFurtherRepair());
        assertEquals(TabletHealthStatus.REDUNDANT, fixture.health());
    }

    @Test
    public void testPromotedMetadataDoesNotReleaseProtection() throws Exception {
        Fixture fixture = furtherRepair();
        fixture.promoteMetadata();
        assertEquals(LATEST, fixture.target.getVersion());
        assertTrue(fixture.target.needFurtherRepair());
        assertEquals(TabletHealthStatus.NEED_FURTHER_REPAIR, fixture.health());
    }

    @Test
    public void testPromotedMetadataDoesNotPermitSourceRetirement() throws Exception {
        Fixture fixture = furtherRepair();
        fixture.promoteMetadata();
        assertFalse(fixture.replacements(LATEST));
    }

    @Test
    public void testPromotedTargetCannotBeAHealthyCloneSource() throws Exception {
        Fixture fixture = furtherRepair();
        fixture.promoteMetadata();
        fixture.context.setVersionInfo(LATEST, LATEST, 0, System.currentTimeMillis());
        assertFalse(fixture.context.getHealthyReplicas(false).contains(fixture.target));
    }

    @Test
    public void testRawReportAllowsRetirementWithoutEndingProtection() throws Exception {
        Fixture fixture = furtherRepair();
        fixture.promoteMetadata();
        fixture.target.updateRowCount(LATEST, SNAPSHOT, 100, 10);
        assertTrue(fixture.target.needFurtherRepair());
        assertEquals(TabletHealthStatus.REDUNDANT, fixture.health());
        assertTrue(fixture.replacements(LATEST));
    }

    @Test
    public void testThreeArgumentReportAlsoUpdatesReadableWatermark() throws Exception {
        Fixture fixture = furtherRepair();
        fixture.promoteMetadata();
        fixture.target.updateRowCount(LATEST, 100, 10);
        assertTrue(fixture.replacements(LATEST));
    }

    @Test
    public void testOlderReportDoesNotRegressReadableWatermark() throws Exception {
        Fixture fixture = furtherRepair();
        fixture.finish(VISIBLE, VISIBLE);
        fixture.target.updateRowCount(SNAPSHOT, SNAPSHOT, 100, 10);
        assertTrue(fixture.replacements(VISIBLE));
    }

    @Test
    public void testSourceRemovalEndsTargetProtection() throws Exception {
        Fixture fixture = furtherRepair();
        fixture.finish(VISIBLE, VISIBLE);
        assertTrue(fixture.context.deleteReplica(fixture.sourceA));
        assertFalse(fixture.target.needFurtherRepair());
        assertEquals(2, fixture.tablet.getImmutableReplicas().size());
        assertEquals(2, invertedIndex.getReplicasByTabletId(TABLET_ID).size());
    }

    @Test
    public void testIncompleteTargetCannotReplaceHealthySource() throws Exception {
        Fixture fixture = furtherRepair();
        fixture.finish(VISIBLE + 1, SNAPSHOT);
        assertFalse(fixture.replacements(VISIBLE));
    }

    @Test
    public void testCaughtUpTargetAndHealthyPeerAllowRetirement() throws Exception {
        Fixture fixture = furtherRepair();
        fixture.finish(VISIBLE, VISIBLE);
        assertTrue(fixture.replacements(VISIBLE));
    }

    @Test
    public void testBadPeerDoesNotCountAsReplacement() throws Exception {
        Fixture fixture = furtherRepair();
        fixture.finish(VISIBLE, VISIBLE);
        fixture.sourceB.setBad(true);
        assertFalse(fixture.replacements(VISIBLE));
    }

    @Test
    public void testDecommissionedPeerDoesNotCountAsReplacement() throws Exception {
        Fixture fixture = furtherRepair();
        fixture.finish(VISIBLE, VISIBLE);
        fixture.sourceB.setState(ReplicaState.DECOMMISSION);
        assertFalse(fixture.replacements(VISIBLE));
    }

    @Test
    public void testDeadBackendDoesNotCountAsReplacement() throws Exception {
        Fixture fixture = furtherRepair();
        fixture.finish(VISIBLE, VISIBLE);
        infoService.getBackend(SOURCE_B).setAlive(false);
        assertFalse(fixture.replacements(VISIBLE));
    }

    @Test
    public void testSameHostDoesNotCountAsTwoReplacementCopies() throws Exception {
        Fixture fixture = furtherRepair();
        fixture.finish(VISIBLE, VISIBLE);
        Backend duplicateHost = new Backend(SOURCE_B, "192.0.2.3", 9051);
        duplicateHost.setAlive(true);
        infoService.addBackend(duplicateHost);
        assertFalse(fixture.replacements(VISIBLE));
    }

    @Test
    public void testReplacementCheckUsesCurrentVisibleVersion() throws Exception {
        Fixture fixture = furtherRepair();
        fixture.finish(VISIBLE, VISIBLE);
        assertEquals(VISIBLE, fixture.context.getVisibleVersion());
        assertFalse(fixture.replacements(VISIBLE + 1));
    }

    @Test
    public void testSourceRetirementReadsCurrentPartitionVersion() throws Exception {
        Fixture fixture = furtherRepair();
        fixture.finish(VISIBLE, VISIBLE);
        fixture.partition.setVisibleVersion(VISIBLE + 1, System.currentTimeMillis());
        TabletScheduler scheduler = new TabletScheduler(new TabletSchedulerStat());
        SchedException failure = assertThrows(SchedException.class,
                () -> scheduler.deleteReplicaInternal(fixture.context, fixture.sourceA, "src replica of rebalance", false));
        assertTrue(failure.getMessage().contains("replacement replicas"));
        assertEquals(ReplicaState.NORMAL, fixture.sourceA.getState());
        assertEquals(-1, fixture.sourceA.getWatermarkTxnId());
    }

    @Test
    public void testFinalSourceDeletionWaitsWhenReplacementFallsBehind() throws Exception {
        Fixture fixture = furtherRepair();
        fixture.finish(VISIBLE, VISIBLE);
        fixture.sourceA.setState(ReplicaState.DECOMMISSION);
        fixture.sourceA.setWatermarkTxnId(1000);
        fixture.partition.setVisibleVersion(VISIBLE + 1, System.currentTimeMillis());
        TabletScheduler scheduler = new TabletScheduler(new TabletSchedulerStat());
        assertThrows(SchedException.class,
                () -> scheduler.deleteReplicaInternal(fixture.context, fixture.sourceA, "DECOMMISSION state", false));
        assertEquals(3, fixture.tablet.getImmutableReplicas().size());
        assertTrue(fixture.target.needFurtherRepair());
    }

    @Test
    public void testFailedVersionCleanupSkipsPinnedTarget() throws Exception {
        Fixture fixture = furtherRepair();
        RecordingScheduler scheduler = new RecordingScheduler();
        assertFalse(select(scheduler, fixture, "deleteReplicaWithFailedVersion", false));
        assertEquals(-1, scheduler.selectedReplica);
    }

    @Test
    public void testLowerVersionCleanupSkipsPinnedTarget() throws Exception {
        Fixture fixture = furtherRepair();
        RecordingScheduler scheduler = new RecordingScheduler();
        assertFalse(select(scheduler, fixture, "deleteReplicaWithLowerVersion", false));
        assertEquals(-1, scheduler.selectedReplica);
    }

    @Test
    public void testForceCleanupCanStillSelectPinnedTarget() throws Exception {
        Fixture fixture = furtherRepair();
        RecordingScheduler scheduler = new RecordingScheduler();
        assertTrue(select(scheduler, fixture, "deleteReplicaWithFailedVersion", true));
        assertEquals(fixture.target.getId(), scheduler.selectedReplica);
    }

    @Test
    public void testExpiredProtectionDoesNotAddRetirementRestrictions() throws Exception {
        Fixture fixture = furtherRepair();
        fixture.protectionStart(System.currentTimeMillis() - 1200001);
        assertTrue(fixture.replacements(VISIBLE));
    }

    @Test
    public void testOrdinaryUnprotectedCleanupKeepsExistingSelection() throws Exception {
        Fixture fixture = furtherRepair();
        fixture.target.setNeedFurtherRepair(false);
        RecordingScheduler scheduler = new RecordingScheduler();
        assertTrue(select(scheduler, fixture, "deleteReplicaWithFailedVersion", false));
        assertEquals(fixture.target.getId(), scheduler.selectedReplica);
    }

    @Test
    public void testCloneDiagnosticIncludesReadableWatermarkAndProtection() throws Exception {
        Fixture fixture = furtherRepair();
        String diagnostic = fixture.finish(VISIBLE + 1, SNAPSHOT);
        assertTrue(diagnostic.contains("readable_version:" + SNAPSHOT));
        assertTrue(diagnostic.contains("need_further_repair:true"));
    }
}
