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

package com.starrocks.scheduler.mv;

import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.RandomDistributionInfo;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.persist.ChangeMaterializedViewRefreshSchemeLog;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.WALApplier;
import com.starrocks.scheduler.MvTaskRunContext;
import com.starrocks.scheduler.TaskRun;
import com.starrocks.scheduler.TaskRunContext;
import com.starrocks.scheduler.mv.pct.PCTTableSnapshotInfo;
import com.starrocks.scheduler.persist.TaskRunStatus;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.common.PCellSortedSet;
import com.starrocks.type.IntegerType;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MVVersionManagerTest {

    private final AtomicInteger editLogCount = new AtomicInteger();
    // The test MVs live in no catalog; this stands in for whether a DROP has removed the MV.
    private boolean mvInCatalog;

    @BeforeEach
    public void setUp() {
        editLogCount.set(0);
        mvInCatalog = true;
        new MockUp<MaterializedView>() {
            @Mock
            public boolean isInCatalog() {
                return mvInCatalog;
            }
        };
        new MockUp<Locker>() {
            @Mock
            public boolean tryLockTableWithIntensiveDbLock(Long dbId, Long tableId, LockType lockType,
                                                           long timeout, TimeUnit unit) {
                return true;
            }

            @Mock
            public void unLockTableWithIntensiveDbLock(Long dbId, Long tableId, LockType lockType) {
            }
        };
        new MockUp<EditLog>() {
            @Mock
            public void logMvChangeRefreshScheme(ChangeMaterializedViewRefreshSchemeLog log, WALApplier walApplier) {
                editLogCount.incrementAndGet();
                walApplier.apply(null);
            }
        };
        new MockUp<GlobalStateMgr>() {
            @Mock
            public EditLog getEditLog() {
                return new EditLog(null);
            }

            @Mock
            public MaterializedViewMgr getMaterializedViewMgr() {
                return new MaterializedViewMgr();
            }
        };
        new MockUp<MaterializedViewMgr>() {
            @Mock
            public void triggerTimelessInfoEvent(MaterializedView mv, MVTimelinessMgr.MVChangeEvent event) {
            }
        };
    }

    private static MaterializedView buildMv(long lastFreshnessConfirmedAt) {
        List<Column> columns = new LinkedList<>();
        columns.add(new Column("k1", IntegerType.TINYINT, true, null, "", ""));
        RandomDistributionInfo distributionInfo = new RandomDistributionInfo(10);
        PartitionInfo partitionInfo = new SinglePartitionInfo();
        partitionInfo.setDataProperty(1, DataProperty.DEFAULT_DATA_PROPERTY);
        partitionInfo.setReplicationNum(1, (short) 3);
        MaterializedView.MvRefreshScheme refreshScheme = new MaterializedView.MvRefreshScheme();
        refreshScheme.setLastFreshnessConfirmedAt(lastFreshnessConfirmedAt);
        return new MaterializedView(1000, 100, "mv_version_manager_test", columns, KeysType.AGG_KEYS,
                partitionInfo, distributionInfo, refreshScheme);
    }

    private static MvTaskRunContext buildContext(Map<String, String> properties, TaskRunStatus status) {
        MvTaskRunContext context = new MvTaskRunContext(new TaskRunContext());
        context.setProperties(properties);
        context.setStatus(status);
        return context;
    }

    private static TaskRunStatus statusWithProcessStartTime(long processStartTime) {
        TaskRunStatus status = new TaskRunStatus();
        status.setProcessStartTime(processStartTime);
        return status;
    }

    @Test
    public void confirmFreshnessAdvancesAndLogsOnce() {
        MaterializedView mv = buildMv(1000L);
        MVVersionManager manager = new MVVersionManager(mv, buildContext(null, statusWithProcessStartTime(2000L)));

        manager.confirmFreshness();

        Assertions.assertEquals(2000L, mv.getRefreshScheme().getLastFreshnessConfirmedAt());
        Assertions.assertEquals(1, editLogCount.get());
    }

    @Test
    public void confirmFreshnessIsNoopWhenAlreadyConfirmed() {
        MaterializedView mv = buildMv(2000L);
        MVVersionManager manager = new MVVersionManager(mv, buildContext(null, statusWithProcessStartTime(2000L)));

        manager.confirmFreshness();

        Assertions.assertEquals(2000L, mv.getRefreshScheme().getLastFreshnessConfirmedAt());
        Assertions.assertEquals(0, editLogCount.get());
    }

    @Test
    public void confirmFreshnessUsesBatchBaselineProperty() {
        Map<String, String> properties = new HashMap<>();
        properties.put(TaskRun.MV_FRESHNESS_BASELINE_TIME, "1500");
        MaterializedView mv = buildMv(1000L);
        MVVersionManager manager = new MVVersionManager(mv, buildContext(properties, statusWithProcessStartTime(2000L)));

        manager.confirmFreshness();

        Assertions.assertEquals(1500L, mv.getRefreshScheme().getLastFreshnessConfirmedAt());
        Assertions.assertEquals(1, editLogCount.get());
    }

    @Test
    public void confirmFreshnessFallsBackOnMalformedBaseline() {
        Map<String, String> properties = new HashMap<>();
        properties.put(TaskRun.MV_FRESHNESS_BASELINE_TIME, "not-a-number");
        MaterializedView mv = buildMv(1000L);
        MVVersionManager manager = new MVVersionManager(mv, buildContext(properties, statusWithProcessStartTime(2000L)));

        manager.confirmFreshness();

        Assertions.assertEquals(2000L, mv.getRefreshScheme().getLastFreshnessConfirmedAt());
        Assertions.assertEquals(1, editLogCount.get());
    }

    @Test
    public void confirmFreshnessSkipsAnMvDroppedBeforeTheLock() {
        MaterializedView mv = buildMv(1000L);
        MVVersionManager manager = new MVVersionManager(mv, buildContext(null, statusWithProcessStartTime(2000L)));
        mvInCatalog = false;

        manager.confirmFreshness();

        Assertions.assertEquals(1000L, mv.getRefreshScheme().getLastFreshnessConfirmedAt());
        Assertions.assertEquals(0, editLogCount.get());
    }

    @Test
    public void confirmFreshnessSkipsWhenLockTimedOut() {
        new MockUp<Locker>() {
            @Mock
            public boolean tryLockTableWithIntensiveDbLock(Long dbId, Long tableId, LockType lockType,
                                                           long timeout, TimeUnit unit) {
                return false;
            }
        };
        MaterializedView mv = buildMv(1000L);
        MVVersionManager manager = new MVVersionManager(mv, buildContext(null, statusWithProcessStartTime(2000L)));

        manager.confirmFreshness();

        Assertions.assertEquals(1000L, mv.getRefreshScheme().getLastFreshnessConfirmedAt());
        Assertions.assertEquals(0, editLogCount.get());
    }

    @Test
    public void confirmFreshnessIsNoopWhenBaselineMarksIneligible() {
        Map<String, String> properties = new HashMap<>();
        properties.put(TaskRun.MV_FRESHNESS_BASELINE_TIME, "0");
        MaterializedView mv = buildMv(1000L);
        MVVersionManager manager = new MVVersionManager(mv, buildContext(properties, statusWithProcessStartTime(2000L)));

        manager.confirmFreshness();

        Assertions.assertEquals(1000L, mv.getRefreshScheme().getLastFreshnessConfirmedAt());
        Assertions.assertEquals(0, editLogCount.get());
    }

    @Test
    public void confirmFreshnessIsNoopWithoutRunStatus() {
        MaterializedView mv = buildMv(1000L);
        MVVersionManager manager = new MVVersionManager(mv, buildContext(null, null));

        manager.confirmFreshness();

        Assertions.assertEquals(1000L, mv.getRefreshScheme().getLastFreshnessConfirmedAt());
        Assertions.assertEquals(0, editLogCount.get());
    }

    // A base table can be processed by a run without any of its partitions being refreshed: the run is not
    // skipped (isOlapTableRefreshed is set unconditionally), but the derived max is 0.
    @Test
    public void updateMVVersionInfoKeepsLastRefreshTimeWhenNoPartitionWasRefreshed() {
        long absorbedRefreshTime = 1767225600000L;
        MaterializedView mv = buildMv(1000L);
        mv.getRefreshScheme().setLastRefreshTime(absorbedRefreshTime);

        OlapTable baseTable = mock(OlapTable.class);
        when(baseTable.isNativeTableOrMaterializedView()).thenReturn(true);
        when(baseTable.isOlapOrCloudNativeTable()).thenReturn(true);
        when(baseTable.getId()).thenReturn(2000L);
        when(baseTable.getVisiblePartitionNames()).thenReturn(Set.of());
        PCTTableSnapshotInfo snapshot = new PCTTableSnapshotInfo(mock(BaseTableInfo.class), baseTable);
        Assertions.assertTrue(snapshot.getRefreshedPartitionInfos().isEmpty());

        MVVersionManager manager = new MVVersionManager(mv, buildContext(null, statusWithProcessStartTime(2000L)));
        manager.updateMVVersionInfo(Map.of(2000L, snapshot), PCellSortedSet.of(), Set.of(2000L),
                Map.of(), null, Map.of(), true);

        Assertions.assertEquals(absorbedRefreshTime, mv.getRefreshScheme().getLastRefreshTime(),
                "a run that refreshed no partition must not report the MV as never refreshed");
    }

    // PartitionUtil.getPartitionNames reaches a connector table's remote metadata, so it must not run under the
    // mv write lock: that lock is contended by other refresh runs and DDLs, which tryLock with a bounded timeout.
    @Test
    public void collectExternalTablePartitionNamesResolvesOnlyExternalBaseTables() {
        AtomicInteger resolveCount = new AtomicInteger();
        mockGetPartitionNames(resolveCount);

        MaterializedView mv = buildMv(1000L);
        OlapTable olapBaseTable = mock(OlapTable.class);
        when(olapBaseTable.isNativeTableOrMaterializedView()).thenReturn(true);
        when(olapBaseTable.getId()).thenReturn(2000L);
        Table externalBaseTable = mock(Table.class);
        when(externalBaseTable.isNativeTableOrMaterializedView()).thenReturn(false);
        when(externalBaseTable.getId()).thenReturn(3000L);

        PCTTableSnapshotInfo olapSnapshot = new PCTTableSnapshotInfo(mock(BaseTableInfo.class), olapBaseTable);
        PCTTableSnapshotInfo externalSnapshot = new PCTTableSnapshotInfo(mock(BaseTableInfo.class), externalBaseTable);

        MVVersionManager manager = new MVVersionManager(mv, buildContext(null, statusWithProcessStartTime(2000L)));
        Map<PCTTableSnapshotInfo, List<String>> partitionNames = manager.collectExternalTablePartitionNames(
                Map.of(2000L, olapSnapshot, 3000L, externalSnapshot), Set.of(2000L, 3000L));

        Assertions.assertEquals(Set.of(externalSnapshot), partitionNames.keySet(),
                "only external base tables need their partition names resolved from the connector");
        Assertions.assertEquals(List.of("p1", "p2"), partitionNames.get(externalSnapshot));
        Assertions.assertEquals(1, resolveCount.get());
    }

    @Test
    public void updateMVVersionInfoPrunesExternalPartitionsWithoutCallingTheConnector() {
        AtomicInteger resolveCount = new AtomicInteger();
        mockGetPartitionNames(resolveCount);

        MaterializedView mv = buildMv(1000L);
        Table externalBaseTable = mock(Table.class);
        when(externalBaseTable.isNativeTableOrMaterializedView()).thenReturn(false);
        when(externalBaseTable.getId()).thenReturn(3000L);
        BaseTableInfo baseTableInfo = mock(BaseTableInfo.class);
        when(baseTableInfo.matchTable(externalBaseTable)).thenReturn(true);
        mv.setBaseTableInfos(List.of(baseTableInfo));
        PCTTableSnapshotInfo snapshot = new PCTTableSnapshotInfo(baseTableInfo, externalBaseTable);
        snapshot.getRefreshedPartitionInfos().put("p1", new MaterializedView.BasePartitionInfo(1L, 1L, 1L));

        // p3 no longer exists in the base table, so the version map entry must be pruned.
        Map<String, MaterializedView.BasePartitionInfo> versionMap = new HashMap<>();
        versionMap.put("p3", new MaterializedView.BasePartitionInfo(1L, 1L, 1L));
        mv.getRefreshScheme().getAsyncRefreshContext().getBaseTableInfoVisibleVersionMap()
                .put(baseTableInfo, versionMap);

        MVVersionManager manager = new MVVersionManager(mv, buildContext(null, statusWithProcessStartTime(2000L)));
        manager.updateMVVersionInfo(Map.of(3000L, snapshot), PCellSortedSet.of(), Set.of(3000L),
                Map.of(), null, Map.of(snapshot, List.of("p1", "p2")), true);

        Assertions.assertEquals(Set.of("p1"), mv.getRefreshScheme().getAsyncRefreshContext()
                        .getBaseTableInfoVisibleVersionMap().get(baseTableInfo).keySet(),
                "the refreshed partition must be kept and the dropped one pruned");
        Assertions.assertEquals(0, resolveCount.get(),
                "the partition names were resolved before the lock, so the critical section must not call the "
                        + "connector again");
    }

    /**
     * Refreshes partition p1 of an external base table on a run that started before the MV's version map was
     * reset; returns the version entries the MV holds for that table afterwards.
     */
    private Map<String, MaterializedView.BasePartitionInfo> refreshP1AcrossAReset(MaterializedView mv,
                                                                                Runnable resetBeforeCommit) {
        mockGetPartitionNames(new AtomicInteger());
        Table externalBaseTable = mock(Table.class);
        when(externalBaseTable.isNativeTableOrMaterializedView()).thenReturn(false);
        when(externalBaseTable.getId()).thenReturn(3000L);
        BaseTableInfo baseTableInfo = mock(BaseTableInfo.class);
        PCTTableSnapshotInfo snapshot = new PCTTableSnapshotInfo(baseTableInfo, externalBaseTable);
        snapshot.getRefreshedPartitionInfos().put("p1", new MaterializedView.BasePartitionInfo(1L, 1L, 1L));

        MvTaskRunContext context = buildContext(null, statusWithProcessStartTime(2000L));
        context.setVersionMapResetEpoch(mv.getVersionMapResetEpoch());
        MVVersionManager manager = new MVVersionManager(mv, context);
        resetBeforeCommit.run();
        manager.updateMVVersionInfo(Map.of(3000L, snapshot), PCellSortedSet.of(), Set.of(3000L),
                Map.of(), null, Map.of(snapshot, List.of("p1")), true);
        return mv.getRefreshScheme().getAsyncRefreshContext().getBaseTableInfoVisibleVersionMap()
                .getOrDefault(baseTableInfo, Map.of());
    }

    @Test
    public void updateMVVersionInfoPublishesNoVersionEntriesAfterAResetDuringTheRun() {
        // ALTER VIEW (or a base table schema change) inactivated the MV and reset its version map while this run
        // was computing data from the old definition; recording p1 as refreshed would make a later activation
        // skip it.
        MaterializedView mv = buildMv(1000L);
        Map<String, MaterializedView.BasePartitionInfo> entries = refreshP1AcrossAReset(mv, mv::resetVisibleVersionMap);
        Assertions.assertTrue(entries.isEmpty(), "a run that started before the reset published " + entries);
        // Nor its freshness: within a staleness window it would stand in for the empty version map.
        Assertions.assertEquals(0L, mv.getRefreshScheme().getLastFreshnessConfirmedAt());
        Assertions.assertEquals(1, editLogCount.get());
    }

    @Test
    public void updateMVVersionInfoClearsAgainWhenAResetRacesThePublish() {
        // The reset takes no MV lock: it can land after the run checked the epoch and before its copy of the
        // scheme is published, clearing the scheme the publish then replaces.
        MaterializedView mv = buildMv(1000L);
        List<ChangeMaterializedViewRefreshSchemeLog> journaled = new LinkedList<>();
        new MockUp<EditLog>() {
            @Mock
            public void logMvChangeRefreshScheme(ChangeMaterializedViewRefreshSchemeLog log, WALApplier walApplier) {
                if (journaled.isEmpty()) {
                    mv.resetVisibleVersionMap();
                }
                journaled.add(log);
                walApplier.apply(null);
            }
        };
        Map<String, MaterializedView.BasePartitionInfo> entries = refreshP1AcrossAReset(mv, () -> { });
        Assertions.assertTrue(entries.isEmpty(), "the reset racing the publish was undone: " + entries);
        Assertions.assertEquals(0L, mv.getRefreshScheme().getLastFreshnessConfirmedAt());
        // The run's entry is already in the journal; the clear must be journaled after it, or a replay restores it.
        Assertions.assertEquals(2, journaled.size());
        ChangeMaterializedViewRefreshSchemeLog last = journaled.get(1);
        Assertions.assertTrue(last.getAsyncRefreshContext().getBaseTableInfoVisibleVersionMap().isEmpty());
        Assertions.assertEquals(0L, last.getLastFreshnessConfirmedAt());
    }

    @Test
    public void updateMVVersionInfoNoticesAResetThatWasInFlightWhenTheRunStarted() {
        // The inactivation bumps the epoch before journaling and clears the maps in the journal's applier. A run
        // that starts in between records the bumped epoch, and its publish can straddle the clear.
        MaterializedView mv = buildMv(1000L);
        mv.markVersionMapReset();
        List<ChangeMaterializedViewRefreshSchemeLog> journaled = new LinkedList<>();
        new MockUp<EditLog>() {
            @Mock
            public void logMvChangeRefreshScheme(ChangeMaterializedViewRefreshSchemeLog log, WALApplier walApplier) {
                if (journaled.isEmpty()) {
                    // the inactivation's applier, landing while this run publishes
                    mv.getRefreshScheme().clearVersionState();
                    mv.markVersionMapReset();
                }
                journaled.add(log);
                walApplier.apply(null);
            }
        };
        Map<String, MaterializedView.BasePartitionInfo> entries = refreshP1AcrossAReset(mv, () -> { });
        Assertions.assertTrue(entries.isEmpty(), "the in-flight reset was undone: " + entries);
        Assertions.assertEquals(0L, mv.getRefreshScheme().getLastFreshnessConfirmedAt());
        // The odd epoch the run started with stops it before its first journal write; nothing has to be undone.
        Assertions.assertEquals(1, journaled.size());
        Assertions.assertTrue(journaled.get(0).getAsyncRefreshContext().getBaseTableInfoVisibleVersionMap().isEmpty());
    }

    @Test
    public void updateMVVersionInfoPublishesNothingWhenTheRunStartedDuringAReset() {
        // Worst case of the above: the clear lands while this run publishes, and the run is done before the reset's
        // second bump, so the epoch it compares against never moves. The odd epoch it started with is the tell.
        MaterializedView mv = buildMv(1000L);
        mv.markVersionMapReset();
        List<ChangeMaterializedViewRefreshSchemeLog> journaled = new LinkedList<>();
        new MockUp<EditLog>() {
            @Mock
            public void logMvChangeRefreshScheme(ChangeMaterializedViewRefreshSchemeLog log, WALApplier walApplier) {
                if (journaled.isEmpty()) {
                    mv.getRefreshScheme().clearVersionState();
                }
                journaled.add(log);
                walApplier.apply(null);
            }
        };
        Map<String, MaterializedView.BasePartitionInfo> entries = refreshP1AcrossAReset(mv, () -> { });
        Assertions.assertTrue(entries.isEmpty(), "a run that started during the reset published " + entries);
        Assertions.assertEquals(0L, mv.getRefreshScheme().getLastFreshnessConfirmedAt());
        Assertions.assertTrue(journaled.get(0).getAsyncRefreshContext().getBaseTableInfoVisibleVersionMap().isEmpty());
    }

    @Test
    public void updateMVVersionInfoKeepsVersionEntriesWithoutAReset() {
        MaterializedView mv = buildMv(1000L);
        Map<String, MaterializedView.BasePartitionInfo> entries = refreshP1AcrossAReset(mv, () -> { });
        Assertions.assertEquals(Set.of("p1"), entries.keySet());
    }

    private static void mockGetPartitionNames(AtomicInteger resolveCount) {
        new MockUp<PartitionUtil>() {
            @Mock
            public List<String> getPartitionNames(Table table) {
                resolveCount.incrementAndGet();
                return List.of("p1", "p2");
            }
        };
    }
}
