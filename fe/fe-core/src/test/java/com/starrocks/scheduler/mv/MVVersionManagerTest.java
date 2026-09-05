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

import com.starrocks.catalog.Column;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.RandomDistributionInfo;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.persist.ChangeMaterializedViewRefreshSchemeLog;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.WALApplier;
import com.starrocks.scheduler.MvTaskRunContext;
import com.starrocks.scheduler.TaskRun;
import com.starrocks.scheduler.TaskRunContext;
import com.starrocks.scheduler.persist.TaskRunStatus;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.KeysType;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class MVVersionManagerTest {

    private final AtomicInteger editLogCount = new AtomicInteger();

    @BeforeEach
    public void setUp() {
        editLogCount.set(0);
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
}
