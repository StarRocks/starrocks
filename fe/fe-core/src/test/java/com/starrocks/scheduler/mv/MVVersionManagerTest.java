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
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.RandomDistributionInfo;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.scheduler.MvTaskRunContext;
import com.starrocks.scheduler.TableSnapshotInfo;
import com.starrocks.scheduler.TaskRunContext;
import com.starrocks.scheduler.persist.TaskRunStatus;
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
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MVVersionManagerTest {

    @BeforeEach
    public void setUp() {
        new MockUp<MVVersionManager>() {
            @Mock
            public void updateEditLogAfterVersionMetaChanged(MaterializedView mv, long maxChangedTableRefreshTime) {
            }
        };
        new MockUp<MaterializedViewMgr>() {
            @Mock
            public void triggerTimelessInfoEvent(MaterializedView mv, MVTimelinessMgr.MVChangeEvent event) {
            }
        };
    }

    // PartitionUtil.getPartitionNames reaches a connector table's remote metadata, so it must not run under the
    // mv write lock: that lock is contended by other refresh runs and DDLs, which tryLock with a bounded timeout.
    @Test
    public void collectExternalTablePartitionNamesResolvesOnlyExternalBaseTables() {
        AtomicInteger resolveCount = new AtomicInteger();
        mockGetPartitionNames(resolveCount);

        MaterializedView mv = buildMv();
        OlapTable olapBaseTable = mock(OlapTable.class);
        when(olapBaseTable.isNativeTableOrMaterializedView()).thenReturn(true);
        when(olapBaseTable.getId()).thenReturn(2000L);
        Table externalBaseTable = mock(Table.class);
        when(externalBaseTable.isNativeTableOrMaterializedView()).thenReturn(false);
        when(externalBaseTable.getId()).thenReturn(3000L);

        TableSnapshotInfo olapSnapshot = new TableSnapshotInfo(mock(BaseTableInfo.class), olapBaseTable);
        TableSnapshotInfo externalSnapshot = new TableSnapshotInfo(mock(BaseTableInfo.class), externalBaseTable);

        MVVersionManager manager = new MVVersionManager(mv, buildContext());
        Map<TableSnapshotInfo, List<String>> partitionNames = manager.collectExternalTablePartitionNames(
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

        MaterializedView mv = buildMv();
        Table externalBaseTable = mock(Table.class);
        when(externalBaseTable.isNativeTableOrMaterializedView()).thenReturn(false);
        when(externalBaseTable.getId()).thenReturn(3000L);
        BaseTableInfo baseTableInfo = mock(BaseTableInfo.class);
        TableSnapshotInfo snapshot = new TableSnapshotInfo(baseTableInfo, externalBaseTable);
        snapshot.getRefreshedPartitionInfos().put("p1", new MaterializedView.BasePartitionInfo(1L, 1L, 1L));

        // p3 no longer exists in the base table, so the version map entry must be pruned.
        Map<String, MaterializedView.BasePartitionInfo> versionMap = new HashMap<>();
        versionMap.put("p3", new MaterializedView.BasePartitionInfo(1L, 1L, 1L));
        mv.getRefreshScheme().getAsyncRefreshContext().getBaseTableInfoVisibleVersionMap()
                .put(baseTableInfo, versionMap);

        MVVersionManager manager = new MVVersionManager(mv, buildContext());
        manager.updateMVVersionInfo(Map.of(3000L, snapshot), Set.of(), Set.of(3000L),
                Map.of(), Map.of(snapshot, List.of("p1", "p2")), true);

        Assertions.assertEquals(Set.of("p1"), mv.getRefreshScheme().getAsyncRefreshContext()
                        .getBaseTableInfoVisibleVersionMap().get(baseTableInfo).keySet(),
                "the refreshed partition must be kept and the dropped one pruned");
        Assertions.assertEquals(0, resolveCount.get(),
                "the partition names were resolved before the lock, so the critical section must not call the "
                        + "connector again");
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

    private static MaterializedView buildMv() {
        List<Column> columns = new LinkedList<>();
        columns.add(new Column("k1", Type.TINYINT, true, null, "", ""));
        RandomDistributionInfo distributionInfo = new RandomDistributionInfo(10);
        PartitionInfo partitionInfo = new SinglePartitionInfo();
        partitionInfo.setDataProperty(1, DataProperty.DEFAULT_DATA_PROPERTY);
        partitionInfo.setReplicationNum(1, (short) 3);
        MaterializedView.MvRefreshScheme refreshScheme = new MaterializedView.MvRefreshScheme();
        return new MaterializedView(1000, 100, "mv_version_manager_test", columns, KeysType.AGG_KEYS,
                partitionInfo, distributionInfo, refreshScheme);
    }

    private static MvTaskRunContext buildContext() {
        MvTaskRunContext context = new MvTaskRunContext(new TaskRunContext());
        TaskRunStatus status = new TaskRunStatus();
        status.setProcessStartTime(2000L);
        context.setStatus(status);
        return context;
    }
}
