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

package com.starrocks.sql.ast;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableName;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.common.DdlException;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.proto.TabletCacheStats;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.type.TypeFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class RefreshTabletCacheStatsStatement extends RefreshCacheStatsStatement {
    private static final ShowResultSetMetaData TABLET_META_DATA = ShowResultSetMetaData.builder()
            .addColumn(new Column("TABLET_ID", TypeFactory.createVarchar(16)))
            .addColumn(new Column("DETAIL", TypeFactory.createVarchar(4096)))
            .build();

    private final List<Long> tabletIds;
    private Map<Long, TreeMap<Long, TabletCacheStats>> cacheStats;

    public RefreshTabletCacheStatsStatement(TableName tableName, List<Long> tabletIds, NodePosition pos) {
        super(tableName, pos);
        this.tabletIds = tabletIds;
        this.cacheStats = new HashMap<>();
    }

    @Override
    public Map<Long, PartitionSnapshot> prepare() throws DdlException {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(tableName.getDb());
        if (db == null) {
            throw new DdlException(String.format("db %s does not exist.", tableName.getDb()));
        }
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), tableName.getTbl());
        if (table == null) {
            throw new DdlException(String.format("table %s does not exist.", tableName.getTbl()));
        }
        if (!table.isCloudNativeTableOrMaterializedView()) {
            throw new DdlException("only support cloud table or cloud mv.");
        }
        OlapTable olapTable = (OlapTable) table;
        if (!olapTable.getTableProperty().getStorageInfo().isEnableDataCache()) {
            throw new DdlException(String.format("table %s property(%s) is false, does not support refresh.", tableName.getTbl(),
                    PropertyAnalyzer.PROPERTIES_DATACACHE_ENABLE));
        }
        TabletInvertedIndex index = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
        List<TabletMeta> metas = Lists.newArrayList();
        for (Long tabletId : tabletIds) {
            TabletMeta meta = index.getTabletMeta(tabletId);
            if (meta == null) {
                throw new DdlException(String.format("tablet %d not found", tabletId));
            }
            metas.add(meta);
        }
        Locker locker = new Locker();
        locker.lockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(olapTable.getId()), LockType.READ);
        try {
            for (int i = 0; i < metas.size(); ++i) {
                long tabletId = tabletIds.get(i);
                PhysicalPartition physicalPartition = olapTable.getPhysicalPartition(metas.get(i).getPhysicalPartitionId());
                if (physicalPartition == null) {
                    throw new DdlException(String.format("can not find physical partition %d for tablet %d",
                            metas.get(i).getPhysicalPartitionId(), tabletId));
                }
                tablets.put(tabletId, new PartitionSnapshot(-1, physicalPartition.getVisibleVersion()));
            }
        } finally {
            locker.unLockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(olapTable.getId()), LockType.READ);
        }
        return tablets;
    }

    @Override
    public void submitResult(long workerGroupId, List<TabletCacheStats> tabletCacheStats) {
        for (TabletCacheStats e : tabletCacheStats) {
            Map<Long, TabletCacheStats> mapStats = cacheStats.computeIfAbsent(e.tabletId, k -> new TreeMap<>());
            TabletCacheStats stats = mapStats.computeIfAbsent(workerGroupId, k -> {
                TabletCacheStats s = new TabletCacheStats();
                s.cachedBytes = 0L;
                s.totalBytes = 0L;
                return s;
            });
            stats.cachedBytes += e.cachedBytes;
            stats.totalBytes += e.totalBytes;
        }
    }

    @Override
    public ShowResultSet getResult() {
        List<List<String>> rows = Lists.newArrayList();
        for (Long tabletId : tabletIds) {
            List<String> row = Lists.newArrayList();
            row.add(Long.toString(tabletId));
            TreeMap<Long, TabletCacheStats> stats = cacheStats.get(tabletId);
            row.add(getResult(stats));
            rows.add(row);
        }
        return new ShowResultSet(TABLET_META_DATA, rows);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("REFRESH CACHE STATS FOR TABLE ");
        sb.append(tableName.getTbl());
        sb.append(" TABLET ");
        for (Long tabletId : tabletIds) {
            sb.append(Long.toString(tabletId));
            sb.append(',');
        }
        sb.deleteCharAt(sb.length() - 1);
        return sb.toString();
    }
}
