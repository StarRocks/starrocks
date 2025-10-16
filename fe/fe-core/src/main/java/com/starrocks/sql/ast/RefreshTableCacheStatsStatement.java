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
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableName;
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

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class RefreshTableCacheStatsStatement extends RefreshCacheStatsStatement {
    private static final ShowResultSetMetaData TABLE_META_DATA = ShowResultSetMetaData.builder()
            .addColumn(new Column("TABLE_NAME", TypeFactory.createVarchar(256)))
            .addColumn(new Column("DETAIL", TypeFactory.createVarchar(4096)))
            .build();

    private TreeMap<Long, TabletCacheStats> cacheStats;

    public RefreshTableCacheStatsStatement(TableName tableName, NodePosition pos) {
        super(tableName, pos);
        this.cacheStats = new TreeMap<>();
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
        Locker locker = new Locker();
        locker.lockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(olapTable.getId()), LockType.READ);
        try {
            olapTable.getAllPartitions().forEach(partition -> {
                partition.getSubPartitions().forEach(physicalPartition -> {
                    long visibleVersion = physicalPartition.getVisibleVersion();
                    physicalPartition.getMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE)
                            .forEach(materializedIndex ->
                                    materializedIndex.getTablets().forEach(tablet ->
                                    tablets.put(tablet.getId(), new PartitionSnapshot(-1, visibleVersion))));
                });
            });
        } finally {
            locker.unLockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(olapTable.getId()), LockType.READ);
        }
        return tablets;
    }

    @Override
    public void submitResult(long workerGroupId, List<TabletCacheStats> tabletCacheStats) {
        TabletCacheStats stats = cacheStats.computeIfAbsent(workerGroupId, k -> {
            TabletCacheStats s = new TabletCacheStats();
            s.cachedBytes = 0L;
            s.totalBytes = 0L;
            return s;
        });
        for (TabletCacheStats e : tabletCacheStats) {
            stats.cachedBytes += e.cachedBytes;
            stats.totalBytes += e.totalBytes;
        }
    }

    @Override
    public ShowResultSet getResult() {
        List<List<String>> rows = Lists.newArrayList();
        List<String> row = Lists.newArrayList();
        row.add(tableName.getTbl());
        row.add(getResult(cacheStats));
        rows.add(row);
        return new ShowResultSet(TABLE_META_DATA, rows);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("REFRESH CACHE STATS FOR TABLE ");
        sb.append(tableName.getTbl());
        return sb.toString();
    }
}
