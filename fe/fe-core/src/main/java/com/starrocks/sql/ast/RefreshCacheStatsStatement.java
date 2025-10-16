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

import com.starrocks.catalog.TableName;
import com.starrocks.common.DdlException;
import com.starrocks.monitor.unit.ByteSizeValue;
import com.starrocks.proto.TabletCacheStats;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.sql.parser.NodePosition;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public abstract class RefreshCacheStatsStatement extends DdlStmt {
    public class PartitionSnapshot {
        public PartitionSnapshot(int partitionNameIndex, long visibleVersion) {
            this.partitionNameIndex = partitionNameIndex;
            this.visibleVersion = visibleVersion;
        }
        public final int partitionNameIndex; // index in `partitionNames`, only used for `PARTITION` level
        public final long visibleVersion;
    }

    protected final TableName tableName;

    protected Map<Long, PartitionSnapshot> tablets;

    public RefreshCacheStatsStatement(TableName tableName, NodePosition pos) {
        super(pos);
        this.tableName = tableName;
        this.tablets = new HashMap<>();
    }

    public TableName getTableName() {
        return tableName;
    }

    public abstract Map<Long, PartitionSnapshot> prepare() throws DdlException;

    public abstract void submitResult(long workerGroupId, List<TabletCacheStats> tabletCacheStats);

    public abstract ShowResultSet getResult();

    public String getResult(TreeMap<Long, TabletCacheStats> stats) {
        StringBuilder sb = new StringBuilder();
        if (stats.entrySet() == null) {
            return sb.toString();
        }
        for (Map.Entry<Long, TabletCacheStats> e : stats.entrySet()) {
            sb.append("{");
            sb.append("cn_group: ");
            sb.append(Long.toString(e.getKey()));
            sb.append(", ");
            sb.append("cached_size: ");
            sb.append(new ByteSizeValue(e.getValue().cachedBytes).toString());
            sb.append(", ");
            sb.append("total_size: ");
            sb.append(new ByteSizeValue(e.getValue().totalBytes).toString());
            sb.append(", ");
            sb.append("percent: ");
            if (e.getValue().totalBytes > 0) {
                sb.append(String.format("%.1f", (double) e.getValue().cachedBytes / e.getValue().totalBytes * 100));
                sb.append("%");
            }
            sb.append("}\n");
        }
        if (!sb.isEmpty()) {
            sb.deleteCharAt(sb.length() - 1);
        }
        return sb.toString();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return ((AstVisitorExtendInterface<R, C>) visitor).visitRefreshCacheStatsStatement(this, context);
    }
}
