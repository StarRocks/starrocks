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

package com.starrocks.connector.partitiontraits;

import com.google.common.collect.Sets;
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.DeltaLakePartitionKey;
import com.starrocks.catalog.DeltaLakeTable;
import com.starrocks.catalog.LightWeightDeltaLakeTable;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.connector.PartitionInfo;
import io.delta.kernel.Snapshot;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * MV freshness of a Delta table is tracked by its table version, recorded for every partition an MV refresh read.
 */
public class DeltaLakePartitionTraits extends DefaultTraits {
    private static final Logger LOG = LogManager.getLogger(DeltaLakePartitionTraits.class);

    @Override
    public PartitionKey createEmptyKey() {
        return new DeltaLakePartitionKey();
    }

    @Override
    public boolean isSupportPCTRefresh() {
        return false;
    }

    @Override
    public boolean readsPartitionInfoToDetectUpdates() {
        return false;
    }

    public static boolean isTableVersionTracked(Table table) {
        return versionOf(table) >= 0;
    }

    private static long versionOf(Table table) {
        if (!(table instanceof DeltaLakeTable) || table instanceof LightWeightDeltaLakeTable) {
            return -1;
        }
        Snapshot snapshot = ((DeltaLakeTable) table).getDeltaSnapshot();
        return snapshot == null ? -1 : snapshot.getVersion();
    }

    @Override
    protected Object partitionInfoSnapshot() {
        // the partition infos carry the table version, which DeltaLakeTable#equals ignores
        return pinnedVersionRange != null ? pinnedVersionRange : versionOf(table);
    }

    @Override
    Map<String, PartitionInfo> fetchPartitionNameWithPartitionInfo() {
        // listing the partitions of a partitioned Delta table scans all its data files
        if (!table.isUnPartitioned()) {
            throw new UnsupportedOperationException(
                    "Listing the partitions of partitioned Delta table " + table.getName() + " is not supported");
        }
        return super.fetchPartitionNameWithPartitionInfo();
    }

    @Override
    public List<PartitionInfo> getPartitions(List<String> partitionNames) {
        if (pinnedVersionRange != null || !isTableVersionTracked(table)) {
            return super.getPartitions(partitionNames);
        }
        PartitionInfo tableVersion = new DeltaLakeTableVersion((DeltaLakeTable) table);
        return partitionNames.stream().map(name -> tableVersion).collect(Collectors.toList());
    }

    @Override
    public Set<String> getUpdatedPartitionNames(List<BaseTableInfo> baseTables,
                                                MaterializedView.AsyncRefreshContext context) {
        long currentVersion = versionOf(table);
        if (currentVersion < 0) {
            return null;
        }
        for (BaseTableInfo baseTableInfo : baseTables) {
            if (!baseTableInfo.getTableIdentifier().equalsIgnoreCase(table.getTableIdentifier())) {
                continue;
            }
            // never report a change as null: CachedPartitionTraits turns null into {}
            Map<String, MaterializedView.BasePartitionInfo> versionMap = context.getBaseTableRefreshInfo(baseTableInfo);
            if (versionMap.isEmpty()) {
                // nothing recorded, so the MV is stale. Which names are reported doesn't matter: a Delta table is
                // never the partition ref table of an MV, so any change makes the whole MV stale.
                return Sets.newHashSet(table.getName());
            }
            boolean changed = versionMap.values().stream()
                    .anyMatch(refreshed -> refreshed == null || refreshed.getVersion() != currentVersion);
            return changed ? Sets.newHashSet(versionMap.keySet()) : Sets.newHashSet();
        }
        return null;
    }

    // The table version is only for MVs: statistics collection keeps treating Delta tables as of unknown update time.
    @Override
    public Set<String> getUpdatedPartitionNames(LocalDateTime checkTime, int extraSeconds) {
        return null;
    }

    @Override
    public LocalDateTime getTableLastUpdateTime(int extraSeconds) {
        return null;
    }

    // TODO: maxPartitionRefreshTs() is not implemented, so mv_rewrite_staleness_second is not supported for MVs
    //  over Delta tables.

    private static final class DeltaLakeTableVersion implements PartitionInfo {
        private final DeltaLakeTable table;
        private Long commitTime;

        DeltaLakeTableVersion(DeltaLakeTable table) {
            this.table = table;
        }

        @Override
        public long getModifiedTime() {
            if (commitTime == null) {
                commitTime = readCommitTime();
            }
            return commitTime;
        }

        private long readCommitTime() {
            try {
                return table.getDeltaSnapshot().getTimestamp(table.getDeltaEngine());
            } catch (Exception e) {
                LOG.warn("Failed to get the commit time of version {} of Delta table {}.{}", getVersion(),
                        table.getCatalogDBName(), table.getCatalogTableName(), e);
                return System.currentTimeMillis();
            }
        }

        @Override
        public TimeUnit getModifiedTimeUnit() {
            return TimeUnit.MILLISECONDS;
        }

        @Override
        public long getVersion() {
            return versionOf(table);
        }
    }
}
