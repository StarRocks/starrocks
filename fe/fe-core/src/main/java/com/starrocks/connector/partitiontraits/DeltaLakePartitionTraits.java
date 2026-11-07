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
 * MV freshness of an unpartitioned Delta table is tracked by its table version, recorded under the table's name, its
 * only partition. Partitioned Delta tables are not tracked: listing their partitions scans all their data files.
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
        return table.isUnPartitioned() && versionOf(table) >= 0;
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
    public List<PartitionInfo> getPartitions(List<String> partitionNames) {
        if (pinnedVersionRange != null || !isTableVersionTracked(table)) {
            return super.getPartitions(partitionNames);
        }
        PartitionInfo tableVersion = new DeltaLakeTableVersion((DeltaLakeTable) table);
        return partitionNames.stream().map(name -> tableVersion).collect(Collectors.toList());
    }

    /**
     * @return {} if the MV was refreshed from the current table version, otherwise the table name. Never null:
     * {@link CachedPartitionTraits} turns null into {}, i.e. fresh. Which names are reported doesn't matter, since a
     * Delta table is never the partition ref table of an MV and any change makes the whole MV stale.
     */
    @Override
    public Set<String> getUpdatedPartitionNames(List<BaseTableInfo> baseTables,
                                                MaterializedView.AsyncRefreshContext context) {
        Set<String> changed = Sets.newHashSet(table.getName());
        if (!isTableVersionTracked(table)) {
            return changed;
        }
        long currentVersion = versionOf(table);
        for (BaseTableInfo baseTableInfo : baseTables) {
            // a dropped and recreated table has a new Delta table id, so it matches no base table info
            if (!baseTableInfo.getTableIdentifier().equalsIgnoreCase(table.getTableIdentifier())) {
                continue;
            }
            Map<String, MaterializedView.BasePartitionInfo> versionMap =
                    context.getBaseTableInfoVisibleVersionMap().get(baseTableInfo);
            MaterializedView.BasePartitionInfo refreshed = versionMap == null ? null : versionMap.get(table.getName());
            return refreshed != null && refreshed.getVersion() == currentVersion ? Sets.newHashSet() : changed;
        }
        return changed;
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
