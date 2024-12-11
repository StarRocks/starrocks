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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.sql.common.PCell;
import org.apache.commons.lang.NotImplementedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class OlapPartitionTraits extends DefaultTraits {
    private static final Logger LOG = LogManager.getLogger(OlapPartitionTraits.class);

    @Override
    public PartitionKey createEmptyKey() {
        throw new NotImplementedException("not support olap table");
    }

    @Override
    public String getCatalogDBName() {
        throw new NotImplementedException("not support olap table");
    }

    @Override
    public boolean isSupportPCTRefresh() {
        return true;
    }

    @Override
    public Map<String, Range<PartitionKey>> getPartitionKeyRange(Column partitionColumn, Expr partitionExpr) {
        if (!((OlapTable) table).getPartitionInfo().isRangePartition()) {
            throw new IllegalArgumentException("Must be range partitioned table");
        }
        return ((OlapTable) table).getRangePartitionMap();
    }

    @Override
    public Map<String, PCell> getPartitionCells(List<Column> partitionColumns) {
        return ((OlapTable) table).getPartitionCells(Optional.of(partitionColumns));
    }

    @Override
    public Optional<Long> maxPartitionRefreshTs() {
        OlapTable olapTable = (OlapTable) table;
        return olapTable.getPhysicalPartitions().stream().map(PhysicalPartition::getVisibleVersionTime).max(Long::compareTo);
    }

    @Override
    public Set<String> getUpdatedPartitionNames(List<BaseTableInfo> baseTables,
                                                MaterializedView.AsyncRefreshContext context) {
        OlapTable baseTable = (OlapTable) table;
        Map<String, MaterializedView.BasePartitionInfo> mvBaseTableVisibleVersionMap =
                context.getBaseTableVisibleVersionMap()
                        .computeIfAbsent(baseTable.getId(), k -> Maps.newHashMap());
        if (LOG.isDebugEnabled()) {
            List<String> baseTablePartitionInfos = Lists.newArrayList();
            for (String p : baseTable.getVisiblePartitionNames()) {
                Partition partition = baseTable.getPartition(p);
                baseTablePartitionInfos.add(String.format("%s:%s:%s:%s", p, partition.getId(),
                        partition.getDefaultPhysicalPartition().getVisibleVersion(),
                        partition.getDefaultPhysicalPartition().getVisibleVersionTime()));
            }
            LOG.debug("baseTable: {}, baseTablePartitions:{}, mvBaseTableVisibleVersionMap: {}",
                    baseTable.getName(), baseTablePartitionInfos, mvBaseTableVisibleVersionMap);
        }

        Set<String> result = Sets.newHashSet();
        // If there are new added partitions, add it into refresh result.
        for (String partitionName : baseTable.getVisiblePartitionNames()) {
            if (!mvBaseTableVisibleVersionMap.containsKey(partitionName)) {
                Partition partition = baseTable.getPartition(partitionName);
                if (partition.getDefaultPhysicalPartition().getVisibleVersion() != 1) {
                    result.add(partitionName);
                }
            }
        }

        for (Map.Entry<String, MaterializedView.BasePartitionInfo> versionEntry : mvBaseTableVisibleVersionMap.entrySet()) {
            String basePartitionName = versionEntry.getKey();
            Partition basePartition = baseTable.getPartition(basePartitionName);
            if (basePartition == null) {
                // If this partition is dropped, ignore it.
                continue;
            }
            MaterializedView.BasePartitionInfo mvRefreshedPartitionInfo = versionEntry.getValue();
            if (mvRefreshedPartitionInfo == null) {
                result.add(basePartitionName);
            } else {
                // Ignore partitions if mv's partition is the same with the basic table.
                if (!isBaseTableChanged(basePartition, mvRefreshedPartitionInfo)) {
                    continue;
                }

                // others will add into the result.
                result.add(basePartitionName);
            }
        }
        return result;
    }

    /**
     * Check whether the base table's partition has changed or not.
     * </p>
     * NOTE: If the base table is materialized view, partition is overwritten each time, so we need to compare
     * version and modified time.
     */
    public static boolean isBaseTableChanged(Partition partition,
                                             MaterializedView.BasePartitionInfo mvRefreshedPartitionInfo) {
        return mvRefreshedPartitionInfo.getId() != partition.getId()
                || partition.getDefaultPhysicalPartition().getVisibleVersion() != mvRefreshedPartitionInfo.getVersion()
                || partition.getDefaultPhysicalPartition().getVisibleVersionTime()
                > mvRefreshedPartitionInfo.getLastRefreshTime();
    }

    public List<Column> getPartitionColumns() {
        return ((OlapTable) table).getPartitionInfo().getPartitionColumns(table.getIdToColumn());
    }
}

