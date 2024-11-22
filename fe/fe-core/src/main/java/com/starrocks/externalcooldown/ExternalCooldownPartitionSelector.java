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

package com.starrocks.externalcooldown;

import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.ListPartitionInfo;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.common.PListCell;
import com.starrocks.sql.common.SyncPartitionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class ExternalCooldownPartitionSelector {
    private static final Logger LOG = LogManager.getLogger(ExternalCooldownPartitionSelector.class);
    private final OlapTable olapTable;
    private org.apache.iceberg.Table targetIcebergTable;
    private long externalCoolDownWaitSeconds;
    private boolean tableSatisfied;
    private String tableName;
    private final String partitionStart;
    private final String partitionEnd;
    private final boolean isForce;
    private PartitionInfo partitionInfo;
    private List<Partition> satisfiedPartitions;

    public ExternalCooldownPartitionSelector(OlapTable olapTable) {
        this(olapTable, null, null, false);
    }

    public ExternalCooldownPartitionSelector(OlapTable olapTable,
                                             String partitionStart, String partitionEnd, boolean isForce) {
        this.olapTable = olapTable;
        this.partitionStart = partitionStart;
        this.partitionEnd = partitionEnd;
        this.isForce = isForce;
        this.satisfiedPartitions = new ArrayList<>();
        this.init();
    }

    public void init() {
        tableName = olapTable.getName();
        reloadSatisfiedPartitions();
    }

    public void reloadSatisfiedPartitions() {
        tableSatisfied = true;
        partitionInfo = olapTable.getPartitionInfo();

        // check table has external cool down wait second
        Long waitSeconds = olapTable.getExternalCoolDownWaitSecond();
        if (waitSeconds == null || waitSeconds <= 0) {
            LOG.info("table[{}] has no set `{}` property or not satisfied. ignore", tableName,
                    PropertyAnalyzer.PROPERTIES_EXTERNAL_COOLDOWN_WAIT_SECOND);
            tableSatisfied = false;
            return;
        }

        Table targetTable = olapTable.getExternalCoolDownTable();
        if (targetTable == null) {
            LOG.debug("table[{}]'s `{}` not found. ignore", tableName,
                    PropertyAnalyzer.PROPERTIES_EXTERNAL_COOLDOWN_TARGET);
            tableSatisfied = false;
        }
        if (!(targetTable instanceof IcebergTable)) {
            LOG.debug("table[{}]'s `{}` property is not iceberg table. ignore", tableName,
                    PropertyAnalyzer.PROPERTIES_EXTERNAL_COOLDOWN_TARGET);
            tableSatisfied = false;
            return;
        }
        targetIcebergTable = ((IcebergTable) targetTable).getNativeTable();
        if (targetIcebergTable == null) {
            LOG.debug("table[{}]'s `{}` property related native iceberg table not found. ignore",
                    tableName, PropertyAnalyzer.PROPERTIES_EXTERNAL_COOLDOWN_TARGET);
            tableSatisfied = false;
        }

        externalCoolDownWaitSeconds = waitSeconds;
        satisfiedPartitions = this.computeSatisfiedPartitions(-1);
    }

    public boolean isTableSatisfied() {
        return tableSatisfied;
    }

    /**
     * check whether partition satisfy external cool down condition
     */
    protected boolean isPartitionSatisfied(Partition partition) {
        try {
            return checkPartitionSatisfied(partition);
        } catch (Exception e) {
            LOG.warn("check partition [{}-{}] satisfy external cool down condition failed",
                    tableName, partition.getName(), e);
            return false;
        }
    }

    protected boolean checkPartitionSatisfied(Partition partition) {
        // force cooldown don't need check cooldown state and consistency check result,
        // and initial partition could also be cooldown
        if (isForce) {
            return true;
        }

        long externalCoolDownWaitMillis = externalCoolDownWaitSeconds * 1000L;
        if (externalCoolDownWaitSeconds <= 0) {
            return false;
        }
        PhysicalPartition physicalPartition = partition.getDefaultPhysicalPartition();
        if (physicalPartition == null) {
            return false;
        }

        // partition with init version has no data, doesn't need do external cool down
        if (physicalPartition.getVisibleVersion() == PhysicalPartition.PARTITION_INIT_VERSION) {
            LOG.debug("table[{}] partition[{}] is init version. ignore", tableName, partition.getName());
            return false;
        }

        // after partition update, should wait a while to avoid unnecessary duplicate external cool down
        long changedMillis = System.currentTimeMillis() - physicalPartition.getVisibleVersionTime();
        if (changedMillis <= externalCoolDownWaitMillis) {
            LOG.debug("partition[{}]'s changed time hasn't reach {} {}. ignore", partition.getId(),
                    PropertyAnalyzer.PROPERTIES_EXTERNAL_COOLDOWN_WAIT_SECOND, externalCoolDownWaitSeconds);
            return false;
        }

        // check if this partition has changed since last external cool down
        Long partitionCoolDownSyncedTimeMs = partitionInfo.getExternalCoolDownSyncedTimeMs(partition.getId());
        Long consistencyCheckTimeMs = partitionInfo.getExternalCoolDownConsistencyCheckTimeMs(partition.getId());
        // partition has never do external cool down or has changed since last external cool down
        if (partitionCoolDownSyncedTimeMs == null || partitionCoolDownSyncedTimeMs < physicalPartition.getVisibleVersionTime()) {
            return true;
        }
        // wait consistency check if has done external cooldown
        if (consistencyCheckTimeMs == null || partitionCoolDownSyncedTimeMs > consistencyCheckTimeMs) {
            // wait to do consistency check after external cool down
            LOG.debug("table [{}] partition[{}] external cool down time newer then consistency check time",
                    tableName, partition.getName());
            return false;
        }
        Long diff = partitionInfo.getExternalCoolDownConsistencyCheckDifference(partition.getId());
        if (diff == null || diff == 0) {
            // has consistency check after external cool down, and check result ok
            LOG.debug("table [{}] partition[{}] external cool down consistency check result ok",
                    tableName, partition.getName());
            return false;
        }

        return true;
    }

    public Partition getOneSatisfiedPartition() {
        if (satisfiedPartitions.isEmpty()) {
            reloadSatisfiedPartitions();
            if (satisfiedPartitions.isEmpty()) {
                return null;
            }
        }
        return satisfiedPartitions.remove(0);
    }

    public Partition getNextSatisfiedPartition(Partition currentPartition) {
        for (Partition partition : satisfiedPartitions) {
            if (partition.getName().compareTo(currentPartition.getName()) <= 0) {
                continue;
            }
            return partition;
        }
        return null;
    }

    public Set<String> getPartitionNamesByListWithPartitionLimit() {
        Set<String> result = Sets.newHashSet();

        Map<String, PListCell> listMap = olapTable.getValidListPartitionMap(-1);
        for (Map.Entry<String, PListCell> entry : listMap.entrySet()) {
            if (entry.getKey().compareTo(partitionStart) >= 0 && entry.getKey().compareTo(partitionEnd) <= 0) {
                result.add(entry.getKey());
            }
        }
        return result;
    }

    public Set<String> getPartitionNamesByRangeWithPartitionLimit()
            throws SemanticException {
        Set<String> result = Sets.newHashSet();
        List<Column> partitionColumns = olapTable.getPartitionInfo().getPartitionColumns(olapTable.getIdToColumn());
        Column partitionColumn = partitionColumns.get(0);
        Range<PartitionKey> rangeToInclude;
        try {
            rangeToInclude = SyncPartitionUtils.createRange(partitionStart, partitionEnd, partitionColumn);
        } catch (Exception e) {
            throw new SemanticException(e.getMessage());
        }
        Map<String, Range<PartitionKey>> rangeMap;
        try {
            rangeMap = olapTable.getValidRangePartitionMap(-1);
        } catch (Exception e) {
            throw new SemanticException(e.getMessage());
        }
        for (Map.Entry<String, Range<PartitionKey>> entry : rangeMap.entrySet()) {
            Range<PartitionKey> rangeToCheck = entry.getValue();
            int lowerCmp = rangeToInclude.lowerEndpoint().compareTo(rangeToCheck.upperEndpoint());
            int upperCmp = rangeToInclude.upperEndpoint().compareTo(rangeToCheck.lowerEndpoint());
            if (!(lowerCmp >= 0 || upperCmp <= 0)) {
                result.add(entry.getKey());
            }
        }
        return result;
    }

    // get partition names in range [partitionStart, partitionEnd]
    private Set<String> getPartitionsInRange() throws SemanticException {
        if (partitionStart == null && partitionEnd == null) {
            if (partitionInfo instanceof SinglePartitionInfo) {
                return olapTable.getVisiblePartitionNames();
            } else if (partitionInfo instanceof ListPartitionInfo) {
                return olapTable.getValidListPartitionMap(-1).keySet();
            } else {
                try {
                    return olapTable.getValidRangePartitionMap(-1).keySet();
                } catch (Exception e) {
                    throw new SemanticException(e.getMessage());
                }
            }
        }

        if (partitionInfo instanceof SinglePartitionInfo) {
            return olapTable.getVisiblePartitionNames();
        } else if (partitionInfo instanceof RangePartitionInfo) {
            return getPartitionNamesByRangeWithPartitionLimit();
        } else {
            // ListPartitionInfo
            return getPartitionNamesByListWithPartitionLimit();
        }
    }

    public List<Partition> computeSatisfiedPartitions(int limit) {
        boolean isOlapTablePartitioned = olapTable.getPartitions().size() > 1 || olapTable.dynamicPartitionExists();
        if (!isOlapTablePartitioned && targetIcebergTable.spec() != null
                && !targetIcebergTable.spec().fields().isEmpty()) {
            LOG.warn("source table: {} is a none partitioned table, target table shouldn't partitionSpec fields",
                    tableName);
            return new ArrayList<>();
        }

        Set<String> partitionNames;
        try {
            partitionNames = getPartitionsInRange();
        } catch (SemanticException e) {
            LOG.warn("table: {} get partitions in range failed, {}", olapTable.getName(), e);
            return new ArrayList<>();
        }

        List<String> sortedPartitionNames = new ArrayList<>(partitionNames);
        Collections.sort(sortedPartitionNames);
        List<Partition> chosenPartitions = new ArrayList<>();
        for (String partitionName : sortedPartitionNames) {
            Partition partition = olapTable.getPartition(partitionName);
            if (partition == null) {
                continue;
            }
            boolean isSatisfied = isPartitionSatisfied(partition);
            if (isSatisfied) {
                LOG.info("choose partition[{}-{}] to external cool down", tableName, partition.getName());
                chosenPartitions.add(partition);
            }
            if (limit > 0 && chosenPartitions.size() >= limit) {
                LOG.info("stop choose partition as no remain jobs");
                return chosenPartitions;
            }
        }
        return chosenPartitions;
    }

    public boolean hasPartitionSatisfied() {
        return tableSatisfied && !satisfiedPartitions.isEmpty();
    }
}
