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
import com.starrocks.catalog.Database;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.ListPartitionInfo;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.sql.common.DmlException;
import com.starrocks.sql.common.PListCell;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.starrocks.sql.common.SyncPartitionUtils.createRange;

public class ExternalCooldownPartitionSelector {
    private static final Logger LOG = LogManager.getLogger(ExternalCooldownPartitionSelector.class);
    private final Database db;
    private final OlapTable olapTable;
    private org.apache.iceberg.Table targetIcebergTable;
    private long externalCoolDownWaitSeconds;
    private boolean tableSatisfied;
    private String fullTableName;
    private final String partitionStart;
    private final String partitionEnd;
    private final boolean isForce;
    private PartitionInfo partitionInfo;
    private List<Partition> satisfiedPartitions;

    public ExternalCooldownPartitionSelector(Database db, OlapTable olapTable) {
        this(db, olapTable, null, null, false);
    }

    public ExternalCooldownPartitionSelector(Database db, OlapTable olapTable,
                                             String partitionStart, String partitionEnd, boolean isForce) {
        this.db = db;
        this.olapTable = olapTable;
        this.partitionStart = partitionStart;
        this.partitionEnd = partitionEnd;
        this.isForce = isForce;
        this.satisfiedPartitions = new ArrayList<>();
        this.init();
    }

    public void init() {
        fullTableName = db.getFullName() + "." + olapTable.getName();
        reloadSatisfiedPartitions();
    }

    public void reloadSatisfiedPartitions() {
        tableSatisfied = true;
        partitionInfo = olapTable.getPartitionInfo();
        if (olapTable.getExternalCoolDownWaitSecond() == null) {
            tableSatisfied = false;
            return;
        }

        Table targetTable = olapTable.getExternalCoolDownTable();
        if (targetTable == null) {
            LOG.debug("table[{}]'s `{}` not found. ignore", fullTableName,
                    PropertyAnalyzer.PROPERTIES_EXTERNAL_COOLDOWN_TARGET);
            tableSatisfied = false;
        }
        if (!(targetTable instanceof IcebergTable)) {
            LOG.debug("table[{}]'s `{}` property is not iceberg table. ignore", fullTableName,
                    PropertyAnalyzer.PROPERTIES_EXTERNAL_COOLDOWN_TARGET);
            tableSatisfied = false;
            return;
        }
        targetIcebergTable = ((IcebergTable) targetTable).getNativeTable();
        if (targetIcebergTable == null) {
            LOG.debug("table[{}]'s `{}` property related native iceberg table not found. ignore",
                    fullTableName, PropertyAnalyzer.PROPERTIES_EXTERNAL_COOLDOWN_TARGET);
            tableSatisfied = false;
        }

        // check table has external cool down wait second
        Long waitSeconds = olapTable.getExternalCoolDownWaitSecond();
        if (waitSeconds == null || waitSeconds == 0) {
            LOG.info("table[{}] has no set `{}` property. ignore", fullTableName,
                    PropertyAnalyzer.PROPERTIES_EXTERNAL_COOLDOWN_WAIT_SECOND);
            tableSatisfied = false;
        } else {
            externalCoolDownWaitSeconds = waitSeconds;
        }
        satisfiedPartitions = this.getSatisfiedPartitions(-1);
    }

    public boolean isTableSatisfied() {
        return tableSatisfied;
    }

    /**
     * check whether partition satisfy external cool down condition
     */
    private boolean isPartitionSatisfied(Partition partition) {
        long externalCoolDownWaitMillis = externalCoolDownWaitSeconds * 1000L;
        // partition with init version has no data, doesn't need do external cool down
        if (partition.getVisibleVersion() == Partition.PARTITION_INIT_VERSION) {
            LOG.debug("table[{}] partition[{}] is init version. ignore", fullTableName, partition.getName());
            return false;
        }

        // force cooldown don't need check cooldown state and consistency check result
        if (isForce) {
            return true;
        }

        // after partition update, should wait a while to avoid unnecessary duplicate external cool down
        long changedMillis = System.currentTimeMillis() - partition.getVisibleVersionTime();
        if (changedMillis <= externalCoolDownWaitMillis) {
            LOG.info("partition[{}]'s changed time hasn't reach {} {}. ignore", partition.getId(),
                    PropertyAnalyzer.PROPERTIES_EXTERNAL_COOLDOWN_WAIT_SECOND, externalCoolDownWaitSeconds);
            return false;
        }

        // check if this partition has changed since last external cool down
        Long partitionCoolDownSyncedTimeMs = partitionInfo.getExternalCoolDownSyncedTimeMs(partition.getId());
        Long consistencyCheckTimeMs = partitionInfo.getExternalCoolDownConsistencyCheckTimeMs(partition.getId());
        // partition has never do external cool down or has changed since last external cool down
        if (partitionCoolDownSyncedTimeMs == null || partitionCoolDownSyncedTimeMs < partition.getVisibleVersionTime()) {
            return true;
        }
        // wait consistency check if has done external cool down
        if (consistencyCheckTimeMs == null || partitionCoolDownSyncedTimeMs > consistencyCheckTimeMs) {
            // wait to do consistency check after external cool down
            LOG.debug("table [{}] partition[{}] external cool down time newer then consistency check time",
                    fullTableName, partition.getName());
            return false;
        }
        Long diff = partitionInfo.getExternalCoolDownConsistencyCheckDifference(partition.getId());
        if (diff == null || diff == 0) {
            // has consistency check after external cool down, and check result ok
            LOG.debug("table [{}] partition[{}] external cool down consistency check result ok",
                    fullTableName, partition.getName());
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
        if (satisfiedPartitions.isEmpty()) {
            return null;
        }
        for (Partition partition : satisfiedPartitions) {
            if (partition.getName().compareTo(currentPartition.getName()) <= 0) {
                continue;
            }
            return partition;
        }
        return null;
    }

    public Set<String> getPartitionNamesByListWithPartitionLimit() {
        boolean hasPartitionRange = StringUtils.isNoneEmpty(partitionStart) || StringUtils.isNoneEmpty(partitionEnd);

        if (hasPartitionRange) {
            Set<String> result = Sets.newHashSet();

            Map<String, PListCell> listMap = olapTable.getValidListPartitionMap(-1);
            for (Map.Entry<String, PListCell> entry : listMap.entrySet()) {
                if (entry.getKey().compareTo(partitionStart) >= 0 && entry.getKey().compareTo(partitionEnd) <= 0) {
                    result.add(entry.getKey());
                }
            }
            return result;
        }

        return olapTable.getValidListPartitionMap(-1).keySet();
    }

    public Set<String> getPartitionNamesByRangeWithPartitionLimit()
            throws AnalysisException {
        boolean hasPartitionRange = StringUtils.isNoneEmpty(partitionStart) || StringUtils.isNoneEmpty(partitionEnd);

        if (hasPartitionRange) {
            Set<String> result = Sets.newHashSet();
            List<Column> partitionColumns = olapTable.getPartitionInfo().getPartitionColumns(olapTable.getIdToColumn());
            Column partitionColumn = partitionColumns.get(0);
            Range<PartitionKey> rangeToInclude = createRange(partitionStart, partitionEnd, partitionColumn);
            Map<String, Range<PartitionKey>> rangeMap = olapTable.getValidRangePartitionMap(-1);
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

        return olapTable.getValidRangePartitionMap(-1).keySet();
    }

    // get partition names in range [partitionStart, partitionEnd]
    private Set<String> getPartitionsInRange() throws AnalysisException {
        if (partitionStart == null && partitionEnd == null) {
            if (partitionInfo instanceof SinglePartitionInfo) {
                return olapTable.getVisiblePartitionNames();
            } else {
                if (partitionInfo instanceof ListPartitionInfo) {
                    return olapTable.getValidListPartitionMap(-1).keySet();
                } else {
                    return olapTable.getValidRangePartitionMap(-1).keySet();
                }
            }
        }

        if (partitionInfo instanceof SinglePartitionInfo) {
            return olapTable.getVisiblePartitionNames();
        } else if (partitionInfo instanceof RangePartitionInfo) {
            return getPartitionNamesByRangeWithPartitionLimit();
        } else if (partitionInfo instanceof ListPartitionInfo) {
            return getPartitionNamesByListWithPartitionLimit();
        } else {
            throw new DmlException("unsupported partition info type:" + partitionInfo.getClass().getName());
        }
    }

    public List<Partition> getSatisfiedPartitions(int limit) {
        List<Partition> chosenPartitions = new ArrayList<>();

        boolean isOlapTablePartitioned = olapTable.getPartitions().size() > 1 || olapTable.dynamicPartitionExists();
        if (!isOlapTablePartitioned && targetIcebergTable.spec() != null && !targetIcebergTable.spec().fields().isEmpty()) {
            LOG.warn("table: {} is a none partitioned table, cannot have partitionSpec fields",
                    fullTableName);
            return chosenPartitions;
        }

        boolean isSatisfied;
        Set<String> partitionNames;
        try {
            partitionNames = getPartitionsInRange();
        } catch (AnalysisException e) {
            LOG.warn("table: {} get partitions in range failed, {}", olapTable.getName(), e);
            return chosenPartitions;
        }

        List<String> sortedPartitionNames = new ArrayList<>(partitionNames);
        Collections.sort(sortedPartitionNames);

        for (String partitionName : sortedPartitionNames) {
            Partition partition = olapTable.getPartition(partitionName);
            if (partition != null) {
                try {
                    isSatisfied = isPartitionSatisfied(partition);
                } catch (Exception e) {
                    isSatisfied = false;
                    String msg = String.format("check partition [%s-%s] satisfy external cool down condition failed",
                            fullTableName, partition.getName());
                    LOG.warn(msg, e);
                }
                if (isSatisfied) {
                    LOG.info("choose partition[{}-{}] to external cool down", fullTableName, partition.getName());
                    chosenPartitions.add(partition);
                }

                if (limit > 0 && chosenPartitions.size() >= limit) {
                    LOG.info("stop choose partition as no remain jobs");
                    break;
                }
            }
        }
        return chosenPartitions;
    }

    public boolean hasPartitionSatisfied() {
        return tableSatisfied && !satisfiedPartitions.isEmpty();
    }
}
