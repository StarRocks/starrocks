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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/common/proc/PartitionsProcDir.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.common.proc;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.BinaryType;
import com.starrocks.analysis.DateLiteral;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.LimitElement;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.DistributionInfo.DistributionInfoType;
import com.starrocks.catalog.ExpressionRangePartitionInfo;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.ListPartitionInfo;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionType;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.util.ListComparator;
import com.starrocks.common.util.OrderByPair;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.lake.DataCacheInfo;
import com.starrocks.lake.compaction.PartitionIdentifier;
import com.starrocks.lake.compaction.PartitionStatistics;
import com.starrocks.lake.compaction.Quantiles;
import com.starrocks.monitor.unit.ByteSizeValue;
import com.starrocks.server.GlobalStateMgr;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/*
 * SHOW PROC /dbs/dbId/tableId/partitions, or
 * SHOW PROC /dbs/dbId/tableId/temp_partitions
 * show [temp] partitions' detail info within a table
 */
public class PartitionsProcDir implements ProcDirInterface {
    private final PartitionType partitionType;
    private ImmutableList<String> titleNames;
    private Database db;
    private OlapTable table;
    private boolean isTempPartition = false;

    public PartitionsProcDir(Database db, OlapTable table, boolean isTempPartition) {
        this.db = db;
        this.table = table;
        this.isTempPartition = isTempPartition;
        this.partitionType = table.getPartitionInfo().getType();
        this.createTitleNames();
    }

    private void createTitleNames() {
        if (table.isCloudNativeTableOrMaterializedView()) {
            ImmutableList.Builder<String> builder = new ImmutableList.Builder<String>()
                    .add("PartitionId")
                    .add("PartitionName")
                    .add("CompactVersion")
                    .add("VisibleVersion")
                    .add("NextVersion")
                    .add("State")
                    .add("PartitionKey")
                    .add(partitionType == PartitionType.LIST ? "List" : "Range")
                    .add("DistributionKey")
                    .add("Buckets")
                    .add("DataSize")
                    .add("RowCount")
                    .add("EnableDataCache")
                    .add("AsyncWrite")
                    .add("AvgCS") // Average compaction score
                    .add("P50CS") // 50th percentile compaction score
                    .add("MaxCS"); // Maximum compaction score
            this.titleNames = builder.build();
        } else {
            ImmutableList.Builder<String> builder = new ImmutableList.Builder<String>()
                    .add("PartitionId")
                    .add("PartitionName")
                    .add("VisibleVersion")
                    .add("VisibleVersionTime")
                    .add("VisibleVersionHash")
                    .add("State")
                    .add("PartitionKey")
                    .add(partitionType == PartitionType.LIST ? "List" : "Range")
                    .add("DistributionKey")
                    .add("Buckets")
                    .add("ReplicationNum")
                    .add("StorageMedium")
                    .add("CooldownTime")
                    .add("LastConsistencyCheckTime")
                    .add("DataSize")
                    .add("IsInMemory")
                    .add("RowCount");
            this.titleNames = builder.build();
        }
    }

    public boolean filter(String columnName, Comparable element, Map<String, Expr> filterMap) throws AnalysisException {
        if (filterMap == null) {
            return true;
        }
        Expr subExpr = filterMap.get(columnName.toLowerCase());
        if (subExpr == null) {
            return true;
        }
        if (subExpr instanceof BinaryPredicate) {
            BinaryPredicate binaryPredicate = (BinaryPredicate) subExpr;
            if (subExpr.getChild(1) instanceof StringLiteral &&
                    binaryPredicate.getOp() == BinaryType.EQ) {
                return ((StringLiteral) subExpr.getChild(1)).getValue().equals(element);
            }
            long leftVal;
            long rightVal;
            if (subExpr.getChild(1) instanceof DateLiteral) {
                leftVal = (new DateLiteral((String) element, Type.DATETIME)).getLongValue();
                rightVal = ((DateLiteral) subExpr.getChild(1)).getLongValue();
            } else {
                leftVal = Long.parseLong(element.toString());
                rightVal = ((IntLiteral) subExpr.getChild(1)).getLongValue();
            }
            switch (binaryPredicate.getOp()) {
                case EQ:
                case EQ_FOR_NULL:
                    return leftVal == rightVal;
                case GE:
                    return leftVal >= rightVal;
                case GT:
                    return leftVal > rightVal;
                case LE:
                    return leftVal <= rightVal;
                case LT:
                    return leftVal < rightVal;
                case NE:
                    return leftVal != rightVal;
                default:
                    Preconditions.checkState(false, "No defined binary operator.");
            }
        } else {
            return like((String) element, ((StringLiteral) subExpr.getChild(1)).getValue());
        }
        return true;
    }

    public boolean like(String str, String expr) {
        expr = expr.toLowerCase();
        expr = expr.replace(".", "\\.");
        expr = expr.replace("?", ".");
        expr = expr.replace("%", ".*");
        str = str.toLowerCase();
        return str.matches(expr);
    }

    public ProcResult fetchResultByFilter(Map<String, Expr> filterMap, List<OrderByPair> orderByPairs,
                                          LimitElement limitElement) throws AnalysisException {
        List<List<Comparable>> partitionInfos = getPartitionInfos();
        List<List<Comparable>> filterPartitionInfos;

        // where
        if (filterMap == null || filterMap.isEmpty()) {
            filterPartitionInfos = partitionInfos;
        } else {
            filterPartitionInfos = Lists.newArrayList();
            for (List<Comparable> partitionInfo : partitionInfos) {
                if (partitionInfo.size() != this.titleNames.size()) {
                    throw new AnalysisException("PartitionInfos.size() " + partitionInfos.size()
                            + " not equal TITLE_NAMES.size() " + this.titleNames.size());
                }
                boolean isNeed = true;
                for (int i = 0; i < partitionInfo.size(); i++) {
                    isNeed = filter(this.titleNames.get(i), partitionInfo.get(i), filterMap);
                    if (!isNeed) {
                        break;
                    }
                }

                if (isNeed) {
                    filterPartitionInfos.add(partitionInfo);
                }
            }
        }

        // order by
        if (orderByPairs != null && !orderByPairs.isEmpty()) {
            ListComparator<List<Comparable>> comparator;
            OrderByPair[] orderByPairArr = new OrderByPair[orderByPairs.size()];
            comparator = new ListComparator<>(orderByPairs.toArray(orderByPairArr));
            filterPartitionInfos.sort(comparator);
        }

        // limit
        if (limitElement != null && limitElement.hasLimit()) {
            int beginIndex = (int) limitElement.getOffset();
            int endIndex = (int) (beginIndex + limitElement.getLimit());
            if (endIndex > filterPartitionInfos.size()) {
                endIndex = filterPartitionInfos.size();
            }
            filterPartitionInfos = filterPartitionInfos.subList(beginIndex, endIndex);
        }

        return getBasicProcResult(filterPartitionInfos);
    }

    public BaseProcResult getBasicProcResult(List<List<Comparable>> partitionInfos) {
        // set result
        BaseProcResult result = new BaseProcResult();
        result.setNames(this.titleNames);
        for (List<Comparable> info : partitionInfos) {
            List<String> row = new ArrayList<String>(info.size());
            for (Comparable comparable : info) {
                row.add(comparable.toString());
            }
            result.addRow(row);
        }

        return result;
    }

    private List<List<Comparable>> getPartitionInfos() {
        Preconditions.checkNotNull(db);
        Preconditions.checkNotNull(table);

        // get info
        List<List<Comparable>> partitionInfos = new ArrayList<List<Comparable>>();
        Locker locker = new Locker();
        locker.lockDatabase(db, LockType.READ);
        try {
            List<Long> partitionIds;
            PartitionInfo tblPartitionInfo = table.getPartitionInfo();

            // for range partitions, we return partitions in ascending range order by default.
            // this is to be consistent with the behaviour before 0.12
            if (tblPartitionInfo.isRangePartition()) {
                RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) tblPartitionInfo;
                partitionIds = rangePartitionInfo.getSortedRangeMap(isTempPartition).stream()
                        .map(Map.Entry::getKey).collect(Collectors.toList());
            } else {
                Collection<Partition> partitions =
                        isTempPartition ? table.getTempPartitions() : table.getPartitions();
                partitionIds = partitions.stream().map(Partition::getId).collect(Collectors.toList());
            }

            for (Long partitionId : partitionIds) {
                Partition partition = table.getPartition(partitionId);
                String partitionName = partition.getName();
                for (PhysicalPartition physicalPartition : partition.getSubPartitions()) {
                    if (partitionName != null &&
                            !partitionName.startsWith(ExpressionRangePartitionInfo.SHADOW_PARTITION_PREFIX)) {
                        if (table.isOlapTableOrMaterializedView()) {
                            partitionInfos.add(getOlapPartitionInfo(tblPartitionInfo, partition, physicalPartition));
                        } else {
                            partitionInfos.add(getLakePartitionInfo(tblPartitionInfo, partition, physicalPartition));
                        }
                    } else if (Config.enable_display_shadow_partitions) {
                        if (table.isOlapTableOrMaterializedView()) {
                            partitionInfos.add(getOlapPartitionInfo(tblPartitionInfo, partition, physicalPartition));
                        } else {
                            partitionInfos.add(getLakePartitionInfo(tblPartitionInfo, partition, physicalPartition));
                        }
                    }
                }
            }
        } finally {
            locker.unLockDatabase(db, LockType.READ);
        }
        return partitionInfos;
    }

    private String distributionKeyAsString(DistributionInfo distributionInfo) {
        if (distributionInfo.getType() == DistributionInfoType.HASH) {
            HashDistributionInfo hashDistributionInfo = (HashDistributionInfo) distributionInfo;
            List<Column> distributionColumns = hashDistributionInfo.getDistributionColumns();
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < distributionColumns.size(); i++) {
                if (i != 0) {
                    sb.append(", ");
                }
                sb.append(distributionColumns.get(i).getName());
            }
            return sb.toString();
        } else {
            return "ALL KEY";
        }
    }

    private List<Comparable> getOlapPartitionInfo(PartitionInfo tblPartitionInfo, Partition partition,
                                                  PhysicalPartition physicalPartition) {
        List<Comparable> partitionInfo = new ArrayList<Comparable>();
        partitionInfo.add(physicalPartition.getId()); // PartitionId
        partitionInfo.add(partition.getName()); // PartitionName
        partitionInfo.add(physicalPartition.getVisibleVersion()); // VisibleVersion
        partitionInfo.add(TimeUtils.longToTimeString(physicalPartition.getVisibleVersionTime())); // VisibleVersionTime
        partitionInfo.add(0); // VisibleVersionHash
        partitionInfo.add(partition.getState()); // State

        // partition key , range or list value
        partitionInfo.add(Joiner.on(", ").join(this.findPartitionColNames(tblPartitionInfo)));
        partitionInfo.add(this.findRangeOrListValues(tblPartitionInfo, partition.getId()));
        DistributionInfo distributionInfo = partition.getDistributionInfo();
        partitionInfo.add(distributionKeyAsString(distributionInfo));
        partitionInfo.add(distributionInfo.getBucketNum());

        short replicationNum = tblPartitionInfo.getReplicationNum(partition.getId());
        partitionInfo.add(String.valueOf(replicationNum));

        long dataSize = physicalPartition.storageDataSize();
        ByteSizeValue byteSizeValue = new ByteSizeValue(dataSize);
        DataProperty dataProperty = tblPartitionInfo.getDataProperty(partition.getId());
        partitionInfo.add(dataProperty.getStorageMedium().name());
        partitionInfo.add(TimeUtils.longToTimeString(dataProperty.getCooldownTimeMs()));
        partitionInfo.add(TimeUtils.longToTimeString(partition.getLastCheckTime()));
        partitionInfo.add(byteSizeValue);
        partitionInfo.add(tblPartitionInfo.getIsInMemory(partition.getId()));
        partitionInfo.add(physicalPartition.storageRowCount());

        return partitionInfo;
    }

    private List<Comparable> getLakePartitionInfo(PartitionInfo tblPartitionInfo, Partition partition,
                                                  PhysicalPartition physicalPartition) {
        PartitionIdentifier identifier = new PartitionIdentifier(db.getId(), table.getId(), physicalPartition.getId());
        PartitionStatistics statistics = GlobalStateMgr.getCurrentState().getCompactionMgr().getStatistics(identifier);
        Quantiles compactionScore = statistics != null ? statistics.getCompactionScore() : null;
        DataCacheInfo cacheInfo = tblPartitionInfo.getDataCacheInfo(partition.getId());
        List<Comparable> partitionInfo = new ArrayList<Comparable>();

        partitionInfo.add(physicalPartition.getId()); // PartitionId
        partitionInfo.add(partition.getName()); // PartitionName
        partitionInfo.add(statistics != null ? statistics.getCompactionVersion().getVersion() : 0); // CompactVersion
        partitionInfo.add(physicalPartition.getVisibleVersion()); // VisibleVersion
        partitionInfo.add(physicalPartition.getNextVersion()); // NextVersion
        partitionInfo.add(partition.getState()); // State
        partitionInfo.add(Joiner.on(", ").join(this.findPartitionColNames(tblPartitionInfo))); // PartitionKey
        partitionInfo.add(this.findRangeOrListValues(tblPartitionInfo, partition.getId())); // List or Range
        partitionInfo.add(distributionKeyAsString(partition.getDistributionInfo())); // DistributionKey
        partitionInfo.add(partition.getDistributionInfo().getBucketNum()); // Buckets
        partitionInfo.add(new ByteSizeValue(physicalPartition.storageDataSize())); // DataSize
        partitionInfo.add(physicalPartition.storageRowCount()); // RowCount
        partitionInfo.add(cacheInfo.isEnabled()); // EnableCache
        partitionInfo.add(cacheInfo.isAsyncWriteBack()); // AsyncWrite
        partitionInfo.add(String.format("%.2f", compactionScore != null ? compactionScore.getAvg() : 0.0)); // AvgCS
        partitionInfo.add(String.format("%.2f", compactionScore != null ? compactionScore.getP50() : 0.0)); // P50CS
        partitionInfo.add(String.format("%.2f", compactionScore != null ? compactionScore.getMax() : 0.0)); // MaxCS
        return partitionInfo;
    }

    private List<String> findPartitionColNames(PartitionInfo partitionInfo) {
        List<Column> partitionColumns;
        if (this.partitionType == PartitionType.LIST) {
            partitionColumns = ((ListPartitionInfo) partitionInfo).getPartitionColumns();
        } else if (partitionInfo.isRangePartition()) {
            partitionColumns = ((RangePartitionInfo) partitionInfo).getPartitionColumns();
        } else {
            partitionColumns = new ArrayList<>();
        }
        return partitionColumns.stream().map(Column::getName).collect(Collectors.toList());
    }

    private String findRangeOrListValues(PartitionInfo partitionInfo, long partitionId) {
        if (this.partitionType == PartitionType.LIST) {
            return ((ListPartitionInfo) partitionInfo).getValuesFormat(partitionId);
        }
        if (partitionInfo.isRangePartition()) {
            return ((RangePartitionInfo) partitionInfo).getRange(partitionId).toString();
        }
        return "";
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        List<List<Comparable>> partitionInfos = getPartitionInfos();
        return getBasicProcResult(partitionInfos);
    }

    @Override
    public boolean register(String name, ProcNodeInterface node) {
        return false;
    }

    @Override
    public ProcNodeInterface lookup(String partitionIdOrName) throws AnalysisException {
        long partitionId = -1L;

        Locker locker = new Locker();
        locker.lockDatabase(db, LockType.READ);
        try {
            PhysicalPartition partition;
            try {
                partition = table.getPhysicalPartition(Long.parseLong(partitionIdOrName));
            } catch (NumberFormatException e) {
                partition = table.getPartition(partitionIdOrName, false);
                if (partition == null) {
                    partition = table.getPartition(partitionIdOrName, true);
                }
            }

            if (partition == null) {
                throw new AnalysisException("Unknown partition id or name \"" + partitionIdOrName + "\"");
            }

            return new IndicesProcDir(db, table, partition);
        } finally {
            locker.unLockDatabase(db, LockType.READ);
        }
    }

    public int analyzeColumn(String columnName) {
        for (int i = 0; i < this.titleNames.size(); ++i) {
            if (this.titleNames.get(i).equalsIgnoreCase(columnName)) {
                return i;
            }
        }
        ErrorReport.reportSemanticException(ErrorCode.ERR_WRONG_COLUMN_NAME, columnName);
        return -1;
    }
}
