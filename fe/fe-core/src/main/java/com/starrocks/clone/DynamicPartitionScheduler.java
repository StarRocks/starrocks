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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/clone/DynamicPartitionScheduler.java

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

package com.starrocks.clone;

import com.google.api.client.util.Preconditions;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.starrocks.analysis.TimestampArithmeticExpr;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.DynamicPartitionProperty;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.RandomDistributionInfo;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.common.util.DynamicPartitionUtil;
import com.starrocks.common.util.FrontendDaemon;
import com.starrocks.common.util.RangeUtils;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.common.util.Util;
import com.starrocks.common.util.concurrent.lock.AutoCloseableLock;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.analyzer.AlterTableClauseAnalyzer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AddPartitionClause;
import com.starrocks.sql.ast.DistributionDesc;
import com.starrocks.sql.ast.DropPartitionClause;
import com.starrocks.sql.ast.HashDistributionDesc;
import com.starrocks.sql.ast.PartitionKeyDesc;
import com.starrocks.sql.ast.PartitionValue;
import com.starrocks.sql.ast.RandomDistributionDesc;
import com.starrocks.sql.ast.SingleRangePartitionDesc;
import com.starrocks.sql.common.MetaUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;
import org.threeten.extra.PeriodDuration;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static com.starrocks.catalog.TableProperty.INVALID;

/**
 * This class is used to periodically add or drop partition on an olapTable which specify dynamic partition properties
 * Config.dynamic_partition_enable determine whether this feature is enable, Config.dynamic_partition_check_interval_seconds
 * determine how often the task is performed
 */
public class DynamicPartitionScheduler extends FrontendDaemon {
    private static final Logger LOG = LogManager.getLogger(DynamicPartitionScheduler.class);
    public static final String LAST_SCHEDULER_TIME = "lastSchedulerTime";
    public static final String LAST_UPDATE_TIME = "lastUpdateTime";
    public static final String DYNAMIC_PARTITION_STATE = "dynamicPartitionState";
    public static final String CREATE_PARTITION_MSG = "createPartitionMsg";
    public static final String DROP_PARTITION_MSG = "dropPartitionMsg";

    private static final String DEFAULT_RUNTIME_VALUE = FeConstants.NULL_STRING;

    // runtime information for dynamic partitions key -> <tableName -> value>
    private final Map<String, Map<String, String>> runtimeInfos = Maps.newConcurrentMap();
    // (DbId, TableId) for a collection of objects marked with "dynamic_partition.enable" = "true" on the table
    private final Set<Pair<Long, Long>> dynamicPartitionTableInfo = Sets.newConcurrentHashSet();
    // (DbId, TableId) for a collection of objects marked with partition_ttl_number > 0 on the table
    private final Set<Pair<Long, Long>> ttlPartitionInfo = Sets.newConcurrentHashSet();

    private long lastFindingTime = -1;

    public enum State {
        NORMAL, ERROR
    }

    public boolean isInScheduler(long dbId, long tableId) {
        return dynamicPartitionTableInfo.contains(new Pair<>(dbId, tableId));
    }

    public DynamicPartitionScheduler(String name, long intervalMs) {
        super(name, intervalMs);
    }

    public void registerDynamicPartitionTable(Long dbId, Long tableId) {
        dynamicPartitionTableInfo.add(new Pair<>(dbId, tableId));
    }

    public void removeDynamicPartitionTable(Long dbId, Long tableId) {
        dynamicPartitionTableInfo.remove(new Pair<>(dbId, tableId));
    }

    public void registerTtlPartitionTable(Long dbId, Long tableId) {
        ttlPartitionInfo.add(new Pair<>(dbId, tableId));
    }

    public void removeTtlPartitionTable(Long dbId, Long tableId) {
        ttlPartitionInfo.remove(new Pair<>(dbId, tableId));
    }

    @VisibleForTesting
    public Set<Pair<Long, Long>> getTtlPartitionInfo() {
        return ttlPartitionInfo;
    }

    public String getRuntimeInfo(String tableName, String key) {
        Map<String, String> tableRuntimeInfo = runtimeInfos.getOrDefault(tableName, createDefaultRuntimeInfo());
        return tableRuntimeInfo.getOrDefault(key, DEFAULT_RUNTIME_VALUE);
    }

    public void removeRuntimeInfo(String tableName) {
        runtimeInfos.remove(tableName);
    }

    public void createOrUpdateRuntimeInfo(String tableName, String key, String value) {
        Map<String, String> runtimeInfo = runtimeInfos.get(tableName);
        if (runtimeInfo == null) {
            runtimeInfo = createDefaultRuntimeInfo();
            runtimeInfo.put(key, value);
            runtimeInfos.put(tableName, runtimeInfo);
        } else {
            runtimeInfo.put(key, value);
        }
    }

    private Map<String, String> createDefaultRuntimeInfo() {
        Map<String, String> defaultRuntimeInfo = Maps.newConcurrentMap();
        defaultRuntimeInfo.put(LAST_UPDATE_TIME, DEFAULT_RUNTIME_VALUE);
        defaultRuntimeInfo.put(LAST_SCHEDULER_TIME, DEFAULT_RUNTIME_VALUE);
        defaultRuntimeInfo.put(DYNAMIC_PARTITION_STATE, State.NORMAL.toString());
        defaultRuntimeInfo.put(CREATE_PARTITION_MSG, DEFAULT_RUNTIME_VALUE);
        defaultRuntimeInfo.put(DROP_PARTITION_MSG, DEFAULT_RUNTIME_VALUE);
        return defaultRuntimeInfo;
    }

    private ArrayList<AddPartitionClause> getAddPartitionClause(Database db, OlapTable olapTable,
                                                                Column partitionColumn, String partitionFormat) {
        ArrayList<AddPartitionClause> addPartitionClauses = new ArrayList<>();
        DynamicPartitionProperty dynamicPartitionProperty = olapTable.getTableProperty().getDynamicPartitionProperty();
        RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) olapTable.getPartitionInfo();
        ZonedDateTime now = ZonedDateTime.now(dynamicPartitionProperty.getTimeZone().toZoneId());

        int idx;
        int start = dynamicPartitionProperty.getStart();
        int historyPartitionNum = dynamicPartitionProperty.getHistoryPartitionNum();

        // start < 0 , historyPartitionNum >= 0
        idx = Math.max(start, -historyPartitionNum);

        for (; idx <= dynamicPartitionProperty.getEnd(); idx++) {
            String prevBorder =
                        DynamicPartitionUtil.getPartitionRangeString(dynamicPartitionProperty, now, idx, partitionFormat);
            String nextBorder = DynamicPartitionUtil.getPartitionRangeString(dynamicPartitionProperty, now, idx + 1,
                        partitionFormat);
            PartitionValue lowerValue = new PartitionValue(prevBorder);
            PartitionValue upperValue = new PartitionValue(nextBorder);

            boolean isPartitionExists = false;
            Range<PartitionKey> addPartitionKeyRange;
            try {
                PartitionKey lowerBound = PartitionKey.createPartitionKey(Collections.singletonList(lowerValue),
                            Collections.singletonList(partitionColumn));
                PartitionKey upperBound = PartitionKey.createPartitionKey(Collections.singletonList(upperValue),
                            Collections.singletonList(partitionColumn));
                addPartitionKeyRange = Range.closedOpen(lowerBound, upperBound);
            } catch (AnalysisException | IllegalArgumentException e) {
                // AnalysisException: keys.size is always equal to column.size, cannot reach this exception
                // IllegalArgumentException: lb is greater than ub
                LOG.warn("Error in gen addPartitionKeyRange. Error={}, db: {}, table: {}", e.getMessage(),
                            db.getOriginName(), olapTable.getName());
                continue;
            }

            for (Range<PartitionKey> partitionKeyRange : rangePartitionInfo.getIdToRange(false).values()) {
                // only support single column partition now
                try {
                    RangeUtils.checkRangeIntersect(partitionKeyRange, addPartitionKeyRange);
                } catch (DdlException e) {
                    /*
                     * If the old partition range for [(' 2022-08-01 00:00:00), (' 2022-09-01 00:00:00)), the range of
                     * the new partition for [(' 2022-08-29 00:00:00), (' 2022-09-05 00:00:00 ')), Is automatically cut
                     * out for the new partition range [(' 2022-09-01 00:00:00), (' 2022-09-05 00:00:00))
                     */
                    if (partitionKeyRange.contains(addPartitionKeyRange.lowerEndpoint()) &&
                                addPartitionKeyRange.contains(partitionKeyRange.upperEndpoint()) &&
                                !addPartitionKeyRange.upperEndpoint().equals(partitionKeyRange.upperEndpoint())) {
                        addPartitionKeyRange = Range.closedOpen(partitionKeyRange.upperEndpoint(),
                                    addPartitionKeyRange.upperEndpoint());
                        continue;
                    }
                    isPartitionExists = true;
                    if (addPartitionKeyRange.equals(partitionKeyRange)) {
                        clearCreatePartitionFailedMsg(olapTable.getName());
                    } else {
                        recordCreatePartitionFailedMsg(db.getOriginName(), olapTable.getName(), e.getMessage());
                    }
                    break;
                }
            }
            if (isPartitionExists) {
                continue;
            }

            // construct partition desc
            PartitionKeyDesc partitionKeyDesc =
                        new PartitionKeyDesc(Collections.singletonList(lowerValue), Collections.singletonList(upperValue));
            HashMap<String, String> partitionProperties = new HashMap<>(1);
            if (dynamicPartitionProperty.getReplicationNum() == DynamicPartitionProperty.NOT_SET_REPLICATION_NUM) {
                partitionProperties.put("replication_num", String.valueOf(olapTable.getDefaultReplicationNum()));
            } else {
                partitionProperties.put("replication_num",
                            String.valueOf(dynamicPartitionProperty.getReplicationNum()));
            }

            if (partitionColumn.getPrimitiveType() == PrimitiveType.DATE &&
                        dynamicPartitionProperty.getTimeUnit()
                                    .equalsIgnoreCase(TimestampArithmeticExpr.TimeUnit.HOUR.toString())) {
                throw new SemanticException("Date type partition does not support dynamic partitioning granularity of hour");
            }

            String partitionName = dynamicPartitionProperty.getPrefix() +
                        DynamicPartitionUtil.getFormattedPartitionName(dynamicPartitionProperty.getTimeZone(), prevBorder,
                                    dynamicPartitionProperty.getTimeUnit());
            SingleRangePartitionDesc rangePartitionDesc =
                        new SingleRangePartitionDesc(false, partitionName, partitionKeyDesc, partitionProperties);
            if (dynamicPartitionProperty.getBuckets() == 0) {
                addPartitionClauses.add(new AddPartitionClause(rangePartitionDesc, null, null, false));
            } else {
                // construct distribution desc
                DistributionDesc distributionDesc = createDistributionDesc(olapTable, dynamicPartitionProperty);
                // add partition according to partition desc and distribution desc
                addPartitionClauses.add(new AddPartitionClause(rangePartitionDesc, distributionDesc, null, false));
            }
        }
        return addPartitionClauses;
    }

    @Nullable
    private static DistributionDesc createDistributionDesc(OlapTable olapTable,
                                                           DynamicPartitionProperty dynamicPartitionProperty) {
        DistributionInfo distributionInfo = olapTable.getDefaultDistributionInfo();
        DistributionDesc distributionDesc = null;
        if (distributionInfo instanceof HashDistributionInfo) {
            HashDistributionInfo hashDistributionInfo = (HashDistributionInfo) distributionInfo;
            List<String> distColumnNames = MetaUtils.getColumnNamesByColumnIds(
                        olapTable.getIdToColumn(), hashDistributionInfo.getDistributionColumns());
            distributionDesc = new HashDistributionDesc(dynamicPartitionProperty.getBuckets(),
                        distColumnNames);
        } else if (distributionInfo instanceof RandomDistributionInfo) {
            distributionDesc = new RandomDistributionDesc(dynamicPartitionProperty.getBuckets());
        }
        return distributionDesc;
    }

    /**
     * 1. get the range of [start, 0) as a reserved range.
     * 2. get DropPartitionClause of partitions which range are before this reserved range.
     */
    private ArrayList<DropPartitionClause> getDropPartitionClause(Database db, OlapTable olapTable,
                                                                  Column partitionColumn, String partitionFormat) {
        ArrayList<DropPartitionClause> dropPartitionClauses = new ArrayList<>();
        DynamicPartitionProperty dynamicPartitionProperty = olapTable.getTableProperty().getDynamicPartitionProperty();
        if (dynamicPartitionProperty.getStart() == DynamicPartitionProperty.MIN_START_OFFSET) {
            // not set start offset, so not drop any partition
            return dropPartitionClauses;
        }

        ZonedDateTime now = ZonedDateTime.now(dynamicPartitionProperty.getTimeZone().toZoneId());
        String lowerBorder = DynamicPartitionUtil.getPartitionRangeString(dynamicPartitionProperty, now,
                    dynamicPartitionProperty.getStart(), partitionFormat);
        String upperBorder =
                    DynamicPartitionUtil.getPartitionRangeString(dynamicPartitionProperty, now, 0, partitionFormat);
        PartitionValue lowerPartitionValue = new PartitionValue(lowerBorder);
        PartitionValue upperPartitionValue = new PartitionValue(upperBorder);
        Range<PartitionKey> reservePartitionKeyRange;
        try {
            PartitionKey lowerBound = PartitionKey.createPartitionKey(Collections.singletonList(lowerPartitionValue),
                        Collections.singletonList(partitionColumn));
            PartitionKey upperBound = PartitionKey.createPartitionKey(Collections.singletonList(upperPartitionValue),
                        Collections.singletonList(partitionColumn));
            reservePartitionKeyRange = Range.closedOpen(lowerBound, upperBound);
        } catch (AnalysisException | IllegalArgumentException e) {
            // AnalysisException: keys.size is always equal to column.size, cannot reach this exception
            // IllegalArgumentException: lb is greater than ub
            LOG.warn("Error in gen reservePartitionKeyRange. Error={}, db: {}, table: {}", e.getMessage(),
                        db.getOriginName(), olapTable.getName());
            return dropPartitionClauses;
        }
        RangePartitionInfo info = (RangePartitionInfo) (olapTable.getPartitionInfo());

        List<Map.Entry<Long, Range<PartitionKey>>> idToRanges = new ArrayList<>(info.getIdToRange(false).entrySet());
        idToRanges.sort(Comparator.comparing(o -> o.getValue().upperEndpoint()));
        for (Map.Entry<Long, Range<PartitionKey>> idToRange : idToRanges) {
            try {
                Long checkDropPartitionId = idToRange.getKey();
                Range<PartitionKey> checkDropPartitionKey = idToRange.getValue();
                RangeUtils.checkRangeIntersect(reservePartitionKeyRange, checkDropPartitionKey);
                if (checkDropPartitionKey.upperEndpoint().compareTo(reservePartitionKeyRange.lowerEndpoint()) <= 0) {
                    String dropPartitionName = olapTable.getPartition(checkDropPartitionId).getName();
                    dropPartitionClauses.add(new DropPartitionClause(false, dropPartitionName, false, true));
                }
            } catch (DdlException e) {
                break;
            }
        }
        return dropPartitionClauses;
    }

    private void scheduleDynamicPartition() {
        Iterator<Pair<Long, Long>> iterator = dynamicPartitionTableInfo.iterator();
        while (iterator.hasNext()) {
            Pair<Long, Long> tableInfo = iterator.next();
            Long dbId = tableInfo.first;
            Long tableId = tableInfo.second;
            boolean shouldRemove = executeDynamicPartitionForTable(dbId, tableId);
            if (shouldRemove) {
                iterator.remove();
            }
        }
    }

    public boolean executeDynamicPartitionForTable(Long dbId, Long tableId) {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
        if (db == null) {
            LOG.warn("Automatically removes the schedule because database does not exist, dbId: {}", dbId);
            return true;
        }

        ArrayList<AddPartitionClause> addPartitionClauses = new ArrayList<>();
        ArrayList<DropPartitionClause> dropPartitionClauses;
        String tableName;
        boolean skipAddPartition = false;
        OlapTable olapTable;
        olapTable = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getId(), tableId);
        if (olapTable == null) {
            LOG.warn("Automatically removes the schedule because table does not exist, " +
                        "tableId: {}", tableId);
            return true;
        }
        // Only OlapTable has DynamicPartitionProperty
        try (AutoCloseableLock ignore =
                    new AutoCloseableLock(new Locker(), db.getId(), Lists.newArrayList(olapTable.getId()), LockType.READ)) {
            if (!olapTable.dynamicPartitionExists()) {
                LOG.warn("Automatically removes the schedule because " +
                            "table[{}] does not have dynamic partition", olapTable.getName());
                return true;
            }
            if (!olapTable.getTableProperty().getDynamicPartitionProperty().isEnabled()) {
                LOG.warn("Automatically removes the schedule because table[{}] " +
                            "does not enable dynamic partition", olapTable.getName());
                return true;
            }

            if (olapTable.getState() != OlapTable.OlapTableState.NORMAL) {
                String errorMsg = "Table[" + olapTable.getName() + "]'s state is not NORMAL." +
                            "Do not allow doing dynamic add partition. table state=" + olapTable.getState();
                recordCreatePartitionFailedMsg(db.getOriginName(), olapTable.getName(), errorMsg);
                skipAddPartition = true;
            }

            // Determine the partition column type
            // if column type is Date, format partition name as yyyyMMdd
            // if column type is DateTime, format partition name as yyyyMMddHHssmm
            // scheduler time should be record even no partition added
            createOrUpdateRuntimeInfo(olapTable.getName(), LAST_SCHEDULER_TIME, TimeUtils.getCurrentFormatTime());
            RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) olapTable.getPartitionInfo();
            if (rangePartitionInfo.getPartitionColumnsSize() != 1) {
                // currently only support partition with single column.
                LOG.warn("Automatically removes the schedule because " +
                            "table[{}] has more than one partition column", olapTable.getName());
                return true;
            }

            try {
                Column partitionColumn = rangePartitionInfo.getPartitionColumns(olapTable.getIdToColumn()).get(0);
                String partitionFormat = DynamicPartitionUtil.getPartitionFormat(partitionColumn);
                if (!skipAddPartition) {
                    addPartitionClauses = getAddPartitionClause(db, olapTable, partitionColumn, partitionFormat);
                }
                dropPartitionClauses = getDropPartitionClause(db, olapTable, partitionColumn, partitionFormat);
                tableName = olapTable.getName();
            } catch (Exception e) {
                LOG.warn("create or drop partition failed", e);
                recordCreatePartitionFailedMsg(db.getOriginName(), olapTable.getName(), e.getMessage());
                return false;
            }
        }

        WarehouseManager warehouseManager = GlobalStateMgr.getCurrentState().getWarehouseMgr();
        ConnectContext ctx = Util.getOrCreateConnectContext();
        ctx.setCurrentWarehouse(warehouseManager.getBackgroundWarehouse().getName());

        Locker locker = new Locker();
        for (DropPartitionClause dropPartitionClause : dropPartitionClauses) {
            if (!locker.lockDatabaseAndCheckExist(db, LockType.WRITE)) {
                LOG.warn("db: {}({}) has been dropped, skip", db.getFullName(), db.getId());
                return false;
            }
            try {
                AlterTableClauseAnalyzer analyzer = new AlterTableClauseAnalyzer(olapTable);
                analyzer.analyze(ctx, dropPartitionClause);

                GlobalStateMgr.getCurrentState().getLocalMetastore().dropPartition(db, olapTable, dropPartitionClause);
                clearDropPartitionFailedMsg(tableName);
            } catch (DdlException e) {
                recordDropPartitionFailedMsg(db.getOriginName(), tableName, e.getMessage());
            } finally {
                locker.unLockDatabase(db.getId(), LockType.WRITE);
            }
        }

        if (!skipAddPartition) {
            for (AddPartitionClause addPartitionClause : addPartitionClauses) {
                try {
                    AlterTableClauseAnalyzer alterTableClauseVisitor = new AlterTableClauseAnalyzer(olapTable);
                    alterTableClauseVisitor.analyze(ctx, addPartitionClause);

                    GlobalStateMgr.getCurrentState().getLocalMetastore().addPartitions(ctx,
                                db, tableName, addPartitionClause);
                    clearCreatePartitionFailedMsg(tableName);
                } catch (DdlException e) {
                    recordCreatePartitionFailedMsg(db.getOriginName(), tableName, e.getMessage());
                }
            }
        }
        return false;
    }

    private void scheduleTTLPartition() {
        Iterator<Pair<Long, Long>> iterator = ttlPartitionInfo.iterator();
        while (iterator.hasNext()) {
            Pair<Long, Long> tableInfo = iterator.next();
            Long dbId = tableInfo.first;
            Long tableId = tableInfo.second;
            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
            if (db == null) {
                iterator.remove();
                LOG.warn("Could not get database={} info. remove it from scheduler", dbId);
                continue;
            }
            Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getId(), tableId);
            OlapTable olapTable;
            if (table instanceof OlapTable) {
                olapTable = (OlapTable) table;
            } else {
                iterator.remove();
                LOG.warn("database={}-{}, table={}. is not olap table. remove it from scheduler",
                            db.getFullName(), dbId, tableId);
                continue;
            }

            PartitionInfo partitionInfo = olapTable.getPartitionInfo();
            RangePartitionInfo rangePartitionInfo;
            if (partitionInfo instanceof RangePartitionInfo) {
                rangePartitionInfo = (RangePartitionInfo) olapTable.getPartitionInfo();
            } else {
                LOG.warn("currently only support range partition." +
                            "remove database={}, table={} from scheduler", dbId, tableId);
                continue;
            }

            if (rangePartitionInfo.getPartitionColumnsSize() != 1) {
                iterator.remove();
                LOG.warn("currently only support partition with single column. " +
                            "remove database={}, table={} from scheduler", dbId, tableId);
                continue;
            }

            int ttlNumber = olapTable.getTableProperty().getPartitionTTLNumber();
            PeriodDuration ttlDuration = olapTable.getTableProperty().getPartitionTTL();
            if (Objects.equals(ttlNumber, INVALID) && ttlDuration.isZero()) {
                iterator.remove();
                LOG.warn("database={}, table={} have no ttl. remove it from scheduler", dbId, tableId);
                continue;
            }

            ArrayList<DropPartitionClause> dropPartitionClauses = null;
            try {
                if (!ttlDuration.isZero()) {
                    dropPartitionClauses = buildDropPartitionClauseByTTLDuration(olapTable, ttlDuration);
                } else {
                    dropPartitionClauses = buildDropPartitionClauseByTTLNumber(olapTable, ttlNumber);
                }
            } catch (AnalysisException e) {
                LOG.warn("database={}-{}, table={}-{} failed to build drop partition statement.",
                            db.getFullName(), dbId, table.getName(), tableId, e);
            }
            if (dropPartitionClauses == null) {
                continue;
            }

            String tableName = olapTable.getName();
            for (DropPartitionClause dropPartitionClause : dropPartitionClauses) {
                try (AutoCloseableLock ignore = new AutoCloseableLock(
                            new Locker(), db.getId(), Lists.newArrayList(olapTable.getId()), LockType.WRITE)) {
                    AlterTableClauseAnalyzer analyzer = new AlterTableClauseAnalyzer(olapTable);
                    analyzer.analyze(new ConnectContext(), dropPartitionClause);
                    GlobalStateMgr.getCurrentState().getLocalMetastore().dropPartition(db, olapTable, dropPartitionClause);
                    clearDropPartitionFailedMsg(tableName);
                } catch (DdlException e) {
                    recordDropPartitionFailedMsg(db.getOriginName(), tableName, e.getMessage());
                }
            }
        }
    }

    /**
     * Build drop partitions by TTL.
     * Drop the partition if partition upper endpoint less than TTL lower bound
     */
    private ArrayList<DropPartitionClause> buildDropPartitionClauseByTTLDuration(OlapTable olapTable,
                                                                                 PeriodDuration ttlDuration)
                throws AnalysisException {
        if (ttlDuration.isZero()) {
            return Lists.newArrayList();
        }
        ArrayList<DropPartitionClause> dropPartitionClauses = new ArrayList<>();
        RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) (olapTable.getPartitionInfo());
        List<Column> partitionColumns = rangePartitionInfo.getPartitionColumns(olapTable.getIdToColumn());
        Preconditions.checkArgument(partitionColumns.size() == 1);
        Type partitionType = partitionColumns.get(0).getType();
        PartitionKey ttlLowerBound;

        LocalDateTime ttlTime = LocalDateTime.now().minus(ttlDuration);
        if (partitionType.isDatetime()) {
            ttlLowerBound = PartitionKey.ofDateTime(ttlTime);
        } else if (partitionType.isDate()) {
            ttlLowerBound = PartitionKey.ofDate(ttlTime.toLocalDate());
        } else {
            throw new SemanticException("partition_ttl not support partition type: " + partitionType);
        }

        PartitionKey shadowPartitionKey = PartitionKey.createShadowPartitionKey(partitionColumns);

        Map<Long, Range<PartitionKey>> idToRange = rangePartitionInfo.getIdToRange(false);
        for (Map.Entry<Long, Range<PartitionKey>> partitionRange : idToRange.entrySet()) {
            PartitionKey left = partitionRange.getValue().lowerEndpoint();
            if (left.compareTo(shadowPartitionKey) == 0) {
                continue;
            }

            PartitionKey right = partitionRange.getValue().upperEndpoint();
            if (right.compareTo(ttlLowerBound) <= 0) {
                long partitionId = partitionRange.getKey();
                String dropPartitionName = olapTable.getPartition(partitionId).getName();
                dropPartitionClauses.add(new DropPartitionClause(true, dropPartitionName, false, true));
            }
        }
        return dropPartitionClauses;
    }

    private ArrayList<DropPartitionClause> buildDropPartitionClauseByTTLNumber(OlapTable olapTable, int ttlNumber)
                throws AnalysisException {
        ArrayList<DropPartitionClause> dropPartitionClauses = new ArrayList<>();
        RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) (olapTable.getPartitionInfo());
        List<Column> partitionColumns = rangePartitionInfo.getPartitionColumns(olapTable.getIdToColumn());

        // Currently, materialized views and automatically created partition tables
        // only support single-column partitioning.
        Preconditions.checkArgument(partitionColumns.size() == 1);
        Type partitionType = partitionColumns.get(0).getType();
        List<Map.Entry<Long, Range<PartitionKey>>> candidatePartitionList = Lists.newArrayList();

        if (partitionType.isDateType()) {
            PartitionKey currentPartitionKey = partitionType.isDatetime() ?
                        PartitionKey.ofDateTime(LocalDateTime.now()) : PartitionKey.ofDate(LocalDate.now());
            // For expr partitioning table, always has a shadow partition, we should avoid deleting it.
            PartitionKey shadowPartitionKey = PartitionKey.createShadowPartitionKey(partitionColumns);

            Map<Long, Range<PartitionKey>> idToRange = rangePartitionInfo.getIdToRange(false);
            for (Map.Entry<Long, Range<PartitionKey>> partitionRange : idToRange.entrySet()) {
                PartitionKey lowerPartitionKey = partitionRange.getValue().lowerEndpoint();

                if (lowerPartitionKey.compareTo(shadowPartitionKey) == 0) {
                    continue;
                }

                if (lowerPartitionKey.compareTo(currentPartitionKey) <= 0) {
                    candidatePartitionList.add(partitionRange);
                }
            }
        } else if (partitionType.isNumericType()) {
            candidatePartitionList = new ArrayList<>(rangePartitionInfo.getIdToRange(false).entrySet());
        } else {
            throw new AnalysisException("Partition ttl does not support type:" + partitionType);
        }

        candidatePartitionList.sort(Comparator.comparing(o -> o.getValue().upperEndpoint()));

        int allPartitionNumber = candidatePartitionList.size();
        if (allPartitionNumber > ttlNumber) {
            int dropSize = allPartitionNumber - ttlNumber;
            for (int i = 0; i < dropSize; i++) {
                Long checkDropPartitionId = candidatePartitionList.get(i).getKey();
                Partition partition = olapTable.getPartition(checkDropPartitionId);
                if (partition != null) {
                    String dropPartitionName = partition.getName();
                    dropPartitionClauses.add(new DropPartitionClause(false, dropPartitionName,
                                false, true));
                }
            }
        }
        return dropPartitionClauses;
    }

    private void recordCreatePartitionFailedMsg(String dbName, String tableName, String msg) {
        LOG.warn("dynamic add partition failed: {}, db: {}, table: {}", msg, dbName, tableName);
        createOrUpdateRuntimeInfo(tableName, DYNAMIC_PARTITION_STATE, State.ERROR.toString());
        createOrUpdateRuntimeInfo(tableName, CREATE_PARTITION_MSG, msg);
    }

    private void clearCreatePartitionFailedMsg(String tableName) {
        createOrUpdateRuntimeInfo(tableName, DYNAMIC_PARTITION_STATE, State.NORMAL.toString());
        createOrUpdateRuntimeInfo(tableName, CREATE_PARTITION_MSG, DEFAULT_RUNTIME_VALUE);
    }

    private void recordDropPartitionFailedMsg(String dbName, String tableName, String msg) {
        LOG.warn("dynamic drop partition failed: {}, db: {}, table: {}", msg, dbName, tableName);
        createOrUpdateRuntimeInfo(tableName, DYNAMIC_PARTITION_STATE, State.ERROR.toString());
        createOrUpdateRuntimeInfo(tableName, DROP_PARTITION_MSG, msg);
    }

    private void clearDropPartitionFailedMsg(String tableName) {
        createOrUpdateRuntimeInfo(tableName, DYNAMIC_PARTITION_STATE, State.NORMAL.toString());
        createOrUpdateRuntimeInfo(tableName, DROP_PARTITION_MSG, DEFAULT_RUNTIME_VALUE);
    }

    private void findSchedulableTables() {
        Map<String, List<String>> dynamicPartitionTables = new HashMap<>();
        Map<String, List<String>> ttlPartitionTables = new HashMap<>();
        long start = System.currentTimeMillis();
        for (Long dbId : GlobalStateMgr.getCurrentState().getLocalMetastore().getDbIds()) {
            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
            if (db == null) {
                continue;
            }
            if (db.isSystemDatabase() || db.isStatisticsDatabase()) {
                continue;
            }

            Locker locker = new Locker();
            locker.lockDatabase(db.getId(), LockType.READ);
            try {
                for (Table table : GlobalStateMgr.getCurrentState().getLocalMetastore().getTables(dbId)) {
                    if (DynamicPartitionUtil.isDynamicPartitionTable(table)) {
                        registerDynamicPartitionTable(db.getId(), table.getId());
                        dynamicPartitionTables.computeIfAbsent(db.getFullName(), k -> new ArrayList<>())
                                    .add(table.getName());
                    } else if (DynamicPartitionUtil.isTTLPartitionTable(table)) {
                        // Table(MV) with dynamic partition enabled should not specify partition_ttl_number(MV) or
                        // partition_live_number property.
                        registerTtlPartitionTable(db.getId(), table.getId());
                        ttlPartitionTables.computeIfAbsent(db.getFullName(), k -> new ArrayList<>())
                                    .add(table.getName());
                    }
                }
            } finally {
                locker.unLockDatabase(db.getId(), LockType.READ);
            }
        }
        LOG.info("finished to find all schedulable tables, cost: {}ms, dynamic partition tables: {}, " +
                                "ttl partition tables: {}, scheduler enabled: {}, scheduler interval: {}s",
                    System.currentTimeMillis() - start, dynamicPartitionTables, ttlPartitionTables,
                    Config.dynamic_partition_enable, Config.dynamic_partition_check_interval_seconds);
        lastFindingTime = System.currentTimeMillis();
    }

    @VisibleForTesting
    public void runOnceForTest() {
        runAfterCatalogReady();
    }

    @Override
    protected void runAfterCatalogReady() {
        // Find all tables that need to be scheduled.
        long now = System.currentTimeMillis();
        if ((now - lastFindingTime) > Math.max(300000, Config.dynamic_partition_check_interval_seconds)) {
            findSchedulableTables();
        }

        // Update scheduler interval.
        setInterval(Config.dynamic_partition_check_interval_seconds * 1000L);

        // Schedule tables with dynamic partition enabled(only works for base table).
        if (Config.dynamic_partition_enable) {
            scheduleDynamicPartition();
        }

        // Schedule tables(mvs) with ttl partition enabled.
        // For now, partition_live_number works for base table with
        // single column range partitioning(including expr partitioning, e.g. ... partition by date_trunc('month', col).
        // partition_ttl_number and partition_ttl work for mv with
        // single column range partitioning(including expr partitioning).
        scheduleTTLPartition();
    }
}
