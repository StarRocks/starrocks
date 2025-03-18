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
package com.starrocks.clone;

import com.google.api.client.util.Preconditions;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.ListPartitionInfo;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.common.Pair;
import com.starrocks.common.util.concurrent.lock.AutoCloseableLock;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AlterTableClauseAnalyzer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.DropPartitionClause;
import com.starrocks.sql.optimizer.rule.transformation.partition.PartitionSelector;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.threeten.extra.PeriodDuration;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.catalog.TableProperty.INVALID;

public class PartitionTTLScheduler {
    private static final Logger LOG = LogManager.getLogger(PartitionTTLScheduler.class);

    // (DbId, TableId) for a collection of objects marked with partition_ttl_number > 0 on the table
    private final Set<Pair<Long, Long>> ttlPartitionInfo = Sets.newConcurrentHashSet();
    private final SchedulerRuntimeInfoCollector runtimeInfoCollector;

    PartitionTTLScheduler(SchedulerRuntimeInfoCollector runtimeInfoCollector) {
        this.runtimeInfoCollector = runtimeInfoCollector;
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

    public void scheduleTTLPartition() {
        Iterator<Pair<Long, Long>> iterator = ttlPartitionInfo.iterator();
        while (iterator.hasNext()) {
            Pair<Long, Long> tableInfo = iterator.next();
            Long dbId = tableInfo.first;
            Long tableId = tableInfo.second;

            // check if db exists
            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
            if (db == null) {
                iterator.remove();
                LOG.warn("Could not get database={} info. remove it from scheduler", dbId);
                continue;
            }
            // check if table exists
            Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getId(), tableId);
            if (!(table instanceof OlapTable)) {
                iterator.remove();
                LOG.warn("database={}-{}, table={}. is not olap table. remove it from scheduler",
                        db.getFullName(), dbId, tableId);
                continue;
            }
            OlapTable olapTable = (OlapTable) table;
            PartitionInfo partitionInfo = olapTable.getPartitionInfo();

            // check ttl is valid, if not, remove it from scheduler
            if (!isValidRangePartitionTTL(db, olapTable, partitionInfo)) {
                iterator.remove();
                continue;
            }

            // get expired partition names
            List<String> dropPartitionNames = getExpiredPartitionNames(db, olapTable, partitionInfo);
            if (CollectionUtils.isEmpty(dropPartitionNames)) {
                continue;
            }
            LOG.info("database={}, table={} has ttl partitions to drop: {}", db.getOriginName(), olapTable.getName(),
                    dropPartitionNames);

            // do drop partitions
            String tableName = olapTable.getName();
            List<DropPartitionClause> dropPartitionClauses = buildDropPartitionClauses(dropPartitionNames);
            for (DropPartitionClause dropPartitionClause : dropPartitionClauses) {
                try (AutoCloseableLock ignore = new AutoCloseableLock(
                        new Locker(), db.getId(), Lists.newArrayList(olapTable.getId()), LockType.WRITE)) {
                    AlterTableClauseAnalyzer analyzer = new AlterTableClauseAnalyzer(olapTable);
                    analyzer.analyze(new ConnectContext(), dropPartitionClause);
                    GlobalStateMgr.getCurrentState().getLocalMetastore().dropPartition(db, olapTable, dropPartitionClause);
                    runtimeInfoCollector.clearDropPartitionFailedMsg(tableName);
                } catch (DdlException e) {
                    runtimeInfoCollector.recordDropPartitionFailedMsg(db.getOriginName(), tableName, e.getMessage());
                }
            }
        }
    }

    private boolean isValidRangePartitionTTL(Database db,
                                             OlapTable olapTable,
                                             PartitionInfo partitionInfo) {
        long dbId = db.getId();
        long tableId = olapTable.getId();
        String ttlCondition = olapTable.getTableProperty().getPartitionRetentionCondition();
        if (partitionInfo instanceof RangePartitionInfo) {
            RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) partitionInfo;
            if (rangePartitionInfo.getPartitionColumnsSize() != 1) {
                LOG.warn("currently only support partition with single column. " +
                        "remove database={}, table={} from scheduler", dbId, tableId);
                return false;
            }

            int ttlNumber = olapTable.getTableProperty().getPartitionTTLNumber();
            PeriodDuration ttlDuration = olapTable.getTableProperty().getPartitionTTL();
            if (Objects.equals(ttlNumber, INVALID) && ttlDuration.isZero() && Strings.isNullOrEmpty(ttlCondition)) {
                LOG.warn("database={}, table={} have no ttl. remove it from scheduler", dbId, tableId);
                return false;
            }
        } else if (partitionInfo instanceof ListPartitionInfo) {
            if (Strings.isNullOrEmpty(ttlCondition)) {
                LOG.warn("database={}, table={} have no retention condition. remove it from scheduler", dbId, tableId);
                return false;
            }
        }
        return true;
    }

    private List<String> getExpiredPartitionNames(Database db,
                                                  OlapTable olapTable,
                                                  PartitionInfo partitionInfo) {
        long dbId = db.getId();
        long tableId = olapTable.getId();
        List<String> dropPartitionNames = null;
        try {
            String ttlCondition = olapTable.getTableProperty().getPartitionRetentionCondition();
            if (partitionInfo instanceof RangePartitionInfo) {
                int ttlNumber = olapTable.getTableProperty().getPartitionTTLNumber();
                PeriodDuration ttlDuration = olapTable.getTableProperty().getPartitionTTL();
                // prefer ttlCondition first
                if (!Strings.isNullOrEmpty(ttlCondition)) {
                    dropPartitionNames = PartitionSelector.getExpiredPartitionsByRetentionCondition(db, olapTable, ttlCondition);
                } else if (!ttlDuration.isZero()) {
                    dropPartitionNames = buildDropPartitionClauseByTTLDuration(olapTable, ttlDuration);
                } else if (ttlNumber != INVALID) {
                    dropPartitionNames = buildDropPartitionClauseByTTLNumber(olapTable, ttlNumber);
                }
            } else if (partitionInfo instanceof ListPartitionInfo) {
                if (!Strings.isNullOrEmpty(ttlCondition)) {
                    dropPartitionNames = PartitionSelector.getExpiredPartitionsByRetentionCondition(db, olapTable, ttlCondition);
                }
            }
        } catch (AnalysisException e) {
            LOG.warn("database={}-{}, table={}-{} failed to build drop partition statement.",
                    db.getFullName(), dbId, olapTable.getName(), tableId, e);
        }
        return dropPartitionNames;
    }

    /**
     * Build drop partitions by TTL.
     * Drop the partition if partition upper endpoint less than TTL lower bound
     */
    private List<String> buildDropPartitionClauseByTTLDuration(OlapTable olapTable,
                                                               PeriodDuration ttlDuration) throws AnalysisException {
        if (ttlDuration.isZero()) {
            return Lists.newArrayList();
        }
        List<String> dropPartitionNames = Lists.newArrayList();
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
                dropPartitionNames.add(olapTable.getPartition(partitionId).getName());
            }
        }
        return dropPartitionNames;
    }

    private List<DropPartitionClause> buildDropPartitionClauses(List<String> dropPartitionNames) {
        if (CollectionUtils.isEmpty(dropPartitionNames)) {
            return Lists.newArrayList();
        }
        return dropPartitionNames.stream()
                .map(dropPartitionName ->
                        new DropPartitionClause(false, dropPartitionName, false, true))
                .collect(Collectors.toList());
    }

    private List<String> buildDropPartitionClauseByTTLNumber(OlapTable olapTable, int ttlNumber) throws AnalysisException {
        List<String> dropPartitionNames = Lists.newArrayList();
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
                    dropPartitionNames.add(dropPartitionName);
                }
            }
        }
        return dropPartitionNames;
    }
}
