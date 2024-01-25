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

package com.starrocks.catalog;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.DescriptorTable.ReferencedPartitionInfo;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.SlotDescriptor;
import com.starrocks.analysis.SlotId;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TableName;
import com.starrocks.authentication.AuthenticationMgr;
import com.starrocks.backup.Status;
import com.starrocks.backup.mv.MvBackupInfo;
import com.starrocks.backup.mv.MvBaseTableBackupInfo;
import com.starrocks.backup.mv.MvRestoreContext;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.common.FeConstants;
import com.starrocks.common.MaterializedViewExceptions;
import com.starrocks.common.Pair;
import com.starrocks.common.UserException;
import com.starrocks.common.io.DeepCopy;
import com.starrocks.common.io.Text;
import com.starrocks.common.util.DateUtils;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.connector.ConnectorPartitionTraits;
import com.starrocks.connector.ConnectorTableInfo;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.persist.AlterMaterializedViewBaseTableInfosLog;
import com.starrocks.persist.gson.GsonPostProcessable;
import com.starrocks.persist.gson.GsonPreProcessable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.privilege.PrivilegeBuiltinConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.scheduler.TaskBuilder;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AnalyzeState;
import com.starrocks.sql.analyzer.ExpressionAnalyzer;
import com.starrocks.sql.analyzer.Field;
import com.starrocks.sql.analyzer.RelationFields;
import com.starrocks.sql.analyzer.RelationId;
import com.starrocks.sql.analyzer.Scope;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.sql.common.RangePartitionDiff;
import com.starrocks.sql.common.SyncPartitionUtils;
import com.starrocks.sql.common.UnsupportedException;
import com.starrocks.sql.optimizer.CachingMvPlanContextBuilder;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.statistic.StatsConstants;
import com.starrocks.thrift.TTableDescriptor;
import com.starrocks.thrift.TTableType;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.starrocks.backup.mv.MVRestoreUpdater.checkMvDefinedQuery;
import static com.starrocks.backup.mv.MVRestoreUpdater.restoreBaseTableInfoIfNoRestored;
import static com.starrocks.backup.mv.MVRestoreUpdater.restoreBaseTableInfoIfRestored;

/**
 * meta structure for materialized view
 */
public class MaterializedView extends OlapTable implements GsonPreProcessable, GsonPostProcessable {
    private static final Logger LOG = LogManager.getLogger(MaterializedView.class);

    public enum RefreshType {
        SYNC,
        ASYNC,
        MANUAL,
        INCREMENTAL
    }

    public enum RefreshMoment {
        IMMEDIATE,
        DEFERRED
    }

    @Override
    public boolean getUseFastSchemaEvolution() {
        return false;
    }

    @Override
    public void setUseFastSchemaEvolution(boolean useFastSchemaEvolution) {}

    public static class BasePartitionInfo {

        @SerializedName(value = "id")
        private final long id;

        @SerializedName(value = "version")
        private final long version;

        @SerializedName(value = "lastRefreshTime")
        private final long lastRefreshTime;

        public BasePartitionInfo(long id, long version, long lastRefreshTime) {
            this.id = id;
            this.version = version;
            this.lastRefreshTime = lastRefreshTime;
        }

        public static BasePartitionInfo fromExternalTable(com.starrocks.connector.PartitionInfo info) {
            // TODO: id and version
            return new BasePartitionInfo(-1, -1, info.getModifiedTime());
        }

        public static BasePartitionInfo fromOlapTable(Partition partition) {
            return new BasePartitionInfo(partition.getId(), partition.getVisibleVersion(), -1);
        }

        public long getId() {
            return id;
        }

        public long getVersion() {
            return version;
        }

        public long getLastRefreshTime() {
            return lastRefreshTime;
        }

        @Override
        public String toString() {
            return "BasePartitionInfo{" +
                    "id=" + id +
                    ", version=" + version +
                    ", lastRefreshTime=" + lastRefreshTime +
                    '}';
        }
    }

    public static class AsyncRefreshContext {
        // base table id -> (partition name -> partition info (id, version))
        // partition id maybe changed after insert overwrite, so use partition name as key.
        // partition id which in BasePartitionInfo can be used to check partition is changed
        @SerializedName("baseTableVisibleVersionMap")
        private final Map<Long, Map<String, BasePartitionInfo>> baseTableVisibleVersionMap;

        @SerializedName("baseTableInfoVisibleVersionMap")
        private final Map<BaseTableInfo, Map<String, BasePartitionInfo>> baseTableInfoVisibleVersionMap;

        @SerializedName(value = "defineStartTime")
        private boolean defineStartTime;

        @SerializedName(value = "starTime")
        private long startTime;

        @SerializedName(value = "step")
        private long step;

        @SerializedName(value = "timeUnit")
        private String timeUnit;

        public AsyncRefreshContext() {
            this.baseTableVisibleVersionMap = Maps.newConcurrentMap();
            this.baseTableInfoVisibleVersionMap = Maps.newConcurrentMap();
            this.defineStartTime = false;
            this.startTime = Utils.getLongFromDateTime(LocalDateTime.now());
            this.step = 0;
            this.timeUnit = null;
        }

        public Map<Long, Map<String, BasePartitionInfo>> getBaseTableVisibleVersionMap() {
            return baseTableVisibleVersionMap;
        }

        public Map<BaseTableInfo, Map<String, BasePartitionInfo>> getBaseTableInfoVisibleVersionMap() {
            return baseTableInfoVisibleVersionMap;
        }

        public Map<String, BasePartitionInfo> getBaseTableRefreshInfo(BaseTableInfo info) {
            return getBaseTableInfoVisibleVersionMap()
                    .computeIfAbsent(info, k -> Maps.newHashMap());
        }

        public void clearVisibleVersionMap() {
            this.baseTableInfoVisibleVersionMap.clear();
            this.baseTableVisibleVersionMap.clear();
        }

        public boolean isDefineStartTime() {
            return defineStartTime;
        }

        public void setDefineStartTime(boolean defineStartTime) {
            this.defineStartTime = defineStartTime;
        }

        public long getStartTime() {
            return startTime;
        }

        public void setStartTime(long startTime) {
            this.startTime = startTime;
        }

        public long getStep() {
            return step;
        }

        public void setStep(long step) {
            this.step = step;
        }

        public String getTimeUnit() {
            return timeUnit;
        }

        public void setTimeUnit(String timeUnit) {
            this.timeUnit = timeUnit;
        }

        @Override
        public String toString() {
            return "AsyncRefreshContext{" +
                    "baseTableVisibleVersionMap=" + baseTableVisibleVersionMap +
                    ", defineStartTime=" + defineStartTime +
                    ", startTime=" + startTime +
                    ", step=" + step +
                    ", timeUnit='" + timeUnit + '\'' +
                    '}';
        }

        public AsyncRefreshContext copy() {
            AsyncRefreshContext arc = new AsyncRefreshContext();
            arc.baseTableVisibleVersionMap.putAll(this.baseTableVisibleVersionMap);
            arc.baseTableInfoVisibleVersionMap.putAll(this.baseTableInfoVisibleVersionMap);
            arc.defineStartTime = this.defineStartTime;
            arc.startTime = this.startTime;
            arc.step = this.step;
            arc.timeUnit = this.timeUnit;
            return arc;
        }
    }

    public static class MvRefreshScheme {
        @SerializedName(value = "moment")
        private RefreshMoment moment;
        @SerializedName(value = "type")
        private RefreshType type;
        // when type is ASYNC
        // asyncRefreshContext is used to store refresh context
        @SerializedName(value = "asyncRefreshContext")
        private AsyncRefreshContext asyncRefreshContext;
        @SerializedName(value = "lastRefreshTime")
        private long lastRefreshTime;

        public MvRefreshScheme() {
            this.moment = RefreshMoment.IMMEDIATE;
            this.type = RefreshType.ASYNC;
            this.asyncRefreshContext = new AsyncRefreshContext();
            this.lastRefreshTime = 0;
        }

        public MvRefreshScheme(RefreshType type) {
            this.type = type;
            this.moment = RefreshMoment.IMMEDIATE;
            this.asyncRefreshContext = new AsyncRefreshContext();
            this.lastRefreshTime = 0;
        }

        public boolean isIncremental() {
            return this.type.equals(RefreshType.INCREMENTAL);
        }

        public boolean isSync() {
            return this.type.equals(RefreshType.SYNC);
        }

        public RefreshMoment getMoment() {
            return moment;
        }

        public void setMoment(RefreshMoment moment) {
            this.moment = moment;
        }

        public RefreshType getType() {
            return type;
        }

        public void setType(RefreshType type) {
            this.type = type;
        }

        public AsyncRefreshContext getAsyncRefreshContext() {
            return asyncRefreshContext;
        }

        public void setAsyncRefreshContext(AsyncRefreshContext asyncRefreshContext) {
            this.asyncRefreshContext = asyncRefreshContext;
        }

        public long getLastRefreshTime() {
            return lastRefreshTime;
        }

        public void setLastRefreshTime(long lastRefreshTime) {
            this.lastRefreshTime = lastRefreshTime;
        }

        public MvRefreshScheme copy() {
            MvRefreshScheme res = new MvRefreshScheme();
            res.type = this.type;
            res.lastRefreshTime = this.lastRefreshTime;
            if (this.asyncRefreshContext != null) {
                res.asyncRefreshContext = this.asyncRefreshContext.copy();
            }
            return res;
        }
    }

    @SerializedName(value = "dbId")
    private long dbId;

    @SerializedName(value = "refreshScheme")
    private MvRefreshScheme refreshScheme;

    @SerializedName(value = "baseTableIds")
    private Set<Long> baseTableIds;

    @SerializedName(value = "baseTableInfos")
    private List<BaseTableInfo> baseTableInfos;

    @SerializedName(value = "active")
    private boolean active;

    @SerializedName(value = "inactiveReason")
    private String inactiveReason;

    // TODO: now it is original definition sql
    // for show create mv, constructing refresh job(insert into select)
    @SerializedName(value = "viewDefineSql")
    private String viewDefineSql;

    @SerializedName(value = "simpleDefineSql")
    private String simpleDefineSql;

    // record expression table column
    @SerializedName(value = "partitionRefTableExprs")
    private List<GsonUtils.ExpressionSerializedObject> serializedPartitionRefTableExprs;
    private List<Expr> partitionRefTableExprs;

    // Maintenance plan for this MV
    private transient ExecPlan maintenancePlan;

    // NOTE: The `maxMVRewriteStaleness` option helps you achieve consistently high performance
    // with controlled costs when processing large, frequently changing datasets.
    //
    // MV's Refresh Time = max(BaseTables' Refresh Time)
    // MV's Staleness    = now() - MV's Refresh Time
    // If MV's Staleness <= maxMVRewriteStaleness: returns data directly from the MV without reading the base tables.
    // otherwise:  reads data from the base tables.
    @SerializedName(value = "maxMVRewriteStaleness")
    private int maxMVRewriteStaleness = 0;

    // Materialized view's output columns may be different from defined query's output columns.
    // Record the indexes based on materialized view's column output.
    // eg: create materialized view mv as select col1, col2, col3 from tbl
    //  desc mv             :  col2, col1, col3
    //  queryOutputIndexes  :  1, 0, 2
    // which means 0th of query output column is in 1th mv's output columns, and 1th -> 0th, 2th -> 2th.
    @SerializedName(value = "queryOutputIndices")
    protected List<Integer> queryOutputIndices = Lists.newArrayList();

    public MaterializedView() {
        super(TableType.MATERIALIZED_VIEW);
        this.tableProperty = null;
        this.state = OlapTableState.NORMAL;
        this.active = true;
    }

    public MaterializedView(long id, long dbId, String mvName, List<Column> baseSchema, KeysType keysType,
                            PartitionInfo partitionInfo, DistributionInfo defaultDistributionInfo,
                            MvRefreshScheme refreshScheme) {
        super(id, mvName, baseSchema, keysType, partitionInfo, defaultDistributionInfo,
                GlobalStateMgr.getCurrentState().getClusterId(), null, TableType.MATERIALIZED_VIEW);
        this.dbId = dbId;
        this.refreshScheme = refreshScheme;
        this.active = true;
    }

    // Used for sync mv
    public MaterializedView(Database db, String mvName,
                            MaterializedIndexMeta indexMeta, OlapTable baseTable,
                            PartitionInfo partitionInfo, DistributionInfo distributionInfo,
                            MvRefreshScheme refreshScheme) {
        this(indexMeta.getIndexId(), db.getId(), mvName, indexMeta.getSchema(), indexMeta.getKeysType(),
                partitionInfo, distributionInfo, refreshScheme);
        Preconditions.checkState(baseTable.getIndexIdByName(mvName) != null);
        long indexId = indexMeta.getIndexId();
        this.state = baseTable.state;
        this.baseIndexId = indexMeta.getIndexId();

        this.indexNameToId.put(baseTable.getIndexNameById(indexId), indexId);
        this.indexIdToMeta.put(indexId, indexMeta);

        this.baseTableInfos = Lists.newArrayList();
        this.baseTableInfos.add(
                new BaseTableInfo(db.getId(), db.getFullName(), baseTable.getName(), baseTable.getId()));

        Map<Long, Partition> idToPartitions = new HashMap<>(baseTable.idToPartition.size());
        Map<String, Partition> nameToPartitions = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        for (Map.Entry<Long, Partition> kv : baseTable.idToPartition.entrySet()) {
            // TODO: only copy mv's partition index.
            Partition copiedPartition = kv.getValue().shallowCopy();
            idToPartitions.put(kv.getKey(), copiedPartition);
            nameToPartitions.put(kv.getValue().getName(), copiedPartition);
        }
        this.idToPartition = idToPartitions;
        this.nameToPartition = nameToPartitions;
        if (baseTable.tableProperty != null) {
            this.tableProperty = baseTable.tableProperty.copy();
        }
    }

    public MvId getMvId() {
        return new MvId(getDbId(), id);
    }

    public long getDbId() {
        return dbId;
    }

    public void setDbId(long dbId) {
        this.dbId = dbId;
    }

    public boolean isActive() {
        return active;
    }

    /**
     * active the materialized again & reload the state.
     */
    public void setActive() {
        LOG.warn("set {} to active", name);
        this.active = true;
        this.inactiveReason = null;
        // reset mv rewrite cache when it is active again
        CachingMvPlanContextBuilder.getInstance().invalidateFromCache(this);
    }

    public void setInactiveAndReason(String reason) {
        LOG.warn("set {} to inactive because of {}", name, reason);
        this.active = false;
        this.inactiveReason = reason;
        CachingMvPlanContextBuilder.getInstance().invalidateFromCache(this);
    }

    public String getInactiveReason() {
        return inactiveReason;
    }

    public String getViewDefineSql() {
        return viewDefineSql;
    }

    public void setViewDefineSql(String viewDefineSql) {
        this.viewDefineSql = viewDefineSql;
    }

    public String getSimpleDefineSql() {
        return simpleDefineSql;
    }

    public void setSimpleDefineSql(String simple) {
        this.simpleDefineSql = simple;
    }

    public String getTaskDefinition() {
        return String.format("insert overwrite `%s` %s", getName(), getViewDefineSql());
    }

    public List<BaseTableInfo> getBaseTableInfos() {
        return baseTableInfos;
    }

    public void setBaseTableInfos(List<BaseTableInfo> baseTableInfos) {
        this.baseTableInfos = baseTableInfos;
    }

    public List<TableType> getBaseTableTypes() {
        if (baseTableInfos == null) {
            return Lists.newArrayList();
        }
        List<TableType> baseTableTypes = Lists.newArrayList();
        baseTableInfos.forEach(tableInfo -> baseTableTypes.add(tableInfo.getTableChecked().getType()));
        return baseTableTypes;
    }

    public void setPartitionRefTableExprs(List<Expr> partitionRefTableExprs) {
        this.partitionRefTableExprs = partitionRefTableExprs;
    }

    public List<Expr> getPartitionRefTableExprs() {
        return partitionRefTableExprs;
    }

    public MvRefreshScheme getRefreshScheme() {
        return refreshScheme;
    }

    public void setRefreshScheme(MvRefreshScheme refreshScheme) {
        this.refreshScheme = refreshScheme;
    }

    /**
     * @param base           : The base table of the materialized view to check the updated partition names
     * @param isQueryRewrite : Mark the caller is from query rewrite or not, when it's true we can use staleness to
     *                       optimize.
     * @return
     */
    public Set<String> getUpdatedPartitionNamesOfTable(Table base, boolean isQueryRewrite) {
        return getUpdatedPartitionNamesOfTable(base, false, isQueryRewrite, MaterializedView.getPartitionExpr(this));
    }

    public int getMaxMVRewriteStaleness() {
        return maxMVRewriteStaleness;
    }

    public void setMaxMVRewriteStaleness(int maxMVRewriteStaleness) {
        this.maxMVRewriteStaleness = maxMVRewriteStaleness;
    }

    public List<Integer> getQueryOutputIndices() {
        return queryOutputIndices;
    }

    public void setQueryOutputIndices(List<Integer> queryOutputIndices) {
        this.queryOutputIndices = queryOutputIndices;
    }

    /**
     * @param materializedView : materialized view to check
     * @return : return the column slot ref which materialized view's partition column comes from.
     * <p>
     * NOTE: Only support one column for Materialized View's partition column for now.
     */
    public static SlotRef getRefBaseTablePartitionSlotRef(MaterializedView materializedView) {
        List<SlotRef> slotRefs = Lists.newArrayList();
        Expr partitionExpr = materializedView.getFirstPartitionRefTableExpr();
        partitionExpr.collect(SlotRef.class, slotRefs);
        // if partitionExpr is FunctionCallExpr, get first SlotRef
        Preconditions.checkState(slotRefs.size() == 1);
        return slotRefs.get(0);
    }

    public Expr getFirstPartitionRefTableExpr() {
        if (partitionRefTableExprs == null) {
            return null;
        }
        if (partitionRefTableExprs.get(0).getType() == Type.INVALID) {
            ExpressionRangePartitionInfo expressionRangePartitionInfo = (ExpressionRangePartitionInfo) partitionInfo;
            partitionRefTableExprs.get(0).setType(expressionRangePartitionInfo.getPartitionExprs().get(0).getType());
        }
        return partitionRefTableExprs.get(0);
    }

    public static Expr getPartitionExpr(MaterializedView materializedView) {
        if (!(materializedView.getPartitionInfo() instanceof ExpressionRangePartitionInfo)) {
            return null;
        }
        ExpressionRangePartitionInfo expressionRangePartitionInfo =
                ((ExpressionRangePartitionInfo) materializedView.getPartitionInfo());
        // currently, mv only supports one expression
        Preconditions.checkState(expressionRangePartitionInfo.getPartitionExprs().size() == 1);
        return materializedView.getFirstPartitionRefTableExpr();
    }

    public Set<String> getUpdatedPartitionNamesOfOlapTable(OlapTable baseTable, boolean isQueryRewrite) {
        if (isQueryRewrite && isStalenessSatisfied()) {
            return Sets.newHashSet();
        }

        return ConnectorPartitionTraits.build(baseTable).getUpdatedPartitionNames(
                this.getBaseTableInfos(),
                this.getRefreshScheme().getAsyncRefreshContext());
    }

    /**
     * @return Return max timestamp of all table's max refresh timestamp
     * which is computed by checking all its partitions' modified time.
     */
    public Optional<Long> maxBaseTableRefreshTimestamp() {
        long maxRefreshTimestamp = -1;
        for (BaseTableInfo baseTableInfo : baseTableInfos) {
            Table baseTable = baseTableInfo.getTableChecked();

            if (baseTable instanceof View) {
                continue;
            } else if (baseTable instanceof MaterializedView) {
                MaterializedView mv = (MaterializedView) baseTable;
                if (!mv.isStalenessSatisfied()) {
                    return Optional.empty();
                }
            }
            Optional<Long> baseTableTs = ConnectorPartitionTraits.build(baseTable).maxPartitionRefreshTs();
            if (!baseTableTs.isPresent()) {
                return Optional.empty();
            }
            maxRefreshTimestamp = Math.max(maxRefreshTimestamp, baseTableTs.get());
        }
        return Optional.of(maxRefreshTimestamp);
    }

    public long getLastRefreshTime() {
        return refreshScheme.getLastRefreshTime();
    }

    /**
     * Check weather this materialized view's staleness is satisfied.
     *
     * @return
     */
    @VisibleForTesting
    public boolean isStalenessSatisfied() {
        if (this.maxMVRewriteStaleness <= 0) {
            return false;
        }
        // Define:
        //      MV's stalness = max of all base tables' refresh timestamp  - mv's refresh timestamp .
        // Check staleness by using all base tables' refresh timestamp and this mv's refresh timestamp,
        // if MV's staleness is greater than user's config `maxMVRewriteStaleness`:
        // we think this mv is outdated, otherwise we can use this mv to rewrite user's query.
        long mvRefreshTimestamp = getLastRefreshTime();
        Optional<Long> baseTableRefreshTimestampOpt = maxBaseTableRefreshTimestamp();
        // If we can not find the base table's refresh timestamp, just return false directly.
        if (!baseTableRefreshTimestampOpt.isPresent()) {
            return false;
        }

        long baseTableRefreshTimestamp = baseTableRefreshTimestampOpt.get();
        long mvStaleness = (baseTableRefreshTimestamp - mvRefreshTimestamp) / 1000;
        if (mvStaleness > this.maxMVRewriteStaleness) {
            ZoneId currentTimeZoneId = TimeUtils.getTimeZone().toZoneId();
            LOG.info("MV is outdated because MV's staleness {} (baseTables' lastRefreshTime {} - " +
                            "MV's lastRefreshTime {}) is greater than the staleness config {}",
                    DateUtils.formatTimeStampInMill(baseTableRefreshTimestamp, currentTimeZoneId),
                    DateUtils.formatTimeStampInMill(mvRefreshTimestamp, currentTimeZoneId),
                    mvStaleness,
                    maxMVRewriteStaleness);
            return false;
        }
        return true;
    }

    public Map<String, BasePartitionInfo> getBaseTableRefreshInfo(BaseTableInfo baseTable) {
        return getRefreshScheme()
                .getAsyncRefreshContext()
                .getBaseTableRefreshInfo(baseTable);
    }

    public List<BasePartitionInfo> getBaseTableLatestPartitionInfo(Table baseTable) {
        if (baseTable.isNativeTableOrMaterializedView()) {
            return baseTable.getPartitions().stream()
                    .map(BasePartitionInfo::fromOlapTable).collect(Collectors.toList());
        }

        return MapUtils.emptyIfNull(PartitionUtil.getPartitionNameWithPartitionInfo(baseTable)).values()
                .stream().map(BasePartitionInfo::fromExternalTable).collect(Collectors.toList());
    }

    private Set<String> getUpdatedPartitionNamesOfExternalTable(Table baseTable, boolean isQueryRewrite) {
        Set<String> result = Sets.newHashSet();
        // NOTE: For query dump replay, ignore updated partition infos only to check mv can rewrite query or not.
        // Ignore partitions when mv 's last refreshed time period is less than `maxMVRewriteStaleness`
        if (FeConstants.isReplayFromQueryDump || (isQueryRewrite && isStalenessSatisfied())) {
            return result;
        }

        return ConnectorPartitionTraits.build(baseTable).getUpdatedPartitionNames(
                this.getBaseTableInfos(),
                this.refreshScheme.getAsyncRefreshContext());
    }

    /**
     * Return base tables' updated partition names, if `withMv` is true check base tables recursively when the base
     * table is materialized view too.
     *
     * @param baseTable : materialized view's base table to be checked
     * @param withMv    : whether to check the base table recursively when it is also a mv.
     * @return
     */
    public Set<String> getUpdatedPartitionNamesOfTable(
            Table baseTable, boolean withMv, boolean isQueryRewrite, Expr partitionExpr) {
        if (baseTable.isView()) {
            return Sets.newHashSet();
        } else if (baseTable.isNativeTableOrMaterializedView()) {
            Set<String> result = Sets.newHashSet();
            OlapTable olapBaseTable = (OlapTable) baseTable;
            result.addAll(getUpdatedPartitionNamesOfOlapTable(olapBaseTable, isQueryRewrite));

            if (withMv && baseTable.isMaterializedView()) {
                ((MaterializedView) baseTable).getPartitionNamesToRefreshForMv(result, isQueryRewrite);
            }
            return result;
        } else {
            Set<String> updatePartitionNames = getUpdatedPartitionNamesOfExternalTable(baseTable, isQueryRewrite);
            Pair<Table, Column> partitionTableAndColumn = getBaseTableAndPartitionColumn();
            if (partitionTableAndColumn == null) {
                return updatePartitionNames;
            }
            if (!baseTable.getTableIdentifier().equals(partitionTableAndColumn.first.getTableIdentifier())) {
                return updatePartitionNames;
            }
            try {
                boolean isListPartition = partitionInfo instanceof ListPartitionInfo;
                return PartitionUtil.getMVPartitionName(baseTable, partitionTableAndColumn.second,
                        Lists.newArrayList(updatePartitionNames), isListPartition, partitionExpr);
            } catch (AnalysisException e) {
                LOG.warn("Mv {}'s base table {} get partition name fail", name, baseTable.name, e);
                return null;
            }
        }
    }

    @Override
    public TTableDescriptor toThrift(List<ReferencedPartitionInfo> partitions) {
        return new TTableDescriptor(id, TTableType.MATERIALIZED_VIEW,
                fullSchema.size(), 0, getName(), "");
    }

    @Override
    public void copyOnlyForQuery(OlapTable olapTable) {
        super.copyOnlyForQuery(olapTable);
        MaterializedView mv = (MaterializedView) olapTable;
        mv.dbId = this.dbId;
        mv.active = this.active;
        mv.refreshScheme = this.refreshScheme.copy();
        mv.maxMVRewriteStaleness = this.maxMVRewriteStaleness;
        mv.viewDefineSql = this.viewDefineSql;
        if (this.baseTableIds != null) {
            mv.baseTableIds = Sets.newHashSet(this.baseTableIds);
        }
        if (this.baseTableInfos != null) {
            mv.baseTableInfos = Lists.newArrayList(this.baseTableInfos);
        }
        if (this.partitionRefTableExprs != null) {
            mv.partitionRefTableExprs = Lists.newArrayList(this.partitionRefTableExprs);
        }
        if (!queryOutputIndices.isEmpty()) {
            mv.setQueryOutputIndices(Lists.newArrayList(queryOutputIndices));
        }
    }

    @Override
    public MaterializedView selectiveCopy(Collection<String> reservedPartitions, boolean resetState,
                                          MaterializedIndex.IndexExtState extState) {
        MaterializedView copied = DeepCopy.copyWithGson(this, MaterializedView.class);
        if (copied == null) {
            LOG.warn("failed to copy materialized view: " + getName());
            return null;
        }
        return ((MaterializedView) selectiveCopyInternal(copied, reservedPartitions, resetState, extState));
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // write type first
        Text.writeString(out, type.name());
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static MaterializedView read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, MaterializedView.class);
    }

    @Override
    public void onReload() {
        try {
            boolean desiredActive = active;
            active = false;
            boolean reloadActive = onReloadImpl();
            if (desiredActive && reloadActive) {
                setActive();
            }
        } catch (Throwable e) {
            LOG.error("reload mv failed: {}", this, e);
            setInactiveAndReason("reload failed: " + e.getMessage());
        }
    }

    /**
     * Try to fix relationship between base table and mv.
     * It will set the state to inactive if it finds any issues
     * <p>
     * NOTE: caller need to hold the db lock
     */
    public void fixRelationship() {
        onReloadImpl();
    }

    /**
     * @return active or not
     */
    private boolean onReloadImpl() {
        Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
        if (db == null) {
            LOG.warn("db:{} do not exist. materialized view id:{} name:{} should not exist", dbId, id, name);
            setInactiveAndReason("db not exists: " + dbId);
            return false;
        }
        if (baseTableInfos == null) {
            baseTableInfos = Lists.newArrayList();
            if (baseTableIds != null) {
                // for compatibility
                for (long tableId : baseTableIds) {
                    Table table = db.getTable(tableId);
                    if (table == null) {
                        setInactiveAndReason(String.format("mv's base table %s does not exist ", tableId));
                        return false;
                    }
                    baseTableInfos.add(new BaseTableInfo(dbId, db.getFullName(), table.getName(), tableId));
                }
            } else {
                active = false;
                return false;
            }
        } else {
            // for compatibility
            if (baseTableInfos.stream().anyMatch(t -> Strings.isNullOrEmpty(t.getTableName()))) {
                // fill table name for base table info.
                List<BaseTableInfo> newBaseTableInfos = Lists.newArrayList();
                for (BaseTableInfo baseTableInfo : baseTableInfos) {
                    Optional<Table> table = baseTableInfo.mayGetTable();
                    if (!table.isPresent()) {
                        setInactiveAndReason(String.format("mv's base table %s does not exist ",
                                baseTableInfo.getTableId()));
                        return false;
                    }
                    newBaseTableInfos.add(
                            new BaseTableInfo(dbId, db.getFullName(), table.get().getName(), table.get().getId()));
                }
                this.baseTableInfos = newBaseTableInfos;
            }
        }

        boolean res = true;
        for (BaseTableInfo baseTableInfo : baseTableInfos) {
            Table table = null;
            try {
                table = baseTableInfo.getTable();
            } catch (Exception e) {
                LOG.warn("failed to get base table {} of MV {}", baseTableInfo, this, e);
            }
            if (table == null) {
                res = false;
                setInactiveAndReason(
                        MaterializedViewExceptions.inactiveReasonForBaseTableNotExists(baseTableInfo.getTableName()));
                continue;
            } else if (table.isMaterializedView()) {
                MaterializedView baseMV = (MaterializedView) table;
                // recursive reload MV, to guarantee the order of hierarchical MV
                baseMV.onReload();
                if (!baseMV.isActive()) {
                    LOG.warn("tableName :{} is invalid. set materialized view:{} to invalid",
                            baseTableInfo.getTableName(), id);
                    setInactiveAndReason("base mv is not active: " + baseTableInfo.getTableName());
                    res = false;
                }
            }

            // Build the relationship
            MvId mvId = getMvId();
            table.addRelatedMaterializedView(mvId);
            if (!table.isNativeTableOrMaterializedView() && !table.isView()) {
                GlobalStateMgr.getCurrentState().getConnectorTblMetaInfoMgr().addConnectorTableInfo(
                        baseTableInfo.getCatalogName(), baseTableInfo.getDbName(),
                        baseTableInfo.getTableIdentifier(),
                        ConnectorTableInfo.builder().setRelatedMaterializedViews(
                                Sets.newHashSet(mvId)).build()
                );
            }
        }
        analyzePartitionInfo();
        return res;
    }

    private void analyzePartitionInfo() {
        Database db = GlobalStateMgr.getCurrentState().getDb(dbId);

        if (partitionInfo instanceof SinglePartitionInfo) {
            return;
        }
        // analyze expression, because it converts to sql for serialize
        ConnectContext connectContext = new ConnectContext();
        connectContext.setDatabase(db.getFullName());
        // set privilege
        connectContext.setQualifiedUser(AuthenticationMgr.ROOT_USER);
        connectContext.setCurrentUserIdentity(UserIdentity.ROOT);
        connectContext.setCurrentRoleIds(Sets.newHashSet(PrivilegeBuiltinConstants.ROOT_ROLE_ID));
        // currently, mv only supports one expression
        if (partitionInfo instanceof ExpressionRangePartitionInfo) {
            ExpressionRangePartitionInfo expressionRangePartitionInfo = (ExpressionRangePartitionInfo) partitionInfo;
            Expr partitionExpr = expressionRangePartitionInfo.getPartitionExprs().get(0);
            // for Partition slot ref, the SlotDescriptor is not serialized, so should recover it here.
            // the SlotDescriptor is used by toThrift, which influences the execution process.
            List<SlotRef> slotRefs = Lists.newArrayList();
            partitionExpr.collect(SlotRef.class, slotRefs);
            Preconditions.checkState(slotRefs.size() == 1);
            if (slotRefs.get(0).getSlotDescriptorWithoutCheck() == null) {
                for (int i = 0; i < fullSchema.size(); i++) {
                    Column column = fullSchema.get(i);
                    if (column.getName().equalsIgnoreCase(slotRefs.get(0).getColumnName())) {
                        SlotDescriptor slotDescriptor =
                                new SlotDescriptor(new SlotId(i), column.getName(), column.getType(),
                                        column.isAllowNull());
                        slotRefs.get(0).setDesc(slotDescriptor);
                    }
                }
            }

            TableName tableName =
                    new TableName(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, db.getFullName(), this.name);
            ExpressionAnalyzer.analyzeExpression(partitionExpr, new AnalyzeState(),
                    new Scope(RelationId.anonymous(),
                            new RelationFields(this.getBaseSchema().stream()
                                    .map(col -> new Field(col.getName(), col.getType(), tableName, null))
                                    .collect(Collectors.toList()))), connectContext);
        }
    }

    /**
     * Refresh the materialized view if the following conditions are met:
     * 1. Refresh type of materialized view is ASYNC
     * 2. timeunit and step not set for AsyncRefreshContext
     *
     * @return
     */
    public boolean isLoadTriggeredRefresh() {
        if (this.refreshScheme.getType() == MaterializedView.RefreshType.INCREMENTAL) {
            return true;
        }
        AsyncRefreshContext asyncRefreshContext = this.refreshScheme.asyncRefreshContext;
        return this.refreshScheme.getType() == MaterializedView.RefreshType.ASYNC &&
                asyncRefreshContext.step == 0 && null == asyncRefreshContext.timeUnit;
    }

    public boolean shouldTriggeredRefreshBy(String dbName, String tableName) {
        if (!isLoadTriggeredRefresh()) {
            return false;
        }
        TableProperty tableProperty = getTableProperty();
        if (tableProperty == null) {
            return true;
        }
        List<TableName> excludedTriggerTables = tableProperty.getExcludedTriggerTables();
        if (excludedTriggerTables == null) {
            return true;
        }
        for (TableName tables : excludedTriggerTables) {
            if (tables.getDb() == null) {
                if (tables.getTbl().equals(tableName)) {
                    return false;
                }
            } else {
                if (tables.getDb().equals(dbName) && tables.getTbl().equals(tableName)) {
                    return false;
                }
            }
        }
        return true;
    }

    protected void appendUniqueProperties(StringBuilder sb) {
        Preconditions.checkNotNull(sb);

        // storageMedium
        sb.append(StatsConstants.TABLE_PROPERTY_SEPARATOR)
                .append(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM).append("\" = \"");
        sb.append(getStorageMedium()).append("\"");

    }

    public String getMaterializedViewDdlStmt(boolean simple) {
        return getMaterializedViewDdlStmt(simple, false);
    }

    public String getMaterializedViewDdlStmt(boolean simple, boolean isReplay) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE MATERIALIZED VIEW `").append(getName()).append("` (");
        List<String> colDef = Lists.newArrayList();
        List<Integer> outputIndices =
                CollectionUtils.isNotEmpty(queryOutputIndices) ? queryOutputIndices :
                        IntStream.range(0, getBaseSchema().size()).boxed().collect(Collectors.toList());
        for (int index : outputIndices) {
            Column column = getBaseSchema().get(index);
            StringBuilder colSb = new StringBuilder();
            // Since mv supports complex expressions as the output column, add `` to support to replay it.
            colSb.append("`" + column.getName() + "`");
            if (!Strings.isNullOrEmpty(column.getComment())) {
                colSb.append(" COMMENT ").append("\"").append(column.getDisplayComment()).append("\"");
            }
            colDef.add(colSb.toString());
        }
        sb.append(Joiner.on(", ").join(colDef));

        // bitmapIndex
        if (CollectionUtils.isNotEmpty(getIndexes())) {
            for (Index index : getIndexes()) {
                sb.append(",\n");
                sb.append("  ").append(index.toSql());
            }
        }

        sb.append(")");
        if (!Strings.isNullOrEmpty(this.getComment())) {
            sb.append("\nCOMMENT \"").append(this.getDisplayComment()).append("\"");
        }

        // partition
        PartitionInfo partitionInfo = this.getPartitionInfo();
        if (!(partitionInfo instanceof SinglePartitionInfo)) {
            sb.append("\n").append(partitionInfo.toSql(this, null));
        }

        // distribution
        DistributionInfo distributionInfo = this.getDefaultDistributionInfo();
        sb.append("\n").append(distributionInfo.toSql());

        // order by
        if (CollectionUtils.isNotEmpty(getTableProperty().getMvSortKeys())) {
            String str = Joiner.on(",").join(getTableProperty().getMvSortKeys());
            sb.append("\nORDER BY (").append(str).append(")");
        }

        // refresh scheme
        MvRefreshScheme refreshScheme = this.getRefreshScheme();
        if (refreshScheme == null) {
            sb.append("\nREFRESH ").append("UNKNOWN");
        } else {
            if (refreshScheme.getMoment().equals(RefreshMoment.DEFERRED)) {
                sb.append(String.format("\nREFRESH %s %s", refreshScheme.getMoment(), refreshScheme.getType()));
            } else {
                sb.append("\nREFRESH ").append(refreshScheme.getType());
            }
        }
        if (refreshScheme != null && refreshScheme.getType() == RefreshType.ASYNC) {
            AsyncRefreshContext asyncRefreshContext = refreshScheme.getAsyncRefreshContext();
            if (asyncRefreshContext.isDefineStartTime()) {
                sb.append(" START(\"").append(Utils.getDatetimeFromLong(asyncRefreshContext.getStartTime())
                                .format(DateUtils.DATE_TIME_FORMATTER))
                        .append("\")");
            }
            if (asyncRefreshContext.getTimeUnit() != null) {
                sb.append(" EVERY(INTERVAL ").append(asyncRefreshContext.getStep()).append(" ")
                        .append(asyncRefreshContext.getTimeUnit()).append(")");
            }
        }

        // properties
        sb.append("\nPROPERTIES (\n");
        boolean first = true;
        Map<String, String> properties = this.getTableProperty().getProperties();
        boolean hasStorageMedium = false;
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            String name = entry.getKey();
            String value = entry.getValue();
            if (!first) {
                sb.append(",\n");
            }
            first = false;
            if (name.equalsIgnoreCase(PropertyAnalyzer.PROPERTIES_FOREIGN_KEY_CONSTRAINT)) {
                sb.append("\"")
                        .append(PropertyAnalyzer.PROPERTIES_FOREIGN_KEY_CONSTRAINT)
                        .append("\" = \"")
                        .append(ForeignKeyConstraint.getShowCreateTableConstraintDesc(getForeignKeyConstraints()))
                        .append("\"");
            } else if (name.equalsIgnoreCase(PropertyAnalyzer.PROPERTIES_STORAGE_COOLDOWN_TIME)) {
                sb.append("\"").append(PropertyAnalyzer.PROPERTIES_STORAGE_COOLDOWN_TIME)
                        .append("\" = \"")
                        .append(TimeUtils.longToTimeString(
                                Long.parseLong(properties.get(PropertyAnalyzer.PROPERTIES_STORAGE_COOLDOWN_TIME))))
                        .append("\"");
            } else if (name.equalsIgnoreCase(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM)) {
                // handled in appendUniqueProperties
                hasStorageMedium = true;
                sb.append("\"").append(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM)
                        .append("\" = \"")
                        .append(getStorageMedium())
                        .append("\"");
            } else {
                sb.append("\"").append(name).append("\"");
                sb.append(" = ");
                sb.append("\"").append(value).append("\"");
            }
        }
        // NOTE: why not append unique properties when replaying ?
        // Actually we don't need any properties of MV when replaying, but only the schema information
        // And in ShareData mode, the storage_volume property cannot be retrieved in the Checkpointer thread
        if (!hasStorageMedium && !isReplay) {
            appendUniqueProperties(sb);
        }

        // bloom filter
        Set<String> bfColumnNames = getCopiedBfColumns();
        if (bfColumnNames != null) {
            sb.append(StatsConstants.TABLE_PROPERTY_SEPARATOR).append(PropertyAnalyzer.PROPERTIES_BF_COLUMNS)
                    .append("\" = \"");
            sb.append(Joiner.on(", ").join(getCopiedBfColumns())).append("\"");
        }

        // colocate_with
        String colocateGroup = getColocateGroup();
        if (colocateGroup != null) {
            sb.append(StatsConstants.TABLE_PROPERTY_SEPARATOR).append(PropertyAnalyzer.PROPERTIES_COLOCATE_WITH)
                    .append("\" = \"");
            sb.append(colocateGroup).append("\"");
        }

        sb.append("\n)");
        String define = this.getSimpleDefineSql();
        if (StringUtils.isEmpty(define) || !simple) {
            define = this.getViewDefineSql();
        }
        sb.append("\nAS ").append(define);
        sb.append(";");
        return sb.toString();
    }

    private static final ImmutableSet<String> NEED_SHOW_PROPS;

    static {
        NEED_SHOW_PROPS = new ImmutableSet.Builder<String>()
                .add(PropertyAnalyzer.PROPERTIES_STORAGE_COOLDOWN_TIME)
                .add(PropertyAnalyzer.PROPERTIES_PARTITION_TTL_NUMBER)
                .add(PropertyAnalyzer.PROPERTIES_PARTITION_TTL)
                .add(PropertyAnalyzer.PROPERTIES_AUTO_REFRESH_PARTITIONS_LIMIT)
                .add(PropertyAnalyzer.PROPERTIES_PARTITION_REFRESH_NUMBER)
                .add(PropertyAnalyzer.PROPERTIES_EXCLUDED_TRIGGER_TABLES)
                .build();
    }

    public Map<String, String> getMaterializedViewPropMap() {

        Map<String, String> propsMap = new HashMap<>();
        // replicationNum
        Short replicationNum = this.getDefaultReplicationNum();
        propsMap.put(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM, String.valueOf(replicationNum));

        // storageMedium
        String storageMedium = this.getStorageMedium();
        propsMap.put(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM, storageMedium);
        Map<String, String> properties = this.getTableProperty().getProperties();

        // maxMVRewriteStaleness
        propsMap.put(PropertyAnalyzer.PROPERTIES_MV_REWRITE_STALENESS_SECOND, String.valueOf(maxMVRewriteStaleness));

        // NEED_SHOW_PROPS
        NEED_SHOW_PROPS.forEach(prop -> {
            if (properties.containsKey(prop)) {
                if (prop.equals(PropertyAnalyzer.PROPERTIES_STORAGE_COOLDOWN_TIME)) {
                    propsMap.put(prop, TimeUtils.longToTimeString(
                            Long.parseLong(properties.get(prop))));
                } else {
                    propsMap.put(prop, properties.get(prop));
                }
            }
        });
        return propsMap;
    }

    public boolean containsBaseTable(TableName tableName) {
        for (BaseTableInfo baseTableInfo : baseTableInfos) {
            if (tableName.getDb() == null) {
                if (tableName.getTbl().equals(baseTableInfo.getTableName())) {
                    return true;
                }
            } else {
                if (tableName.getTbl().equals(baseTableInfo.getTableName()) &&
                        tableName.getDb().equals(baseTableInfo.getDbName())) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Once the materialized view's base tables have updated, we need to check correspond materialized views' partitions
     * to be refreshed.
     *
     * @return : Collect all need refreshed partitions of materialized view.
     * @isQueryRewrite : Mark whether this caller is query rewrite or not, when it's true we can use staleness to shortcut
     * the update check.
     */
    public boolean getPartitionNamesToRefreshForMv(Set<String> toRefreshPartitions,
                                                   boolean isQueryRewrite) {
        // Skip check for sync materialized view.
        if (refreshScheme.isSync()) {
            return true;
        }

        // check mv's query rewrite consistency mode property only in query rewrite.
        if (isQueryRewrite) {
            TableProperty.QueryRewriteConsistencyMode mvConsistencyRewriteMode
                    = tableProperty.getQueryRewriteConsistencyMode();
            switch (mvConsistencyRewriteMode) {
                case DISABLE:
                    return false;
                case LOOSE:
                    return true;
                case CHECKED:
                default:
                    break;
            }
        }

        if (partitionInfo instanceof SinglePartitionInfo) {
            return getNonPartitionedMVRefreshPartitions(toRefreshPartitions, isQueryRewrite);
        } else if (partitionInfo instanceof ExpressionRangePartitionInfo) {
            // partitions to refresh
            // 1. dropped partitions
            // 2. newly added partitions
            // 3. partitions loaded with new data
            return getPartitionedMVRefreshPartitions(toRefreshPartitions, isQueryRewrite);
        } else {
            throw UnsupportedException.unsupportedException("unsupported partition info type:"
                    + partitionInfo.getClass().getName());
        }
    }

    /**
     * For non-partitioned materialized view, once its base table have updated, we need refresh the
     * materialized view's totally.
     *
     * @return : non-partitioned materialized view's all need updated partition names.
     */
    private boolean getNonPartitionedMVRefreshPartitions(Set<String> toRefreshPartitions,
                                                         boolean isQueryRewrite) {
        Preconditions.checkState(partitionInfo instanceof SinglePartitionInfo);
        for (BaseTableInfo tableInfo : baseTableInfos) {
            Table table = tableInfo.getTableChecked();
            // skip check freshness of view
            if (table.isView()) {
                continue;
            }

            // skip check external table if the external does not support rewrite.
            if (!table.isNativeTableOrMaterializedView()) {
                if (tableProperty.getForceExternalTableQueryRewrite() == TableProperty.QueryRewriteConsistencyMode.DISABLE) {
                    toRefreshPartitions.addAll(getVisiblePartitionNames());
                    return false;
                }
            }

            // once mv's base table has updated, refresh the materialized view totally.
            Set<String> partitionNames = getUpdatedPartitionNamesOfTable(
                    table, true, isQueryRewrite, MaterializedView.getPartitionExpr(this));
            if (CollectionUtils.isNotEmpty(partitionNames)) {
                toRefreshPartitions.addAll(getVisiblePartitionNames());
                return true;
            }
        }
        return true;
    }

    /**
     * Materialized Views' base tables have two kinds: ref base table and non-ref base table.
     * - If non ref base tables updated, need refresh all mv partitions.
     * - If ref base table updated, need refresh the ref base table's updated partitions.
     * <p>
     * eg:
     * CREATE MATERIALIZED VIEW mv1
     * PARTITION BY k1
     * DISTRIBUTED BY HASH(k1) BUCKETS 10
     * AS
     * SELECT k1, v1 as k2, v2 as k3
     * from t1 join t2
     * on t1.k1 and t2.kk1;
     * <p>
     * - t1 is mv1's ref base table because mv1's partition column k1 is deduced from t1
     * - t2 is mv1's non ref base table because mv1's partition column k1 is not associated with t2.
     *
     * @return : partitioned materialized view's all need updated partition names.
     */
    private boolean getPartitionedMVRefreshPartitions(Set<String> toRefreshedPartitioins,
                                                      boolean isQueryRewrite) {
        Preconditions.checkState(partitionInfo instanceof ExpressionRangePartitionInfo);
        // If non-partition-by table has changed, should refresh all mv partitions
        Expr partitionExpr = getFirstPartitionRefTableExpr();
        Pair<Table, Column> partitionInfo = getBaseTableAndPartitionColumn();
        if (partitionInfo == null) {
            setInactiveAndReason("partition configuration changed");
            LOG.warn("mark mv:{} inactive for get partition info failed", name);
            throw new RuntimeException(String.format("getting partition info failed for mv: %s", name));
        }

        Table refBaseTable = partitionInfo.first;
        Column refBasePartitionCol = partitionInfo.second;
        for (BaseTableInfo tableInfo : baseTableInfos) {
            Table baseTable = tableInfo.getTableChecked();
            // skip view
            if (baseTable.isView()) {
                continue;
            }
            // skip external table that is not supported for query rewrite, return all partition ?
            // skip check external table if the external does not support rewrite.
            if (!baseTable.isNativeTableOrMaterializedView()) {
                if (tableProperty.getForceExternalTableQueryRewrite() == TableProperty.QueryRewriteConsistencyMode.DISABLE) {
                    toRefreshedPartitioins.addAll(getVisiblePartitionNames());
                    return false;
                }
            }
            if (baseTable.getTableIdentifier().equals(refBaseTable.getTableIdentifier())) {
                continue;
            }
            // If the non ref table has already changed, need refresh all materialized views' partitions.
            Set<String> partitionNames =
                    getUpdatedPartitionNamesOfTable(baseTable, true, isQueryRewrite, partitionExpr);
            if (CollectionUtils.isNotEmpty(partitionNames)) {
                toRefreshedPartitioins.addAll(getVisiblePartitionNames());
                return true;
            }
        }

        // Step1: collect updated partitions by partition name to range name:
        // - deleted partitions.
        // - added partitions.
        Set<String> needRefreshMvPartitionNames = Sets.newHashSet();
        Map<String, Range<PartitionKey>> basePartitionNameToRangeMap;
        try {
            basePartitionNameToRangeMap =
                    PartitionUtil.getPartitionKeyRange(refBaseTable, refBasePartitionCol, partitionExpr);
        } catch (UserException e) {
            LOG.warn("Materialized view compute partition difference with base table failed.", e);
            toRefreshedPartitioins.addAll(getVisiblePartitionNames());
            return false;
        }

        Map<String, Range<PartitionKey>> mvPartitionNameToRangeMap = getRangePartitionMap();
        // TODO: prune the partitions based on ttl
        RangePartitionDiff rangePartitionDiff = PartitionUtil.getPartitionDiff(partitionExpr, partitionInfo.second,
                basePartitionNameToRangeMap, mvPartitionNameToRangeMap, null);
        needRefreshMvPartitionNames.addAll(rangePartitionDiff.getDeletes().keySet());
        // remove ref base table's deleted partitions from `mvPartitionMap`
        for (String deleted : rangePartitionDiff.getDeletes().keySet()) {
            mvPartitionNameToRangeMap.remove(deleted);
        }

        // step1.2: refresh ref base table's new added partitions
        needRefreshMvPartitionNames.addAll(rangePartitionDiff.getAdds().keySet());
        mvPartitionNameToRangeMap.putAll(rangePartitionDiff.getAdds());

        Map<String, Set<String>> baseToMvNameRef = SyncPartitionUtils
                .getIntersectedPartitions(basePartitionNameToRangeMap, mvPartitionNameToRangeMap);
        Map<String, Set<String>> mvToBaseNameRef = SyncPartitionUtils
                .getIntersectedPartitions(mvPartitionNameToRangeMap, basePartitionNameToRangeMap);

        // step2: check ref base table's updated partition names by checking its ref tables recursively.
        Set<String> baseChangedPartitionNames =
                getUpdatedPartitionNamesOfTable(refBaseTable, true, isQueryRewrite, partitionExpr);
        if (baseChangedPartitionNames == null) {
            toRefreshedPartitioins.addAll(mvToBaseNameRef.keySet());
            return true;
        }

        if (partitionExpr instanceof SlotRef) {
            baseChangedPartitionNames.stream().forEach(x -> needRefreshMvPartitionNames.addAll(baseToMvNameRef.get(x)));
        } else if (partitionExpr instanceof FunctionCallExpr) {
            baseChangedPartitionNames.stream().forEach(x -> needRefreshMvPartitionNames.addAll(baseToMvNameRef.get(x)));
            // because the relation of partitions between materialized view and base partition table is n : m,
            // should calculate the candidate partitions recursively.
            SyncPartitionUtils.calcPotentialRefreshPartition(needRefreshMvPartitionNames, baseChangedPartitionNames,
                    baseToMvNameRef, mvToBaseNameRef);
        }
        toRefreshedPartitioins.addAll(needRefreshMvPartitionNames);
        return true;
    }

    /**
     * Materialized View's partition column can only refer one base table's partition column, get the referred
     * base table and its partition column.
     * TODO: support multi-column partitions later.
     *
     * @return : The materialized view's referred base table and its partition column.
     */
    public Pair<Table, Column> getBaseTableAndPartitionColumn() {
        if (partitionRefTableExprs == null ||
                !(partitionInfo instanceof ExpressionRangePartitionInfo || partitionInfo instanceof ListPartitionInfo)) {
            return null;
        }
        Expr partitionExpr = getPartitionRefTableExprs().get(0);
        List<SlotRef> slotRefs = Lists.newArrayList();
        partitionExpr.collect(SlotRef.class, slotRefs);
        Preconditions.checkState(slotRefs.size() == 1);
        SlotRef partitionSlotRef = slotRefs.get(0);
        for (BaseTableInfo baseTableInfo : baseTableInfos) {
            Table table = baseTableInfo.getTableChecked();
            if (partitionSlotRef.getTblNameWithoutAnalyzed().getTbl().equals(table.getName())) {
                return Pair.create(table, table.getColumn(partitionSlotRef.getColumnName()));
            }
        }
        String baseTableNames = baseTableInfos.stream()
                .map(tableInfo -> tableInfo.getTableChecked().getName()).collect(Collectors.joining(","));
        throw new RuntimeException(
                String.format("can not find partition info for mv:%s on base tables:%s", name, baseTableNames));
    }

    public ExecPlan getMaintenancePlan() {
        return maintenancePlan;
    }

    public void setMaintenancePlan(ExecPlan maintenancePlan) {
        this.maintenancePlan = maintenancePlan;
    }

    /**
     * Infer the distribution info based on tables and MV query.
     * Currently is max{bucket_num of base_table}
     * TODO: infer the bucket number according to MV pattern and cardinality
     */
    @Override
    public void inferDistribution(DistributionInfo info) throws DdlException {
        if (info.getBucketNum() == 0) {
            int inferredBucketNum = 0;
            for (BaseTableInfo base : getBaseTableInfos()) {
                if (base.getTableChecked().isNativeTableOrMaterializedView()) {
                    OlapTable olapTable = (OlapTable) base.getTable();
                    DistributionInfo dist = olapTable.getDefaultDistributionInfo();
                    inferredBucketNum = Math.max(inferredBucketNum, dist.getBucketNum());
                }
            }
            if (inferredBucketNum == 0) {
                inferredBucketNum = CatalogUtils.calBucketNumAccordingToBackends();
            }
            info.setBucketNum(inferredBucketNum);
        }
    }

    @Override
    public Map<String, String> getProperties() {
        Map<String, String> properties = super.getProperties();
        // For materialized view, add into session variables into properties.
        if (super.getTableProperty() != null && super.getTableProperty().getProperties() != null) {
            for (Map.Entry<String, String> entry : super.getTableProperty().getProperties().entrySet()) {
                if (entry.getKey().startsWith(PropertyAnalyzer.PROPERTIES_MATERIALIZED_VIEW_SESSION_PREFIX)) {
                    String varKey = entry.getKey().substring(
                            PropertyAnalyzer.PROPERTIES_MATERIALIZED_VIEW_SESSION_PREFIX.length());
                    properties.put(varKey, entry.getValue());
                }
            }
        }
        return properties;
    }

    @Override
    public void gsonPreProcess() throws IOException {
        this.serializedPartitionRefTableExprs = new ArrayList<>();
        if (partitionRefTableExprs != null) {
            for (Expr partitionExpr : partitionRefTableExprs) {
                if (partitionExpr != null) {
                    serializedPartitionRefTableExprs.add(
                            new GsonUtils.ExpressionSerializedObject(partitionExpr.toSql()));
                }
            }
        }
    }

    @Override
    public void gsonPostProcess() throws IOException {
        super.gsonPostProcess();
        partitionRefTableExprs = new ArrayList<>();
        if (serializedPartitionRefTableExprs != null) {
            for (GsonUtils.ExpressionSerializedObject expressionSql : serializedPartitionRefTableExprs) {
                if (expressionSql != null) {
                    partitionRefTableExprs.add(
                            SqlParser.parseSqlToExpr(expressionSql.expressionSql, SqlModeHelper.MODE_DEFAULT));
                }
            }
        }
    }

    public String inspectMeta() {
        return GsonUtils.GSON.toJson(this);
    }

    @Override
    public Status resetIdsForRestore(GlobalStateMgr globalStateMgr,
                                     Database db, int restoreReplicationNum,
                                     MvRestoreContext mvRestoreContext) {
        // change db_id to new restore id
        MvId oldMvId = new MvId(dbId, id);

        this.dbId = db.getId();
        Status status = super.resetIdsForRestore(globalStateMgr, db, restoreReplicationNum, mvRestoreContext);
        if (!status.ok()) {
            return status;
        }
        // store new mvId to for old mvId
        MvId newMvId = new MvId(dbId, id);
        MvBackupInfo mvBackupInfo = mvRestoreContext.getMvIdToTableNameMap()
                .computeIfAbsent(oldMvId, x -> new MvBackupInfo(db.getFullName(), name));
        mvBackupInfo.setLocalMvId(newMvId);
        return Status.OK;
    }

    /**
     * Post actions after restore. Rebuild the materialized view by using table name instead of table ids
     * because the table ids have changed since the restore.
     *
     * @return : rebuild status, ok if success other error status.
     */
    @Override
    public Status doAfterRestore(MvRestoreContext mvRestoreContext) throws DdlException {
        super.doAfterRestore(mvRestoreContext);

        if (baseTableInfos == null) {
            setInactiveAndReason("base mv is not active: base info is null");
            return new Status(Status.ErrCode.NOT_FOUND,
                    String.format("Materialized view %s's base info is not found", this.name));
        }

        Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
        if (db == null) {
            return new Status(Status.ErrCode.NOT_FOUND,
                    String.format("Materialized view %s's db %s is not found", this.name, this.dbId));
        }
        List<BaseTableInfo> newBaseTableInfos = Lists.newArrayList();

        boolean isSetInactive = false;
        Map<TableName, MvBaseTableBackupInfo> mvBaseTableToBackupTableInfo = mvRestoreContext.getMvBaseTableToBackupTableInfo();
        Map<TableName, TableName> remoteToLocalTableName = Maps.newHashMap();
        MvId oldMvId = null;
        boolean isWriteBaseTableInfoChangeEditLog = false;
        for (BaseTableInfo baseTableInfo : baseTableInfos) {
            String remoteDbName = baseTableInfo.getDbName();
            String remoteTableName = baseTableInfo.getTableName();
            TableName remoteDbTblName = new TableName(remoteDbName, remoteTableName);
            if (!mvBaseTableToBackupTableInfo.containsKey(remoteDbTblName)) {
                // baseTableInfo's db/table is not found in the `mvBaseTableToBackupTableInfo`: the base table may not
                // be backed up and restored before.
                LOG.info(String.format("Materialized view %s can not find the base table from mvBaseTableToBackupTableInfo, " +
                        "old base table name:%s, try to find in current env", this.name, remoteDbTblName));
                if (!restoreBaseTableInfoIfNoRestored(this, baseTableInfo, newBaseTableInfos)) {
                    isSetInactive = true;
                }
            } else {
                LOG.info(String.format("Materialized view %s can find the base table from mvBaseTableToBackupTableInfo, " +
                        "old base table name:%s", this.name, remoteDbTblName));
                MvBaseTableBackupInfo mvBaseTableBackupInfo = mvBaseTableToBackupTableInfo.get(remoteDbTblName);

                Pair<Boolean, Optional<MvId>> resetResult = restoreBaseTableInfoIfRestored(mvRestoreContext, this,
                        mvBaseTableBackupInfo, baseTableInfo, remoteToLocalTableName, newBaseTableInfos);
                if (!resetResult.first) {
                    isSetInactive = true;
                    continue;
                }
                if (resetResult.second.isPresent() && oldMvId == null) {
                    oldMvId = resetResult.second.get();
                }
                // Only write edit log when base table also backed up and restored.
                isWriteBaseTableInfoChangeEditLog = true;
            }
        }

        // set it inactive if its base table infos are not complete.
        if (isSetInactive) {
            String errorMsg = String.format("Cannot active the materialized view %s after restore, please check whether " +
                            "its base table has already been backup or restore:%s", name,
                    baseTableInfos.stream().map(x -> x.toString()).collect(Collectors.joining(",")));
            LOG.warn(errorMsg);
            setInactiveAndReason(errorMsg);
            return new Status(Status.ErrCode.NOT_FOUND, errorMsg);
        }

        // Check whether the materialized view's defined sql can be analyzed and built task again.
        Pair<String, String> newDefinedQueries = Pair.create("", "");
        Pair<Status, Boolean> result = checkMvDefinedQuery(this, remoteToLocalTableName, newDefinedQueries);
        if (!result.first.ok()) {
            String createMvSql = getMaterializedViewDdlStmt(false);
            String errorMsg = String.format("Can not active materialized view [%s]" +
                    " because analyze materialized view define sql: \n\n%s", name, createMvSql);
            setInactiveAndReason(errorMsg);
            return result.first;
        }
        if (result.second) {
            if (!Strings.isNullOrEmpty(newDefinedQueries.first)) {
                this.viewDefineSql = newDefinedQueries.first;
            }
            if (!Strings.isNullOrEmpty(newDefinedQueries.second)) {
                this.simpleDefineSql = newDefinedQueries.second;
            }
        }

        String oldBaseTableInfosStr = baseTableInfos.stream().map(x -> x.toString()).collect(Collectors.joining(","));
        String newBaseTableInfosStr = newBaseTableInfos.stream().map(x -> x.toString()).collect(Collectors.joining(","));
        LOG.info("restore materialized view {} succeed, old baseTableInfo {} to new baseTableInfo {}",
                getName(), oldBaseTableInfosStr, newBaseTableInfosStr);
        Preconditions.checkArgument(this.baseTableInfos.size() == newBaseTableInfos.size(),
                String.format("New baseTableInfos' size should be qual to old baseTableInfos, baseTableInfos:%s," +
                                "newBaseTableInfos:%s", oldBaseTableInfosStr, newBaseTableInfosStr));
        this.baseTableInfos = newBaseTableInfos;

        // change ExpressionRangePartitionInfo because mv's db may be changed.
        if (partitionInfo instanceof ExpressionRangePartitionInfo) {
            ExpressionRangePartitionInfo expressionRangePartitionInfo = (ExpressionRangePartitionInfo) partitionInfo;
            Preconditions.checkState(expressionRangePartitionInfo.getPartitionExprs().size() == 1);
            expressionRangePartitionInfo.renameTableName(db.getFullName(), this.name);
        }

        setActive();

        fixRelationship();

        // write edit log
        if (isWriteBaseTableInfoChangeEditLog) {
            Map<Long, Map<String, BasePartitionInfo>> baseTableVisibleVersionMap =
                    this.refreshScheme.asyncRefreshContext.baseTableVisibleVersionMap;
            AlterMaterializedViewBaseTableInfosLog alterMaterializedViewBaseTableInfos =
                    new AlterMaterializedViewBaseTableInfosLog(dbId, getId(), oldMvId, baseTableInfos,
                            baseTableVisibleVersionMap);
            GlobalStateMgr.getCurrentState().getEditLog().logAlterMvBaseTableInfos(alterMaterializedViewBaseTableInfos);
        }

        // rebuild mv tasks to be scheduled in TaskManager.
        TaskBuilder.rebuildMVTask(db.getFullName(), this);

        // clear baseTable ids if it exists
        if (this.baseTableIds != null) {
            this.baseTableIds.clear();
        }

        return Status.OK;
    }

    /**
     * Replay AlterMaterializedViewBaseTableInfosLog and update associated variables.
     */
    public void replayAlterMaterializedViewBaseTableInfos(AlterMaterializedViewBaseTableInfosLog log) {
        this.setBaseTableInfos(log.getBaseTableInfos());
        Map<Long, Map<String, MaterializedView.BasePartitionInfo>> baseTableVisibleVersionMap =
                this.refreshScheme.asyncRefreshContext.baseTableVisibleVersionMap;
        for (Map.Entry<Long, Map<String, MaterializedView.BasePartitionInfo>> e :
                log.getBaseTableVisibleVersionMap().entrySet()) {
            if (!baseTableVisibleVersionMap.containsKey(e.getKey())) {
                baseTableVisibleVersionMap.put(e.getKey(), e.getValue());
            } else {
                Map<String, MaterializedView.BasePartitionInfo> partitionInfoMap = baseTableVisibleVersionMap.get(e.getKey());
                partitionInfoMap.putAll(e.getValue());
            }
        }
        for (BaseTableInfo baseTableInfo : baseTableInfos) {
            Table baseTable = baseTableInfo.getTable();
            if (baseTable == null) {
                continue;
            }
            baseTable.getRelatedMaterializedViews().remove(log.getMvId());
            baseTable.getRelatedMaterializedViews().add(getMvId());
        }
        setActive();

        // recheck again
        fixRelationship();
    }
}
