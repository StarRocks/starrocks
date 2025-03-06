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
import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.DescriptorTable.ReferencedPartitionInfo;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.ParseNode;
import com.starrocks.analysis.SlotDescriptor;
import com.starrocks.analysis.SlotId;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TableName;
import com.starrocks.authentication.AuthenticationMgr;
import com.starrocks.authorization.PrivilegeBuiltinConstants;
import com.starrocks.backup.Status;
import com.starrocks.backup.mv.MvBackupInfo;
import com.starrocks.backup.mv.MvBaseTableBackupInfo;
import com.starrocks.backup.mv.MvRestoreContext;
import com.starrocks.catalog.constraint.ForeignKeyConstraint;
import com.starrocks.catalog.constraint.GlobalConstraintManager;
import com.starrocks.catalog.mv.MVPlanValidationResult;
import com.starrocks.common.DdlException;
import com.starrocks.common.FeConstants;
import com.starrocks.common.MaterializedViewExceptions;
import com.starrocks.common.Pair;
import com.starrocks.common.io.DeepCopy;
import com.starrocks.common.io.Text;
import com.starrocks.common.util.DateUtils;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.connector.ConnectorPartitionTraits;
import com.starrocks.connector.ConnectorTableInfo;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.mv.analyzer.MVPartitionExpr;
import com.starrocks.persist.AlterMaterializedViewBaseTableInfosLog;
import com.starrocks.persist.ExpressionSerializedObject;
import com.starrocks.persist.gson.GsonPostProcessable;
import com.starrocks.persist.gson.GsonPreProcessable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.scheduler.TableWithPartitions;
import com.starrocks.scheduler.Task;
import com.starrocks.scheduler.TaskBuilder;
import com.starrocks.scheduler.TaskManager;
import com.starrocks.scheduler.mv.MVTimelinessMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.analyzer.AnalyzeState;
import com.starrocks.sql.analyzer.ExpressionAnalyzer;
import com.starrocks.sql.analyzer.Field;
import com.starrocks.sql.analyzer.MaterializedViewAnalyzer;
import com.starrocks.sql.analyzer.RelationFields;
import com.starrocks.sql.analyzer.RelationId;
import com.starrocks.sql.analyzer.Scope;
import com.starrocks.sql.analyzer.SelectAnalyzer;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.sql.common.PCell;
import com.starrocks.sql.common.PRangeCell;
import com.starrocks.sql.optimizer.CachingMvPlanContextBuilder;
import com.starrocks.sql.optimizer.MvRewritePreprocessor;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.mv.MVUtils;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.statistic.StatsConstants;
import com.starrocks.thrift.TTableDescriptor;
import com.starrocks.thrift.TTableType;
import com.starrocks.warehouse.Warehouse;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
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
 * A Materialized View is a database object that contains the results of a query.
 * - Base tables are the tables that are referenced in the view definition.
 * - Ref base tables are some special base tables of a materialized view that are referenced by mv's partition expression.
 * </p>
 * In our partition-change-tracking mechanism, we need to track the partition change of ref base tables to refresh the associated
 * partitions of the materialized view.
 * </p>
 * NOTE:
 * - A Materialized View can have multi base-tables which are tables that are referenced in the view definition.
 * - A Materialized View can have multi ref-base-tables which are tables that are referenced by mv's partition expression.
 * - A Materialized View's partition expressions can be range partitioned or list partitioned:
 *      - If mv is range partitioned, it must only have one partition column.
 *      - If mv is list partitioned, it can have multi partition columns.
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
    public void setUseFastSchemaEvolution(boolean useFastSchemaEvolution) {
    }

    public static class BasePartitionInfo {

        @SerializedName(value = "id")
        private final long id;

        @SerializedName(value = "version")
        private final long version;

        @SerializedName(value = "lastRefreshTime")
        private final long lastRefreshTime;

        // last modified time of partition data path
        @SerializedName(value = "lastFileModifiedTime")
        private long extLastFileModifiedTime;

        // file number in the partition data path
        @SerializedName(value = "fileNumber")
        private int fileNumber;

        public BasePartitionInfo(long id, long version, long lastRefreshTime) {
            this.id = id;
            this.version = version;
            this.lastRefreshTime = lastRefreshTime;
            this.extLastFileModifiedTime = -1;
            this.fileNumber = -1;
        }

        public static BasePartitionInfo fromExternalTable(com.starrocks.connector.PartitionInfo info) {
            // TODO: id and version
            return new BasePartitionInfo(-1, -1, info.getModifiedTime());
        }

        public static BasePartitionInfo fromOlapTable(Partition partition) {
            return new BasePartitionInfo(partition.getId(), partition.getDefaultPhysicalPartition().getVisibleVersion(), -1);
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

        public long getExtLastFileModifiedTime() {
            return extLastFileModifiedTime;
        }

        public void setExtLastFileModifiedTime(long extLastFileModifiedTime) {
            this.extLastFileModifiedTime = extLastFileModifiedTime;
        }

        public int getFileNumber() {
            return fileNumber;
        }

        public void setFileNumber(int fileNumber) {
            this.fileNumber = fileNumber;
        }

        @Override
        public String toString() {
            return "BasePartitionInfo{" +
                    "id=" + id +
                    ", version=" + version +
                    ", lastRefreshTime=" + lastRefreshTime +
                    ", lastFileModifiedTime=" + extLastFileModifiedTime +
                    ", fileNumber=" + fileNumber +
                    '}';
        }
    }

    public static class AsyncRefreshContext {
        // Olap base table refreshed meta infos
        // base table id -> (partition name -> partition info (id, version))
        // partition id maybe changed after insert overwrite, so use partition name as key.
        // partition id which in BasePartitionInfo can be used to check partition is changed
        @SerializedName("baseTableVisibleVersionMap")
        private final Map<Long, Map<String, BasePartitionInfo>> baseTableVisibleVersionMap;

        // External base table refreshed meta infos
        @SerializedName("baseTableInfoVisibleVersionMap")
        private final Map<BaseTableInfo, Map<String, BasePartitionInfo>> baseTableInfoVisibleVersionMap;

        // Materialized view partition is updated/added associated with ref-base-table partitions. This meta
        // is kept to track materialized view's partition change and update associated ref-base-table partitions
        // at the same time.
        @SerializedName("mvPartitionNameRefBaseTablePartitionMap")
        private final Map<String, Set<String>> mvPartitionNameRefBaseTablePartitionMap;

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
            this.mvPartitionNameRefBaseTablePartitionMap = Maps.newConcurrentMap();
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

        public Map<String, Set<String>> getMvPartitionNameRefBaseTablePartitionMap() {
            return mvPartitionNameRefBaseTablePartitionMap;
        }

        public void clearVisibleVersionMap() {
            LOG.info("Clear materialized view's version map");
            this.baseTableInfoVisibleVersionMap.clear();
            this.baseTableVisibleVersionMap.clear();
            this.mvPartitionNameRefBaseTablePartitionMap.clear();
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
                    ", baseTableInfoVisibleVersionMap=" + baseTableInfoVisibleVersionMap +
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
            arc.mvPartitionNameRefBaseTablePartitionMap.putAll(this.mvPartitionNameRefBaseTablePartitionMap);
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

        public boolean isAsync() {
            return type.equals(RefreshType.ASYNC);
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

        @Override
        public String toString() {
            return "MvRefreshScheme{" +
                    "moment=" + moment +
                    ", type=" + type +
                    ", asyncRefreshContext=" + asyncRefreshContext +
                    ", lastRefreshTime=" + lastRefreshTime +
                    '}';
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

    // This is a normalized view define SQL by AstToSQLBuilder#toSQL, which is used for show create mv, constructing a refresh job
    // (insert into select)
    @SerializedName(value = "viewDefineSql")
    private String viewDefineSql;
    // This is a normalized view define SQL by AstToSQLBuilder#buildSimple.
    @SerializedName(value = "simpleDefineSql")
    private String simpleDefineSql;
    // This is the original user's view define SQL which can be used to generate ast key in text based rewrite.
    @SerializedName(value = "originalViewDefineSql")
    private String originalViewDefineSql;
    // This is the original database name when the mv is created.
    private String originalDBName;
    // Deprecated field which is used to store single partition ref table exprs of the mv in old version.
    @Deprecated
    @SerializedName(value = "partitionRefTableExprs")
    private List<ExpressionSerializedObject> serializedPartitionRefTableExprs;
    @Deprecated
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

    // Multi ref base table partition expression and ref slot ref map.
    @SerializedName(value = "partitionExprMaps")
    private Map<ExpressionSerializedObject, ExpressionSerializedObject> serializedPartitionExprMaps;
    // Use LinedHashMap to keep the order of partition exprs by the user's defined order which can be used when mv contains multi
    // partition columns.
    private LinkedHashMap<Expr, SlotRef> partitionExprMaps;

    // ref base table to partition expression
    private Optional<Map<Table, List<Expr>>> refBaseTablePartitionExprsOpt = Optional.empty();
    // ref bae table to partition column slot ref
    private Optional<Map<Table, List<SlotRef>>> refBaseTablePartitionSlotsOpt = Optional.empty();
    // ref bae table to partition column
    private Optional<Map<Table, List<Column>>> refBaseTablePartitionColumnsOpt = Optional.empty();
    // cache table to base table info's mapping to refresh table, Iceberg/Delta table needs to refresh table's snapshots
    // to fetch the newest table info.
    private transient volatile Map<Table, BaseTableInfo> tableToBaseTableInfoCache = Maps.newConcurrentMap();
    // partition retention expr which is used to filter out the partitions that need to be retained.
    private transient volatile Optional<Expr> retentionConditionExprOpt = Optional.empty();
    // partition retention scalar operator which is used to filter out the partitions that need to be retained.
    private transient volatile Optional<ScalarOperator> retentionConditionScalarOpOpt = Optional.empty();

    // Materialized view's output columns may be different from defined query's output columns.
    // Record the indexes based on materialized view's column output.
    // eg: create materialized view mv as select col1, col2, col3 from tbl
    //  desc mv             :  col2, col1, col3
    //  queryOutputIndexes  :  1, 0, 2
    // which means 0th of query output column is in 1th mv's output columns, and 1th -> 0th, 2th -> 2th.
    @SerializedName(value = "queryOutputIndices")
    protected List<Integer> queryOutputIndices = Lists.newArrayList();

    @SerializedName(value = "warehouseId")
    private long warehouseId = WarehouseManager.DEFAULT_WAREHOUSE_ID;

    protected volatile ParseNode defineQueryParseNode = null;

    public MaterializedView() {
        super(TableType.MATERIALIZED_VIEW);
        this.tableProperty = null;
        this.state = OlapTableState.NORMAL;
        this.active = true;
    }

    public MaterializedView(long id, long dbId, String mvName, List<Column> baseSchema, KeysType keysType,
                            PartitionInfo partitionInfo, DistributionInfo defaultDistributionInfo,
                            MvRefreshScheme refreshScheme) {
        super(id, mvName, baseSchema, keysType, partitionInfo, defaultDistributionInfo, null, TableType.MATERIALIZED_VIEW);
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
            if (copiedPartition.getDistributionInfo().getType() != distributionInfo.getType()) {
                copiedPartition.setDistributionInfo(distributionInfo);
            }
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

    public synchronized boolean isActive() {
        return active;
    }

    /**
     * active the materialized again & reload the state.
     */
    public synchronized void setActive() {
        LOG.info("set {} to active", name);
        // reset mv rewrite cache when it is active again
        CachingMvPlanContextBuilder.getInstance().updateMvPlanContextCache(this, true);
        this.active = true;
        this.inactiveReason = null;
    }

    public synchronized void setInactiveAndReason(String reason) {
        LOG.warn("set {} to inactive because of {}", name, reason);
        this.active = false;
        this.inactiveReason = reason;
        // reset cached variables
        resetMetadataCache();
        CachingMvPlanContextBuilder.getInstance().updateMvPlanContextCache(this, false);
    }

    /**
     * Reset cached metadata when mv's meta has changed.
     */
    public synchronized void resetMetadataCache() {
        refBaseTablePartitionExprsOpt = Optional.empty();
        refBaseTablePartitionSlotsOpt = Optional.empty();
        refBaseTablePartitionColumnsOpt = Optional.empty();
        tableToBaseTableInfoCache.clear();
    }

    public synchronized String getInactiveReason() {
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

    public String getOriginalViewDefineSql() {
        return originalViewDefineSql;
    }

    public void setOriginalViewDefineSql(String originalViewDefineSql) {
        this.originalViewDefineSql = originalViewDefineSql;
    }

    public String getOriginalDBName() {
        return originalDBName;
    }

    public void setOriginalDBName(String originalDBName) {
        this.originalDBName = originalDBName;
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
        baseTableInfos.forEach(tableInfo -> baseTableTypes.add(MvUtils.getTableChecked(tableInfo).getType()));
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

    public void setWarehouseId(long warehouseId) {
        this.warehouseId = warehouseId;
    }

    public long getWarehouseId() {
        return warehouseId;
    }

    public int getMaxMVRewriteStaleness() {
        return maxMVRewriteStaleness;
    }

    public void setMaxMVRewriteStaleness(int maxMVRewriteStaleness) {
        this.maxMVRewriteStaleness = maxMVRewriteStaleness;
    }

    @VisibleForTesting
    public Map<Expr, SlotRef> getPartitionExprMaps() {
        return partitionExprMaps;
    }

    public void setPartitionExprMaps(LinkedHashMap<Expr, SlotRef> partitionExprMaps) {
        this.partitionExprMaps = partitionExprMaps;
    }

    public List<Integer> getQueryOutputIndices() {
        return queryOutputIndices;
    }

    public void setQueryOutputIndices(List<Integer> queryOutputIndices) {
        this.queryOutputIndices = queryOutputIndices;
    }

    /**
     * Return the partition column of the materialized view.
     * NOTE: Only supports range partition mv for now, because range partition mv only supports one partition column.
     */
    public Optional<Column> getRangePartitionFirstColumn() {
        List<Column> partitionCols = partitionInfo.getPartitionColumns(this.idToColumn);
        if (CollectionUtils.isEmpty(partitionCols)) {
            return Optional.empty();
        }
        Preconditions.checkState(partitionInfo.isRangePartition(), "Only range partition is supported now");
        return Optional.of(partitionCols.get(0));
    }

    /**
     * Return the partition expr of the range partitioned materialized view.
     * NOTE: Only supports range partition mv for now, because range partition mv only supports one partition column.
     */
    public Optional<Expr> getRangePartitionFirstExpr() {
        if (partitionRefTableExprs == null) {
            return Optional.empty();
        }
        Preconditions.checkState(partitionInfo.isRangePartition(), "Only range partition is supported now");
        Expr partitionExpr = partitionRefTableExprs.get(0);
        if (partitionExpr == null) {
            return Optional.empty();
        }
        if (partitionExpr.getType() == Type.INVALID) {
            Optional<Column> partitionColOpt = getRangePartitionFirstColumn();
            if (partitionColOpt.isEmpty()) {
                return Optional.empty();
            }
            Type partitionColType = partitionColOpt.get().getType();
            partitionExpr.setType(partitionColType);
        }
        return Optional.of(partitionExpr);
    }

    /**
     * Return the updated partition names of the base table of the materialized view.
     * @param baseTable: The base table of the materialized view to check the updated partition names
     * @param isQueryRewrite: Whether it's for query rewrite or not
     * @return: Return the updated partition names of the base table
     */
    public Set<String> getUpdatedPartitionNamesOfOlapTable(OlapTable baseTable, boolean isQueryRewrite) {
        if (isQueryRewrite && isStalenessSatisfied()) {
            return Sets.newHashSet();
        }

        return ConnectorPartitionTraits.build(this, baseTable).getUpdatedPartitionNames(
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
            Table baseTable = MvUtils.getTableChecked(baseTableInfo);

            if (baseTable instanceof View) {
                continue;
            } else if (baseTable instanceof MaterializedView) {
                MaterializedView mv = (MaterializedView) baseTable;
                if (!mv.isStalenessSatisfied()) {
                    return Optional.empty();
                }
            }
            Optional<Long> baseTableTs = ConnectorPartitionTraits.build(this, baseTable).maxPartitionRefreshTs();
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

    public long getMaxPartitionRowCount() {
        long maxRowCount = 0;
        for (Map.Entry<Long, Partition> entry : idToPartition.entrySet()) {
            for (PhysicalPartition partition : entry.getValue().getSubPartitions()) {
                maxRowCount = Math.max(maxRowCount, partition.getBaseIndex().getRowCount());
            }
        }
        return maxRowCount;
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

    /**
     * Get the updated partition names of the external base table of the materialized view.
     * @param baseTable: the external base table of the materialized view to check the updated partition names
     * @param isQueryRewrite: whether it's for query rewrite or not
     * @return: the updated partition names of the external base table
     */
    public Set<String> getUpdatedPartitionNamesOfExternalTable(Table baseTable, boolean isQueryRewrite) {
        Set<String> result = Sets.newHashSet();
        // NOTE: For query dump replay, ignore updated partition infos only to check mv can rewrite query or not.
        // Ignore partitions when mv 's last refreshed time period is less than `maxMVRewriteStaleness`
        if (FeConstants.isReplayFromQueryDump || (isQueryRewrite && isStalenessSatisfied())) {
            return result;
        }

        ConnectorPartitionTraits traits = ConnectorPartitionTraits.build(this, baseTable);
        traits.setQueryMVRewrite(isQueryRewrite);
        return traits.getUpdatedPartitionNames(
                this.getBaseTableInfos(),
                this.refreshScheme.getAsyncRefreshContext());
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
        if (this.partitionExprMaps != null) {
            mv.partitionExprMaps = this.partitionExprMaps;
        }
        mv.refBaseTablePartitionExprsOpt = this.refBaseTablePartitionExprsOpt;
        mv.refBaseTablePartitionSlotsOpt = this.refBaseTablePartitionSlotsOpt;
        mv.refBaseTablePartitionColumnsOpt = this.refBaseTablePartitionColumnsOpt;
        mv.tableToBaseTableInfoCache = this.tableToBaseTableInfoCache;
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

    public static SlotRef getMvPartitionSlotRef(Expr expr) {
        if (expr instanceof SlotRef) {
            return ((SlotRef) expr);
        } else {
            List<SlotRef> slotRefs = Lists.newArrayList();
            expr.collect(SlotRef.class, slotRefs);
            Preconditions.checkState(slotRefs.size() == 1);
            return slotRefs.get(0);
        }
    }

    public static MaterializedView read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, MaterializedView.class);
    }

    @Override
    public void dropPartition(long dbId, String partitionName, boolean isForceDrop) {
        super.dropPartition(dbId, partitionName, isForceDrop);

        // if mv's partition is dropped, we need to update mv's timeliness.
        GlobalStateMgr.getCurrentState().getMaterializedViewMgr()
                .triggerTimelessInfoEvent(this, MVTimelinessMgr.MVChangeEvent.MV_PARTITION_DROPPED);
    }

    @Override
    public void onDrop(Database db, boolean force, boolean replay) {
        super.onDrop(db, force, replay);

        // 1. Remove from plan cache
        MvId mvId = new MvId(db.getId(), getId());
        CachingMvPlanContextBuilder.getInstance().updateMvPlanContextCache(this, false);

        // 2. Remove from base tables
        List<BaseTableInfo> baseTableInfos = getBaseTableInfos();
        for (BaseTableInfo baseTableInfo : ListUtils.emptyIfNull(baseTableInfos)) {
            Optional<Table> baseTableOpt;
            try {
                baseTableOpt = MvUtils.getTableWithIdentifier(baseTableInfo);
            } catch (Exception e) {
                if (!(baseTableInfo.isInternalCatalog())) {
                    GlobalStateMgr.getCurrentState().getConnectorTblMetaInfoMgr().
                            removeConnectorTableInfo(baseTableInfo.getCatalogName(),
                                    baseTableInfo.getDbName(),
                                    baseTableInfo.getTableIdentifier(),
                                    ConnectorTableInfo.builder().setRelatedMaterializedViews(
                                            Sets.newHashSet(mvId)).build());
                }
                LOG.error("Failed to get base table: {}", baseTableInfo, e);
                continue;
            }

            if (baseTableOpt.isPresent()) {
                Table baseTable = baseTableOpt.get();
                baseTable.removeRelatedMaterializedView(mvId);
                if (!baseTable.isNativeTableOrMaterializedView()) {
                    // remove relatedMaterializedViews for connector table
                    GlobalStateMgr.getCurrentState().getConnectorTblMetaInfoMgr().
                            removeConnectorTableInfo(baseTableInfo.getCatalogName(),
                                    baseTableInfo.getDbName(),
                                    baseTableInfo.getTableIdentifier(),
                                    ConnectorTableInfo.builder().setRelatedMaterializedViews(
                                            Sets.newHashSet(mvId)).build());
                }
            }
        }

        // 3. Remove relevant tasks
        TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
        Task refreshTask = taskManager.getTask(TaskBuilder.getMvTaskName(getId()));
        if (refreshTask != null) {
            taskManager.dropTasks(Lists.newArrayList(refreshTask.getId()), replay);
        }
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
            analyzePartitionExprs();

            // register constraints from global state manager
            GlobalConstraintManager globalConstraintManager = GlobalStateMgr.getCurrentState().getGlobalConstraintManager();
            globalConstraintManager.registerConstraint(this);
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
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
        if (db == null) {
            LOG.warn("db:{} do not exist. materialized view id:{} name:{} should not exist", dbId, id, name);
            setInactiveAndReason(MaterializedViewExceptions.inactiveReasonForDbNotExists(dbId));
            return false;
        }
        if (baseTableInfos == null) {
            baseTableInfos = Lists.newArrayList();
            if (baseTableIds != null) {
                // for compatibility
                for (long tableId : baseTableIds) {
                    Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getId(), tableId);
                    if (table == null) {
                        setInactiveAndReason(MaterializedViewExceptions.inactiveReasonForBaseTableNotExists(tableId));
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
                    Optional<Table> table = MvUtils.getTableWithIdentifier(baseTableInfo);
                    if (!table.isPresent()) {
                        setInactiveAndReason(MaterializedViewExceptions.inactiveReasonForBaseTableNotExists(
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
                Optional<Table> optTable = MvUtils.getTableWithIdentifier(baseTableInfo);
                if (optTable.isPresent()) {
                    table = optTable.get();
                }
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
                    setInactiveAndReason(
                            MaterializedViewExceptions.inactiveReasonForBaseTableActive(baseTableInfo.getTableName()));
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
        if (partitionInfo.isUnPartitioned()) {
            return;
        }
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
        // analyze expression, because it converts to sql for serialize
        ConnectContext connectContext = ConnectContext.buildInner();
        connectContext.setDatabase(db.getFullName());
        // set privilege
        connectContext.setQualifiedUser(AuthenticationMgr.ROOT_USER);
        connectContext.setCurrentUserIdentity(UserIdentity.ROOT);
        connectContext.setCurrentRoleIds(Sets.newHashSet(PrivilegeBuiltinConstants.ROOT_ROLE_ID));
        // currently, mv only supports one expression
        if (partitionInfo.isExprRangePartitioned()) {
            ExpressionRangePartitionInfo expressionRangePartitionInfo = (ExpressionRangePartitionInfo) partitionInfo;
            Expr partitionExpr = expressionRangePartitionInfo.getPartitionExprs(idToColumn).get(0);
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

    /**
     * Whether this mv can be used for mv rewrite configured by {@code enable_query_rewrite} table property.
     * @return: true if it is configured to enable mv rewrite, otherwise false.
     */
    public boolean isEnableRewrite() {
        TableProperty tableProperty = getTableProperty();
        if (tableProperty == null) {
            return true;
        }
        return tableProperty.getMvQueryRewriteSwitch().isEnable();
    }

    /**
     * Whether this mv can be used for transparent rewrite configured by.
     * @return: true if it is configured to enable transparent rewrite, otherwise false.
     */
    public boolean isEnableTransparentRewrite() {
        TableProperty tableProperty = getTableProperty();
        if (tableProperty == null) {
            // default is false
            return false;
        }
        return tableProperty.getMvTransparentRewriteMode().isEnable();
    }

    /**
     * @return the transparent rewrite mode of the materialized view
     */
    public TableProperty.MVTransparentRewriteMode getTransparentRewriteMode() {
        TableProperty tableProperty = getTableProperty();
        if (tableProperty == null) {
            return TableProperty.MVTransparentRewriteMode.defaultValue();
        }
        return tableProperty.getMvTransparentRewriteMode();
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
        return matchTable(excludedTriggerTables, dbName, tableName);
    }

    public boolean shouldRefreshTable(String tableName) {
        long dbId = this.getDbId();
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
        if (db == null) {
            LOG.warn("failed to get Database when pending refresh, DBId: {}", dbId);
            return false;
        }
        String dbName = db.getFullName();

        TableProperty tableProperty = getTableProperty();
        if (tableProperty == null) {
            return true;
        }
        List<TableName> excludedRefreshTables = tableProperty.getExcludedRefreshTables();
        return matchTable(excludedRefreshTables, dbName, tableName);
    }

    private boolean matchTable(List<TableName> excludedRefreshBaseTables, String dbName, String tableName) {
        if (excludedRefreshBaseTables == null) {
            return true;
        }
        for (TableName tables : excludedRefreshBaseTables) {
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
                sb.append("  ").append(index.toSql(this));
            }
        }

        sb.append(")");
        if (!Strings.isNullOrEmpty(this.getComment())) {
            sb.append("\nCOMMENT \"").append(this.getDisplayComment()).append("\"");
        }

        // partition
        PartitionInfo partitionInfo = this.getPartitionInfo();
        if (!partitionInfo.isUnPartitioned()) {
            sb.append("\n").append(partitionInfo.toSql(this, null));
        }

        // distribution
        DistributionInfo distributionInfo = this.getDefaultDistributionInfo();
        sb.append("\n").append(distributionInfo.toSql(this.getIdToColumn()));

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

            // It's invisible
            if (name.equalsIgnoreCase(PropertyAnalyzer.PROPERTY_MV_SORT_KEYS)) {
                continue;
            }
            if (!first) {
                sb.append(",\n");
            }
            first = false;
            if (name.equalsIgnoreCase(PropertyAnalyzer.PROPERTIES_FOREIGN_KEY_CONSTRAINT)) {
                sb.append("\"")
                        .append(PropertyAnalyzer.PROPERTIES_FOREIGN_KEY_CONSTRAINT)
                        .append("\" = \"")
                        .append(ForeignKeyConstraint.getShowCreateTableConstraintDesc(this, getForeignKeyConstraints()))
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
        Set<String> bfColumnNames = getBfColumnNames();
        if (bfColumnNames != null) {
            sb.append(StatsConstants.TABLE_PROPERTY_SEPARATOR).append(PropertyAnalyzer.PROPERTIES_BF_COLUMNS)
                    .append("\" = \"");
            sb.append(Joiner.on(", ").join(bfColumnNames)).append("\"");
        }

        // colocate_with
        String colocateGroup = getColocateGroup();
        if (colocateGroup != null) {
            sb.append(StatsConstants.TABLE_PROPERTY_SEPARATOR).append(PropertyAnalyzer.PROPERTIES_COLOCATE_WITH)
                    .append("\" = \"");
            sb.append(colocateGroup).append("\"");
        }

        // append warehouse
        if (RunMode.getCurrentRunMode() == RunMode.SHARED_DATA) {
            sb.append(",\n");
            sb.append("\"").append(PropertyAnalyzer.PROPERTIES_WAREHOUSE)
                    .append("\" = \"");
            Warehouse warehouse = GlobalStateMgr.getCurrentState().getWarehouseMgr().getWarehouseAllowNull(this.warehouseId);
            if (warehouse != null) {
                sb.append(warehouse.getName()).append("\"");
            } else {
                sb.append("null").append("\"");
            }
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

        if (RunMode.isSharedDataMode()) {
            String sv = GlobalStateMgr.getCurrentState().getStorageVolumeMgr().getStorageVolumeNameOfTable(this.getId());
            propsMap.put(PropertyAnalyzer.PROPERTIES_STORAGE_VOLUME, sv);
        }
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

    public Optional<Expr> getRetentionConditionExpr() {
        return retentionConditionExprOpt;
    }

    public Optional<ScalarOperator> getRetentionConditionScalarOp() {
        return retentionConditionScalarOpOpt;
    }

    /**
     * NOTE: The ref-base-table partition expressions' order is guaranteed as the order of mv's defined partition columns' order.
     * @return table to the partition expr map, multi values if mv contains multi ref base tables, empty if it's un-partitioned
     */
    public Map<Table, List<Expr>> getRefBaseTablePartitionExprs() {
        return refBaseTablePartitionExprsOpt.map(this::refreshBaseTable).orElse(Maps.newHashMap());
    }

    /**
     * Get table to the partition slot ref map of the materialized view.
     * </p>
     * NOTE: The ref-base-table slot refs' order is guaranteed as the order of mv's defined partition columns' order.
     * </p>
     * @return table to the partition slot ref map, multi values if mv contains multi ref base tables, empty if it's
     * un-partitioned
     */
    public Map<Table, List<SlotRef>> getRefBaseTablePartitionSlots() {
        return refBaseTablePartitionSlotsOpt.map(this::refreshBaseTable).orElse(Maps.newHashMap());
    }

    /**
     * Get the related partition table and column of the materialized view since one mv can contain multi ref base tables.
     * NOTE: The ref-base-table columns' order is guaranteed as the order of mv's defined partition columns' order.
     */
    public Map<Table, List<Column>> getRefBaseTablePartitionColumns() {
        return refBaseTablePartitionColumnsOpt.map(this::refreshBaseTable).orElse(Maps.newHashMap());
    }

    /**
     * According base table and materialized view's partition range, we can define those mappings from base to mv:
     * <p>
     * One-to-One
     *  src:     |----|    |----|
     *  dst:     |----|    |----|
     * eg: base table is partitioned by one day, and mv is partition by one day
     * </p>
     * <p>
     * Many-to-One
     *  src:     |----|    |----|     |----|    |----|
     *  dst:     |--------------|     |--------------|
     * eg: base table is partitioned by one day, and mv is partition by date_trunc('month', dt)
     * <p>
     * One/Many-to-Many
     *  src:     |----| |----| |----| |----| |----| |----|
     *  dst:     |--------------| |--------------| |--------------|
     * eg: base table is partitioned by three days, and mv is partition by date_trunc('month', dt)
     *  </p>
     *
     *  For one-to-one or many-to-one we can trigger to refresh by materialized view's partition, but for many-to-many
     *  we need also consider affected materialized view partitions also.
     *
     *  eg:
     *  ref table's partitions:
     *   p0:   [2023-07-27, 2023-07-30)
     *   p1:   [2023-07-30, 2023-08-02)
     *   p2:   [2023-08-02, 2023-08-05)
     *  materialized view's partition:
     *   p0:   [2023-07-01, 2023-08-01)
     *   p1:   [2023-08-01, 2023-09-01)
     *   p2:   [2023-09-01, 2023-10-01)
     *
     *  So ref table's p1 has been changed, materialized view to refresh partition: p0, p1. And when we refresh p0,p1
     *  we also need to consider other ref table partitions(p0); otherwise, the mv's final result will lose data.
     */
    public boolean isCalcPotentialRefreshPartition(List<TableWithPartitions> baseChangedPartitionNames,
                                                   Map<Table, Map<String, PCell>> refBaseTablePartitionToCells,
                                                   Set<String> mvPartitions,
                                                   Map<String, PCell> mvPartitionToCells) {
        List<PRangeCell> mvSortedPartitionRanges =
                TableWithPartitions.getSortedPartitionRanges(mvPartitionToCells, mvPartitions);
        for (TableWithPartitions baseTableWithPartition : baseChangedPartitionNames) {
            Map<String, PCell> baseRangePartitionMap =
                    refBaseTablePartitionToCells.get(baseTableWithPartition.getTable());
            List<PRangeCell> baseSortedPartitionRanges =
                    baseTableWithPartition.getSortedPartitionRanges(baseRangePartitionMap);
            for (PRangeCell basePartitionRange : baseSortedPartitionRanges) {
                if (isManyToManyPartitionRangeMapping(basePartitionRange, mvSortedPartitionRanges)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Whether srcRange is intersected with many dest ranges.
     */
    private boolean isManyToManyPartitionRangeMapping(PRangeCell srcRange,
                                                      List<PRangeCell> dstRanges) {
        int mid = Collections.binarySearch(dstRanges, srcRange);
        if (mid < 0) {
            return false;
        }

        int lower = mid - 1;
        if (lower >= 0 && dstRanges.get(lower).isIntersected(srcRange)) {
            return true;
        }

        int higher = mid + 1;
        if (higher < dstRanges.size() && dstRanges.get(higher).isIntersected(srcRange)) {
            return true;
        }
        return false;
    }

    private Map<Table, List<Column>> getBaseTablePartitionColumnMapImpl() {
        Map<Table, List<Column>> result = Maps.newHashMap();
        if (partitionExprMaps == null || partitionExprMaps.isEmpty()) {
            return result;
        }

        // find the partition column for each base table
        Preconditions.checkArgument(refBaseTablePartitionSlotsOpt.isPresent());
        Map<Table, List<SlotRef>> baseTablePartitionSlotMap = refBaseTablePartitionSlotsOpt.get();
        for (BaseTableInfo baseTableInfo : baseTableInfos) {
            Table baseTable = MvUtils.getTableChecked(baseTableInfo);
            if (!baseTablePartitionSlotMap.containsKey(baseTable)) {
                continue;
            }
            // ref table's partition columns
            List<Column> basePartitionColumns = PartitionUtil.getPartitionColumns(baseTable);
            if (basePartitionColumns.isEmpty()) {
                continue;
            }
            // If the partition column is the same with the slot ref, put it into the result.
            // We can guarantee that output column orders are same with ref-base table's columns.
            List<SlotRef> baseSlotRefs = baseTablePartitionSlotMap.get(baseTable);
            List<Column> refPartitionColumns = Lists.newArrayList();
            for (SlotRef slotRef : baseSlotRefs) {
                Optional<Column> partitionColumnOpt = MvUtils.getColumnBySlotRef(basePartitionColumns, slotRef);
                if (partitionColumnOpt.isEmpty()) {
                    LOG.warn("Partition slot ref {} is not in the base table {}, baseTablePartitionSlotMap:{}", slotRef,
                            baseTable.getName(), baseTablePartitionSlotMap);
                    continue;
                }
                refPartitionColumns.add(partitionColumnOpt.get());
            }
            if (!refPartitionColumns.isEmpty()) {
                result.put(baseTable, refPartitionColumns);
            }
        }
        if (result.isEmpty()) {
            throw new RuntimeException(String.format("Can not find partition column map for mv:%s on base tables:%s", name,
                    MvUtils.formatBaseTableInfos(baseTableInfos)));
        }
        return result;
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
                Optional<Table> optTable = MvUtils.getTable(base);
                if (optTable.isEmpty()) {
                    continue;
                }
                Table table = optTable.get();
                if (table.isNativeTableOrMaterializedView()) {
                    OlapTable olapTable = (OlapTable) table;
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

    /**
     * Return the status and reason about query rewrite
     */
    public String getQueryRewriteStatus() {
        // since check mv valid to rewrite query is a heavy operation, we only check it when it's in the plan cache.
        final MVPlanValidationResult result = MvRewritePreprocessor.isMVValidToRewriteQuery(ConnectContext.get(),
                this, false, Sets.newHashSet(), true);
        switch (result.getStatus()) {
            case VALID:
                return "VALID";
            case INVALID:
                return "INVALID: " + result.getReason();
            default:
                return "UNKNOWN: " + result.getReason();
        }
    }

    @Override
    public Map<String, String> getProperties() {
        Map<String, String> properties = super.getProperties();
        return properties;
    }

    /**
     * Get session properties from materialized view's table property.
     * @return session properties that are ensured to be not null
     */
    public Map<String, String> getSessionProperties() {
        Map<String, String> properties = Maps.newHashMap();
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
                            new ExpressionSerializedObject(partitionExpr.toSql()));
                }
            }
        }
        this.serializedPartitionExprMaps = Maps.newLinkedHashMap();
        if (partitionExprMaps != null) {
            for (Map.Entry<Expr, SlotRef> entry : partitionExprMaps.entrySet()) {
                if (entry.getKey() != null && entry.getValue() != null) {
                    serializedPartitionExprMaps.put(
                            new ExpressionSerializedObject(entry.getKey().toSql()),
                            new ExpressionSerializedObject(entry.getValue().toSql())
                    );
                }
            }
        }
    }

    @Override
    public void gsonPostProcess() throws IOException {
        super.gsonPostProcess();
        List<Column> partitionCols = getPartitionColumns();
        if (CollectionUtils.isEmpty(partitionCols)) {
            return;
        }

        // for single ref base table, recover from serializedPartitionRefTableExprs
        partitionRefTableExprs = new ArrayList<>();
        partitionExprMaps = Maps.newLinkedHashMap();
        if (serializedPartitionRefTableExprs != null) {
            for (ExpressionSerializedObject expressionSql : serializedPartitionRefTableExprs) {
                Expr partitionExpr = parsePartitionExpr(expressionSql.getExpressionSql());
                if (partitionExpr == null) {
                    LOG.warn("parse partition expr failed, sql: {}", expressionSql.getExpressionSql());
                    continue;
                }
                partitionRefTableExprs.add(partitionExpr);
                // for compatibility
                SlotRef partitionSlotRef = getMvPartitionSlotRef(partitionExpr);
                partitionExprMaps.put(partitionExpr, partitionSlotRef);
            }
        }

        // for multi ref base tables, recover from serializedPartitionExprMaps
        if (serializedPartitionExprMaps != null) {
            for (Map.Entry<ExpressionSerializedObject, ExpressionSerializedObject> entry :
                    serializedPartitionExprMaps.entrySet()) {
                if (entry.getKey() != null && entry.getValue() != null) {
                    Expr partitionExpr = parsePartitionExpr(entry.getKey().getExpressionSql());
                    if (partitionExpr == null) {
                        LOG.warn("parse partition expr failed, sql: {}", entry.getKey().getExpressionSql());
                        continue;
                    }
                    SlotRef partitionSlotRef = getMvPartitionSlotRef(partitionExpr);
                    partitionExprMaps.put(partitionExpr, partitionSlotRef);
                }
            }
        }
    }

    /**
     * Analyze partition exprs in mv's structures.
     * NOTE: This method should be only called once in FE restart phase.
     */
    private void analyzePartitionExprs() {
        try {
            // initialize table to base table info cache
            for (BaseTableInfo tableInfo : this.baseTableInfos) {
                this.tableToBaseTableInfoCache.put(MvUtils.getTableChecked(tableInfo), tableInfo);
            }
            // analyze partition exprs for ref base tables
            analyzeRefBaseTablePartitionExprs();
            // analyze partition exprs
            Map<Table, List<Expr>> refBaseTablePartitionExprs = getRefBaseTablePartitionExprs();
            ConnectContext connectContext = ConnectContext.buildInner();
            if (refBaseTablePartitionExprs != null) {
                for (BaseTableInfo baseTableInfo : baseTableInfos) {
                    Optional<Table> refBaseTableOpt = MvUtils.getTable(baseTableInfo);
                    if (refBaseTableOpt.isEmpty()) {
                        continue;
                    }
                    Table refBaseTable = refBaseTableOpt.get();
                    if (!refBaseTablePartitionExprs.containsKey(refBaseTable)) {
                        continue;
                    }
                    List<Expr> partitionExprs = refBaseTablePartitionExprs.get(refBaseTable);
                    TableName tableName = new TableName(baseTableInfo.getCatalogName(),
                            baseTableInfo.getDbName(), baseTableInfo.getTableName());
                    for (Expr partitionExpr : partitionExprs) {
                        analyzePartitionExpr(connectContext, refBaseTable, tableName, partitionExpr);
                    }
                }
            }
            // analyze partition slots for ref base tables
            analyzeRefBaseTablePartitionSlots();
            // analyze partition columns for ref base tables
            analyzeRefBaseTablePartitionColumns();
            // analyze partition retention condition
            analyzeMVRetentionCondition(connectContext);
        } catch (Exception e) {
            LOG.warn("Analyze partition exprs failed", e);
        }
    }

    public synchronized void analyzeMVRetentionCondition(ConnectContext connectContext) {
        PartitionInfo partitionInfo = getPartitionInfo();
        if (partitionInfo.isUnPartitioned()) {
            return;
        }
        String retentionCondition = getTableProperty().getPartitionRetentionCondition();
        if (Strings.isNullOrEmpty(retentionCondition)) {
            return;
        }

        final Map<Table, List<Column>> refBaseTablePartitionColumns = getRefBaseTablePartitionColumns();
        if (refBaseTablePartitionColumns == null || refBaseTablePartitionColumns.size() != 1) {
            return;
        }
        Table refBaseTable = refBaseTablePartitionColumns.keySet().iterator().next();
        this.retentionConditionExprOpt =
                MaterializedViewAnalyzer.analyzeMVRetentionCondition(connectContext, this, refBaseTable, retentionCondition);
        this.retentionConditionScalarOpOpt = MaterializedViewAnalyzer.analyzeMVRetentionConditionOperator(
                connectContext, this, refBaseTable, this.retentionConditionExprOpt);
    }

    /**
     * Since the table is cached in the Optional, needs to refresh it again for each query.
     */
    private <K> Map<Table, K> refreshBaseTable(Map<Table, K> cached) {
        Map<Table, K> result = Maps.newHashMap();
        for (Map.Entry<Table, K> e : cached.entrySet()) {
            Table table = e.getKey();
            if (table instanceof IcebergTable || table instanceof DeltaLakeTable) {
                Preconditions.checkState(tableToBaseTableInfoCache.containsKey(table));
                // TODO: get table from current context rather than metadata catalog
                // it's fine to re-get table from metadata catalog again since metadata catalog should cache
                // the newest table info.
                Table refreshedTable = MvUtils.getTableChecked(tableToBaseTableInfoCache.get(table));
                result.put(refreshedTable, e.getValue());
            } else {
                result.put(table, e.getValue());
            }
        }
        return result;
    }

    private void analyzeRefBaseTablePartitionExprs() {
        Map<Table, List<Expr>> refBaseTablePartitionExprMap = Maps.newHashMap();
        for (BaseTableInfo tableInfo : baseTableInfos) {
            Table table = MvUtils.getTableChecked(tableInfo);
            List<MVPartitionExpr> mvPartitionExprs = MvUtils.getMvPartitionExpr(partitionExprMaps, table);
            if (CollectionUtils.isEmpty(mvPartitionExprs)) {
                LOG.info("Base table {} contains no partition expr, skip", table.getName());
                continue;
            }

            List<Expr> exprs = mvPartitionExprs.stream().map(MVPartitionExpr::getExpr).collect(Collectors.toList());
            refBaseTablePartitionExprMap.put(table, exprs);
        }
        LOG.info("The refBaseTablePartitionExprMap of mv {} is {}", getName(), refBaseTablePartitionExprMap);
        refBaseTablePartitionExprsOpt = Optional.of(refBaseTablePartitionExprMap);
    }

    public void analyzeRefBaseTablePartitionSlots() {
        Map<Table, List<SlotRef>> refBaseTablePartitionSlotMap = Maps.newHashMap();
        Preconditions.checkState(refBaseTablePartitionExprsOpt.isPresent());
        Map<Table, List<Expr>> refBaseTablePartitionExprMap = refBaseTablePartitionExprsOpt.get();
        for (BaseTableInfo tableInfo : baseTableInfos) {
            Table table = MvUtils.getTableChecked(tableInfo);
            List<Expr> mvPartitionExprs = refBaseTablePartitionExprMap.get(table);
            if (CollectionUtils.isEmpty(mvPartitionExprs)) {
                LOG.info("Base table {} contains no partition expr, skip", table.getName());
                continue;
            }
            List<SlotRef> slotRefs = Lists.newArrayList();
            for (Expr expr : mvPartitionExprs) {
                List<SlotRef> exprSlotRefs = expr.collectAllSlotRefs();
                if (exprSlotRefs.size() != 1) {
                    LOG.warn("The partition expr {} of table {} contains more than one slot ref, skip", expr, table.getName());
                    continue;
                }
                slotRefs.add(exprSlotRefs.get(0));
            }
            refBaseTablePartitionSlotMap.put(table, slotRefs);
        }
        LOG.info("The refBaseTablePartitionSlotMap of mv {} is {}", getName(), refBaseTablePartitionSlotMap);
        refBaseTablePartitionSlotsOpt = Optional.of(refBaseTablePartitionSlotMap);
    }

    private void analyzeRefBaseTablePartitionColumns() {
        Map<Table, List<Column>> result = getBaseTablePartitionColumnMapImpl();
        refBaseTablePartitionColumnsOpt = Optional.of(result);
    }

    private void analyzePartitionExpr(ConnectContext connectContext,
                                      Table refBaseTable,
                                      TableName tableName,
                                      Expr partitionExpr) {
        if (partitionExpr == null) {
            return;
        }
        if (tableName == null) {
            return;
        }
        SelectAnalyzer.SlotRefTableNameCleaner visitor = MVUtils.buildSlotRefTableNameCleaner(
                connectContext, refBaseTable, tableName);
        partitionExpr.accept(visitor, null);
        ExpressionAnalyzer.analyzeExpression(partitionExpr, new AnalyzeState(),
                new Scope(RelationId.anonymous(),
                        new RelationFields(refBaseTable.getBaseSchema().stream()
                                .map(col -> new Field(col.getName(), col.getType(),
                                        tableName, null))
                                .collect(Collectors.toList()))), connectContext);

    }

    /**
     * Parse partition expr from sql
     * @param sql serialized partition expr sql
     * @return parsed and unanalyzed partition expr
     */
    private Expr parsePartitionExpr(String sql) {
        if (Strings.isNullOrEmpty(sql)) {
            return null;
        }
        return SqlParser.parseSqlToExpr(sql, SqlModeHelper.MODE_DEFAULT);
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
            setInactiveAndReason(MaterializedViewExceptions.inactiveReasonForBaseInfoMissed());
            return new Status(Status.ErrCode.NOT_FOUND,
                    String.format("Materialized view %s's base info is not found", this.name));
        }

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
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
        if (partitionInfo.isExprRangePartitioned()) {
            ExpressionRangePartitionInfo expressionRangePartitionInfo = (ExpressionRangePartitionInfo) partitionInfo;
            Preconditions.checkState(expressionRangePartitionInfo.getPartitionExprsSize() == 1);
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
            Optional<Table> baseTableOpt = MvUtils.getTableWithIdentifier(baseTableInfo);
            if (baseTableOpt.isEmpty()) {
                continue;
            }
            Table baseTable = baseTableOpt.get();
            baseTable.getRelatedMaterializedViews().remove(log.getMvId());
            baseTable.getRelatedMaterializedViews().add(getMvId());
        }
        setActive();

        // recheck again
        fixRelationship();
    }

    /**
     * `defineQueryParseNode` is safe for multi threads since it is only initialized when mv becomes to active.
     */
    public synchronized ParseNode getDefineQueryParseNode() {
        if (this.defineQueryParseNode == null) {
            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
            if (db == null) {
                return null;
            }
            ConnectContext connectContext = ConnectContext.buildInner();
            if (!Strings.isNullOrEmpty(originalViewDefineSql)) {
                try {
                    String currentDBName = Strings.isNullOrEmpty(originalDBName) ? db.getOriginName() : originalDBName;
                    connectContext.setDatabase(currentDBName);
                    this.defineQueryParseNode = MvUtils.getQueryAst(originalViewDefineSql, connectContext);
                } catch (Exception e) {
                    // ignore
                    LOG.warn("parse original view define sql failed:", e);
                }
            }
            if (this.defineQueryParseNode == null) {
                try {
                    connectContext.setDatabase(db.getOriginName());
                    this.defineQueryParseNode = MvUtils.getQueryAst(viewDefineSql, connectContext);
                } catch (Exception e) {
                    // ignore
                    LOG.warn("parse view define sql failed:", e);
                }
            }
        }
        return this.defineQueryParseNode;
    }

    /**
     * Get mv's ordered columns if the mv has defined its output columns order.
     * @return: mv's defined output columns in the defined order
     */
    public List<Column> getOrderedOutputColumns() {
        if (CollectionUtils.isEmpty(this.queryOutputIndices)) {
            return this.getBaseSchemaWithoutGeneratedColumn();
        } else {
            List<Column> schema = this.getBaseSchemaWithoutGeneratedColumn();
            List<Column> outputColumns = Lists.newArrayList();
            for (Integer index : this.queryOutputIndices) {
                outputColumns.add(schema.get(index));
            }
            return outputColumns;
        }
    }
}
