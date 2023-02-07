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
import com.starrocks.analysis.StringLiteral;
import com.starrocks.analysis.TableName;
import com.starrocks.analysis.UserIdentity;
import com.starrocks.authentication.AuthenticationManager;
import com.starrocks.common.Pair;
import com.starrocks.common.UserException;
import com.starrocks.common.io.DeepCopy;
import com.starrocks.common.io.Text;
import com.starrocks.common.util.DateUtils;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.persist.gson.GsonPostProcessable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AnalyzeState;
import com.starrocks.sql.analyzer.ExpressionAnalyzer;
import com.starrocks.sql.analyzer.Field;
import com.starrocks.sql.analyzer.RelationFields;
import com.starrocks.sql.analyzer.RelationId;
import com.starrocks.sql.analyzer.Scope;
import com.starrocks.sql.common.PartitionDiff;
import com.starrocks.sql.common.SyncPartitionUtils;
import com.starrocks.sql.common.UnsupportedException;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.statistic.StatsConstants;
import com.starrocks.thrift.TTableDescriptor;
import com.starrocks.thrift.TTableType;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.server.CatalogMgr.isInternalCatalog;

/**
 * meta structure for materialized view
 */
public class MaterializedView extends OlapTable implements GsonPostProcessable {
    private static final Logger LOG = LogManager.getLogger(MaterializedView.class);


    public enum RefreshType {
        SYNC,
        ASYNC,
        MANUAL,
        INCREMENTAL
    }

    public static class BaseTableInfo {
        @SerializedName(value = "catalogName")
        private final String catalogName;

        @SerializedName(value = "dbId")
        private long dbId = -1;

        @SerializedName(value = "tableId")
        private long tableId = -1;

        @SerializedName(value = "dbName")
        private String dbName;

        @SerializedName(value = "tableIdentifier")
        private String tableIdentifier;

        @SerializedName(value = "tableName")
        private String tableName;

        public BaseTableInfo(long dbId, String dbName, long tableId) {
            this.catalogName = InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME;
            this.dbId = dbId;
            this.dbName = dbName;
            this.tableId = tableId;
        }

        public BaseTableInfo(long dbId, long tableId) {
            this(dbId, null, tableId);
        }

        public BaseTableInfo(String catalogName, String dbName, String tableIdentifier) {
            this.catalogName = catalogName;
            this.dbName = dbName;
            this.tableIdentifier = tableIdentifier;
            this.tableName = tableIdentifier.split(":")[0];
        }

        public String getTableInfoStr() {
            if (isInternalCatalog(catalogName)) {
                return Joiner.on(".").join(dbId, tableId);
            } else {
                return Joiner.on(".").join(catalogName, dbName, tableName);
            }
        }

        public String getDbInfoStr() {
            if (isInternalCatalog(catalogName)) {
                return String.valueOf(dbId);
            } else {
                return Joiner.on(".").join(catalogName, dbName);
            }
        }

        public String getCatalogName() {
            return this.catalogName;
        }

        public String getDbName() {
            return this.dbName != null ? this.dbName : getDb().getFullName();
        }

        public String getTableName() {
            if (this.tableName != null) {
                return this.tableName;
            } else {
                Table table = getTable();
                return table == null ? null : table.getName();
            }
        }

        public String getTableIdentifier() {
            return this.tableIdentifier;
        }

        public long getDbId() {
            return this.dbId;
        }

        public long getTableId() {
            return this.tableId;
        }

        public Table getTable() {
            if (isInternalCatalog(catalogName)) {
                Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
                if (db == null) {
                    return null;
                } else {
                    return db.getTable(tableId);
                }
            } else {
                if (!GlobalStateMgr.getCurrentState().getCatalogMgr().catalogExists(catalogName)) {
                    LOG.warn("catalog {} not exist", catalogName);
                    return null;
                }
                Table table = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(catalogName, dbName, tableName);
                if (table == null) {
                    LOG.warn("table {}.{}.{} not exist", catalogName, dbName, tableName);
                    return null;
                }
                if (table.getTableIdentifier().equals(tableIdentifier)) {
                    return table;
                }
                return null;
            }
        }

        public Database getDb() {
            if (isInternalCatalog(catalogName)) {
                return GlobalStateMgr.getCurrentState().getDb(dbId);
            } else {
                return GlobalStateMgr.getCurrentState().getMetadataMgr().getDb(catalogName, dbName);
            }
        }
    }

    public static class BasePartitionInfo {

        @SerializedName(value = "id")
        private long id;

        @SerializedName(value = "version")
        private long version;

        public BasePartitionInfo(long id, long version) {
            this.id = id;
            this.version = version;
        }

        public long getId() {
            return id;
        }

        public void setId(long id) {
            this.id = id;
        }

        public long getVersion() {
            return version;
        }

        public void setVersion(long version) {
            this.version = version;
        }

        @Override
        public String toString() {
            return "BasePartitionInfo{" +
                    "id=" + id +
                    ", version=" + version +
                    '}';
        }
    }

    public static class AsyncRefreshContext {
        // base table id -> (partition name -> partition info (id, version))
        // partition id maybe changed after insert overwrite, so use partition name as key.
        // partition id which in BasePartitionInfo can be used to check partition is changed
        @SerializedName("baseTableVisibleVersionMap")
        private final Map<Long, Map<String, BasePartitionInfo>> baseTableVisibleVersionMap;

        @SerializedName(value = "defineStartTime")
        private boolean defineStartTime;

        @SerializedName(value = "starTime")
        private long startTime;

        @SerializedName(value = "step")
        private long step;

        @SerializedName(value = "timeUnit")
        private String timeUnit;

        public AsyncRefreshContext() {
            this.baseTableVisibleVersionMap = Maps.newHashMap();
            this.defineStartTime = false;
            this.startTime = Utils.getLongFromDateTime(LocalDateTime.now());
            this.step = 0;
            this.timeUnit = null;
        }

        public Map<Long, Map<String, BasePartitionInfo>> getBaseTableVisibleVersionMap() {
            return baseTableVisibleVersionMap;
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
    }

    public static class MvRefreshScheme {
        @SerializedName(value = "type")
        private RefreshType type;
        // when type is ASYNC
        // asyncRefreshContext is used to store refresh context
        @SerializedName(value = "asyncRefreshContext")
        private AsyncRefreshContext asyncRefreshContext;
        @SerializedName(value = "lastRefreshTime")
        private long lastRefreshTime;

        public MvRefreshScheme() {
            this.type = RefreshType.ASYNC;
            this.asyncRefreshContext = new AsyncRefreshContext();
            this.lastRefreshTime = 0;
        }

        public boolean isIncremental() {
            return this.type.equals(RefreshType.INCREMENTAL);
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

    // TODO: now it is original definition sql
    // for show create mv, constructing refresh job(insert into select)
    @SerializedName(value = "viewDefineSql")
    private String viewDefineSql;

    @SerializedName(value = "simpleDefineSql")
    private String simpleDefineSql;

    // record expression table column
    @SerializedName(value = "partitionRefTableExprs")
    private List<Expr> partitionRefTableExprs;

    // Maintenance plan for this MV
    private transient ExecPlan maintenancePlan;

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

    public void setActive(boolean active) {
        this.active = active;
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

    public List<BaseTableInfo> getBaseTableInfos() {
        return baseTableInfos;
    }

    public void setBaseTableInfos(List<BaseTableInfo> baseTableInfos) {
        this.baseTableInfos = baseTableInfos;
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

    public Set<String> getUpdatedPartitionNamesOfTable(Table base) {
        return getUpdatedPartitionNamesOfTable(base, false);
    }
    public Set<String> getUpdatedPartitionNamesOfTable(Table base, boolean withMv) {
        if (!base.isLocalTable()) {
            // TODO(ywb): support external table refresh according to partition later
            return Sets.newHashSet();
        }

        OlapTable baseTable = (OlapTable) base;
        Map<String, BasePartitionInfo> baseTableVisibleVersionMap = getRefreshScheme()
                .getAsyncRefreshContext()
                .getBaseTableVisibleVersionMap()
                .computeIfAbsent(baseTable.getId(), k -> Maps.newHashMap());
        Set<String> result = Sets.newHashSet();
        // check whether there are partitions added and have data
        for (String partitionName : baseTable.getPartitionNames()) {
            if (!baseTableVisibleVersionMap.containsKey(partitionName)
                    && baseTable.getPartition(partitionName).getVisibleVersion() != 1) {
                result.add(partitionName);
            }
        }

        for (Map.Entry<String, BasePartitionInfo> versionEntry : baseTableVisibleVersionMap.entrySet()) {
            String basePartitionName = versionEntry.getKey();
            Partition basePartition = baseTable.getPartition(basePartitionName);
            if (basePartition == null) {
                // partitions deleted
                result.addAll(baseTable.getPartitionNames());
                return result;
            }
            BasePartitionInfo basePartitionInfo = versionEntry.getValue();
            if (basePartitionInfo == null
                    || basePartitionInfo.getId() != basePartition.getId()
                    || basePartition.getVisibleVersion() > basePartitionInfo.getVersion()) {
                result.add(basePartitionName);
            }
        }
        if (withMv && baseTable.isMaterializedView()) {
            Set<String> partitionNames = ((MaterializedView) baseTable).getPartitionNamesToRefreshForMv();
            result.addAll(partitionNames);
        }
        return result;
    }

    @Override
    public TTableDescriptor toThrift(List<ReferencedPartitionInfo> partitions) {
        return new TTableDescriptor(id, TTableType.MATERIALIZED_VIEW,
                fullSchema.size(), 0, getName(), "");
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

    @Override
    public void onCreate() {
        Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
        if (db == null) {
            LOG.warn("db:{} do not exist. materialized view id:{} name:{} should not exist", dbId, id, name);
            active = false;
            return;
        }
        if (baseTableInfos == null) {
            baseTableInfos = Lists.newArrayList();
            if (baseTableIds != null) {
                // for compatibility
                for (long tableId : baseTableIds) {
                    baseTableInfos.add(new BaseTableInfo(dbId, db.getFullName(), tableId));
                }
            } else {
                active = false;
                return;
            }
        }

        for (MaterializedView.BaseTableInfo baseTableInfo : baseTableInfos) {
            // Do not set the active when table is null, it would be checked in MVActiveChecker
            Table table = baseTableInfo.getTable();
            if (table != null) {
                if (table instanceof MaterializedView && !((MaterializedView) table).isActive()) {
                    LOG.warn("tableName :{} is invalid. set materialized view:{} to invalid",
                            baseTableInfo.getTableName(), id);
                    active = false;
                    continue;
                }
                MvId mvId = new MvId(db.getId(), id);
                table.addRelatedMaterializedView(mvId);
            }
        }
        analyzePartitionInfo();
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
        connectContext.setQualifiedUser(AuthenticationManager.ROOT_USER);
        connectContext.setCurrentUserIdentity(UserIdentity.ROOT);
        ExpressionRangePartitionInfo expressionRangePartitionInfo = (ExpressionRangePartitionInfo) partitionInfo;
        // currently, mv only supports one expression
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
                            new SlotDescriptor(new SlotId(i), column.getName(), column.getType(), column.isAllowNull());
                    slotRefs.get(0).setDesc(slotDescriptor);
                }
            }
        }

        ExpressionAnalyzer.analyzeExpression(partitionExpr, new AnalyzeState(),
                new Scope(RelationId.anonymous(),
                        new RelationFields(this.getBaseSchema().stream()
                                .map(col -> new Field(col.getName(), col.getType(),
                                        new TableName(db.getFullName(), this.name), null))
                                .collect(Collectors.toList()))), connectContext);
    }

    public static MaterializedView read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, MaterializedView.class);
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

    public boolean isForceExternalTableQueryRewrite() {
        return tableProperty.getForceExternalTableQueryRewrite();
    }

    public boolean shouldTriggeredRefreshBy(String dbName, String tableName) {
        if (!isLoadTriggeredRefresh()) {
            return false;
        }
        TableProperty tableProperty = getTableProperty();
        if (tableProperty == null) {
            return true;
        }
        List<TableName> excludedTriggerTables =  tableProperty.getExcludedTriggerTables();
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

    public String getMaterializedViewDdlStmt(boolean simple) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE MATERIALIZED VIEW `").append(this.getName()).append("`");
        if (!Strings.isNullOrEmpty(this.getComment())) {
            sb.append("\nCOMMENT \"").append(this.getComment()).append("\"");
        }

        // partition
        PartitionInfo partitionInfo = this.getPartitionInfo();
        if (!(partitionInfo instanceof SinglePartitionInfo)) {
            sb.append("\n").append(partitionInfo.toSql(this, null));
        }

        // distribution
        DistributionInfo distributionInfo = this.getDefaultDistributionInfo();
        sb.append("\n").append(distributionInfo.toSql());

        // refresh scheme
        MvRefreshScheme refreshScheme = this.getRefreshScheme();
        if (refreshScheme == null) {
            sb.append("\nREFRESH ").append("UNKNOWN");
        } else {
            sb.append("\nREFRESH ").append(refreshScheme.getType());
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

        // replicationNum
        Short replicationNum = this.getDefaultReplicationNum();
        sb.append("\"").append(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM).append("\" = \"");
        sb.append(replicationNum).append("\"");

        // storageMedium
        String storageMedium = this.getStorageMedium();
        sb.append(StatsConstants.TABLE_PROPERTY_SEPARATOR).append(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM)
                .append("\" = \"");
        sb.append(storageMedium).append("\"");

        // storageCooldownTime
        Map<String, String> properties = this.getTableProperty().getProperties();
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_STORAGE_COOLDOWN_TIME)) {
            sb.append(StatsConstants.TABLE_PROPERTY_SEPARATOR).append(PropertyAnalyzer.PROPERTIES_STORAGE_COOLDOWN_TIME)
                    .append("\" = \"");
            sb.append(TimeUtils.longToTimeString(
                    Long.parseLong(properties.get(PropertyAnalyzer.PROPERTIES_STORAGE_COOLDOWN_TIME)))).append("\"");
        }

        // partition TTL
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_PARTITION_TTL_NUMBER)) {
            sb.append(StatsConstants.TABLE_PROPERTY_SEPARATOR).append(PropertyAnalyzer.PROPERTIES_PARTITION_TTL_NUMBER)
                    .append("\" = \"");
            sb.append(properties.get(PropertyAnalyzer.PROPERTIES_PARTITION_TTL_NUMBER)).append("\"");
        }

        // auto refresh partitions limit
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_AUTO_REFRESH_PARTITIONS_LIMIT)) {
            sb.append(StatsConstants.TABLE_PROPERTY_SEPARATOR).append(PropertyAnalyzer.PROPERTIES_AUTO_REFRESH_PARTITIONS_LIMIT)
                    .append("\" = \"");
            sb.append(properties.get(PropertyAnalyzer.PROPERTIES_AUTO_REFRESH_PARTITIONS_LIMIT)).append("\"");
        }

        // partition refresh number
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_PARTITION_REFRESH_NUMBER)) {
            sb.append(StatsConstants.TABLE_PROPERTY_SEPARATOR).append(PropertyAnalyzer.PROPERTIES_PARTITION_REFRESH_NUMBER)
                    .append("\" = \"");
            sb.append(properties.get(PropertyAnalyzer.PROPERTIES_PARTITION_REFRESH_NUMBER)).append("\"");
        }

        // excluded trigger tables
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_EXCLUDED_TRIGGER_TABLES)) {
            sb.append(StatsConstants.TABLE_PROPERTY_SEPARATOR).append(PropertyAnalyzer.PROPERTIES_EXCLUDED_TRIGGER_TABLES)
                    .append("\" = \"");
            sb.append(properties.get(PropertyAnalyzer.PROPERTIES_EXCLUDED_TRIGGER_TABLES)).append("\"");
        }

        // force_external_table_query_rewrite
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_FORCE_EXTERNAL_TABLE_QUERY_REWRITE)) {
            sb.append(StatsConstants.TABLE_PROPERTY_SEPARATOR).append(
                    PropertyAnalyzer.PROPERTIES_FORCE_EXTERNAL_TABLE_QUERY_REWRITE).append("\" = \"");
            sb.append(properties.get(PropertyAnalyzer.PROPERTIES_FORCE_EXTERNAL_TABLE_QUERY_REWRITE)).append("\"");
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

    public Set<String> getPartitionNamesToRefreshForMv() {
        PartitionInfo partitionInfo = getPartitionInfo();

        boolean forceExternalTableQueryRewrite = isForceExternalTableQueryRewrite();
        if (partitionInfo instanceof SinglePartitionInfo) {
            // for non-partitioned materialized view
            for (BaseTableInfo tableInfo : baseTableInfos) {
                Table table = tableInfo.getTable();

                // we can not judge whether mv based on external table is update-to-date,
                // because we do not know that any changes in external table.
                if (!table.isLocalTable()) {
                    if (forceExternalTableQueryRewrite) {
                        // if forceExternalTableQueryRewrite set to true, no partition need to refresh for mv.
                        continue;
                    } else {
                        return getPartitionNames();
                    }
                }
                Set<String> partitionNames = getUpdatedPartitionNamesOfTable(table, true);
                if (!partitionNames.isEmpty()) {
                    return getPartitionNames();
                }
            }
        } else if (partitionInfo instanceof ExpressionRangePartitionInfo) {
            // partitions to refresh
            // 1. dropped partitions
            // 2. newly added partitions
            // 3. partitions loaded with new data
            return getPartitionNamesToRefreshForPartitionedMv();
        } else {
            throw UnsupportedException.unsupportedException("unsupported partition info type:"
                    + partitionInfo.getClass().getName());
        }
        return Sets.newHashSet();
    }

    private Set<String> getPartitionNamesToRefreshForPartitionedMv() {
        Expr partitionExpr = getPartitionRefTableExprs().get(0);
        Pair<Table, Column> partitionInfo = getPartitionTableAndColumn();
        // if non-partition-by table has changed, should refresh all mv partitions
        Table partitionTable = partitionInfo.first;
        boolean forceExternalTableQueryRewrite = isForceExternalTableQueryRewrite();
        for (BaseTableInfo tableInfo : baseTableInfos) {
            Table table = tableInfo.getTable();
            if (!table.isLocalTable()) {
                if (forceExternalTableQueryRewrite) {
                    // if forceExternalTableQueryRewrite set to true, no partition need to refresh for mv.
                    continue;
                } else {
                    return getPartitionNames();
                }
            }
            if (table.getTableIdentifier().equals(partitionTable.getTableIdentifier())) {
                continue;
            }
            Set<String> partitionNames = getUpdatedPartitionNamesOfTable(table, true);
            if (!partitionNames.isEmpty()) {
                return getPartitionNames();
            }
        }
        // check partition-by table
        Set<String> needRefreshMvPartitionNames = Sets.newHashSet();
        Map<String, Range<PartitionKey>> basePartitionMap;
        try {
            basePartitionMap = PartitionUtil.getPartitionRange(partitionTable,
                    partitionInfo.second);
        } catch (UserException e) {
            LOG.warn("Materialized view compute partition difference with base table failed.", e);
            return getPartitionNames();
        }
        Map<String, Range<PartitionKey>> mvPartitionMap = getRangePartitionMap();
        PartitionDiff partitionDiff = getPartitionDiff(partitionExpr, partitionInfo.second,
                basePartitionMap, mvPartitionMap);
        needRefreshMvPartitionNames.addAll(partitionDiff.getDeletes().keySet());
        for (String deleted : partitionDiff.getDeletes().keySet()) {
            mvPartitionMap.remove(deleted);
        }
        needRefreshMvPartitionNames.addAll(partitionDiff.getAdds().keySet());
        for (Map.Entry<String, Range<PartitionKey>> addEntry : partitionDiff.getAdds().entrySet()) {
            mvPartitionMap.put(addEntry.getKey(), addEntry.getValue());
        }

        Map<String, Set<String>> baseToMvNameRef = SyncPartitionUtils
                .generatePartitionRefMap(basePartitionMap, mvPartitionMap);
        Map<String, Set<String>> mvToBaseNameRef = SyncPartitionUtils
                .generatePartitionRefMap(mvPartitionMap, basePartitionMap);

        Set<String> baseChangedPartitionNames = getUpdatedPartitionNamesOfTable(partitionTable, true);
        if (partitionExpr instanceof SlotRef) {
            for (String basePartitionName : baseChangedPartitionNames) {
                needRefreshMvPartitionNames.addAll(baseToMvNameRef.get(basePartitionName));
            }
        } else if (partitionExpr instanceof FunctionCallExpr) {
            for (String baseChangedPartitionName : baseChangedPartitionNames) {
                needRefreshMvPartitionNames.addAll(baseToMvNameRef.get(baseChangedPartitionName));
            }
            // because the relation of partitions between materialized view and base partition table is n : m,
            // should calculate the candidate partitions recursively.
            SyncPartitionUtils.calcPotentialRefreshPartition(needRefreshMvPartitionNames, baseChangedPartitionNames,
                    baseToMvNameRef, mvToBaseNameRef);
        }
        return needRefreshMvPartitionNames;
    }

    private PartitionDiff getPartitionDiff(Expr partitionExpr, Column partitionColumn,
                                           Map<String, Range<PartitionKey>> basePartitionMap,
                                           Map<String, Range<PartitionKey>> mvPartitionMap) {
        if (partitionExpr instanceof SlotRef) {
            return SyncPartitionUtils.calcSyncSamePartition(basePartitionMap, mvPartitionMap);
        } else if (partitionExpr instanceof FunctionCallExpr) {
            FunctionCallExpr functionCallExpr = (FunctionCallExpr) partitionExpr;
            String granularity = ((StringLiteral) functionCallExpr.getChild(0)).getValue().toLowerCase();
            return SyncPartitionUtils.calcSyncRollupPartition(basePartitionMap, mvPartitionMap,
                    granularity, partitionColumn.getPrimitiveType());
        } else {
            throw UnsupportedException.unsupportedException("unsupported partition expr:" + partitionExpr);
        }
    }

    public Pair<Table, Column> getPartitionTableAndColumn() {
        if (!(partitionInfo instanceof ExpressionRangePartitionInfo)) {
            return null;
        }
        Expr partitionExpr = getPartitionRefTableExprs().get(0);
        List<SlotRef> slotRefs = Lists.newArrayList();
        partitionExpr.collect(SlotRef.class, slotRefs);
        Preconditions.checkState(slotRefs.size() == 1);
        SlotRef partitionSlotRef = slotRefs.get(0);
        for (BaseTableInfo baseTableInfo : baseTableInfos) {
            Table table = baseTableInfo.getTable();
            if (partitionSlotRef.getTblNameWithoutAnalyzed().getTbl().equals(table.getName())) {
                return Pair.create(table, table.getColumn(partitionSlotRef.getColumnName()));
            }
        }
        return null;
    }

    public ExecPlan getMaintenancePlan() {
        return maintenancePlan;
    }

    public void setMaintenancePlan(ExecPlan maintenancePlan) {
        this.maintenancePlan = maintenancePlan;
    }
}
