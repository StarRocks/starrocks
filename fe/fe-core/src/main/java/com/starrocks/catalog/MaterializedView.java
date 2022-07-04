// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.catalog;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.DescriptorTable.ReferencedPartitionInfo;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.UserIdentity;
import com.starrocks.common.io.DeepCopy;
import com.starrocks.common.io.Text;
import com.starrocks.mysql.privilege.Auth;
import com.starrocks.persist.gson.GsonPostProcessable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.MaterializedViewAnalyzer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.parser.ParsingException;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TTableDescriptor;
import com.starrocks.thrift.TTableType;
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

/**
 * meta structure for materialized view
 */
public class MaterializedView extends OlapTable implements GsonPostProcessable {
    private static final Logger LOG = LogManager.getLogger(MaterializedView.class);

    public enum RefreshType {
        SYNC,
        ASYNC,
        MANUAL
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
    }

    public static class AsyncRefreshContext {
        // base table id -> (partition name -> partition info (id, version))
        // partition id maybe changed after insert overwrite, so use partition name as key.
        // partition id which in BasePartitionInfo can be used to check partition is changed
        @SerializedName(value = "baseTableVisibleVersionMap")
        private Map<Long, Map<String, BasePartitionInfo>> baseTableVisibleVersionMap;

        @SerializedName(value = "starTime")
        private long startTime;

        @SerializedName(value = "step")
        private long step;

        @SerializedName(value = "timeUnit")
        private String timeUnit;

        public AsyncRefreshContext() {
            this.baseTableVisibleVersionMap = Maps.newHashMap();
            this.startTime = Utils.getLongFromDateTime(LocalDateTime.now());
            this.step = 0;
            this.timeUnit = null;
        }

        public AsyncRefreshContext(Map<Long, Map<String, BasePartitionInfo>> baseTableVisibleVersionMap) {
            this.baseTableVisibleVersionMap = baseTableVisibleVersionMap;
        }

        public Map<Long, Map<String, BasePartitionInfo>> getBaseTableVisibleVersionMap() {
            return baseTableVisibleVersionMap;
        }

        Map<String, BasePartitionInfo> getPartitionVisibleVersionMapForTable(long tableId) {
            return baseTableVisibleVersionMap.get(tableId);
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

        public MvRefreshScheme(RefreshType type, AsyncRefreshContext asyncRefreshContext, long lastRefreshTime) {
            this.type = type;
            this.asyncRefreshContext = asyncRefreshContext;
            this.lastRefreshTime = lastRefreshTime;
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

    @SerializedName(value = "active")
    private boolean active;

    // TODO: now it is original definition sql
    // for show create mv, constructing refresh job(insert into select)
    @SerializedName(value = "viewDefineSql")
    private String viewDefineSql;
    // table partition name <-> mv partition names
    @SerializedName(value = "tableMvPartitionNameRefMap")
    private Map<String, Set<String>> tableMvPartitionNameRefMap = new HashMap<>();
    // mv partition name <->  table partition names
    @SerializedName(value = "mvTablePartitionNameRefMap")
    private Map<String, Set<String>> mvTablePartitionNameRefMap = new HashMap<>();

    // record expression table column
    @SerializedName(value = "partitionRefTableExprs")
    private List<Expr> partitionRefTableExprs;

    public MaterializedView() {
        super(TableType.MATERIALIZED_VIEW);
        this.clusterId = GlobalStateMgr.getCurrentState().getClusterId();
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

    public long getDbId() {
        return dbId;
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

    public Set<Long> getBaseTableIds() {
        return baseTableIds;
    }

    public void setBaseTableIds(Set<Long> baseTableIds) {
        this.baseTableIds = baseTableIds;
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

    public void addPartitionIdRef(String basePartitionName, String mvPartitionName) {
        tableMvPartitionNameRefMap.computeIfAbsent(basePartitionName, k -> Sets.newHashSet()).add(mvPartitionName);
        mvTablePartitionNameRefMap.computeIfAbsent(mvPartitionName, k -> Sets.newHashSet()).add(basePartitionName);
    }

    public void dropPartitionIdRef(String mvPartitionName) {
        Set<String> basePartitionNames = mvTablePartitionNameRefMap.get(mvPartitionName);
        for (String basePartitionName : basePartitionNames) {
            tableMvPartitionNameRefMap.get(basePartitionName).remove(mvPartitionName);
        }
        mvTablePartitionNameRefMap.remove(mvPartitionName);
    }

    public Set<String> getMvPartitionNameByTable(String tablePartitionName) {
        return tableMvPartitionNameRefMap.get(tablePartitionName);
    }

    public Set<String> getTablePartitionNameByMv(String mvPartitionName) {
        return mvTablePartitionNameRefMap.get(mvPartitionName);
    }

    public Set<String> getExistBasePartitionNames(long baseTableId) {
        return this.getRefreshScheme().getAsyncRefreshContext().getBaseTableVisibleVersionMap()
                .computeIfAbsent(baseTableId, k -> Maps.newHashMap()).keySet();
    }

    public Set<String> getNoExistBasePartitionNames(long baseTableId, Set<String> partitionNames) {
        Map<String, BasePartitionInfo> basePartitionInfoMap = this.getRefreshScheme().getAsyncRefreshContext()
                .getBaseTableVisibleVersionMap()
                .computeIfAbsent(baseTableId, k -> Maps.newHashMap());
        return basePartitionInfoMap.keySet().stream()
                .filter(partitionName -> !partitionNames.contains(partitionName))
                .collect(Collectors.toSet());
    }

    public boolean needRefreshPartition(long baseTableId, Partition baseTablePartition) {
        BasePartitionInfo basePartitionInfo = this.getRefreshScheme().getAsyncRefreshContext()
                .getBaseTableVisibleVersionMap()
                .computeIfAbsent(baseTableId, k -> Maps.newHashMap())
                .get(baseTablePartition.getName());
        if (basePartitionInfo == null
                || basePartitionInfo.getId() != baseTablePartition.getId()
                || baseTablePartition.getVisibleVersion() > basePartitionInfo.getVersion()) {
            return true;
        }
        return false;
    }

    public boolean needAddBasePartition(long baseTableId, Partition baseTablePartition) {
        Map<String, BasePartitionInfo> basePartitionInfoMap = this.getRefreshScheme().getAsyncRefreshContext()
                .getBaseTableVisibleVersionMap()
                .computeIfAbsent(baseTableId, k -> Maps.newHashMap());
        return basePartitionInfoMap.get(baseTablePartition.getName()) == null;
    }

    public void addOrUpdateBasePartition(long baseTableId, Partition baseTablePartition) {
        Map<String, BasePartitionInfo> basePartitionInfoMap = this.getRefreshScheme().getAsyncRefreshContext()
                .getBaseTableVisibleVersionMap()
                .computeIfAbsent(baseTableId, k -> Maps.newHashMap());
        basePartitionInfoMap.put(baseTablePartition.getName(),
                new BasePartitionInfo(baseTablePartition.getId(), baseTablePartition.getVisibleVersion()));
    }

    public void removeBasePartition(long baseTableId, String baseTablePartitionName) {
        Map<String, BasePartitionInfo> basePartitionInfoMap = this.getRefreshScheme().getAsyncRefreshContext()
                .getBaseTableVisibleVersionMap()
                .computeIfAbsent(baseTableId, k -> Maps.newHashMap());
        basePartitionInfoMap.remove(baseTablePartitionName);
    }

    @Override
    public TTableDescriptor toThrift(List<ReferencedPartitionInfo> partitions) {
        TTableDescriptor tTableDescriptor = new TTableDescriptor(id, TTableType.MATERIALIZED_VIEW,
                fullSchema.size(), 0, getName(), "");
        return tTableDescriptor;
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
    public void gsonPostProcess() throws IOException {
        super.gsonPostProcess();

        Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
        if (db == null) {
            LOG.warn("db:{} do not exist. materialized view id:{} name:{} should not exist", dbId, id, name);
            active = false;
            return;
        }
        // register materialized view to base tables
        for (long tableId : baseTableIds) {
            Table table = db.getTable(tableId);
            if (table == null) {
                LOG.warn("tableId:{} do not exist. set materialized view:{} to invalid", tableId, id);
                active = false;
                continue;
            }
            // now table must be OlapTable
            // it is checked when creation
            ((OlapTable) table).addRelatedMaterializedView(id);
        }
        // analyze expression, because it converts to sql for serialize
        ConnectContext connectContext = new ConnectContext();
        connectContext.setCluster(SystemInfoService.DEFAULT_CLUSTER);
        connectContext.setDatabase(db.getFullName());
        // set privilege
        connectContext.setQualifiedUser(Auth.ROOT_USER);
        connectContext.setCurrentUserIdentity(UserIdentity.ROOT);
        PartitionInfo partitionInfo = this.getPartitionInfo();
        if (partitionInfo instanceof SinglePartitionInfo) {
            return;
        }
        ExpressionRangePartitionInfo expressionRangePartitionInfo = (ExpressionRangePartitionInfo) partitionInfo;
        // currently, mv only supports one expression
        Expr partitionExpr = expressionRangePartitionInfo.getPartitionExprs().get(0);
        try {
            QueryStatement queryStatement = ((QueryStatement) SqlParser.parse(
                    this.viewDefineSql, connectContext.getSessionVariable().getSqlMode()).get(0));
            Analyzer.analyze(queryStatement, connectContext);
            MaterializedViewAnalyzer.analyzeExp(partitionExpr, queryStatement, connectContext);
        } catch (ParsingException parsingException) {
            LOG.warn("Parsing viewDefineSql:{} failed, exception:{}", this.viewDefineSql, parsingException.getMessage());
        } catch (SemanticException semanticException) {
            LOG.warn("Analyzing viewDefineSql:{} failed, exception:{}", this.viewDefineSql, semanticException.getMessage());
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // write type first
        Text.writeString(out, type.name());
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static MaterializedView read(DataInput in) throws IOException {
        String json = Text.readString(in);
        MaterializedView mv = GsonUtils.GSON.fromJson(json, MaterializedView.class);
        return mv;
    }
}
