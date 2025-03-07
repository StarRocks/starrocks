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

package com.starrocks.sql.optimizer.function;

import com.google.gson.JsonArray;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.TableName;
import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.authorization.ObjectType;
import com.starrocks.authorization.PrivilegeType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvId;
import com.starrocks.catalog.MvPlanContext;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.connector.PartitionInfo;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.connector.hive.Partition;
import com.starrocks.memory.MemoryTrackable;
import com.starrocks.memory.MemoryUsageTracker;
import com.starrocks.monitor.unit.ByteSizeValue;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SimpleExecutor;
import com.starrocks.scheduler.TaskRunManager;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.CachingMvPlanContextBuilder;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.dump.QueryDumper;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.rewrite.ConstantFunction;
import com.starrocks.thrift.TResultBatch;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.collections4.SetUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.util.SizeEstimator;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.starrocks.catalog.PrimitiveType.BOOLEAN;
import static com.starrocks.catalog.PrimitiveType.VARCHAR;

/**
 * Meta functions can be used to inspect the content of in-memory structures, for debug purpose.
 */
public class MetaFunctions {

    public static Table inspectExternalTable(TableName tableName) {
        Table table = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(tableName)
                .orElseThrow(() -> ErrorReport.buildSemanticException(ErrorCode.ERR_BAD_TABLE_ERROR, tableName));
        ConnectContext connectContext = ConnectContext.get();
        try {
            Authorizer.checkAnyActionOnTable(connectContext, tableName);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    tableName.getCatalog(),
                    connectContext.getCurrentUserIdentity(), connectContext.getCurrentRoleIds(),
                    PrivilegeType.ANY.name(), ObjectType.TABLE.name(), tableName.getTbl());
        }
        return table;
    }

    public static Pair<Database, Table> inspectTable(TableName tableName) {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().mayGetDb(tableName.getDb())
                .orElseThrow(() -> ErrorReport.buildSemanticException(ErrorCode.ERR_BAD_DB_ERROR, tableName.getDb()));
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().mayGetTable(tableName.getDb(), tableName.getTbl())
                .orElseThrow(() -> ErrorReport.buildSemanticException(ErrorCode.ERR_BAD_TABLE_ERROR, tableName));
        ConnectContext connectContext = ConnectContext.get();
        try {
            Authorizer.checkAnyActionOnTable(
                    connectContext,
                    tableName);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    tableName.getCatalog(),
                    connectContext.getCurrentUserIdentity(), connectContext.getCurrentRoleIds(),
                    PrivilegeType.ANY.name(), ObjectType.TABLE.name(), tableName.getTbl());
        }
        return Pair.of(db, table);
    }

    private static void authOperatorPrivilege() {
        ConnectContext connectContext = ConnectContext.get();
        try {
            Authorizer.checkSystemAction(
                    connectContext,
                    PrivilegeType.OPERATE);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    connectContext.getCurrentUserIdentity(), connectContext.getCurrentRoleIds(),
                    PrivilegeType.OPERATE.name(), ObjectType.SYSTEM.name(), null);
        }
    }

    /**
     * Return verbose metadata of a materialized-view
     */
    @ConstantFunction(name = "inspect_mv_meta", argTypes = {VARCHAR}, returnType = VARCHAR, isMetaFunction = true)
    public static ConstantOperator inspectMvMeta(ConstantOperator mvName) {
        TableName tableName = TableName.fromString(mvName.getVarchar());
        Pair<Database, Table> dbTable = inspectTable(tableName);
        Table table = dbTable.getRight();
        if (!table.isMaterializedView()) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_INVALID_PARAMETER,
                    tableName + " is not materialized view");
        }
        Locker locker = new Locker();
        try {
            locker.lockDatabase(dbTable.getLeft().getId(), LockType.READ);
            MaterializedView mv = (MaterializedView) table;
            String meta = mv.inspectMeta();
            return ConstantOperator.createVarchar(meta);
        } finally {
            locker.unLockDatabase(dbTable.getLeft().getId(), LockType.READ);
        }
    }

    /**
     * Return related materialized-views of a table, in JSON array format
     */
    @ConstantFunction(name = "inspect_related_mv", argTypes = {VARCHAR}, returnType = VARCHAR, isMetaFunction = true)
    public static ConstantOperator inspectRelatedMv(ConstantOperator name) {
        TableName tableName = TableName.fromString(name.getVarchar());
        Optional<Database> mayDb;
        Table table = inspectExternalTable(tableName);
        if (table.isNativeTableOrMaterializedView()) {
            mayDb = GlobalStateMgr.getCurrentState().getLocalMetastore().mayGetDb(tableName.getDb());
        } else {
            mayDb = Optional.empty();
        }

        Locker locker = new Locker();
        try {
            mayDb.ifPresent(database -> locker.lockDatabase(database.getId(), LockType.READ));

            Set<MvId> relatedMvs = table.getRelatedMaterializedViews();
            JsonArray array = new JsonArray();
            for (MvId mv : SetUtils.emptyIfNull(relatedMvs)) {
                String mvName = GlobalStateMgr.getCurrentState().getLocalMetastore().mayGetTable(mv.getDbId(), mv.getId())
                        .map(Table::getName)
                        .orElse(null);
                JsonObject obj = new JsonObject();
                obj.add("id", new JsonPrimitive(mv.getId()));
                obj.add("name", mvName != null ? new JsonPrimitive(mvName) : JsonNull.INSTANCE);

                array.add(obj);
            }

            String json = array.toString();
            return ConstantOperator.createVarchar(json);
        } finally {
            mayDb.ifPresent(database -> locker.unLockDatabase(database.getId(), LockType.READ));
        }
    }

    /**
     * Return the content in ConnectorTblMetaInfoMgr, which contains mapping information from base table to mv
     */
    @ConstantFunction(name = "inspect_mv_relationships", argTypes = {}, returnType = VARCHAR, isMetaFunction = true)
    public static ConstantOperator inspectMvRelationships() {
        ConnectContext context = ConnectContext.get();
        try {
            Authorizer.checkSystemAction(context, PrivilegeType.OPERATE);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    "", context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.OPERATE.name(), ObjectType.FUNCTION.name(), "inspect_mv_relationships");
        }

        String json = GlobalStateMgr.getCurrentState().getConnectorTblMetaInfoMgr().inspect();
        return ConstantOperator.createVarchar(json);
    }

    /**
     * Return Hive partition info
     */
    @ConstantFunction(name = "inspect_hive_part_info",
            argTypes = {VARCHAR},
            returnType = VARCHAR,
            isMetaFunction = true)
    public static ConstantOperator inspectHivePartInfo(ConstantOperator name) {
        TableName tableName = TableName.fromString(name.getVarchar());
        Table table = inspectExternalTable(tableName);

        Map<String, PartitionInfo> info = PartitionUtil.getPartitionNameWithPartitionInfo(table);
        JsonObject obj = new JsonObject();
        for (Map.Entry<String, PartitionInfo> entry : MapUtils.emptyIfNull(info).entrySet()) {
            if (entry.getValue() instanceof Partition) {
                Partition part = (Partition) entry.getValue();
                obj.add(entry.getKey(), part.toJson());
            }
        }
        String json = obj.toString();
        return ConstantOperator.createVarchar(json);
    }

    /**
     * Return meta data of all pipes in current database
     */
    @ConstantFunction(name = "inspect_all_pipes", argTypes = {}, returnType = VARCHAR, isMetaFunction = true)
    public static ConstantOperator inspectAllPipes() {
        ConnectContext connectContext = ConnectContext.get();
        authOperatorPrivilege();
        String currentDb = connectContext.getDatabase();
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().mayGetDb(connectContext.getDatabase())
                .orElseThrow(() -> ErrorReport.buildSemanticException(ErrorCode.ERR_BAD_DB_ERROR, currentDb));
        String json = GlobalStateMgr.getCurrentState().getPipeManager().getPipesOfDb(db.getId());
        return ConstantOperator.createVarchar(json);
    }

    /**
     * Return all status about the TaskManager
     */
    @ConstantFunction(name = "inspect_task_runs", argTypes = {}, returnType = VARCHAR, isMetaFunction = true)
    public static ConstantOperator inspectTaskRuns() {
        ConnectContext connectContext = ConnectContext.get();
        authOperatorPrivilege();
        TaskRunManager trm = GlobalStateMgr.getCurrentState().getTaskManager().getTaskRunManager();
        return ConstantOperator.createVarchar(trm.inspect());
    }

    @ConstantFunction(name = "inspect_memory", argTypes = {VARCHAR}, returnType = VARCHAR, isMetaFunction = true)
    public static ConstantOperator inspectMemory(ConstantOperator moduleName) {
        Map<String, MemoryTrackable> statMap = MemoryUsageTracker.REFERENCE.get(moduleName.getVarchar());
        if (statMap == null) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_INVALID_PARAMETER,
                    "Module " + moduleName + " not found.");
        }
        long estimateSize = 0;
        for (Map.Entry<String, MemoryTrackable> statEntry : statMap.entrySet()) {
            MemoryTrackable tracker = statEntry.getValue();
            estimateSize += tracker.estimateSize();
        }

        return ConstantOperator.createVarchar(new ByteSizeValue(estimateSize).toString());
    }

    @ConstantFunction(name = "inspect_memory_detail", argTypes = {VARCHAR, VARCHAR},
            returnType = VARCHAR, isMetaFunction = true)
    public static ConstantOperator inspectMemoryDetail(ConstantOperator moduleName, ConstantOperator clazzInfo) {
        Map<String, MemoryTrackable> statMap = MemoryUsageTracker.REFERENCE.get(moduleName.getVarchar());
        if (statMap == null) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_INVALID_PARAMETER,
                    "Module " + moduleName + " not found.");
        }
        String classInfo = clazzInfo.getVarchar();
        String clazzName;
        String fieldName = null;
        if (classInfo.contains(".")) {
            clazzName = classInfo.split("\\.")[0];
            fieldName = classInfo.split("\\.")[1];
        } else {
            clazzName = classInfo;
        }
        MemoryTrackable memoryTrackable = statMap.get(clazzName);
        if (memoryTrackable == null) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_INVALID_PARAMETER,
                    "In module " + moduleName + " - " + clazzName + " not found.");
        }
        long estimateSize = 0;
        if (fieldName == null) {
            estimateSize = memoryTrackable.estimateSize();
        } else {
            try {
                Field field = memoryTrackable.getClass().getDeclaredField(fieldName);
                field.setAccessible(true);
                Object object = field.get(memoryTrackable);
                estimateSize = SizeEstimator.estimate(object);
            } catch (NoSuchFieldException e) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_INVALID_PARAMETER,
                        "In module " + moduleName + " - " + clazzName + " field " + fieldName  + " not found.");
            } catch (IllegalAccessException e) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_INVALID_PARAMETER,
                        "Get module " + moduleName + " - " + clazzName + " field " + fieldName  + " error.");
            }
        }

        return ConstantOperator.createVarchar(new ByteSizeValue(estimateSize).toString());
    }

    /**
     * Return the logical plan of a materialized view with cache
     */
    @ConstantFunction(name = "inspect_mv_plan", argTypes = {VARCHAR}, returnType = VARCHAR, isMetaFunction = true)
    public static ConstantOperator inspectMvPlan(ConstantOperator mvName) {
        return inspectMvPlan(mvName, ConstantOperator.TRUE);
    }

    /**
     * Return verbose metadata of a materialized view
     */
    @ConstantFunction(name = "inspect_mv_plan", argTypes = {VARCHAR, BOOLEAN}, returnType = VARCHAR, isMetaFunction = true)
    public static ConstantOperator inspectMvPlan(ConstantOperator mvName, ConstantOperator useCache) {
        TableName tableName = TableName.fromString(mvName.getVarchar());
        Pair<Database, Table> dbTable = inspectTable(tableName);
        Table table = dbTable.getRight();
        if (!table.isMaterializedView()) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_INVALID_PARAMETER,
                    tableName + " is not materialized view");
        }
        try {
            MaterializedView mv = (MaterializedView) table;
            String plans = "";
            ConnectContext connectContext = ConnectContext.get() == null ? new ConnectContext() : ConnectContext.get();
            boolean defaultUseCacheValue = connectContext.getSessionVariable().isEnableMaterializedViewPlanCache();
            connectContext.getSessionVariable().setEnableMaterializedViewPlanCache(useCache.getBoolean());
            List<MvPlanContext> planContexts =
                    CachingMvPlanContextBuilder.getInstance().getPlanContext(connectContext.getSessionVariable(), mv);
            connectContext.getSessionVariable().setEnableMaterializedViewPlanCache(defaultUseCacheValue);
            int size = planContexts.size();
            for (int i = 0; i < size; i++) {
                MvPlanContext context = planContexts.get(i);
                if (context != null) {
                    OptExpression plan = context.getLogicalPlan();
                    String debugString = plan.debugString();
                    plans += String.format("plan %d: \n%s\n", i, debugString);
                } else {
                    plans += String.format("plan %d: null\n", i);
                }
            }
            return ConstantOperator.createVarchar(plans);
        } catch (Exception e) {
            ErrorReport.report(ErrorCode.ERR_UNKNOWN_ERROR, e.getMessage());
            return ConstantOperator.createVarchar("failed");
        }
    }

    @ConstantFunction(name = "get_query_dump", argTypes = {VARCHAR, BOOLEAN}, returnType = VARCHAR, isMetaFunction = true)
    public static ConstantOperator getQueryDump(ConstantOperator query, ConstantOperator enableMock) {
        com.starrocks.common.Pair<HttpResponseStatus, String> statusAndRes =
                QueryDumper.dumpQuery("", "", query.getVarchar(), enableMock.getBoolean());
        if (statusAndRes.first != HttpResponseStatus.OK) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_INVALID_PARAMETER, "get_query_dump: " + statusAndRes.second);
        }
        return ConstantOperator.createVarchar(statusAndRes.second);
    }

    @ConstantFunction(name = "get_query_dump", argTypes = {VARCHAR}, returnType = VARCHAR, isMetaFunction = true)
    public static ConstantOperator getQueryDump(ConstantOperator query) {
        return getQueryDump(query, ConstantOperator.createBoolean(false));
    }

    public static class LookupRecord {

        @SerializedName("data")
        public List<String> data;

        public static LookupRecord fromJson(String json) {
            return GsonUtils.GSON.fromJson(json, LookupRecord.class);
        }
    }

    private static ConstantOperator deserializeLookupResult(List<TResultBatch> batches) {
        for (TResultBatch batch : ListUtils.emptyIfNull(batches)) {
            for (ByteBuffer buffer : batch.getRows()) {
                ByteBuf copied = Unpooled.copiedBuffer(buffer);
                String jsonString = copied.toString(Charset.defaultCharset());
                List<String> data = LookupRecord.fromJson(jsonString).data;
                if (CollectionUtils.isNotEmpty(data)) {
                    return ConstantOperator.createVarchar(data.get(0));
                } else {
                    return ConstantOperator.NULL;
                }
            }
        }
        return ConstantOperator.NULL;
    }

    /**
     * Lookup a value from a primary table, and evaluate in the optimizer
     *
     * @param tableName    table to lookup, must be a primary-key table
     * @param lookupKey    key to lookup, must be a string type
     * @param returnColumn column to return
     * @return NULL if not found, otherwise return the value
     */
    @ConstantFunction(name = "lookup_string",
            argTypes = {VARCHAR, VARCHAR, VARCHAR},
            returnType = VARCHAR,
            isMetaFunction = true)
    public static ConstantOperator lookupString(ConstantOperator tableName,
                                                 ConstantOperator lookupKey,
                                                 ConstantOperator returnColumn) {
        TableName tableNameValue = TableName.fromString(tableName.getVarchar());
        Optional<Table> maybeTable = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(tableNameValue);
        maybeTable.orElseThrow(() -> ErrorReport.buildSemanticException(ErrorCode.ERR_BAD_TABLE_ERROR, tableNameValue));
        if (!(maybeTable.get() instanceof OlapTable)) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_INVALID_PARAMETER, "must be OLAP_TABLE");
        }
        OlapTable table = (OlapTable) maybeTable.get();
        if (table.getKeysType() != KeysType.PRIMARY_KEYS) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_INVALID_PARAMETER, "must be PRIMARY_KEY");
        }
        if (table.getKeysNum() > 1) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_INVALID_PARAMETER, "too many key columns");
        }
        Column keyColumn = table.getKeyColumns().get(0);

        String sql = String.format("select cast(`%s` as string) from %s where `%s` = '%s' limit 1",
                returnColumn.getVarchar(), tableNameValue.toString(), keyColumn.getName(), lookupKey.getVarchar());
        try {
            List<TResultBatch> result = SimpleExecutor.getRepoExecutor().executeDQL(sql);
            return deserializeLookupResult(result);
        } catch (Throwable e) {
            final String notFoundMessage = "query failed if record not exist in dict table";
            Throwable root = ExceptionUtils.getRootCause(e);
            // Record not found
            if (e.getMessage().contains(notFoundMessage) ||
                    root != null && root.getMessage().contains(notFoundMessage)) {
                return ConstantOperator.NULL;
            }
            if (root instanceof StarRocksPlannerException) {
                throw new SemanticException("lookup failed: " + root.getMessage(), root);
            } else {
                throw new SemanticException("lookup failed: " + e.getMessage(), e);
            }
        }
    }

}
