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

package com.starrocks.sql.optimizer.rewrite;

import com.google.gson.JsonArray;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvId;
import com.starrocks.catalog.Table;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.connector.PartitionInfo;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.connector.hive.Partition;
import com.starrocks.privilege.AccessDeniedException;
import com.starrocks.privilege.ObjectType;
import com.starrocks.privilege.PrivilegeType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.scheduler.TaskRunManager;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.collections4.SetUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.starrocks.catalog.PrimitiveType.VARCHAR;

/**
 * Meta functions can be used to inspect the content of in-memory structures, for debug purpose.
 */
public class MetaFunctions {

    private static Table inspectExternalTable(TableName tableName) {
        Table table = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(tableName)
                .orElseThrow(() -> ErrorReport.buildSemanticException(ErrorCode.ERR_BAD_TABLE_ERROR, tableName));
        ConnectContext connectContext = ConnectContext.get();
        try {
            Authorizer.checkAnyActionOnTable(connectContext.getCurrentUserIdentity(),
                    connectContext.getCurrentRoleIds(),
                    tableName);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    tableName.getCatalog(),
                    connectContext.getCurrentUserIdentity(), connectContext.getCurrentRoleIds(),
                    PrivilegeType.ANY.name(), ObjectType.TABLE.name(), tableName.getTbl());
        }
        return table;
    }

    private static Pair<Database, Table> inspectTable(TableName tableName) {
        Database db = GlobalStateMgr.getCurrentState().mayGetDb(tableName.getDb())
                .orElseThrow(() -> ErrorReport.buildSemanticException(ErrorCode.ERR_BAD_DB_ERROR, tableName.getDb()));
        Table table = db.tryGetTable(tableName.getTbl())
                .orElseThrow(() -> ErrorReport.buildSemanticException(ErrorCode.ERR_BAD_TABLE_ERROR, tableName));
        ConnectContext connectContext = ConnectContext.get();
        try {
            Authorizer.checkAnyActionOnTable(
                    connectContext.getCurrentUserIdentity(),
                    connectContext.getCurrentRoleIds(),
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
                    connectContext.getCurrentUserIdentity(),
                    connectContext.getCurrentRoleIds(),
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
            locker.lockDatabase(dbTable.getLeft(), LockType.READ);
            MaterializedView mv = (MaterializedView) table;
            String meta = mv.inspectMeta();
            return ConstantOperator.createVarchar(meta);
        } finally {
            locker.unLockDatabase(dbTable.getLeft(), LockType.READ);
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
            mayDb = GlobalStateMgr.getCurrentState().mayGetDb(tableName.getDb());
        } else {
            mayDb = Optional.empty();
        }

        Locker locker = new Locker();
        try {
            mayDb.ifPresent(database -> locker.lockDatabase(database, LockType.READ));

            Set<MvId> relatedMvs = table.getRelatedMaterializedViews();
            JsonArray array = new JsonArray();
            for (MvId mv : SetUtils.emptyIfNull(relatedMvs)) {
                String mvName = GlobalStateMgr.getCurrentState().mayGetTable(mv.getDbId(), mv.getId())
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
            mayDb.ifPresent(database -> locker.unLockDatabase(database, LockType.READ));
        }
    }

    /**
     * Return the content in ConnectorTblMetaInfoMgr, which contains mapping information from base table to mv
     */
    @ConstantFunction(name = "inspect_mv_relationships", argTypes = {}, returnType = VARCHAR, isMetaFunction = true)
    public static ConstantOperator inspectMvRelationships() {
        ConnectContext context = ConnectContext.get();
        try {
            Authorizer.checkSystemAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.OPERATE);
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
        Database db = GlobalStateMgr.getCurrentState().mayGetDb(connectContext.getDatabase())
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

}
