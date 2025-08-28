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
package com.starrocks.catalog.system.information;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.starrocks.authentication.UserIdentityUtils;
import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.catalog.system.SystemId;
import com.starrocks.catalog.system.SystemTable;
import com.starrocks.common.CaseSensibility;
import com.starrocks.common.Pair;
import com.starrocks.common.PatternMatcher;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowMaterializedViewStatus;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.thrift.TGetTablesParams;
import com.starrocks.thrift.TListMaterializedViewStatusResult;
import com.starrocks.thrift.TMaterializedViewStatus;
import com.starrocks.thrift.TSchemaTableType;
import com.starrocks.thrift.TUserIdentity;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.meta_data.FieldValueMetaData;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static com.starrocks.thrift.TTableType.MATERIALIZED_VIEW;

public class MaterializedViewsSystemTable extends SystemTable {
    public static final String NAME = "materialized_views";

    private static final Logger LOG = LogManager.getLogger(MaterializedViewsSystemTable.class);
    private static final SystemTable INSTANCE = new MaterializedViewsSystemTable();

    public MaterializedViewsSystemTable() {
        super(SystemId.MATERIALIZED_VIEWS_ID,
                NAME,
                Table.TableType.SCHEMA,
                builder()
                        .column("MATERIALIZED_VIEW_ID", ScalarType.createType(PrimitiveType.BIGINT))
                        .column("TABLE_SCHEMA", ScalarType.createVarchar(20))
                        .column("TABLE_NAME", ScalarType.createVarchar(50))
                        .column("REFRESH_TYPE", ScalarType.createVarchar(20))
                        .column("IS_ACTIVE", ScalarType.createVarchar(10))
                        .column("INACTIVE_REASON", ScalarType.createVarcharType(1024))
                        .column("PARTITION_TYPE", ScalarType.createVarchar(16))
                        .column("TASK_ID", ScalarType.createType(PrimitiveType.BIGINT))
                        .column("TASK_NAME", ScalarType.createVarchar(50))
                        .column("LAST_REFRESH_START_TIME", ScalarType.createType(PrimitiveType.DATETIME))
                        .column("LAST_REFRESH_FINISHED_TIME", ScalarType.createType(PrimitiveType.DATETIME))
                        .column("LAST_REFRESH_DURATION", ScalarType.createType(PrimitiveType.DOUBLE))
                        .column("LAST_REFRESH_STATE", ScalarType.createVarchar(20))
                        .column("LAST_REFRESH_FORCE_REFRESH", ScalarType.createVarchar(8))
                        .column("LAST_REFRESH_START_PARTITION", ScalarType.createVarchar(1024))
                        .column("LAST_REFRESH_END_PARTITION", ScalarType.createVarchar(1024))
                        .column("LAST_REFRESH_BASE_REFRESH_PARTITIONS", ScalarType.createVarchar(1024))
                        .column("LAST_REFRESH_MV_REFRESH_PARTITIONS", ScalarType.createVarchar(1024))
                        .column("LAST_REFRESH_ERROR_CODE", ScalarType.createVarchar(20))
                        .column("LAST_REFRESH_ERROR_MESSAGE", ScalarType.createVarchar(1024))
                        .column("TABLE_ROWS", ScalarType.createType(PrimitiveType.BIGINT))
                        .column("MATERIALIZED_VIEW_DEFINITION",
                                ScalarType.createVarchar(MAX_FIELD_VARCHAR_LENGTH))
                        .column("EXTRA_MESSAGE", ScalarType.createVarchar(1024))
                        .column("QUERY_REWRITE_STATUS", ScalarType.createVarcharType(64))
                        .column("CREATOR", ScalarType.createVarchar(64))
                        .column("LAST_REFRESH_PROCESS_TIME", ScalarType.createType(PrimitiveType.DATETIME))
                        .column("LAST_REFRESH_JOB_ID", ScalarType.createVarchar(64))
                        .build(), TSchemaTableType.SCH_MATERIALIZED_VIEWS);
    }

    public static SystemTable create() {
        return new MaterializedViewsSystemTable();
    }

    private static final Set<String> SUPPORTED_EQUAL_COLUMNS =
            Collections.unmodifiableSet(new TreeSet<>(String.CASE_INSENSITIVE_ORDER) {
                {
                    add("TABLE_SCHEMA");
                    add("TABLE_NAME");
                }
            });

    @Override
    public boolean supportFeEvaluation(ScalarOperator predicate) {
        final List<ScalarOperator> conjuncts = Utils.extractConjuncts(predicate);
        if (conjuncts.isEmpty()) {
            return true;
        }
        if (!isEmptyOrOnlyEqualConstantOps(conjuncts)) {
            return false;
        }
        return isSupportedEqualPredicateColumn(conjuncts, SUPPORTED_EQUAL_COLUMNS);
    }

    @Override
    public List<List<ScalarOperator>> evaluate(ScalarOperator predicate) {
        final List<ScalarOperator> conjuncts = Utils.extractConjuncts(predicate);

        ConnectContext context = Preconditions.checkNotNull(ConnectContext.get(), "not a valid connection");
        TUserIdentity userIdentity = UserIdentityUtils.toThrift(context.getCurrentUserIdentity());
        TGetTablesParams params = new TGetTablesParams();
        params.setCurrent_user_ident(userIdentity);
        params.setDb(context.getDatabase());
        params.setType(MATERIALIZED_VIEW);
        for (ScalarOperator conjunct : conjuncts) {
            BinaryPredicateOperator binary = (BinaryPredicateOperator) conjunct;
            ColumnRefOperator columnRef = binary.getChild(0).cast();
            String name = columnRef.getName();
            ConstantOperator value = binary.getChild(1).cast();
            switch (name.toUpperCase()) {
                case "TABLE_NAME":
                    params.setTable_name(value.getVarchar());
                    break;
                case "TABLE_SCHEMA":
                    params.setDb(value.getVarchar());
                    break;
                default:
                    throw new NotImplementedException("unsupported column: " + name);
            }
        }

        try {
            TListMaterializedViewStatusResult result = query(params, context);
            return result.getMaterialized_views().stream().map(this::infoToScalar).collect(Collectors.toList());
        } catch (Exception e) {
            LOG.warn("Failed to query materialized views", e);
            // Return empty result if query failed
            return Lists.newArrayList();
        }
    }

    private static final Map<String, String> ALIAS_MAP = ImmutableMap.of(
            "materialized_view_id", "id",
            "table_schema", "database_name",
            "table_name", "name",
            "materialized_view_definition", "text",
            "table_rows", "rows"
    );

    private List<ScalarOperator> infoToScalar(TMaterializedViewStatus status) {
        List<ScalarOperator> result = Lists.newArrayList();
        for (Column column : INSTANCE.getBaseSchema()) {
            String name = column.getName().toLowerCase();
            if (ALIAS_MAP.containsKey(name)) {
                name = ALIAS_MAP.get(name);
            }
            TMaterializedViewStatus._Fields field = TMaterializedViewStatus._Fields.findByName(name);
            Preconditions.checkArgument(field != null, "Unknown field: " + name);
            FieldValueMetaData meta = TMaterializedViewStatus.metaDataMap.get(field).valueMetaData;
            Object obj = status.getFieldValue(field);
            Type valueType = thriftToScalarType(meta.type);
            if (valueType.isStringType() && obj == null) {
                obj = ""; // Convert null string to empty string
            }
            ConstantOperator scalar = ConstantOperator.createNullableObject(obj, valueType);
            try {
                scalar = mayCast(scalar, column.getType());
            } catch (Exception e) {
                LOG.debug("Failed to cast scalar operator for column: {}, value: {}, type: {}",
                        column.getName(), obj, valueType, e);
                scalar = ConstantOperator.createNull(column.getType());
            }
            result.add(scalar);
        }
        return result;
    }

    public static TListMaterializedViewStatusResult query(TGetTablesParams params,
                                                          ConnectContext context) throws TException {
        LOG.debug("get list table request: {}", params);
        PatternMatcher matcher = null;
        boolean caseSensitive = CaseSensibility.TABLE.getCaseSensibility();
        if (params.isSetPattern()) {
            matcher = PatternMatcher.createMysqlPattern(params.getPattern(), caseSensitive);
        }

        // database privs should be checked in analysis phrase
        long limit = params.isSetLimit() ? params.getLimit() : -1;
        if (params.isSetCurrent_user_ident()) {
            context.setAuthInfoFromThrift(params.getCurrent_user_ident());
        } else {
            UserIdentity currentUser = UserIdentity.createAnalyzedUserIdentWithIp(params.user, params.user_ip);
            context.setCurrentUserIdentity(currentUser);
            context.setCurrentRoleIds(currentUser);
        }
        Preconditions.checkState(params.isSetType() && MATERIALIZED_VIEW.equals(params.getType()));
        return listMaterializedViewStatus(limit, matcher, context, params);
    }

    // list MaterializedView table match pattern
    private static TListMaterializedViewStatusResult listMaterializedViewStatus(
            long limit,
            PatternMatcher matcher,
            ConnectContext context,
            TGetTablesParams params) {
        TListMaterializedViewStatusResult result = new TListMaterializedViewStatusResult();
        List<TMaterializedViewStatus> tablesResult = Lists.newArrayList();
        result.setMaterialized_views(tablesResult);
        String dbName = params.getDb();
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbName);
        if (db == null) {
            LOG.warn("database not exists: {}", dbName);
            return result;
        }

        listMaterializedViews(limit, matcher, context, params).stream()
                .map(s -> s.toThrift())
                .forEach(t -> tablesResult.add(t));
        return result;
    }

    private static void filterAsynchronousMaterializedView(
            PatternMatcher matcher,
            ConnectContext context,
            String dbName,
            MaterializedView mv,
            TGetTablesParams params,
            List<MaterializedView> result) {
        // check table name
        String mvName = params.table_name;
        if (mvName != null && !mvName.equalsIgnoreCase(mv.getName())) {
            return;
        }

        try {
            Authorizer.checkAnyActionOnTableLikeObject(context, dbName, mv);
        } catch (AccessDeniedException e) {
            return;
        }

        boolean caseSensitive = CaseSensibility.TABLE.getCaseSensibility();
        if (!PatternMatcher.matchPattern(params.getPattern(), mv.getName(), matcher, caseSensitive)) {
            return;
        }
        result.add(mv);
    }

    private static void filterSynchronousMaterializedView(
            OlapTable olapTable,
            PatternMatcher matcher,
            TGetTablesParams params,
            List<Pair<OlapTable, MaterializedIndexMeta>> singleTableMVs) {
        // synchronized materialized view metadata size should be greater than 1.
        if (olapTable.getVisibleIndexMetas().size() <= 1) {
            return;
        }

        // check table name
        String mvName = params.table_name;
        if (mvName != null && !mvName.equalsIgnoreCase(olapTable.getName())) {
            return;
        }

        List<MaterializedIndexMeta> visibleMaterializedViews = olapTable.getVisibleIndexMetas();
        long baseIdx = olapTable.getBaseIndexId();
        boolean caseSensitive = CaseSensibility.TABLE.getCaseSensibility();
        for (MaterializedIndexMeta mvMeta : visibleMaterializedViews) {
            if (baseIdx == mvMeta.getIndexId()) {
                continue;
            }

            if (!PatternMatcher.matchPattern(params.getPattern(), olapTable.getIndexNameById(mvMeta.getIndexId()),
                    matcher, caseSensitive)) {
                continue;
            }
            singleTableMVs.add(Pair.create(olapTable, mvMeta));
        }
    }

    private static List<ShowMaterializedViewStatus> listMaterializedViews(
            long limit,
            PatternMatcher matcher,
            ConnectContext context,
            TGetTablesParams params) {
        String dbName = params.getDb();
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbName);
        List<MaterializedView> materializedViews = com.google.common.collect.Lists.newArrayList();
        List<Pair<OlapTable, MaterializedIndexMeta>> singleTableMVs = com.google.common.collect.Lists.newArrayList();
        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.READ);
        try {
            for (Table table : GlobalStateMgr.getCurrentState().getLocalMetastore().getTables(db.getId())) {
                if (table.isMaterializedView()) {
                    filterAsynchronousMaterializedView(matcher, context, dbName,
                            (MaterializedView) table, params, materializedViews);
                } else if (table.getType() == Table.TableType.OLAP) {
                    filterSynchronousMaterializedView((OlapTable) table, matcher, params, singleTableMVs);
                } else {
                    // continue
                }

                // check limit
                int mvSize = materializedViews.size() + singleTableMVs.size();
                if (limit > 0 && mvSize >= limit) {
                    break;
                }
            }
        } finally {
            locker.unLockDatabase(db.getId(), LockType.READ);
        }
        return ShowMaterializedViewStatus.listMaterializedViewStatus(dbName, materializedViews, singleTableMVs);
    }
}
