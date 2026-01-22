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
import com.google.common.collect.Sets;
import com.starrocks.analysis.TableName;
import com.starrocks.authentication.AuthenticationMgr;
import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.authorization.PrivilegeBuiltinConstants;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.catalog.View;
import com.starrocks.catalog.system.SystemId;
import com.starrocks.catalog.system.SystemTable;
import com.starrocks.common.CaseSensibility;
import com.starrocks.common.PatternMatcher;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.thrift.TGetTablesParams;
import com.starrocks.thrift.TListTableStatusResult;
import com.starrocks.thrift.TSchemaTableType;
import com.starrocks.thrift.TTableStatus;
import com.starrocks.thrift.TTableType;
import com.starrocks.thrift.TUserIdentity;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.parquet.Strings;
import org.apache.thrift.TException;
import org.apache.thrift.meta_data.FieldValueMetaData;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static com.starrocks.thrift.TTableType.VIEW;

public class ViewsSystemTable extends SystemTable {
    public static final String NAME = "views";

    private static final Logger LOG = LogManager.getLogger(ViewsSystemTable.class);

    public ViewsSystemTable(String catalogName) {
        super(
                catalogName,
                SystemId.VIEWS_ID,
                NAME,
                Table.TableType.SCHEMA,
                builder()
                        .column("TABLE_CATALOG", ScalarType.createVarchar(512))
                        .column("TABLE_SCHEMA", ScalarType.createVarchar(64))
                        .column("TABLE_NAME", ScalarType.createVarchar(64))
                        // TODO: Type for EVENT_DEFINITION should be `longtext`, but `varchar(65535)` was set at this stage.
                        .column("VIEW_DEFINITION",
                                ScalarType.createVarchar(MAX_FIELD_VARCHAR_LENGTH))
                        .column("CHECK_OPTION", ScalarType.createVarchar(8))
                        .column("IS_UPDATABLE", ScalarType.createVarchar(3))
                        .column("DEFINER", ScalarType.createVarchar(77))
                        .column("SECURITY_TYPE", ScalarType.createVarchar(7))
                        .column("CHARACTER_SET_CLIENT", ScalarType.createVarchar(32))
                        .column("COLLATION_CONNECTION", ScalarType.createVarchar(32))
                        .build(), TSchemaTableType.SCH_VIEWS);
    }

    public static SystemTable create(String catalogName) {
        return new ViewsSystemTable(catalogName);
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

    /**
     * Get the information_schema.views's result.
     * @param dbName db's name of the current view
     * @param status the view's status info
     */
    public record GetViewResult(String dbName, TTableStatus status) {
    }

    public List<List<ScalarOperator>> evaluate(ScalarOperator predicate) {
        final List<ScalarOperator> conjuncts = Utils.extractConjuncts(predicate);
        ConnectContext context = Preconditions.checkNotNull(ConnectContext.get(), "not a valid connection");
        TUserIdentity userIdentity = context.getCurrentUserIdentity().toThrift();
        TGetTablesParams params = new TGetTablesParams();
        params.setCurrent_user_ident(userIdentity);
        params.setType(VIEW);
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
            return queryImpl(params, context)
                    .stream()
                    .map(t -> infoToScalar(this, t))
                    .collect(Collectors.toList());
        } catch (Exception e) {
            LOG.warn("Failed to query views ", e);
            throw new SemanticException("Failed to query views ", e);
        }
    }

    private static final Map<String, String> ALIAS_MAP = ImmutableMap.of(
            "view_definition", "ddl_sql",
            "table_name", "name"
    );

    private static final Map<String, String> CONSTANT_MAP = ImmutableMap.of(
            "table_catalog", "def",
            "check_option", "NONE",
            "is_updatable", "NO",
            "definer", "",
            "security_type", "",
            "character_set_client", "utf8",
            "collation_connection", "utf8_general_ci"
    );

    public static List<ScalarOperator> infoToScalar(SystemTable systemTable,
                                                    GetViewResult viewResult) {
        List<ScalarOperator> result = Lists.newArrayList();
        for (Column column : systemTable.getBaseSchema()) {
            String name = column.getName().toLowerCase();
            if (CONSTANT_MAP.containsKey(name)) {
                // For some columns, we return a constant value
                Object obj = CONSTANT_MAP.get(name);
                Type valueType = column.getType();
                if (valueType.isStringType() && obj == null) {
                    obj = ""; // Convert null string to empty string
                }
                ConstantOperator scalar = ConstantOperator.createNullableObject(obj, valueType);
                result.add(scalar);
                continue;
            }
            if ("table_schema".equals(name)) {
                // For TABLE_SCHEMA, we return the database name
                ConstantOperator scalar = ConstantOperator.createNullableObject(viewResult.dbName, column.getType());
                result.add(scalar);
                continue;
            }
            if (ALIAS_MAP.containsKey(name)) {
                name = ALIAS_MAP.get(name);
            }
            TTableStatus._Fields field = TTableStatus._Fields.findByName(name);
            Preconditions.checkArgument(field != null, "Unknown field: " + name);
            FieldValueMetaData meta = TTableStatus.metaDataMap.get(field).valueMetaData;
            Object obj = viewResult.status.getFieldValue(field);
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

    public static TListTableStatusResult query(TGetTablesParams params,
                                               ConnectContext context) throws TException {
        TListTableStatusResult result = new TListTableStatusResult();
        List<GetViewResult> views = queryImpl(params, context);
        result.setTables(views.stream().map(GetViewResult::status).collect(Collectors.toList()));
        return result;
    }

    private static List<GetViewResult> queryImpl(TGetTablesParams params,
                                                 ConnectContext context) throws TException {
        LOG.debug("get list table request: {}", params);
        PatternMatcher matcher = null;
        boolean caseSensitive = CaseSensibility.TABLE.getCaseSensibility();
        if (params.isSetPattern()) {
            try {
                matcher = PatternMatcher.createMysqlPattern(params.getPattern(), caseSensitive);
            } catch (SemanticException e) {
                throw new TException("Pattern is in bad format " + params.getPattern());
            }
        }
        UserIdentity currentUser;
        if (params.isSetCurrent_user_ident()) {
            currentUser = UserIdentity.fromThrift(params.current_user_ident);
        } else {
            currentUser = UserIdentity.createAnalyzedUserIdentWithIp(params.user, params.user_ip);
        }
        String tableNameParam = params.isSetTable_name() ? params.getTable_name() : null;
        boolean listingViews = params.isSetType() && TTableType.VIEW.equals(params.getType());
        String pattern = params.pattern;
        List<Database> databases = Lists.newArrayList();
        if (Strings.isNullOrEmpty(params.db)) {
            databases.addAll(GlobalStateMgr.getCurrentState().getLocalMetastore().getAllDbs());
        } else {
            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(params.db);
            databases.add(db);
        }

        long limit = params.isSetLimit() ? params.getLimit() : -1;
        List<GetViewResult> result = Lists.newArrayList();
        for (Database db : databases) {
            if (!collectViewsInDb(context, db, currentUser, tableNameParam, pattern, matcher,
                    limit, listingViews, caseSensitive, result)) {
                break;
            }
        }
        return result;
    }

    /**
     * if the limit is reached, false is returned, otherwise true will be returned.
     */
    private static boolean collectViewsInDb(ConnectContext context, Database db, UserIdentity currentUser,
                                            String paramTableName, String paramPattern, PatternMatcher matcher,
                                            long limit, boolean listingViews, boolean caseSensitive,
                                            List<GetViewResult> result) {
        if (db == null) {
            return true;
        }
        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.READ);
        String dbName = db.getFullName();
        try {
            List<Table> tables = listingViews ? db.getViews() :
                    GlobalStateMgr.getCurrentState().getLocalMetastore().getTables(db.getId());
            OUTER:
            for (Table table : tables) {
                try {
                    context.setCurrentUserIdentity(currentUser);
                    context.setCurrentRoleIds(currentUser);
                    Authorizer.checkAnyActionOnTableLikeObject(context, dbName, table);
                } catch (AccessDeniedException e) {
                    continue;
                }

                if (!PatternMatcher.matchPattern(paramPattern, table.getName(), matcher, caseSensitive)) {
                    continue;
                }
                if (!Strings.isNullOrEmpty(paramTableName) &&
                        !table.getName().equalsIgnoreCase(paramTableName)) {
                    continue;
                }

                TTableStatus status = new TTableStatus();
                status.setName(table.getName());
                status.setType(table.getMysqlType());
                status.setEngine(table.getEngine());
                status.setComment(table.getComment());
                status.setCreate_time(table.getCreateTime());
                status.setLast_check_time(table.getLastCheckTime());
                if (listingViews) {
                    View view = (View) table;
                    String ddlSql = view.getDDLViewDef();

                    ConnectContext connectContext = ConnectContext.buildInner();
                    connectContext.setQualifiedUser(AuthenticationMgr.ROOT_USER);
                    connectContext.setCurrentUserIdentity(UserIdentity.ROOT);
                    connectContext.setCurrentRoleIds(Sets.newHashSet(PrivilegeBuiltinConstants.ROOT_ROLE_ID));

                    try {
                        List<TableName> allTables = view.getTableRefs();
                        for (TableName tableName : allTables) {
                            Table tbl = GlobalStateMgr.getCurrentState().getLocalMetastore()
                                    .getTable(db.getFullName(), tableName.getTbl());
                            if (tbl != null) {
                                try {
                                    context.setCurrentUserIdentity(currentUser);
                                    context.setCurrentRoleIds(currentUser);
                                    Authorizer.checkAnyActionOnTableLikeObject(context, db.getFullName(), tbl);
                                } catch (AccessDeniedException e) {
                                    continue OUTER;
                                }
                            }
                        }
                    } catch (SemanticException e) {
                        // ignore semantic exception because view maybe invalid
                    }
                    status.setDdl_sql(ddlSql);
                }

                result.add(new GetViewResult(dbName, status));
                // if user set limit, then only return limit size result
                if (limit > 0 && result.size() >= limit) {
                    return false;
                }
            }
        } finally {
            locker.unLockDatabase(db.getId(), LockType.READ);
        }
        return true;
    }
}
