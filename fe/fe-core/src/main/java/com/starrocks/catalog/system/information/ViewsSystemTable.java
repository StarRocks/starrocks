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
import com.starrocks.authentication.AuthenticationMgr;
import com.starrocks.authentication.UserIdentityUtils;
import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.authorization.PrivilegeBuiltinConstants;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableName;
import com.starrocks.catalog.UserIdentity;
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
import com.starrocks.thrift.TUserRoles;
import com.starrocks.type.Type;
import com.starrocks.type.TypeFactory;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.parquet.Strings;
import org.apache.thrift.TException;
import org.apache.thrift.meta_data.FieldValueMetaData;

import java.util.ArrayList;
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
                        .column("TABLE_CATALOG", TypeFactory.createVarcharType(512))
                        .column("TABLE_SCHEMA", TypeFactory.createVarcharType(64))
                        .column("TABLE_NAME", TypeFactory.createVarcharType(64))
                        // TODO: Type for EVENT_DEFINITION should be `longtext`, but `varchar(65535)` was set at this stage.
                        .column("VIEW_DEFINITION",
                                TypeFactory.createVarcharType(MAX_FIELD_VARCHAR_LENGTH))
                        .column("CHECK_OPTION", TypeFactory.createVarcharType(8))
                        .column("IS_UPDATABLE", TypeFactory.createVarcharType(3))
                        .column("DEFINER", TypeFactory.createVarcharType(77))
                        .column("SECURITY_TYPE", TypeFactory.createVarcharType(7))
                        .column("CHARACTER_SET_CLIENT", TypeFactory.createVarcharType(32))
                        .column("COLLATION_CONNECTION", TypeFactory.createVarcharType(32))
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
        TUserIdentity userIdentity = UserIdentityUtils.toThrift(context.getCurrentUserIdentity());
        if (context.getCurrentRoleIds() != null) {
            TUserRoles userRoles = new TUserRoles();
            userRoles.setRole_id_list(new ArrayList<>(context.getCurrentRoleIds()));
            userIdentity.setCurrent_role_ids(userRoles);
        }
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
        if (params.isSetCurrent_user_ident()) {
            UserIdentityUtils.setAuthInfoFromThrift(context, params.getCurrent_user_ident());
        } else {
            UserIdentity currentUser = UserIdentity.createAnalyzedUserIdentWithIp(params.user, params.user_ip);
            context.setCurrentUserIdentity(currentUser);
            context.setCurrentRoleIds(currentUser);
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
            if (!collectViewsInDb(context, db, tableNameParam, pattern, matcher,
                    limit, listingViews, caseSensitive, result)) {
                break;
            }
        }
        return result;
    }

    /**
     * Whether the table passes the TABLE_NAME and pattern filters of the request. Evaluated twice
     * per table - once as a cheap pre-filter outside the table lock and once under it - so it is
     * kept in one place to make sure the two never drift apart.
     */
    private static boolean matchesNameFilters(Table table, String paramTableName, String paramPattern,
                                              PatternMatcher matcher, boolean caseSensitive) {
        if (!PatternMatcher.matchPattern(paramPattern, table.getName(), matcher, caseSensitive)) {
            return false;
        }
        return Strings.isNullOrEmpty(paramTableName) || table.getName().equalsIgnoreCase(paramTableName);
    }

    /**
     * if the limit is reached, false is returned, otherwise true will be returned.
     */
    private static boolean collectViewsInDb(ConnectContext context, Database db,
                                            String paramTableName, String paramPattern, PatternMatcher matcher,
                                            long limit, boolean listingViews, boolean caseSensitive,
                                            List<GetViewResult> result) {
        if (db == null) {
            return true;
        }
        // A DB-wide READ lock must not be used here, because this method runs in two very
        // different scopes: the thrift `listTableStatus` handler, which holds no meta lock, and
        // SchemaTableEvaluateRule during optimization, which runs inside PlannerMetaLocker and
        // therefore already holds an INTENTION_SHARED lock on every database referenced by the
        // query. Hierarchical locking forbids requesting a plain database READ lock inside an
        // intention scope - MultiUserLock#tryLock throws NotSupportLockException - which fails
        // the whole query, both for information_schema itself and for every user database the
        // query touches.
        //
        // Instead this follows the split design fe/AGENTS.md prescribes for "snapshot a table
        // list, then do per-table work", in the shape TabletScheduler#getTabletBalanceTypes uses:
        // a database INTENTION_SHARED lock scopes the snapshot only, and each table is then
        // visited under its own intensive READ lock (IS on the database + READ on the table).
        // Keeping the IS to the snapshot means CREATE/DROP TABLE, which take DB WRITE, are only
        // blocked while the list is read rather than for the whole walk.
        //
        // Everything a reported row is built from - the database name, the name and pattern
        // match, the authorization decision and the row itself - is read under that per-table
        // lock. That matters for two different renames:
        //
        //   ALTER TABLE ... RENAME takes IX on the database plus WRITE on the table
        //   (AlterJobExecutor#visitTableRenameClause). IS does not conflict with IX, so only the
        //   table READ half keeps a rename from landing between a name check and the row being
        //   built - which would emit a row carrying the new name while it was selected by the
        //   old one, and would let a name-based external authorizer decide on the pre-rename
        //   name.
        //
        //   ALTER DATABASE ... RENAME takes DB WRITE (LocalMetastore#renameDatabase), which the
        //   IS half does conflict with. Reading db.getFullName() before the lock would leave
        //   TABLE_SCHEMA and that same authorization decision pointing at a stale namespace, so
        //   the name is read inside the lock, per row.
        //
        // A cheap copy of the name filters also runs outside the lock, purely so that a
        // selective request does not lock and authorize every table; that call site explains it.
        List<Table> tables;
        Locker dbLocker = new Locker();
        dbLocker.lockDatabase(db.getId(), LockType.INTENTION_SHARED);
        try {
            tables = listingViews ? db.getViews() :
                    GlobalStateMgr.getCurrentState().getLocalMetastore().getTables(db.getId());
        } finally {
            dbLocker.unLockDatabase(db.getId(), LockType.INTENTION_SHARED);
        }

        OUTER:
        for (Table table : tables) {
            // Cheap pre-filter, outside the lock: without it a request that names one table or a
            // selective pattern would still take a table lock and run an authorization check for
            // every table in the database, and with no TABLE_SCHEMA filter that is every table in
            // the catalog. The name is read unsynchronized here, so a table renamed *into* the
            // filter while the scan is running can be missed; that is acceptable for a concurrent
            // metadata listing, and the evaluation under the lock below stays authoritative for
            // everything that is reported.
            if (!matchesNameFilters(table, paramTableName, paramPattern, matcher, caseSensitive)) {
                continue;
            }

            Locker locker = new Locker();
            locker.lockTableWithIntensiveDbLock(db.getId(), table.getId(), LockType.READ);
            try {
                // The snapshot was taken under a lock that has since been released, so skip a
                // table that a concurrent DROP removed in the meantime.
                if (GlobalStateMgr.getCurrentState().getLocalMetastore()
                        .getTable(db.getId(), table.getId()) == null) {
                    continue;
                }

                // Stable under the IS half of this lock, which conflicts with the DB WRITE that
                // ALTER DATABASE ... RENAME takes.
                String dbName = db.getFullName();

                // Authoritative re-evaluation: the name is stable under the table lock, so a
                // rename that raced with the pre-filter cannot make the emitted row disagree
                // with the filter that selected it.
                if (!matchesNameFilters(table, paramTableName, paramPattern, matcher, caseSensitive)) {
                    continue;
                }

                try {
                    Authorizer.checkAnyActionOnTableLikeObject(context, dbName, table);
                } catch (AccessDeniedException e) {
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
                                    .getTable(dbName, tableName.getTbl());
                            if (tbl != null) {
                                try {
                                    Authorizer.checkAnyActionOnTableLikeObject(context, dbName, tbl);
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
            } finally {
                locker.unLockTableWithIntensiveDbLock(db.getId(), table.getId(), LockType.READ);
            }
        }
        return true;
    }
}
