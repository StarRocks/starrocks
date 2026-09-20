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

package com.starrocks.sql.analyzer;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.starrocks.authentication.AuthenticationMgr;
import com.starrocks.authorization.AccessControlProvider;
import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.authorization.ObjectType;
import com.starrocks.authorization.PEntryObject;
import com.starrocks.authorization.PrivilegeType;
import com.starrocks.catalog.BasicTable;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableName;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.common.util.ProfileManager;
import com.starrocks.epack.authorization.AccessControllerEPack;
import com.starrocks.epack.sql.ast.PolicyType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.QueryDetail;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.ast.AstTraverser;
import com.starrocks.sql.ast.ParseNode;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.ast.pipe.PipeName;
import com.starrocks.thrift.TFunctionBinaryType;
import com.starrocks.warehouse.Warehouse;
import org.apache.commons.collections4.ListUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

public class Authorizer {
    private final AccessControlProvider accessControlProvider;

    public Authorizer(AccessControlProvider accessControlProvider) {
        this.accessControlProvider = accessControlProvider;
    }

    public static AccessControlProvider getInstance() {
        return GlobalStateMgr.getCurrentState().getAuthorizer().accessControlProvider;
    }

    public static void check(StatementBase statement, ConnectContext context) {
        getInstance().getPrivilegeCheckerVisitor().check(statement, context);
    }

    public static void checkSystemAction(ConnectContext context, PrivilegeType privilegeType)
            throws AccessDeniedException {
        getInstance().getAccessControlOrDefault(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                .checkSystemAction(context, privilegeType);
    }

    public static void checkUserAction(ConnectContext context, UserIdentity impersonateUser,
                                       PrivilegeType privilegeType) throws AccessDeniedException {
        getInstance().getAccessControlOrDefault(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                .checkUserAction(context, impersonateUser, privilegeType);
    }

    public static void checkCatalogAction(ConnectContext context, String catalogName,
                                          PrivilegeType privilegeType) throws AccessDeniedException {
        getInstance().getAccessControlOrDefault(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                .checkCatalogAction(context, catalogName, privilegeType);
    }

    public static void checkAnyActionOnCatalog(ConnectContext context, String catalogName)
            throws AccessDeniedException {
        //Any user has an implicit usage permission on the internal catalog
        if (!CatalogMgr.isInternalCatalog(catalogName)) {
            getInstance().getAccessControlOrDefault(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                    .checkAnyActionOnCatalog(context, catalogName);
        }
    }

    public static void checkDbAction(ConnectContext context, String catalogName, String db,
                                     PrivilegeType privilegeType) throws AccessDeniedException {
        getInstance().getAccessControlOrDefault(catalogName)
                .checkDbAction(context, catalogName, db, privilegeType);
    }

    public static void checkAnyActionOnDb(ConnectContext context, String catalogName, String db)
            throws AccessDeniedException {
        getInstance().getAccessControlOrDefault(catalogName).checkAnyActionOnDb(context, catalogName, db);
    }

    public static void checkTableAction(ConnectContext context, String db, String table,
                                        PrivilegeType privilegeType) throws AccessDeniedException {
        TableName tableName = new TableName(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, db, table);
        if (isInsertIntoSomethingThatIsNotATable(context, tableName, privilegeType, null)) {
            return;
        }
        getInstance().getAccessControlOrDefault(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                .checkTableAction(context, tableName, privilegeType);
    }

    public static void checkTableAction(ConnectContext context, String catalog, String db,
                                        String table, PrivilegeType privilegeType) throws AccessDeniedException {
        TableName tableName = new TableName(catalog, db, table);
        if (isInsertIntoSomethingThatIsNotATable(context, tableName, privilegeType, null)) {
            return;
        }
        // Selected with the caller's catalog name rather than the normalized one on tableName: under
        // enable_table_name_case_insensitive the constructor lowercases it, and this lookup is by
        // exact name, so normalizing here would quietly fall back to the internal access controller
        // for a catalog registered under a mixed-case name.
        getInstance().getAccessControlOrDefault(catalog).checkTableAction(context, tableName, privilegeType);
    }

    public static void checkTableAction(ConnectContext context, TableName tableName,
                                        PrivilegeType privilegeType) throws AccessDeniedException {
        checkResolvedTableAction(context, tableName, null, privilegeType);
    }

    /**
     * Same check as {@link #checkTableAction(ConnectContext, TableName, PrivilegeType)}, for a caller
     * that already holds the table object -- the analyzer resolves every table in the statement before
     * the privilege check runs, so handing it over saves resolving it again.
     *
     * <p>A separate name rather than a fourth overload of {@code checkTableAction}: with
     * {@code (context, String db, String table, PrivilegeType)} already taking four arguments, an
     * overload would be ambiguous wherever the argument types are not written out, which is how the
     * existing {@code Mockito.any()} call sites are written.
     *
     * @param resolvedTable what {@code tableName} refers to, or null to resolve it here if needed.
     */
    public static void checkResolvedTableAction(ConnectContext context, TableName tableName, Table resolvedTable,
                                                PrivilegeType privilegeType) throws AccessDeniedException {
        if (isInsertIntoSomethingThatIsNotATable(context, tableName, privilegeType, resolvedTable)) {
            return;
        }
        getInstance().getAccessControlOrDefault(tableName.getCatalog())
                .checkTableAction(context, tableName, privilegeType);
    }

    /**
     * An INSERT into a view or a materialized view is authorized as that object rather than as a
     * table, so the table check is skipped for it. That is the only question the table object answers
     * here -- for every other privilege type the resolution result is never read.
     *
     * <p>Which is why it must not happen unconditionally. For a table in an external catalog,
     * {@code MetadataMgr#getTable} is a connector round trip (HMS, JDBC, Iceberg REST, ...), and this
     * check runs inside the planner's metadata lock: {@code StatementPlanner#plan} calls
     * {@link #check} after analysis and before it drops {@code PlannerMetaLocker}. Resolving a table
     * whose identity cannot change the outcome therefore put a request to a system the FE does not
     * control on the lock critical path, where it stalls every DDL waiting on the database intention
     * lock. So resolve only when the answer matters, and prefer the object the caller already has
     * over asking the connector again.
     */
    private static boolean isInsertIntoSomethingThatIsNotATable(ConnectContext context, TableName tableName,
                                                                PrivilegeType privilegeType, Table resolvedTable) {
        if (!PrivilegeType.INSERT.equals(privilegeType)) {
            return false;
        }
        Table table = resolvedTable != null ? resolvedTable
                : GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(context, tableName).orElse(null);
        return table != null && !table.isTable();
    }

    public static void checkAnyActionOnTable(ConnectContext context, TableName tableName)
            throws AccessDeniedException {
        String catalog = tableName.getCatalog();
        getInstance().getAccessControlOrDefault(catalog).checkAnyActionOnTable(context, tableName);
    }

    public static void checkColumnAction(ConnectContext context,
                                         TableName tableName, String column,
                                         PrivilegeType privilegeType) throws AccessDeniedException {
        getInstance().getAccessControlOrDefault(tableName.getCatalog()).checkColumnAction(context,
                tableName, column, privilegeType);
    }

    public static void checkViewAction(ConnectContext context, TableName tableName,
                                       PrivilegeType privilegeType) throws AccessDeniedException {
        getInstance().getAccessControlOrDefault(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                .checkViewAction(context, tableName, privilegeType);
    }

    public static void checkAnyActionOnView(ConnectContext context, TableName tableName)
            throws AccessDeniedException {
        getInstance().getAccessControlOrDefault(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                .checkAnyActionOnView(context, tableName);
    }

    public static void checkMaterializedViewAction(ConnectContext context, TableName tableName,
                                                   PrivilegeType privilegeType) throws AccessDeniedException {
        getInstance().getAccessControlOrDefault(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                .checkMaterializedViewAction(context, tableName, privilegeType);
    }

    public static void checkAnyActionOnMaterializedView(ConnectContext context,
                                                        TableName tableName) throws AccessDeniedException {
        getInstance().getAccessControlOrDefault(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                .checkAnyActionOnMaterializedView(context, tableName);
    }

    public static void checkActionOnTableLikeObject(ConnectContext context, TableName tableName,
                                                    PrivilegeType privilegeType) throws AccessDeniedException {
        Optional<Table> table = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(context, tableName);
        if (table.isPresent()) {
            doCheckTableLikeObject(context, tableName.getDb(), table.get(), privilegeType);
        }
    }

    public static void checkAnyActionOnTableLikeObject(ConnectContext context, String dbName,
                                                       BasicTable tableBasicInfo) throws AccessDeniedException {
        doCheckTableLikeObject(context, dbName, tableBasicInfo, null);
    }

    private static void doCheckTableLikeObject(ConnectContext context, String dbName,
                                               BasicTable tbl, PrivilegeType privilegeType) throws AccessDeniedException {
        if (tbl == null) {
            return;
        }

        Table.TableType type = tbl.getType();
        switch (type) {
            case OLAP:
            case OLAP_EXTERNAL:
            case CLOUD_NATIVE:
            case MYSQL:
            case ELASTICSEARCH:
            case HIVE:
            case HIVE_VIEW:
            case ICEBERG:
            case ICEBERG_VIEW:
            case HUDI:
            case JDBC:
            case DELTALAKE:
            case FILE:
            case SCHEMA:
            case PAIMON:
            case PAIMON_VIEW:
            case FLUSS:
            case ODPS:
            case KUDU:
            case STARROCKS:
                // `privilegeType == null` meaning we don't check specified action, just any action
                if (privilegeType == null) {
                    checkAnyActionOnTable(context, new TableName(tbl.getCatalogName(), dbName, tbl.getName()));
                } else {
                    checkTableAction(context, dbName, tbl.getName(), privilegeType);
                }
                break;
            case MATERIALIZED_VIEW:
            case CLOUD_NATIVE_MATERIALIZED_VIEW:
                // `privilegeType == null` meaning we don't check specified action, just any action
                if (privilegeType == null) {
                    checkAnyActionOnMaterializedView(context, new TableName(dbName, tbl.getName()));
                } else {
                    checkMaterializedViewAction(context, new TableName(dbName, tbl.getName()),
                            privilegeType);
                }
                break;
            case VIEW:
                // `privilegeType == null` meaning we don't check specified action, just any action
                if (privilegeType == null) {
                    checkAnyActionOnView(context, new TableName(dbName, tbl.getName()));
                } else {
                    checkViewAction(context, new TableName(dbName, tbl.getName()), privilegeType);
                }
                break;
            default:
                throw new AccessDeniedException();
        }
    }

    public static void checkActionForAnalyzeStatement(ConnectContext context, TableName tableName) {
        // Resolve once and hand the same object to both checks. This used to resolve three times --
        // once inside each checkActionOnTableLikeObject call plus once here -- and on an external
        // catalog every one of those is a connector round trip.
        Optional<Table> table = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(context, tableName);
        try {
            doCheckTableLikeObject(context, tableName.getDb(), table.orElse(null), PrivilegeType.SELECT);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    tableName.getCatalog(),
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.SELECT.name(), ObjectType.TABLE.name(), tableName.getTbl());
        }
        if (table.isPresent() && table.get().isTable()) {
            try {
                doCheckTableLikeObject(context, tableName.getDb(), table.get(), PrivilegeType.INSERT);
            } catch (AccessDeniedException e) {
                AccessDeniedException.reportAccessDenied(
                        tableName.getCatalog(),
                        context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                        PrivilegeType.INSERT.name(), ObjectType.TABLE.name(), tableName.getTbl());
            }
        }
    }

    public static void checkFunctionAction(ConnectContext context, Database database,
                                           Function function, PrivilegeType privilegeType) throws AccessDeniedException {
        getInstance().getAccessControlOrDefault(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                .checkFunctionAction(context, database, function, privilegeType);
    }

    public static void checkAnyActionOnFunction(ConnectContext context, String database, Function function)
            throws AccessDeniedException {
        getInstance().getAccessControlOrDefault(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                .checkAnyActionOnFunction(context, database, function);
    }

    public static void checkGlobalFunctionAction(ConnectContext context, Function function,
                                                 PrivilegeType privilegeType) throws AccessDeniedException {
        getInstance().getAccessControlOrDefault(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                .checkGlobalFunctionAction(context, function, privilegeType);
    }

    public static void checkAnyActionOnGlobalFunction(ConnectContext context, Function function)
            throws AccessDeniedException {
        getInstance().getAccessControlOrDefault(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                .checkAnyActionOnGlobalFunction(context, function);
    }

    public static void checkActionInDb(ConnectContext context, String db, PrivilegeType privilegeType)
            throws AccessDeniedException {
        getInstance().getAccessControlOrDefault(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                .checkActionInDb(context, db, privilegeType);
    }

    /**
     * A lambda function that throws AccessDeniedException
     */
    @FunctionalInterface
    public interface AccessControlChecker {
        void check() throws AccessDeniedException;
    }

    /**
     * Check whether current user has any privilege action on the db or objects(table/view/mv) in the db.
     * Currently, it's used by `show databases` or `use database`.
     */
    public static void checkAnyActionOnOrInDb(ConnectContext context, String catalogName, String db)
            throws AccessDeniedException {
        Preconditions.checkNotNull(db, "db should not null");
        AccessControllerEPack controller = (AccessControllerEPack) getInstance().getAccessControlOrDefault(catalogName);

        List<AccessControlChecker> basicCheckers = ImmutableList.of(
                () -> controller.checkAnyActionOnDb(context, catalogName, db),
                () -> controller.checkAnyActionOnAnyTable(context, catalogName, db),
                () -> controller.checkAnyActionOnAnyPolicy(context, PolicyType.MASKING, catalogName, db),
                () -> controller.checkAnyActionOnAnyPolicy(context, PolicyType.ROW_ACCESS, catalogName, db)
        );
        List<AccessControlChecker> extraCheckers = ImmutableList.of(
                () -> controller.checkAnyActionOnAnyView(context, db),
                () -> controller.checkAnyActionOnAnyMaterializedView(context, db),
                () -> controller.checkAnyActionOnAnyFunction(context, db),
                () -> controller.checkAnyActionOnPipe(context, new PipeName("*", "*"))
        );
        List<AccessControlChecker> appliedCheckers = CatalogMgr.isInternalCatalog(catalogName) ?
                ListUtils.union(basicCheckers, extraCheckers) : basicCheckers;

        AccessDeniedException lastExcepton = null;
        for (AccessControlChecker checker : appliedCheckers) {
            try {
                checker.check();
                return;
            } catch (AccessDeniedException e) {
                lastExcepton = e;
            }
        }
        if (lastExcepton != null) {
            throw lastExcepton;
        }
    }

    public static void checkResourceAction(ConnectContext context, String name,
                                           PrivilegeType privilegeType) throws AccessDeniedException {
        getInstance().getAccessControlOrDefault(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                .checkResourceAction(context, name, privilegeType);
    }

    public static void checkAnyActionOnResource(ConnectContext context, String name)
            throws AccessDeniedException {
        getInstance().getAccessControlOrDefault(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                .checkAnyActionOnResource(context, name);
    }

    public static void checkResourceGroupAction(ConnectContext context, String name,
                                                PrivilegeType privilegeType) throws AccessDeniedException {
        getInstance().getAccessControlOrDefault(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                .checkResourceGroupAction(context, name, privilegeType);
    }

    public static void checkPipeAction(ConnectContext context, PipeName name,
                                       PrivilegeType privilegeType) throws AccessDeniedException {
        getInstance().getAccessControlOrDefault(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                .checkPipeAction(context, name, privilegeType);
    }

    public static void checkAnyActionOnPipe(ConnectContext context, PipeName name)
            throws AccessDeniedException {
        getInstance().getAccessControlOrDefault(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                .checkAnyActionOnPipe(context, name);
    }

    public static void checkStorageVolumeAction(ConnectContext context, String storageVolume,
                                                PrivilegeType privilegeType) throws AccessDeniedException {
        getInstance().getAccessControlOrDefault(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                .checkStorageVolumeAction(context, storageVolume, privilegeType);
    }

    public static void checkAnyActionOnStorageVolume(ConnectContext context, String storageVolume)
            throws AccessDeniedException {
        getInstance().getAccessControlOrDefault(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                .checkAnyActionOnStorageVolume(context, storageVolume);
    }

    public static void withGrantOption(ConnectContext context, ObjectType type, List<PrivilegeType> wants,
                                       List<PEntryObject> objects) throws AccessDeniedException {
        getInstance().getAccessControlOrDefault(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME).
                withGrantOption(context, type, wants, objects);
    }

    public static Map<String, Expr> getColumnMaskingPolicy(ConnectContext currentUser, TableName tableName,
                                                           List<Column> columns) {
        String catalog = tableName.getCatalog() == null ? InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME : tableName.getCatalog();
        return getInstance().getAccessControlOrDefault(catalog)
                .getColumnMaskingPolicy(currentUser, tableName, columns);
    }

    public static Expr getRowAccessPolicy(ConnectContext currentUser, TableName tableName) {
        String catalog = tableName.getCatalog() == null ? InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME : tableName.getCatalog();
        return getInstance().getAccessControlOrDefault(catalog).getRowAccessPolicy(currentUser, tableName);
    }

    /**
     * check privilege for `show tablet` statement
     * if current user has 'OPERATE' privilege, it will result all the result
     * otherwise it will only return to the user on which it has any privilege on the corresponding table
     *
     * @return `Pair.first` means that whether user can see this tablet, `Pair.second` means
     * whether we need to hide the ip and port in the returned result
     */
    public static Pair<Boolean, Boolean> checkPrivForShowTablet(ConnectContext context, String dbName, Table table) {
        // if user has 'OPERATE' privilege, can see this tablet, for backward compatibility
        try {
            Authorizer.checkSystemAction(context, PrivilegeType.OPERATE);
            return new Pair<>(true, false);
        } catch (AccessDeniedException ae) {
            try {
                Authorizer.checkAnyActionOnTableLikeObject(context, dbName, table);
                return new Pair<>(true, true);
            } catch (AccessDeniedException e) {
                return new Pair<>(false, true);
            }
        }
    }

    /**
     * A cached query profile may be read by the user who ran the query or by a holder of SYSTEM OPERATE.
     * Shared by SHOW PROFILELIST, ANALYZE PROFILE and the HTTP profile endpoints so the three readers agree;
     * see {@link #canReadQueryProfile} for the rule. A no-op while
     * {@code authorization_enable_query_profile_access_check} is off, which is the default.
     */
    public static void checkQueryProfileAccess(ConnectContext context, ProfileManager.ProfileElement element)
            throws AccessDeniedException {
        if (!Config.authorization_enable_query_profile_access_check) {
            return;
        }
        // The rule needs a caller identity; an unauthenticated caller never passes it.
        if (context == null || context.getCurrentUserIdentity() == null) {
            throw new AccessDeniedException();
        }
        if (!canReadQueryProfile(context, element, () -> hasSystemAction(context, PrivilegeType.OPERATE))) {
            throw new AccessDeniedException();
        }
    }

    /**
     * Whether {@code context} may read the cached profile {@code element}.
     *
     * The owner check is a plain string compare and runs first, so an owner never pays for a privilege
     * lookup. Only a non-owner consults SYSTEM OPERATE through {@code hasOperate}; the caller supplies it so a
     * listing can evaluate it once for all rows instead of once per row (the access controller may be Ranger,
     * which audits every check). Profiles that record no user (export jobs, stream loads) are readable by
     * OPERATE alone. With {@code authorization_enable_admin_user_protection} on, root's profiles are readable
     * only by root, mirroring how SHOW PROCESSLIST hides root's sessions.
     *
     * A profile records the login user name of the session that ran the query, so ownership is matched by name
     * against both names a reader carries: its login name ({@link ConnectContext#getQualifiedUser()}) and its
     * authenticated identity's name. For a SQL session both are the canonical name; on HTTP the login name may
     * keep the header's spelling while the identity is canonical; under EXECUTE AS the login name is the
     * impersonator's while the identity is the impersonated user's, whose own profiles the impersonator may read
     * since it is exercising that user's privileges. Accounts that share a name on different hosts read each
     * other's profiles, as they do in SHOW PROCESSLIST.
     */
    public static boolean canReadQueryProfile(ConnectContext context, ProfileManager.ProfileElement element,
                                              Supplier<Boolean> hasOperate) {
        return canReadQueryProfile(context, element.getUser(), hasOperate);
    }

    /** The rule above keyed by the user name a profile records; see the element overload for the semantics. */
    public static boolean canReadQueryProfile(ConnectContext context, String owner, Supplier<Boolean> hasOperate) {
        if (isProfileOwner(context, owner)) {
            return true;
        }
        if (Config.authorization_enable_admin_user_protection && AuthenticationMgr.ROOT_USER.equals(owner)) {
            return false;
        }
        return hasOperate.get();
    }

    /**
     * {@link #checkQueryProfileAccess} plus the standard denial report. For readers that enforce where the
     * profile is actually read rather than in the analyzer, so that a profile published between the analyzer's
     * lookup and theirs is still checked.
     */
    public static void checkQueryProfileAccessAndReport(ConnectContext context, ProfileManager.ProfileElement element) {
        try {
            checkQueryProfileAccess(context, element);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    context == null ? null : context.getCurrentUserIdentity(),
                    context == null ? null : context.getCurrentRoleIds(),
                    PrivilegeType.OPERATE.name(), ObjectType.SYSTEM.name(), null);
        }
    }

    /**
     * Query-detail records carry the query's profile text, so with the access check on they follow the same
     * owner-or-OPERATE rule: a record the caller may not read is returned as a copy without its profile or its
     * explain text, while the record itself stays listed, since the query-detail APIs exist to enumerate
     * queries. Explain carries the same payload: ANALYZE PROFILE renders the target profile through
     * handleExplainStmt, which stores the rendered text on the issuing session's record. Records relayed
     * from other frontends are redacted here as well, since they arrive as plain records. Returns the input
     * unchanged while the check is off.
     */
    public static List<QueryDetail> redactUnreadableProfiles(ConnectContext context, List<QueryDetail> details) {
        if (!Config.authorization_enable_query_profile_access_check) {
            return details;
        }
        Supplier<Boolean> hasOperate = Suppliers.memoize(() -> hasSystemAction(context, PrivilegeType.OPERATE));
        List<QueryDetail> result = new ArrayList<>(details.size());
        for (QueryDetail detail : details) {
            boolean carriesProfile = detail.getProfile() != null || detail.getExplain() != null;
            if (!carriesProfile || canReadQueryProfile(context, detail.getUser(), hasOperate)) {
                result.add(detail);
            } else {
                QueryDetail redacted = detail.copy();
                redacted.setProfile(null);
                redacted.setExplain(null);
                result.add(redacted);
            }
        }
        return result;
    }

    /**
     * Whether {@code owner} is one of the names the reader carries: the session's login name or its
     * authenticated identity's. Two string compares, no allocation, since a listing calls this once per row.
     */
    private static boolean isProfileOwner(ConnectContext context, String owner) {
        if (context == null || Strings.isNullOrEmpty(owner)) {
            return false;
        }
        if (owner.equals(context.getQualifiedUser())) {
            return true;
        }
        UserIdentity identity = context.getCurrentUserIdentity();
        return identity != null && owner.equals(identity.getUser());
    }

    /**
     * Whether {@code fn} is the builtin get_query_profile(), as opposed to a user function that shares the name.
     * Builtins carry {@link TFunctionBinaryType#BUILTIN}; jar and Python UDFs carry their own binary type and a
     * SQL-defined function carries none, so this holds for the builtin alone.
     */
    public static boolean isGetQueryProfileBuiltin(Function fn) {
        return fn != null && fn.getBinaryType() == TFunctionBinaryType.BUILTIN
                && FunctionSet.GET_QUERY_PROFILE.equalsIgnoreCase(fn.functionName());
    }

    /**
     * Whether an analyzed statement calls the builtin get_query_profile() anywhere in its expressions, scalar
     * subqueries included. PrepareStmtPlanner keeps such statements off its cached-plan path so that every
     * EXECUTE re-analyzes and passes {@link #checkGetQueryProfileAccess} again.
     */
    public static boolean containsGetQueryProfile(ParseNode node) {
        GetQueryProfileFinder finder = new GetQueryProfileFinder();
        finder.visit(node);
        return finder.found;
    }

    private static class GetQueryProfileFinder extends AstTraverser<Void, Void> {
        private boolean found;

        @Override
        public Void visitFunctionCall(FunctionCallExpr expr, Void context) {
            if (isGetQueryProfileBuiltin(expr.getFn())) {
                found = true;
            }
            return super.visitExpression(expr, context);
        }
    }

    /**
     * Access rule for {@code get_query_profile(query_id)}. The BE serves the function through an FE RPC that
     * carries no caller identity, so the rule is enforced at analysis time instead, and prepared statements
     * containing the builtin never reuse a cached plan (PrepareStmtPlanner), since a later EXECUTE could rebind
     * the id without passing here again. A constant id whose profile is cached on this FE is checked against its
     * owner like ANALYZE PROFILE. Everything else needs SYSTEM OPERATE (root alone while admin-user protection
     * is on, since the profile may be root's): a non-constant id can name any profile, and a constant id not
     * cached here is fetched by that RPC from the other frontends, where no caller identity is available to
     * apply the owner rule. A no-op while the check is off or the session bypasses authorization.
     */
    public static void checkGetQueryProfileAccess(ConnectContext context, Expr queryIdArg) {
        if (!Config.authorization_enable_query_profile_access_check || context == null
                || context.isBypassAuthorizerCheck()) {
            return;
        }
        boolean constantId = queryIdArg instanceof StringLiteral;
        ProfileManager.ProfileElement element = constantId
                ? ProfileManager.getInstance().getProfileElement(((StringLiteral) queryIdArg).getStringValue())
                : null;
        try {
            if (element != null) {
                checkQueryProfileAccess(context, element);
                return;
            }
            checkSystemAction(context, PrivilegeType.OPERATE);
            if (Config.authorization_enable_admin_user_protection
                    && !isProfileOwner(context, AuthenticationMgr.ROOT_USER)) {
                throw new AccessDeniedException();
            }
        } catch (AccessDeniedException e) {
            if (!constantId) {
                // Point at the real obstacle: the id could not be resolved here, not a missing privilege on a
                // profile the caller may well own. The two-step form in the get_query_profile() docs works.
                throw new SemanticException("get_query_profile() requires a constant query id unless you hold "
                        + "the SYSTEM-level OPERATE privilege; run the id-producing expression as its own "
                        + "statement (for example 'select last_query_id();') and pass the resulting literal",
                        queryIdArg.getPos());
            }
            AccessDeniedException.reportAccessDenied(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.OPERATE.name(), ObjectType.SYSTEM.name(), null);
        }
    }

    /** {@link #checkSystemAction} as a predicate, for callers that filter rather than fail. */
    public static boolean hasSystemAction(ConnectContext context, PrivilegeType privilegeType) {
        if (context == null) {
            return false;
        }
        try {
            checkSystemAction(context, privilegeType);
            return true;
        } catch (AccessDeniedException e) {
            return false;
        }
    }

    public static void checkWarehouseAction(ConnectContext context, String name,
                                            PrivilegeType privilegeType) throws AccessDeniedException {
        getInstance().getAccessControlOrDefault(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                .checkWarehouseAction(context, name, privilegeType);
    }

    public static void checkAnyActionOnWarehouse(ConnectContext context, String name)
            throws AccessDeniedException {
        // Any user has an implicit usage permission on the default_warehouse
        Warehouse warehouse = GlobalStateMgr.getCurrentState().getWarehouseMgr().getWarehouse(name);
        if (warehouse.getId() != WarehouseManager.DEFAULT_WAREHOUSE_ID) {
            getInstance().getAccessControlOrDefault(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                    .checkAnyActionOnWarehouse(context, name);
        }
    }

    public static void checkContextBaseAction(ConnectContext context, String name,
                                              PrivilegeType privilegeType) throws AccessDeniedException {
        getInstance().getAccessControlOrDefault(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                .checkContextBaseAction(context, name, privilegeType);
    }

    public static void checkAnyActionOnContextBase(ConnectContext context, String name)
            throws AccessDeniedException {
        getInstance().getAccessControlOrDefault(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                .checkAnyActionOnContextBase(context, name);
    }
}
