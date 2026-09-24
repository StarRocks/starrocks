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

package com.starrocks.epack.connector.lakeformation;

import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.authorization.ExternalAccessController;
import com.starrocks.authorization.ObjectType;
import com.starrocks.authorization.PEntryObject;
import com.starrocks.authorization.PrivilegeType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableName;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.epack.authorization.AccessControllerEPack;
import com.starrocks.epack.authorization.NativeAccessControllerEPack;
import com.starrocks.epack.sql.ast.PolicyType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.pipe.PipeName;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;

import java.util.List;
import java.util.Map;

/**
 * Lake Formation for the tables it governs, native RBAC for everything else.
 *
 * Extending ExternalAccessController is what makes a column level refusal report error 5204 instead of the
 * generic one: AccessDeniedException.reportAccessDenied only picks that code for an external controller.
 * Implementing AccessControllerEPack is not optional either - five places cast the active controller to it
 * without any guard, so a plain ExternalAccessController would turn `USE db` into a ClassCastException.
 *
 * The split is by object. A governed table's columns, row and masking policies are Lake Formation's alone;
 * everything it has no object for - unregistered tables, views, databases, platform actions - is delegated
 * verbatim to NativeAccessControllerEPack.
 *
 * Name level entry points still delegate: resolving every SHOW TABLES name through Lake Formation has no budget.
 */
public class LakeFormationAccessController extends ExternalAccessController implements AccessControllerEPack {

    /**
     * The column name RewriteSimpleAggToHDFSScanRule invents for count(*) over an HDFS scan. Compared
     * exactly, including case: the argument for waving it through is about this one name, and nothing has
     * been shown about any other.
     */
    private static final String COUNT_STAR_PLACEHOLDER = "___count___";

    /**
     * Not a subclass: Java has one superclass and it has to be ExternalAccessController for 5204, so the
     * native behaviour is composed in rather than inherited.
     *
     * Declared as the interface rather than the concrete class because that is all this needs, and because
     * it lets a test substitute a probe that proves each override really delegates instead of merely
     * existing - an empty override would be a silent grant.
     */
    private final AccessControllerEPack nativeController;

    public LakeFormationAccessController() {
        this(new NativeAccessControllerEPack());
    }

    /**
     * Visible for testing: the delegation test substitutes a probe that throws for every call, which is how
     * it tells a method that really forwards from one with an empty body. An empty body would be a silent
     * grant, so "the method exists" is not a strong enough check on its own.
     */
    LakeFormationAccessController(AccessControllerEPack nativeController) {
        this.nativeController = nativeController;
    }

    /**
     * ColumnPrivilege gives external controllers a different branch: it checks each pruned scan column with
     * checkColumnAction and never calls checkTableAction at all. So for a table Lake Formation does not
     * govern the native table level decision has to be made here, or it is not made anywhere.
     *
     * Note this runs once per column, and so does the getTable below it. With a query id that is one Lake
     * Formation call for the whole statement because the resolution is memoized per query; without one it is
     * one call per column.
     */
    @Override
    public void checkColumnAction(ConnectContext context, TableName tableName, String column,
                                  PrivilegeType privilegeType) throws AccessDeniedException {
        Table table = GlobalStateMgr.getCurrentState().getMetadataMgr()
                .getTable(context, tableName.getCatalog(), tableName.getDb(), tableName.getTbl());
        if (table == null) {
            // MetadataMgr answers null both when the table is gone and when it could not reach the
            // connector at all. The second case is indistinguishable from the first here, and letting the
            // column through on it would be a fail open.
            throw new AccessDeniedException("Cannot verify Lake Formation authorization for " + tableName
                    + ": its metadata is unavailable.");
        }
        if (!(table instanceof LakeFormationGovernedTable governed)) {
            // Not governed by Lake Formation - an unregistered table in the same catalog, or a view. The
            // native decision is the whole answer.
            nativeController.checkTableAction(context, tableName, privilegeType);
            return;
        }
        // Governed: Lake Formation resolved this table for this statement and named the columns it may
        // read. A native table grant would add nothing it can distinguish between users, so none is asked for.
        if (governed.isColumnAuthorized(column)) {
            return;
        }
        if (!governed.isPhysicalColumn(column) && COUNT_STAR_PLACEHOLDER.equals(column)) {
            // The one name that is not a column of any table and still has to be waved through.
            //
            // ColumnPrivilege collects the columns it checks from the *optimized* plan, and
            // RewriteSimpleAggToHDFSScanRule rewrites count(*) over an HDFS scan into a scan of a
            // placeholder column named ___count___ that carries only a row count. Asking Lake Formation
            // about that name refuses every count(*) on a governed table.
            //
            // Letting this one through cannot expose data: the name is absent from the physical column
            // list, the BE maps columns by name against THdfsTable.columns, and an entry with no matching
            // slot is skipped. Both halves of the condition are load bearing - a *physical* column that
            // happens to be called ___count___ is a real column and goes through the refusal below, and
            // any other synthetic name is refused because nothing has shown it to be harmless.
            return;
        }
        throw new AccessDeniedException("Lake Formation has not granted SELECT on column '" + column
                + "' of " + tableName + ".");
    }

    /**
     * Whether Lake Formation governs the named table. Null when it does not, or when that cannot be told:
     * the callers below treat both as "not governed" and fall through to the native policy, which is the
     * conservative side for a policy (a policy can only restrict) and matches what happened before.
     */
    private LakeFormationGovernedTable governedTable(ConnectContext context, TableName tableName) {
        Table table = GlobalStateMgr.getCurrentState().getMetadataMgr()
                .getTable(context, tableName.getCatalog(), tableName.getDb(), tableName.getTbl());
        return table instanceof LakeFormationGovernedTable governed ? governed : null;
    }

    /**
     * A governed table carries no native policy - Lake Formation is its only policy owner. Any other table
     * delegates, with one translation: a policy whose expression reads a column that Lake Formation did not
     * authorize makes MetaUtils fail with "can not find column by column id", which says nothing about
     * authorization and sends whoever reads it looking in the wrong place. The underlying behaviour is
     * already fail closed; only the message changes.
     */
    @Override
    public Map<String, Expr> getColumnMaskingPolicy(ConnectContext context, TableName tableName,
                                                    List<Column> columns) {
        if (governedTable(context, tableName) != null) {
            return Map.of();
        }
        try {
            return nativeController.getColumnMaskingPolicy(context, tableName, columns);
        } catch (StarRocksPlannerException e) {
            throw translatePolicyColumnFailure(e, tableName);
        }
    }

    @Override
    public Expr getRowAccessPolicy(ConnectContext currentUser, TableName tableName) {
        if (governedTable(currentUser, tableName) != null) {
            return null;
        }
        try {
            return nativeController.getRowAccessPolicy(currentUser, tableName);
        } catch (StarRocksPlannerException e) {
            throw translatePolicyColumnFailure(e, tableName);
        }
    }

    /**
     * Only the one message shape is translated. Swallowing every SemanticException here would turn genuine
     * analysis errors into misleading authorization messages.
     */
    private RuntimeException translatePolicyColumnFailure(RuntimeException e, TableName tableName) {
        String message = e.getMessage();
        if (message != null && message.contains("can not find column by column id")) {
            return new StarRocksPlannerException("A column masking or row access policy on " + tableName
                    + " references a column that Lake Formation has not authorized for you. Ask your"
                    + " administrator to reconcile the policy with the Lake Formation grants.",
                    ErrorType.USER_ERROR);
        }
        return e;
    }

    @Override
    public void checkSystemAction(ConnectContext context, PrivilegeType privilegeType) throws AccessDeniedException {
        nativeController.checkSystemAction(context, privilegeType);
    }

    @Override
    public void checkUserAction(ConnectContext context, UserIdentity impersonateUser, PrivilegeType privilegeType)
            throws AccessDeniedException {
        nativeController.checkUserAction(context, impersonateUser, privilegeType);
    }

    @Override
    public void checkCatalogAction(ConnectContext context, String catalogName, PrivilegeType privilegeType)
            throws AccessDeniedException {
        nativeController.checkCatalogAction(context, catalogName, privilegeType);
    }

    @Override
    public void checkAnyActionOnCatalog(ConnectContext context, String catalogName) throws AccessDeniedException {
        nativeController.checkAnyActionOnCatalog(context, catalogName);
    }

    @Override
    public void checkDbAction(ConnectContext context, String catalogName, String db, PrivilegeType privilegeType)
            throws AccessDeniedException {
        nativeController.checkDbAction(context, catalogName, db, privilegeType);
    }

    @Override
    public void checkAnyActionOnDb(ConnectContext context, String catalogName, String db) throws AccessDeniedException {
        nativeController.checkAnyActionOnDb(context, catalogName, db);
    }

    @Override
    public void checkTableAction(ConnectContext context, TableName tableName, PrivilegeType privilegeType)
            throws AccessDeniedException {
        nativeController.checkTableAction(context, tableName, privilegeType);
    }

    @Override
    public void checkAnyActionOnTable(ConnectContext context, TableName tableName) throws AccessDeniedException {
        nativeController.checkAnyActionOnTable(context, tableName);
    }

    @Override
    public void checkAnyActionOnAnyTable(ConnectContext context, String catalog, String db) throws AccessDeniedException {
        nativeController.checkAnyActionOnAnyTable(context, catalog, db);
    }

    @Override
    public void checkViewAction(ConnectContext context, TableName tableName, PrivilegeType privilegeType)
            throws AccessDeniedException {
        nativeController.checkViewAction(context, tableName, privilegeType);
    }

    @Override
    public void checkAnyActionOnView(ConnectContext context, TableName tableName) throws AccessDeniedException {
        nativeController.checkAnyActionOnView(context, tableName);
    }

    @Override
    public void checkAnyActionOnAnyView(ConnectContext context, String db) throws AccessDeniedException {
        nativeController.checkAnyActionOnAnyView(context, db);
    }

    @Override
    public void checkMaterializedViewAction(ConnectContext context, TableName tableName, PrivilegeType privilegeType)
            throws AccessDeniedException {
        nativeController.checkMaterializedViewAction(context, tableName, privilegeType);
    }

    @Override
    public void checkAnyActionOnMaterializedView(ConnectContext context, TableName tableName) throws AccessDeniedException {
        nativeController.checkAnyActionOnMaterializedView(context, tableName);
    }

    @Override
    public void checkAnyActionOnAnyMaterializedView(ConnectContext context, String db) throws AccessDeniedException {
        nativeController.checkAnyActionOnAnyMaterializedView(context, db);
    }

    @Override
    public void checkFunctionAction(ConnectContext context, Database database, Function function, PrivilegeType privilegeType)
            throws AccessDeniedException {
        nativeController.checkFunctionAction(context, database, function, privilegeType);
    }

    @Override
    public void checkAnyActionOnFunction(ConnectContext context, String database, Function function)
            throws AccessDeniedException {
        nativeController.checkAnyActionOnFunction(context, database, function);
    }

    @Override
    public void checkAnyActionOnAnyFunction(ConnectContext context, String database) throws AccessDeniedException {
        nativeController.checkAnyActionOnAnyFunction(context, database);
    }

    @Override
    public void checkGlobalFunctionAction(ConnectContext context, Function function, PrivilegeType privilegeType)
            throws AccessDeniedException {
        nativeController.checkGlobalFunctionAction(context, function, privilegeType);
    }

    @Override
    public void checkAnyActionOnGlobalFunction(ConnectContext context, Function function) throws AccessDeniedException {
        nativeController.checkAnyActionOnGlobalFunction(context, function);
    }

    @Override
    public void checkActionInDb(ConnectContext context, String db, PrivilegeType privilegeType) throws AccessDeniedException {
        nativeController.checkActionInDb(context, db, privilegeType);
    }

    @Override
    public void checkResourceAction(ConnectContext context, String name, PrivilegeType privilegeType)
            throws AccessDeniedException {
        nativeController.checkResourceAction(context, name, privilegeType);
    }

    @Override
    public void checkAnyActionOnResource(ConnectContext context, String name) throws AccessDeniedException {
        nativeController.checkAnyActionOnResource(context, name);
    }

    @Override
    public void checkResourceGroupAction(ConnectContext context, String name, PrivilegeType privilegeType)
            throws AccessDeniedException {
        nativeController.checkResourceGroupAction(context, name, privilegeType);
    }

    @Override
    public void checkPipeAction(ConnectContext context, PipeName name, PrivilegeType privilegeType) throws AccessDeniedException {
        nativeController.checkPipeAction(context, name, privilegeType);
    }

    @Override
    public void checkAnyActionOnPipe(ConnectContext context, PipeName name) throws AccessDeniedException {
        nativeController.checkAnyActionOnPipe(context, name);
    }

    @Override
    public void checkStorageVolumeAction(ConnectContext context, String storageVolume, PrivilegeType privilegeType)
            throws AccessDeniedException {
        nativeController.checkStorageVolumeAction(context, storageVolume, privilegeType);
    }

    @Override
    public void checkAnyActionOnStorageVolume(ConnectContext context, String storageVolume) throws AccessDeniedException {
        nativeController.checkAnyActionOnStorageVolume(context, storageVolume);
    }

    @Override
    public void withGrantOption(ConnectContext context, ObjectType type, List<PrivilegeType> wants, List<PEntryObject> objects)
            throws AccessDeniedException {
        nativeController.withGrantOption(context, type, wants, objects);
    }

    @Override
    public void checkWarehouseAction(ConnectContext context, String name, PrivilegeType privilegeType)
            throws AccessDeniedException {
        nativeController.checkWarehouseAction(context, name, privilegeType);
    }

    @Override
    public void checkAnyActionOnWarehouse(ConnectContext context, String name) throws AccessDeniedException {
        nativeController.checkAnyActionOnWarehouse(context, name);
    }

    @Override
    public void checkContextBaseAction(ConnectContext context, String name, PrivilegeType privilegeType)
            throws AccessDeniedException {
        nativeController.checkContextBaseAction(context, name, privilegeType);
    }

    @Override
    public void checkAnyActionOnContextBase(ConnectContext context, String name) throws AccessDeniedException {
        nativeController.checkAnyActionOnContextBase(context, name);
    }

    @Override
    public void checkPolicyAction(ConnectContext context, PolicyType policyType, String catalogName, String db, String policy,
                                  PrivilegeType privilegeType) throws AccessDeniedException {
        nativeController.checkPolicyAction(context, policyType, catalogName, db, policy, privilegeType);
    }

    @Override
    public void checkAnyActionOnPolicy(ConnectContext context, PolicyType policyType, String catalogName, String db,
                                       String policy)
            throws AccessDeniedException {
        nativeController.checkAnyActionOnPolicy(context, policyType, catalogName, db, policy);
    }

    @Override
    public void checkAnyActionOnAnyPolicy(ConnectContext context, PolicyType policyType, String catalogName, String db)
            throws AccessDeniedException {
        nativeController.checkAnyActionOnAnyPolicy(context, policyType, catalogName, db);
    }

    @Override
    public void checkFailoverGroupAction(ConnectContext context, String name, PrivilegeType privilegeType)
            throws AccessDeniedException {
        nativeController.checkFailoverGroupAction(context, name, privilegeType);
    }

    @Override
    public void checkAnyActionOnFailoverGroup(ConnectContext context, String name) throws AccessDeniedException {
        nativeController.checkAnyActionOnFailoverGroup(context, name);
    }
}
