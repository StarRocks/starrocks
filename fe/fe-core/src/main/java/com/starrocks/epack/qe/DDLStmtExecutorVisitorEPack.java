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
package com.starrocks.epack.qe;

import com.starrocks.common.ErrorReport;
import com.starrocks.epack.privilege.SecurityPolicyMgr;
import com.starrocks.epack.server.WarehouseManagerEPack;
import com.starrocks.epack.sql.ast.AlterFailoverGroupAddStmt;
import com.starrocks.epack.sql.ast.AlterFailoverGroupPrimaryStmt;
import com.starrocks.epack.sql.ast.AlterFailoverGroupRefreshStmt;
import com.starrocks.epack.sql.ast.AlterFailoverGroupRemoveStmt;
import com.starrocks.epack.sql.ast.AlterFailoverGroupResumeStmt;
import com.starrocks.epack.sql.ast.AlterFailoverGroupSetStmt;
import com.starrocks.epack.sql.ast.AlterFailoverGroupSuspendStmt;
import com.starrocks.epack.sql.ast.AlterPolicyStmt;
import com.starrocks.epack.sql.ast.AlterRoleMappingStatement;
import com.starrocks.epack.sql.ast.AlterSecurityIntegrationStatement;
import com.starrocks.epack.sql.ast.AstVisitorEPack;
import com.starrocks.epack.sql.ast.CreatePolicyStmt;
import com.starrocks.epack.sql.ast.CreatePrimaryFailoverGroupStmt;
import com.starrocks.epack.sql.ast.CreateRoleMappingStatement;
import com.starrocks.epack.sql.ast.CreateSecondaryFailoverGroupStmt;
import com.starrocks.epack.sql.ast.CreateSecurityIntegrationStatement;
import com.starrocks.epack.sql.ast.CreateWarehouseStmt;
import com.starrocks.epack.sql.ast.DropFailoverGroupStmt;
import com.starrocks.epack.sql.ast.DropPolicyStmt;
import com.starrocks.epack.sql.ast.DropRoleMappingStatement;
import com.starrocks.epack.sql.ast.DropSecurityIntegrationStatement;
import com.starrocks.epack.sql.ast.DropWarehouseStmt;
import com.starrocks.epack.sql.ast.RefreshRoleMappingStatement;
import com.starrocks.epack.sql.ast.ResumeWarehouseStmt;
import com.starrocks.epack.sql.ast.SuspendWarehouseStmt;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.qe.ShowResultSet;

public class DDLStmtExecutorVisitorEPack extends DDLStmtExecutor.StmtExecutorVisitor
        implements AstVisitorEPack<ShowResultSet, ConnectContext> {

    private static final DDLStmtExecutorVisitorEPack INSTANCE = new DDLStmtExecutorVisitorEPack();

    public static DDLStmtExecutor.StmtExecutorVisitor getInstance() {
        return INSTANCE;
    }

    protected DDLStmtExecutorVisitorEPack() {
    }

    @Override
    public ShowResultSet visitCreateSecurityIntegrationStatement(CreateSecurityIntegrationStatement stmt,
                                                                 ConnectContext context) {
        ErrorReport.wrapWithRuntimeException(() -> {
            context.getGlobalStateMgr().getAuthenticationMgr().createSecurityIntegration(
                    stmt.getName(), stmt.getPropertyMap(), false);
        });

        return null;
    }

    @Override
    public ShowResultSet visitAlterSecurityIntegrationStatement(AlterSecurityIntegrationStatement stmt,
                                                                ConnectContext context) {
        ErrorReport.wrapWithRuntimeException(() -> {
            context.getGlobalStateMgr().getAuthenticationMgr().alterSecurityIntegration(
                    stmt.getName(), stmt.getProperties(), false);
        });

        return null;
    }

    @Override
    public ShowResultSet visitDropSecurityIntegrationStatement(DropSecurityIntegrationStatement stmt,
                                                               ConnectContext context) {
        ErrorReport.wrapWithRuntimeException(() -> {
            context.getGlobalStateMgr().getAuthenticationMgr().dropSecurityIntegration(
                    stmt.getName(), false);
        });

        return null;
    }

    @Override
    public ShowResultSet visitCreateRoleMappingStatement(CreateRoleMappingStatement stmt,
                                                         ConnectContext context) {
        ErrorReport.wrapWithRuntimeException(() -> {
            context.getGlobalStateMgr().getAuthorizationMgr().getRoleMappingMetaMgr().createRoleMapping(
                    stmt.getName(), stmt.getPropertyMap(), false);
        });

        return null;
    }

    @Override
    public ShowResultSet visitAlterRoleMappingStatement(AlterRoleMappingStatement stmt,
                                                        ConnectContext context) {
        ErrorReport.wrapWithRuntimeException(() -> {
            context.getGlobalStateMgr().getAuthorizationMgr().getRoleMappingMetaMgr().alterRoleMapping(
                    stmt.getName(), stmt.getProperties(), false);
        });

        return null;
    }

    @Override
    public ShowResultSet visitDropRoleMappingStatement(DropRoleMappingStatement stmt,
                                                       ConnectContext context) {
        ErrorReport.wrapWithRuntimeException(() -> {
            context.getGlobalStateMgr().getAuthorizationMgr().getRoleMappingMetaMgr().dropRoleMapping(
                    stmt.getName(), false);
        });

        return null;
    }

    @Override
    public ShowResultSet visitRefreshRoleMappingStatement(RefreshRoleMappingStatement stmt,
                                                          ConnectContext context) {
        ErrorReport.wrapWithRuntimeException(() -> {
            context.getGlobalStateMgr().refreshRoleMapping(stmt);
        });

        return null;
    }

    @Override
    public ShowResultSet visitCreatePolicyStatement(CreatePolicyStmt stmt, ConnectContext context) {
        ErrorReport.wrapWithRuntimeException(() -> {
            SecurityPolicyMgr securityPolicyManagerEE =
                    context.getGlobalStateMgr().getSecurityPolicyManager();
            securityPolicyManagerEE.createMaskingPolicy(stmt);
        });
        return null;
    }

    @Override
    public ShowResultSet visitDropPolicyStatement(DropPolicyStmt stmt, ConnectContext context) {
        ErrorReport.wrapWithRuntimeException(() -> {
            SecurityPolicyMgr securityPolicyManagerEE =
                    context.getGlobalStateMgr().getSecurityPolicyManager();
            securityPolicyManagerEE.dropPolicy(stmt);
        });

        return null;
    }

    @Override
    public ShowResultSet visitAlterPolicyStatement(AlterPolicyStmt stmt, ConnectContext context) {
        ErrorReport.wrapWithRuntimeException(() -> {
            SecurityPolicyMgr securityPolicyManagerEE =
                    context.getGlobalStateMgr().getSecurityPolicyManager();
            securityPolicyManagerEE.alterPolicy(stmt);
        });

        return null;
    }

    //=========================================== Warehouse Statement ==================================================

    @Override
    public ShowResultSet visitCreateWarehouseStatement(CreateWarehouseStmt stmt, ConnectContext context) {
        ErrorReport.wrapWithRuntimeException(() -> {
            WarehouseManagerEPack warehouseMgr = (WarehouseManagerEPack) context.getGlobalStateMgr().getWarehouseMgr();
            warehouseMgr.createWarehouse(stmt);

        });
        return null;
    }

    @Override
    public ShowResultSet visitSuspendWarehouseStatement(SuspendWarehouseStmt stmt, ConnectContext context) {
        ErrorReport.wrapWithRuntimeException(() -> {
            WarehouseManagerEPack warehouseMgr = (WarehouseManagerEPack) context.getGlobalStateMgr().getWarehouseMgr();
            warehouseMgr.suspendWarehouse(stmt);

        });
        return null;
    }

    @Override
    public ShowResultSet visitResumeWarehouseStatement(ResumeWarehouseStmt stmt, ConnectContext context) {
        ErrorReport.wrapWithRuntimeException(() -> {
            WarehouseManagerEPack warehouseMgr = (WarehouseManagerEPack) context.getGlobalStateMgr().getWarehouseMgr();
            warehouseMgr.resumeWarehouse(stmt);
        });
        return null;
    }

    @Override
    public ShowResultSet visitDropWarehouseStatement(DropWarehouseStmt stmt, ConnectContext context) {
        ErrorReport.wrapWithRuntimeException(() -> {
            WarehouseManagerEPack warehouseMgr = (WarehouseManagerEPack) context.getGlobalStateMgr().getWarehouseMgr();
            warehouseMgr.dropWarehouse(stmt);
        });
        return null;
    }

    //=========================================== Failover Group Statement ========================================

    @Override
    public ShowResultSet visitCreatePrimaryFailoverGroupStatement(CreatePrimaryFailoverGroupStmt stmt,
                                                                  ConnectContext context) {
        ErrorReport.wrapWithRuntimeException(() ->
                context.getGlobalStateMgr().getFailoverGroupMgr().createFailoverGroup(stmt)
        );
        return null;
    }

    @Override
    public ShowResultSet visitCreateSecondaryFailoverGroupStatement(CreateSecondaryFailoverGroupStmt stmt,
                                                                    ConnectContext context) {
        ErrorReport.wrapWithRuntimeException(() ->
                context.getGlobalStateMgr().getFailoverGroupMgr().createFailoverGroup(stmt)
        );
        return null;
    }

    @Override
    public ShowResultSet visitDropFailoverGroupStatement(DropFailoverGroupStmt stmt, ConnectContext context) {
        ErrorReport.wrapWithRuntimeException(() ->
                context.getGlobalStateMgr().getFailoverGroupMgr().dropFailoverGroup(stmt)
        );
        return null;
    }

    @Override
    public ShowResultSet visitAlterFailoverGroupSetStatement(AlterFailoverGroupSetStmt stmt,
                                                             ConnectContext context) {
        ErrorReport.wrapWithRuntimeException(() ->
                context.getGlobalStateMgr().getFailoverGroupMgr().alterFailoverGroupSet(stmt)
        );
        return null;
    }

    @Override
    public ShowResultSet visitAlterFailoverGroupAddStatement(AlterFailoverGroupAddStmt stmt,
                                                             ConnectContext context) {
        ErrorReport.wrapWithRuntimeException(() ->
                context.getGlobalStateMgr().getFailoverGroupMgr().alterFailoverGroupAdd(stmt)
        );
        return null;
    }

    @Override
    public ShowResultSet visitAlterFailoverGroupRemoveStatement(AlterFailoverGroupRemoveStmt stmt,
                                                                ConnectContext context) {
        ErrorReport.wrapWithRuntimeException(() ->
                context.getGlobalStateMgr().getFailoverGroupMgr().alterFailoverGroupRemove(stmt)
        );
        return null;
    }

    @Override
    public ShowResultSet visitAlterFailoverGroupRefreshStatement(AlterFailoverGroupRefreshStmt stmt,
                                                                 ConnectContext context) {
        ErrorReport.wrapWithRuntimeException(() ->
                context.getGlobalStateMgr().getFailoverGroupMgr().alterFailoverGroupRefresh(stmt)
        );
        return null;
    }

    @Override
    public ShowResultSet visitAlterFailoverGroupPrimaryStatement(AlterFailoverGroupPrimaryStmt stmt,
                                                                 ConnectContext context) {
        ErrorReport.wrapWithRuntimeException(() ->
                context.getGlobalStateMgr().getFailoverGroupMgr().alterFailoverGroupPrimary(stmt)
        );
        return null;
    }

    @Override
    public ShowResultSet visitAlterFailoverGroupSuspendStatement(AlterFailoverGroupSuspendStmt stmt,
                                                                 ConnectContext context) {
        ErrorReport.wrapWithRuntimeException(() ->
                context.getGlobalStateMgr().getFailoverGroupMgr().alterFailoverGroupSuspend(stmt)
        );
        return null;
    }

    @Override
    public ShowResultSet visitAlterFailoverGroupResumeStatement(AlterFailoverGroupResumeStmt stmt,
                                                                ConnectContext context) {
        ErrorReport.wrapWithRuntimeException(() ->
                context.getGlobalStateMgr().getFailoverGroupMgr().alterFailoverGroupResume(stmt)
        );
        return null;
    }
}
