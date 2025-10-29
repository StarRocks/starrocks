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
import com.starrocks.epack.authorization.SecurityPolicyMgr;
import com.starrocks.epack.sql.ast.AlterFailoverGroupAddStmt;
import com.starrocks.epack.sql.ast.AlterFailoverGroupPrimaryStmt;
import com.starrocks.epack.sql.ast.AlterFailoverGroupRefreshStmt;
import com.starrocks.epack.sql.ast.AlterFailoverGroupRemoveStmt;
import com.starrocks.epack.sql.ast.AlterFailoverGroupResumeStmt;
import com.starrocks.epack.sql.ast.AlterFailoverGroupSetStmt;
import com.starrocks.epack.sql.ast.AlterFailoverGroupSuspendStmt;
import com.starrocks.epack.sql.ast.AlterPolicyStmt;
import com.starrocks.epack.sql.ast.AlterRoleMappingStatement;
import com.starrocks.epack.sql.ast.AstVisitorEPack;
import com.starrocks.epack.sql.ast.CreatePasswordPolicyStmt;
import com.starrocks.epack.sql.ast.CreatePolicyStmt;
import com.starrocks.epack.sql.ast.CreatePrimaryFailoverGroupStmt;
import com.starrocks.epack.sql.ast.CreateRoleMappingStatement;
import com.starrocks.epack.sql.ast.CreateSecondaryFailoverGroupStmt;
import com.starrocks.epack.sql.ast.DropFailoverGroupStmt;
import com.starrocks.epack.sql.ast.DropPasswordPolicyStmt;
import com.starrocks.epack.sql.ast.DropPolicyStmt;
import com.starrocks.epack.sql.ast.DropRoleMappingStatement;
import com.starrocks.epack.sql.ast.RefreshRoleMappingStatement;
import com.starrocks.epack.sql.ast.SetPasswordPolicyStmt;
import com.starrocks.epack.sql.ast.UnsetPasswordPolicyStmt;
import com.starrocks.lake.snapshot.ClusterSnapshotMgrEPack;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.sql.ast.RestoreTableFromSnapshotStmt;

public class DDLStmtExecutorVisitorEPack extends DDLStmtExecutor.StmtExecutorVisitor
        implements AstVisitorEPack<ShowResultSet, ConnectContext> {

    private static final DDLStmtExecutorVisitorEPack INSTANCE = new DDLStmtExecutorVisitorEPack();

    public static DDLStmtExecutor.StmtExecutorVisitor getInstance() {
        return INSTANCE;
    }

    protected DDLStmtExecutorVisitorEPack() {
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
            context.getGlobalStateMgr().getNodeMgr().refreshRoleMapping(stmt,
                    context.getGlobalStateMgr().getLdapGroupCacheMgr());
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

    @Override
    public ShowResultSet visitCreatePasswordPolicyStatement(CreatePasswordPolicyStmt stmt, ConnectContext context) {
        ErrorReport.wrapWithRuntimeException(() ->
                context.getGlobalStateMgr().getSecurityPolicyManager().createPasswordPolicy(stmt)
        );
        return null;
    }

    @Override
    public ShowResultSet visitDropPasswordPolicyStatement(DropPasswordPolicyStmt stmt, ConnectContext context) {
        ErrorReport.wrapWithRuntimeException(() ->
                context.getGlobalStateMgr().getSecurityPolicyManager().dropPasswordPolicy(stmt)
        );
        return null;
    }

    @Override
    public ShowResultSet visitSetPasswordPolicyStatement(SetPasswordPolicyStmt stmt, ConnectContext context) {
        ErrorReport.wrapWithRuntimeException(() ->
                context.getGlobalStateMgr().getSecurityPolicyManager().setGlobalPasswordPolicy(stmt.getPolicyName())
        );
        return null;
    }

    @Override
    public ShowResultSet visitUnsetPasswordPolicyStatement(UnsetPasswordPolicyStmt stmt, ConnectContext context) {
        ErrorReport.wrapWithRuntimeException(() ->
                context.getGlobalStateMgr().getSecurityPolicyManager().unsetGlobalPasswordPolicy()
        );
        return null;
    }

    @Override
    public ShowResultSet visitRestoreTableFromSnapshotStatement(RestoreTableFromSnapshotStmt stmt,
                                                                ConnectContext context) {
        ErrorReport.wrapWithRuntimeException(() -> {
            ClusterSnapshotMgrEPack clusterSnapshotMgr =
                    (ClusterSnapshotMgrEPack) context.getGlobalStateMgr().getClusterSnapshotMgr();
            clusterSnapshotMgr.submitTableSnapshotRestore(stmt, context);
        });
        return null;
    }
}
