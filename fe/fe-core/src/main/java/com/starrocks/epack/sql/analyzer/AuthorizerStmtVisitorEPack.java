// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.sql.analyzer;

import com.starrocks.epack.privilege.AuthorizerEPack;
import com.starrocks.epack.privilege.PrivilegeTypeEPack;
import com.starrocks.epack.sql.ast.AlterPolicyStmt;
import com.starrocks.epack.sql.ast.ApplyMaskingPolicyClause;
import com.starrocks.epack.sql.ast.ApplyRowAccessPolicyClause;
import com.starrocks.epack.sql.ast.CreatePolicyStmt;
import com.starrocks.epack.sql.ast.CreatePrimaryFailoverGroupStmt;
import com.starrocks.epack.sql.ast.CreateSecondaryFailoverGroupStmt;
import com.starrocks.epack.sql.ast.CreateWarehouseStmt;
import com.starrocks.epack.sql.ast.DropPolicyStmt;
import com.starrocks.epack.sql.ast.DropWarehouseStmt;
import com.starrocks.epack.sql.ast.PolicyName;
import com.starrocks.epack.sql.ast.PolicyType;
import com.starrocks.epack.sql.ast.ResumeWarehouseStmt;
import com.starrocks.epack.sql.ast.SetWarehouseStmt;
import com.starrocks.epack.sql.ast.ShowClustersStmt;
import com.starrocks.epack.sql.ast.ShowCreatePolicyStmt;
import com.starrocks.epack.sql.ast.ShowPolicyStmt;
import com.starrocks.epack.sql.ast.ShowWarehousesStmt;
import com.starrocks.epack.sql.ast.SuspendWarehouseStmt;
import com.starrocks.epack.sql.ast.WithColumnMaskingPolicy;
import com.starrocks.epack.sql.ast.WithRowAccessPolicy;
import com.starrocks.privilege.PrivilegeType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.analyzer.AuthorizerStmtVisitor;
import com.starrocks.sql.ast.AlterClause;
import com.starrocks.sql.ast.AlterMaterializedViewStmt;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.AlterViewStmt;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.CreateViewStmt;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class AuthorizerStmtVisitorEPack extends AuthorizerStmtVisitor {
    public AuthorizerStmtVisitorEPack() {
    }

    // ---------------------------------------- Table Statement ---------------------------------------

    @Override
    public Void visitCreateTableStatement(CreateTableStmt statement, ConnectContext context) {
        super.visitCreateTableStatement(statement, context);
        checkPolicyApply(new ArrayList<>(statement.getMaskingPolicyContextMap().values()),
                statement.getWithRowAccessPolicies(), context);
        return null;
    }

    @Override
    public Void visitAlterTableStatement(AlterTableStmt statement, ConnectContext context) {
        super.visitAlterTableStatement(statement, context);
        for (AlterClause alterClause : statement.getOps()) {
            checkAlterClausePolicyApply(alterClause, context);
        }

        return null;
    }

    // ---------------------------------------- View Statement ---------------------------------------

    @Override
    public Void visitCreateViewStatement(CreateViewStmt statement, ConnectContext context) {
        super.visitCreateViewStatement(statement, context);
        checkPolicyApply(new ArrayList<>(statement.getMaskingPolicyContextMap().values()),
                statement.getWithRowAccessPolicies(), context);
        return null;
    }

    @Override
    public Void visitAlterViewStatement(AlterViewStmt statement, ConnectContext context) {
        super.visitAlterViewStatement(statement, context);
        AlterClause alterClause = statement.getAlterClause();
        checkAlterClausePolicyApply(alterClause, context);
        return null;
    }

    // ---------------------------------------- Materialized View stmt --------------------------------

    @Override
    public Void visitCreateMaterializedViewStatement(CreateMaterializedViewStatement statement,
                                                     ConnectContext context) {
        super.visitCreateMaterializedViewStatement(statement, context);
        checkPolicyApply(new ArrayList<>(statement.getMaskingPolicyContextMap().values()),
                statement.getWithRowAccessPolicies(), context);
        return null;
    }

    @Override
    public Void visitAlterMaterializedViewStatement(AlterMaterializedViewStmt statement, ConnectContext context) {
        super.visitAlterMaterializedViewStatement(statement, context);
        if (statement.getOps() != null) {
            for (AlterClause alterClause : statement.getOps()) {
                checkAlterClausePolicyApply(alterClause, context);
            }
        }
        return null;
    }

    // ---------------------------------------- Security Policy Statement ---------------------------------------------------

    @Override
    public Void visitCreatePolicyStatement(CreatePolicyStmt statement, ConnectContext context) {
        PrivilegeType privilegeType = statement.getPolicyType().equals(PolicyType.MASKING) ?
                PrivilegeTypeEPack.CREATE_MASKING_POLICY : PrivilegeTypeEPack.CREATE_ROW_ACCESS_POLICY;
        Authorizer.checkDbAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                statement.getPolicyName().getCatalog(), statement.getPolicyName().getDbName(),
                privilegeType);
        return null;
    }

    @Override
    public Void visitDropPolicyStatement(DropPolicyStmt statement, ConnectContext context) {
        AuthorizerEPack.checkPolicyAction(context.getCurrentUserIdentity(),
                context.getCurrentRoleIds(), statement.getPolicyType(), statement.getPolicyName().getCatalog(),
                statement.getPolicyName().getDbName(), statement.getPolicyName().getName(), PrivilegeType.DROP);
        return null;
    }

    @Override
    public Void visitAlterPolicyStatement(AlterPolicyStmt statement, ConnectContext context) {
        AuthorizerEPack.checkPolicyAction(context.getCurrentUserIdentity(),
                context.getCurrentRoleIds(), statement.getPolicyType(),
                statement.getPolicyName().getCatalog(), statement.getPolicyName().getDbName(),
                statement.getPolicyName().getName(), PrivilegeType.ALTER);
        return null;
    }

    @Override
    public Void visitShowPolicyStatement(ShowPolicyStmt statement, ConnectContext context) {
        return visitShowStatement(statement, context);
    }

    @Override
    public Void visitShowCreatePolicyStatement(ShowCreatePolicyStmt statement, ConnectContext context) {
        AuthorizerEPack.checkAnyActionOnPolicy(context.getCurrentUserIdentity(),
                context.getCurrentRoleIds(), statement.getPolicyType(),
                statement.getPolicyName().getCatalog(), statement.getPolicyName().getDbName(),
                statement.getPolicyName().getName());
        return null;
    }

    private void checkPolicyApply(List<WithColumnMaskingPolicy> withColumnMaskingPolicyMap,
                                  List<WithRowAccessPolicy> withRowAccessPolicyList,
                                  ConnectContext context) {
        if (withColumnMaskingPolicyMap != null) {
            for (WithColumnMaskingPolicy withColumnMaskingPolicy : withColumnMaskingPolicyMap) {
                PolicyName policyName = withColumnMaskingPolicy.getPolicyName();
                AuthorizerEPack.checkPolicyAction(context.getCurrentUserIdentity(),
                        context.getCurrentRoleIds(), PolicyType.MASKING, policyName.getCatalog(), policyName.getDbName(),
                        policyName.getName(), PrivilegeTypeEPack.APPLY);
            }
        }

        if (withRowAccessPolicyList != null) {
            for (WithRowAccessPolicy withRowAccessPolicy : withRowAccessPolicyList) {
                PolicyName policyName = withRowAccessPolicy.getPolicyName();
                AuthorizerEPack.checkPolicyAction(context.getCurrentUserIdentity(),
                        context.getCurrentRoleIds(), PolicyType.ROW_ACCESS, policyName.getCatalog(), policyName.getDbName(),
                        policyName.getName(), PrivilegeTypeEPack.APPLY);
            }
        }
    }

    private void checkAlterClausePolicyApply(AlterClause alterClause, ConnectContext context) {
        if (alterClause instanceof ApplyMaskingPolicyClause) {
            ApplyMaskingPolicyClause applyMaskingPolicyClause = (ApplyMaskingPolicyClause) alterClause;
            WithColumnMaskingPolicy withColumnMaskingPolicy = applyMaskingPolicyClause.getWithColumnMaskingPolicy();
            checkPolicyApply(Collections.singletonList(withColumnMaskingPolicy), null, context);
        } else if (alterClause instanceof ApplyRowAccessPolicyClause) {
            ApplyRowAccessPolicyClause applyMaskingPolicyClause = (ApplyRowAccessPolicyClause) alterClause;
            WithRowAccessPolicy withRowAccessPolicy = applyMaskingPolicyClause.getRowAccessPolicyContext();
            checkPolicyApply(null, Collections.singletonList(withRowAccessPolicy), context);
        }
    }

    // --------------------------------- Warehouse Statement ---------------------------------
    @Override
    public Void visitCreateWarehouseStatement(CreateWarehouseStmt statement, ConnectContext context) {
        Authorizer.checkSystemAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                PrivilegeTypeEPack.CREATE_WAREHOUSE);
        return null;
    }

    @Override
    public Void visitSuspendWarehouseStatement(SuspendWarehouseStmt statement, ConnectContext context) {
        AuthorizerEPack.checkWarehouseAction(context.getCurrentUserIdentity(),
                context.getCurrentRoleIds(), statement.getWarehouseName(),
                PrivilegeType.ALTER);
        return null;
    }

    @Override
    public Void visitResumeWarehouseStatement(ResumeWarehouseStmt statement, ConnectContext context) {
        AuthorizerEPack.checkWarehouseAction(context.getCurrentUserIdentity(),
                context.getCurrentRoleIds(), statement.getWarehouseName(),
                PrivilegeType.ALTER);
        return null;
    }

    public Void visitDropWarehouseStatement(DropWarehouseStmt statement, ConnectContext context) {
        AuthorizerEPack.checkWarehouseAction(context.getCurrentUserIdentity(),
                context.getCurrentRoleIds(), statement.getWarehouseName(),
                PrivilegeType.DROP);
        return null;
    }

    public Void visitSetWarehouseStatement(SetWarehouseStmt statement, ConnectContext context) {
        AuthorizerEPack.checkWarehouseAction(
                context.getCurrentUserIdentity(), context.getCurrentRoleIds(), statement.getWarehouseName(), PrivilegeType.USAGE);
        return null;
    }

    public Void visitShowWarehousesStatement(ShowWarehousesStmt statement, ConnectContext context) {
        // `show warehouses` only show warehouses that user has any privilege on, we will check it in
        // the execution logic, not here, see `handleShowWarehouses()` for details.
        return null;
    }

    public Void visitShowClusterStatement(ShowClustersStmt statement, ConnectContext context) {
        AuthorizerEPack.checkAnyActionOnWarehouse(context.getCurrentUserIdentity(),
                context.getCurrentRoleIds(), statement.getWarehouseName());
        return null;
    }

    // --------------------------------- Failover Group Statement ---------------------------------
    @Override
    public Void visitCreatePrimaryFailoverGroupStatement(CreatePrimaryFailoverGroupStmt statement,
                                                         ConnectContext context) {
        // TODO
        return null;
    }

    @Override
    public Void visitCreateSecondaryFailoverGroupStatement(CreateSecondaryFailoverGroupStmt statement,
                                                           ConnectContext context) {
        // TODO
        return null;
    }
}
