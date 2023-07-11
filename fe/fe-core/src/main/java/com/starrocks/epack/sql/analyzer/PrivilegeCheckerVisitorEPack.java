// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.sql.analyzer;

import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.epack.privilege.PrivilegeActionsEPack;
import com.starrocks.epack.privilege.PrivilegeTypeEPack;
import com.starrocks.epack.sql.ast.AlterPolicyStmt;
import com.starrocks.epack.sql.ast.ApplyMaskingPolicyClause;
import com.starrocks.epack.sql.ast.ApplyRowAccessPolicyClause;
import com.starrocks.epack.sql.ast.CreatePolicyStmt;
import com.starrocks.epack.sql.ast.DropPolicyStmt;
import com.starrocks.epack.sql.ast.PolicyName;
import com.starrocks.epack.sql.ast.PolicyType;
import com.starrocks.epack.sql.ast.ShowCreatePolicyStmt;
import com.starrocks.epack.sql.ast.ShowPolicyStmt;
import com.starrocks.epack.sql.ast.WithColumnMaskingPolicy;
import com.starrocks.epack.sql.ast.WithRowAccessPolicy;
import com.starrocks.privilege.PrivilegeActions;
import com.starrocks.privilege.PrivilegeType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.PrivilegeCheckerVisitor;
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

public class PrivilegeCheckerVisitorEPack extends PrivilegeCheckerVisitor {

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

    private void checkPolicyApply(List<WithColumnMaskingPolicy> withColumnMaskingPolicyMap,
                                  List<WithRowAccessPolicy> withRowAccessPolicyList,
                                  ConnectContext context) {
        if (withColumnMaskingPolicyMap != null) {
            for (WithColumnMaskingPolicy withColumnMaskingPolicy : withColumnMaskingPolicyMap) {
                PolicyName policyName = withColumnMaskingPolicy.getPolicyName();
                if (!PrivilegeActionsEPack.checkPolicyAction(context, PolicyType.MASKING,
                        policyName.getCatalog(), policyName.getDbName(), policyName.getName(),
                        PrivilegeTypeEPack.APPLY)) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "APPLY");
                }
            }
        }

        if (withRowAccessPolicyList != null) {
            for (WithRowAccessPolicy withRowAccessPolicy : withRowAccessPolicyList) {
                PolicyName policyName = withRowAccessPolicy.getPolicyName();
                if (!PrivilegeActionsEPack.checkPolicyAction(context, PolicyType.ROW_ACCESS,
                        policyName.getCatalog(), policyName.getDbName(), policyName.getName(),
                        PrivilegeTypeEPack.APPLY)) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "APPLY");
                }
            }
        }
    }

    // ---------------------------------------- Security Policy Statement ---------------------------------------------------

    @Override
    public Void visitCreatePolicyStatement(CreatePolicyStmt statement, ConnectContext context) {
        PrivilegeType privilegeType = statement.getPolicyType().equals(PolicyType.MASKING) ?
                PrivilegeTypeEPack.CREATE_MASKING_POLICY : PrivilegeTypeEPack.CREATE_ROW_ACCESS_POLICY;
        if (!PrivilegeActions.checkDbAction(context,
                statement.getPolicyName().getCatalog(), statement.getPolicyName().getDbName(),
                privilegeType)) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, privilegeType.name());
        }
        return null;
    }

    @Override
    public Void visitDropPolicyStatement(DropPolicyStmt statement, ConnectContext context) {
        if (!PrivilegeActionsEPack.checkPolicyAction(context, statement.getPolicyType(),
                statement.getPolicyName().getCatalog(),
                statement.getPolicyName().getDbName(), statement.getPolicyName().getName(), PrivilegeType.DROP)) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "DROP");
        }
        return null;
    }

    @Override
    public Void visitAlterPolicyStatement(AlterPolicyStmt statement, ConnectContext context) {
        if (!PrivilegeActionsEPack.checkPolicyAction(context, statement.getPolicyType(),
                statement.getPolicyName().getCatalog(),
                statement.getPolicyName().getDbName(), statement.getPolicyName().getName(), PrivilegeType.ALTER)) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ALTER");
        }
        return null;
    }

    @Override
    public Void visitShowPolicyStatement(ShowPolicyStmt statement, ConnectContext context) {
        return visitShowStatement(statement, context);
    }

    @Override
    public Void visitShowCreatePolicyStatement(ShowCreatePolicyStmt statement, ConnectContext context) {
        if (!PrivilegeActionsEPack.checkAnyActionOnPolicy(context, statement.getPolicyType(),
                statement.getPolicyName().getCatalog(), statement.getPolicyName().getDbName(),
                statement.getPolicyName().getName())) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ANY");
        }
        return null;
    }
}
