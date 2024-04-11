// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.sql.analyzer;

import com.starrocks.analysis.HintNode;
import com.starrocks.analysis.SetVarHint;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.epack.authorization.AuthorizerEPack;
import com.starrocks.epack.authorization.ObjectTypeEPack;
import com.starrocks.epack.authorization.PrivilegeTypeEPack;
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
import com.starrocks.epack.sql.ast.ApplyMaskingPolicyClause;
import com.starrocks.epack.sql.ast.ApplyRowAccessPolicyClause;
import com.starrocks.epack.sql.ast.AstVisitorEPack;
import com.starrocks.epack.sql.ast.CreatePolicyStmt;
import com.starrocks.epack.sql.ast.CreatePrimaryFailoverGroupStmt;
import com.starrocks.epack.sql.ast.CreateRoleMappingStatement;
import com.starrocks.epack.sql.ast.CreateSecondaryFailoverGroupStmt;
import com.starrocks.epack.sql.ast.CreateSecurityIntegrationStatement;
import com.starrocks.epack.sql.ast.CreateWarehouseStmt;
import com.starrocks.epack.sql.ast.DescribeFailoverGroupStmt;
import com.starrocks.epack.sql.ast.DropFailoverGroupStmt;
import com.starrocks.epack.sql.ast.DropPolicyStmt;
import com.starrocks.epack.sql.ast.DropRoleMappingStatement;
import com.starrocks.epack.sql.ast.DropSecurityIntegrationStatement;
import com.starrocks.epack.sql.ast.DropWarehouseStmt;
import com.starrocks.epack.sql.ast.PolicyName;
import com.starrocks.epack.sql.ast.PolicyType;
import com.starrocks.epack.sql.ast.RefreshRoleMappingStatement;
import com.starrocks.epack.sql.ast.ResumeWarehouseStmt;
import com.starrocks.epack.sql.ast.SetWarehouseStmt;
import com.starrocks.epack.sql.ast.ShowClustersStmt;
import com.starrocks.epack.sql.ast.ShowCreatePolicyStmt;
import com.starrocks.epack.sql.ast.ShowCreateSecurityIntegrationStatement;
import com.starrocks.epack.sql.ast.ShowFailoverGroupsStmt;
import com.starrocks.epack.sql.ast.ShowPolicyStmt;
import com.starrocks.epack.sql.ast.ShowRoleMappingStatement;
import com.starrocks.epack.sql.ast.ShowSecurityIntegrationStatement;
import com.starrocks.epack.sql.ast.ShowWarehousesStmt;
import com.starrocks.epack.sql.ast.SuspendWarehouseStmt;
import com.starrocks.epack.sql.ast.WithColumnMaskingPolicy;
import com.starrocks.epack.sql.ast.WithRowAccessPolicy;
import com.starrocks.privilege.AccessDeniedException;
import com.starrocks.privilege.ObjectType;
import com.starrocks.privilege.PrivilegeType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.analyzer.AuthorizerStmtVisitor;
import com.starrocks.sql.ast.AlterClause;
import com.starrocks.sql.ast.AlterMaterializedViewStmt;
import com.starrocks.sql.ast.AlterRoutineLoadStmt;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.AlterViewStmt;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.ast.CreateRoutineLoadStmt;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.CreateViewStmt;
import com.starrocks.sql.ast.LoadStmt;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectRelation;
import org.apache.commons.collections4.CollectionUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class AuthorizerStmtVisitorEPack extends AuthorizerStmtVisitor implements AstVisitorEPack<Void, ConnectContext> {
    public AuthorizerStmtVisitorEPack() {
    }

    private void checkWarehouseUsagePrivilege(String warehouseName, ConnectContext context) {
        try {
            AuthorizerEPack.checkWarehouseAction(context.getCurrentUserIdentity(),
                    context.getCurrentRoleIds(), warehouseName, PrivilegeType.USAGE);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.USAGE.name(), ObjectTypeEPack.WAREHOUSE.name(), warehouseName);
        }
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
        // check warehouse privilege
        Map<String, String> properties = statement.getProperties();
        if (properties != null && properties.containsKey(PropertyAnalyzer.PROPERTIES_WAREHOUSE)) {
            String warehouseName = properties.get(PropertyAnalyzer.PROPERTIES_WAREHOUSE);
            checkWarehouseUsagePrivilege(warehouseName, context);
        }
        return null;
    }

    @Override
    public Void visitAlterMaterializedViewStatement(AlterMaterializedViewStmt statement, ConnectContext context) {
        super.visitAlterMaterializedViewStatement(statement, context);
        checkAlterClausePolicyApply(statement.getAlterTableClause(), context);
        return null;
    }

    // ---------------------------------------- Security Policy Statement ---------------------------------------------------

    @Override
    public Void visitCreatePolicyStatement(CreatePolicyStmt statement, ConnectContext context) {
        PrivilegeType privilegeType = statement.getPolicyType().equals(PolicyType.MASKING) ?
                PrivilegeTypeEPack.CREATE_MASKING_POLICY : PrivilegeTypeEPack.CREATE_ROW_ACCESS_POLICY;
        try {
            Authorizer.checkDbAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    statement.getPolicyName().getCatalog(), statement.getPolicyName().getDbName(), privilegeType);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    statement.getPolicyName().getCatalog(),
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    privilegeType.name(), ObjectType.DATABASE.name(), statement.getPolicyName().getDbName());
        }
        return null;
    }

    @Override
    public Void visitDropPolicyStatement(DropPolicyStmt statement, ConnectContext context) {
        try {
            AuthorizerEPack.checkPolicyAction(context.getCurrentUserIdentity(),
                    context.getCurrentRoleIds(), statement.getPolicyType(), statement.getPolicyName().getCatalog(),
                    statement.getPolicyName().getDbName(), statement.getPolicyName().getName(), PrivilegeType.DROP);
        } catch (AccessDeniedException e) {
            ObjectType objectType = statement.getPolicyType().equals(PolicyType.MASKING) ? ObjectTypeEPack.MASKING_POLICY :
                    ObjectTypeEPack.ROW_ACCESS_POLICY;

            AccessDeniedException.reportAccessDenied(statement.getPolicyName().getCatalog(),
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.DROP.name(), objectType.name(), statement.getPolicyName().getName());
        }
        return null;
    }

    @Override
    public Void visitAlterPolicyStatement(AlterPolicyStmt statement, ConnectContext context) {
        try {
            AuthorizerEPack.checkPolicyAction(context.getCurrentUserIdentity(),
                    context.getCurrentRoleIds(), statement.getPolicyType(),
                    statement.getPolicyName().getCatalog(), statement.getPolicyName().getDbName(),
                    statement.getPolicyName().getName(), PrivilegeType.ALTER);
        } catch (AccessDeniedException e) {
            ObjectType objectType = statement.getPolicyType().equals(PolicyType.MASKING) ? ObjectTypeEPack.MASKING_POLICY :
                    ObjectTypeEPack.ROW_ACCESS_POLICY;

            AccessDeniedException.reportAccessDenied(statement.getPolicyName().getCatalog(),
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.ALTER.name(), objectType.name(), statement.getPolicyName().getName());
        }
        return null;
    }

    @Override
    public Void visitShowPolicyStatement(ShowPolicyStmt statement, ConnectContext context) {
        return visitShowStatement(statement, context);
    }

    @Override
    public Void visitShowCreatePolicyStatement(ShowCreatePolicyStmt statement, ConnectContext context) {
        try {
            AuthorizerEPack.checkAnyActionOnPolicy(context.getCurrentUserIdentity(),
                    context.getCurrentRoleIds(), statement.getPolicyType(),
                    statement.getPolicyName().getCatalog(), statement.getPolicyName().getDbName(),
                    statement.getPolicyName().getName());
        } catch (AccessDeniedException e) {
            ObjectType objectType = statement.getPolicyType().equals(PolicyType.MASKING) ? ObjectTypeEPack.MASKING_POLICY :
                    ObjectTypeEPack.ROW_ACCESS_POLICY;

            AccessDeniedException.reportAccessDenied(statement.getPolicyName().getCatalog(),
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.ANY.name(), objectType.name(), statement.getPolicyName().getName());
        }
        return null;
    }

    private void checkPolicyApply(List<WithColumnMaskingPolicy> withColumnMaskingPolicyMap,
                                  List<WithRowAccessPolicy> withRowAccessPolicyList, ConnectContext context) {
        if (withColumnMaskingPolicyMap != null) {
            for (WithColumnMaskingPolicy withColumnMaskingPolicy : withColumnMaskingPolicyMap) {
                PolicyName policyName = withColumnMaskingPolicy.getPolicyName();

                try {
                    AuthorizerEPack.checkPolicyAction(context.getCurrentUserIdentity(),
                            context.getCurrentRoleIds(), PolicyType.MASKING, policyName.getCatalog(), policyName.getDbName(),
                            policyName.getName(), PrivilegeTypeEPack.APPLY);
                } catch (AccessDeniedException e) {
                    AccessDeniedException.reportAccessDenied(policyName.getCatalog(),
                            context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                            PrivilegeTypeEPack.APPLY.name(), ObjectTypeEPack.MASKING_POLICY.name(), policyName.getName());
                }
            }
        }

        if (withRowAccessPolicyList != null) {
            for (WithRowAccessPolicy withRowAccessPolicy : withRowAccessPolicyList) {
                PolicyName policyName = withRowAccessPolicy.getPolicyName();
                try {
                    AuthorizerEPack.checkPolicyAction(context.getCurrentUserIdentity(),
                            context.getCurrentRoleIds(), PolicyType.ROW_ACCESS, policyName.getCatalog(), policyName.getDbName(),
                            policyName.getName(), PrivilegeTypeEPack.APPLY);
                } catch (AccessDeniedException e) {
                    AccessDeniedException.reportAccessDenied(policyName.getCatalog(),
                            context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                            PrivilegeTypeEPack.APPLY.name(), ObjectTypeEPack.ROW_ACCESS_POLICY.name(), policyName.getName());
                }
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

    // ---------------------------------------- Security Integration Statement ---------------------------------------
    @Override
    public Void visitCreateSecurityIntegrationStatement(CreateSecurityIntegrationStatement statement, ConnectContext context) {
        try {
            Authorizer.checkSystemAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeTypeEPack.SECURITY);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeTypeEPack.SECURITY.name(), ObjectType.SYSTEM.name(), null);
        }
        return null;
    }

    @Override
    public Void visitDropSecurityIntegrationStatement(DropSecurityIntegrationStatement statement, ConnectContext context) {
        try {
            Authorizer.checkSystemAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeTypeEPack.SECURITY);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeTypeEPack.SECURITY.name(), ObjectType.SYSTEM.name(), null);
        }
        return null;
    }

    @Override
    public Void visitAlterSecurityIntegrationStatement(AlterSecurityIntegrationStatement statement, ConnectContext context) {
        try {
            Authorizer.checkSystemAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeTypeEPack.SECURITY);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeTypeEPack.SECURITY.name(), ObjectType.SYSTEM.name(), null);
        }
        return null;
    }

    @Override
    public Void visitShowSecurityIntegrationStatement(ShowSecurityIntegrationStatement statement, ConnectContext context) {
        try {
            Authorizer.checkSystemAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeTypeEPack.SECURITY);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeTypeEPack.SECURITY.name(), ObjectType.SYSTEM.name(), null);
        }
        return null;
    }

    @Override
    public Void visitShowCreateSecurityIntegrationStatement(ShowCreateSecurityIntegrationStatement statement,
                                                            ConnectContext context) {
        try {
            Authorizer.checkSystemAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeTypeEPack.SECURITY);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeTypeEPack.SECURITY.name(), ObjectType.SYSTEM.name(), null);
        }
        return null;
    }

    @Override
    public Void visitCreateRoleMappingStatement(CreateRoleMappingStatement statement, ConnectContext context) {
        try {
            Authorizer.checkSystemAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeTypeEPack.SECURITY);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeTypeEPack.SECURITY.name(), ObjectType.SYSTEM.name(), null);
        }
        return null;
    }

    @Override
    public Void visitAlterRoleMappingStatement(AlterRoleMappingStatement statement, ConnectContext context) {
        try {
            Authorizer.checkSystemAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeTypeEPack.SECURITY);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeTypeEPack.SECURITY.name(), ObjectType.SYSTEM.name(), null);
        }
        return null;
    }

    @Override
    public Void visitDropRoleMappingStatement(DropRoleMappingStatement statement, ConnectContext context) {
        try {
            Authorizer.checkSystemAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeTypeEPack.SECURITY);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeTypeEPack.SECURITY.name(), ObjectType.SYSTEM.name(), null);
        }
        return null;
    }

    @Override
    public Void visitShowRoleMappingStatement(ShowRoleMappingStatement statement, ConnectContext context) {
        try {
            Authorizer.checkSystemAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeTypeEPack.SECURITY);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeTypeEPack.SECURITY.name(), ObjectType.SYSTEM.name(), null);
        }
        return null;
    }

    @Override
    public Void visitRefreshRoleMappingStatement(RefreshRoleMappingStatement statement, ConnectContext context) {
        try {
            Authorizer.checkSystemAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeTypeEPack.SECURITY);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeTypeEPack.SECURITY.name(), ObjectType.SYSTEM.name(), null);
        }
        return null;
    }

    // --------------------------------- Warehouse Statement ---------------------------------
    @Override
    public Void visitCreateWarehouseStatement(CreateWarehouseStmt statement, ConnectContext context) {
        try {
            Authorizer.checkSystemAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeTypeEPack.CREATE_WAREHOUSE);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeTypeEPack.CREATE_WAREHOUSE.name(), ObjectType.SYSTEM.name(), null);
        }
        return null;
    }

    @Override
    public Void visitSuspendWarehouseStatement(SuspendWarehouseStmt statement, ConnectContext context) {
        try {
            AuthorizerEPack.checkWarehouseAction(context.getCurrentUserIdentity(),
                    context.getCurrentRoleIds(), statement.getWarehouseName(), PrivilegeType.ALTER);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.ALTER.name(), ObjectTypeEPack.WAREHOUSE.name(), statement.getWarehouseName());
        }
        return null;
    }

    @Override
    public Void visitResumeWarehouseStatement(ResumeWarehouseStmt statement, ConnectContext context) {
        try {
            AuthorizerEPack.checkWarehouseAction(context.getCurrentUserIdentity(),
                    context.getCurrentRoleIds(), statement.getWarehouseName(), PrivilegeType.ALTER);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.ALTER.name(), ObjectTypeEPack.WAREHOUSE.name(), statement.getWarehouseName());
        }
        return null;
    }

    public Void visitDropWarehouseStatement(DropWarehouseStmt statement, ConnectContext context) {
        try {
            AuthorizerEPack.checkWarehouseAction(context.getCurrentUserIdentity(),
                    context.getCurrentRoleIds(), statement.getWarehouseName(), PrivilegeType.DROP);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.DROP.name(), ObjectTypeEPack.WAREHOUSE.name(), statement.getWarehouseName());
        }
        return null;
    }

    public Void visitSetWarehouseStatement(SetWarehouseStmt statement, ConnectContext context) {
        String warehouseName = statement.getWarehouseName();
        checkWarehouseUsagePrivilege(warehouseName, context);
        return null;
    }

    public Void visitShowWarehousesStatement(ShowWarehousesStmt statement, ConnectContext context) {
        // `show warehouses` only show warehouses that user has any privilege on, we will check it in
        // the execution logic, not here, see `handleShowWarehouses()` for details.
        return null;
    }

    public Void visitShowClusterStatement(ShowClustersStmt statement, ConnectContext context) {
        try {
            AuthorizerEPack.checkAnyActionOnWarehouse(context.getCurrentUserIdentity(),
                    context.getCurrentRoleIds(), statement.getWarehouseName());
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.ANY.name(), ObjectTypeEPack.WAREHOUSE.name(), statement.getWarehouseName());
        }
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

    @Override
    public Void visitDropFailoverGroupStatement(DropFailoverGroupStmt statement, ConnectContext context) {
        // TODO
        return null;
    }

    @Override
    public Void visitShowFailoverGroupsStatement(ShowFailoverGroupsStmt statement, ConnectContext context) {
        // TODO
        return null;
    }

    @Override
    public Void visitDescribeFailoverGroupStatement(DescribeFailoverGroupStmt statement, ConnectContext context) {
        // TODO
        return null;
    }

    @Override
    public Void visitAlterFailoverGroupSetStatement(AlterFailoverGroupSetStmt statement, ConnectContext context) {
        // TODO
        return null;
    }

    @Override
    public Void visitAlterFailoverGroupAddStatement(AlterFailoverGroupAddStmt statement, ConnectContext context) {
        // TODO
        return null;
    }

    @Override
    public Void visitAlterFailoverGroupRemoveStatement(AlterFailoverGroupRemoveStmt statement, ConnectContext context) {
        // TODO
        return null;
    }

    @Override
    public Void visitAlterFailoverGroupRefreshStatement(AlterFailoverGroupRefreshStmt statement, ConnectContext context) {
        // TODO
        return null;
    }

    @Override
    public Void visitAlterFailoverGroupPrimaryStatement(AlterFailoverGroupPrimaryStmt statement, ConnectContext context) {
        // TODO
        return null;
    }

    @Override
    public Void visitAlterFailoverGroupSuspendStatement(AlterFailoverGroupSuspendStmt statement, ConnectContext context) {
        // TODO
        return null;
    }

    @Override
    public Void visitAlterFailoverGroupResumeStatement(AlterFailoverGroupResumeStmt statement, ConnectContext context) {
        // TODO
        return null;
    }

    // --------------------------------- Query Statement -------------------------------------

    @Override
    public Void visitQueryStatement(QueryStatement statement, ConnectContext context) {
        super.visitQueryStatement(statement, context);

        List<HintNode> hintNodes = null;
        if (statement.getQueryRelation() instanceof SelectRelation) {
            SelectRelation selectRelation = (SelectRelation) statement.getQueryRelation();
            hintNodes = selectRelation.getSelectList().getHintNodes();
        }

        if (CollectionUtils.isNotEmpty(hintNodes)) {
            for (HintNode hintNode : hintNodes) {
                if (hintNode instanceof SetVarHint) {
                    Map<String, String> optHints = hintNode.getValue();
                    if (optHints.containsKey(SessionVariable.WAREHOUSE_NAME)) {
                        // check warehouse privilege
                        String warehouseName = optHints.get(SessionVariable.WAREHOUSE_NAME);
                        if (!warehouseName.equalsIgnoreCase(WarehouseManager.DEFAULT_WAREHOUSE_NAME)) {
                            checkWarehouseUsagePrivilege(warehouseName, context);
                        }
                    }
                }
            }
        }

        return null;
    }

    // --------------------------------- LOAD Statement ---------------------------------
    @Override
    public Void visitLoadStatement(LoadStmt statement, ConnectContext context) {
        super.visitLoadStatement(statement, context);
        // check warehouse privilege
        Map<String, String> properties = statement.getProperties();
        if (properties != null && properties.containsKey(PropertyAnalyzer.PROPERTIES_WAREHOUSE)) {
            String warehouseName = properties.get(PropertyAnalyzer.PROPERTIES_WAREHOUSE);
            checkWarehouseUsagePrivilege(warehouseName, context);
        }

        return null;
    }

    // --------------------------------- Routine Load Statement ---------------------------------
    @Override
    public Void visitCreateRoutineLoadStatement(CreateRoutineLoadStmt statement, ConnectContext context) {
        super.visitCreateRoutineLoadStatement(statement, context);
        // check warehouse privilege
        Map<String, String> properties = statement.getJobProperties();
        if (properties != null && properties.containsKey(PropertyAnalyzer.PROPERTIES_WAREHOUSE)) {
            String warehouseName = properties.get(PropertyAnalyzer.PROPERTIES_WAREHOUSE);
            checkWarehouseUsagePrivilege(warehouseName, context);
        }
        return null;
    }

    @Override
    public Void visitAlterRoutineLoadStatement(AlterRoutineLoadStmt statement, ConnectContext context) {
        super.visitAlterRoutineLoadStatement(statement, context);
        // check warehouse privilege
        Map<String, String> properties = statement.getJobProperties();
        if (properties != null && properties.containsKey(PropertyAnalyzer.PROPERTIES_WAREHOUSE)) {
            String warehouseName = properties.get(PropertyAnalyzer.PROPERTIES_WAREHOUSE);
            checkWarehouseUsagePrivilege(warehouseName, context);
        }
        return null;
    }
}
