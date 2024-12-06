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
package com.starrocks.epack.sql.ast;

import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.warehouse.CreateWarehouseStmt;
import com.starrocks.sql.ast.warehouse.DropWarehouseStmt;
import com.starrocks.sql.ast.warehouse.ResumeWarehouseStmt;
import com.starrocks.sql.ast.warehouse.SetWarehouseStmt;
import com.starrocks.sql.ast.warehouse.ShowClustersStmt;
import com.starrocks.sql.ast.warehouse.ShowNodesStmt;
import com.starrocks.sql.ast.warehouse.ShowWarehousesStmt;
import com.starrocks.sql.ast.warehouse.SuspendWarehouseStmt;
import com.starrocks.sql.automv.ast.AlterTunespaceStmt;
import com.starrocks.sql.automv.ast.CreateTunespaceStmt;
import com.starrocks.sql.automv.ast.ShowRecommendationsStmt;

public interface AstVisitorEPack<R, C> extends AstVisitor<R, C> {

    // ---------------------------------------- Warehouse Statement ----------------------------------------------------

    default R visitShowWarehousesStatement(ShowWarehousesStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    default R visitShowClusterStatement(ShowClustersStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    default R visitCreateWarehouseStatement(CreateWarehouseStmt statement, C context) {
        return visitStatement(statement, context);
    }

    default R visitDropWarehouseStatement(DropWarehouseStmt statement, C context) {
        return visitStatement(statement, context);
    }

    default R visitSuspendWarehouseStatement(SuspendWarehouseStmt statement, C context) {
        return visitStatement(statement, context);
    }

    default R visitResumeWarehouseStatement(ResumeWarehouseStmt statement, C context) {
        return visitStatement(statement, context);
    }

    default R visitSetWarehouseStatement(SetWarehouseStmt statement, C context) {
        return visitStatement(statement, context);
    }

    default R visitShowNodesStatement(ShowNodesStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    // ------------------------------------------- Alter Clause --------------------------------------------------------

    default R visitDecommissionDiskClause(DecommissionDiskClause clause, C context) {
        return visitNode(clause, context);
    }

    default R visitCancelDecommissionDiskClause(CancelDecommissionDiskClause clause, C context) {
        return visitNode(clause, context);
    }

    default R visitDisableDiskClause(DisableDiskClause clause, C context) {
        return visitNode(clause, context);
    }

    default R visitApplyMaskingPolicyClause(ApplyMaskingPolicyClause clause, C context) {
        return visitNode(clause, context);
    }

    default R visitRevokeMaskingPolicyClause(RevokeMaskingPolicyClause clause, C context) {
        return visitNode(clause, context);
    }

    default R visitApplyRowAccessPolicyClause(ApplyRowAccessPolicyClause clause, C context) {
        return visitNode(clause, context);
    }

    default R visitRevokeRowAccessPolicyClause(RevokeRowAccessPolicyClause clause, C context) {
        return visitNode(clause, context);
    }

    // ---------------------------------------- Authz Statement ----------------------------------------------------

    default R visitCreateSecurityIntegrationStatement(CreateSecurityIntegrationStatement statement, C context) {
        return visitStatement(statement, context);
    }

    default R visitDropSecurityIntegrationStatement(DropSecurityIntegrationStatement statement, C context) {
        return visitStatement(statement, context);
    }

    default R visitAlterSecurityIntegrationStatement(AlterSecurityIntegrationStatement statement, C context) {
        return visitStatement(statement, context);
    }

    default R visitShowCreateSecurityIntegrationStatement(ShowCreateSecurityIntegrationStatement statement, C context) {
        return visitStatement(statement, context);
    }

    default R visitShowSecurityIntegrationStatement(ShowSecurityIntegrationStatement statement, C context) {
        return visitStatement(statement, context);
    }

    default R visitCreateRoleMappingStatement(CreateRoleMappingStatement statement, C context) {
        return visitStatement(statement, context);
    }

    default R visitAlterRoleMappingStatement(AlterRoleMappingStatement statement, C context) {
        return visitStatement(statement, context);
    }

    default R visitDropRoleMappingStatement(DropRoleMappingStatement statement, C context) {
        return visitStatement(statement, context);
    }

    default R visitShowRoleMappingStatement(ShowRoleMappingStatement statement, C context) {
        return visitStatement(statement, context);
    }

    default R visitRefreshRoleMappingStatement(RefreshRoleMappingStatement statement, C context) {
        return visitStatement(statement, context);
    }

    // ---------------------------------------- Security Policy Statement ---------------------------------------------------

    default R visitCreatePolicyStatement(CreatePolicyStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitDropPolicyStatement(DropPolicyStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitAlterPolicyStatement(AlterPolicyStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitShowPolicyStatement(ShowPolicyStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    default R visitShowCreatePolicyStatement(ShowCreatePolicyStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    default R visitCreatePasswordPolicyStatement(CreatePasswordPolicyStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitDropPasswordPolicyStatement(DropPasswordPolicyStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitShowPasswordPolicyStatement(ShowPasswordPolicyStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    default R visitShowCreatePasswordPolicyStatement(ShowCreatePasswordPolicyStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    default R visitSetPasswordPolicyStatement(SetPasswordPolicyStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitUnsetPasswordPolicyStatement(UnsetPasswordPolicyStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    // -------------------------------------------- Failover Group Statement -----------------------------------------------------

    default R visitCreatePrimaryFailoverGroupStatement(CreatePrimaryFailoverGroupStmt statement, C context) {
        return visitStatement(statement, context);
    }

    default R visitCreateSecondaryFailoverGroupStatement(CreateSecondaryFailoverGroupStmt statement, C context) {
        return visitStatement(statement, context);
    }

    default R visitDropFailoverGroupStatement(DropFailoverGroupStmt statement, C context) {
        return visitStatement(statement, context);
    }

    default R visitShowFailoverGroupsStatement(ShowFailoverGroupsStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    default R visitDescribeFailoverGroupStatement(DescribeFailoverGroupStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    default R visitAlterFailoverGroupSetStatement(AlterFailoverGroupSetStmt statement, C context) {
        return visitStatement(statement, context);
    }

    default R visitAlterFailoverGroupAddStatement(AlterFailoverGroupAddStmt statement, C context) {
        return visitStatement(statement, context);
    }

    default R visitAlterFailoverGroupRemoveStatement(AlterFailoverGroupRemoveStmt statement, C context) {
        return visitStatement(statement, context);
    }

    default R visitAlterFailoverGroupRefreshStatement(AlterFailoverGroupRefreshStmt statement, C context) {
        return visitStatement(statement, context);
    }

    default R visitAlterFailoverGroupPrimaryStatement(AlterFailoverGroupPrimaryStmt statement, C context) {
        return visitStatement(statement, context);
    }

    default R visitAlterFailoverGroupSuspendStatement(AlterFailoverGroupSuspendStmt statement, C context) {
        return visitStatement(statement, context);
    }

    default R visitAlterFailoverGroupResumeStatement(AlterFailoverGroupResumeStmt statement, C context) {
        return visitStatement(statement, context);
    }

    // tunespace
    default R visitCreateTunespaceStmt(CreateTunespaceStmt node, C context) {
        return visitStatement(node, context);
    }

    default R visitAlterTunespaceStmt(AlterTunespaceStmt node, C context) {
        return visitStatement(node, context);
    }

    default R visitShowRecommendationsStmt(ShowRecommendationsStmt node, C context) {
        return visitStatement(node, context);
    }
}
