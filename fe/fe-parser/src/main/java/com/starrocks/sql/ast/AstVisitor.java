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

package com.starrocks.sql.ast;

import com.starrocks.sql.ast.group.CreateGroupProviderStmt;
import com.starrocks.sql.ast.group.DropGroupProviderStmt;
import com.starrocks.sql.ast.warehouse.AlterWarehouseStmt;
import com.starrocks.sql.ast.warehouse.CreateWarehouseStmt;
import com.starrocks.sql.ast.warehouse.DropWarehouseStmt;
import com.starrocks.sql.ast.warehouse.ResumeWarehouseStmt;
import com.starrocks.sql.ast.warehouse.SetWarehouseStmt;
import com.starrocks.sql.ast.warehouse.SuspendWarehouseStmt;
import com.starrocks.sql.ast.warehouse.cngroup.AlterCnGroupStmt;
import com.starrocks.sql.ast.warehouse.cngroup.CreateCnGroupStmt;
import com.starrocks.sql.ast.warehouse.cngroup.EnableDisableCnGroupStmt;

public interface AstVisitor<R, C> {
    default R visit(ParseNode node) {
        return visit(node, null);
    }

    default R visit(ParseNode node, C context) {
        return node.accept(this, context);
    }

    default R visitNode(ParseNode node, C context) {
        return null;
    }

    default R visitStatement(StatementBase statement, C context) {
        return visitNode(statement, context);
    }

    default R visitDDLStatement(DdlStmt statement, C context) {
        return visitStatement(statement, context);
    }

    // ------------------------------------------- Group Provider Statement ----------------------------------------------------

    default R visitCreateGroupProviderStatement(CreateGroupProviderStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitDropGroupProviderStatement(DropGroupProviderStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    // ------------------------------------------- Role Statement ----------------------------------------------------

    default R visitCreateRoleStatement(CreateRoleStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitDropRoleStatement(DropRoleStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitAlterRoleStatement(AlterRoleStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitGrantRevokeRoleStatement(BaseGrantRevokeRoleStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitGrantRoleStatement(GrantRoleStmt statement, C context) {
        return visitGrantRevokeRoleStatement(statement, context);
    }

    default R visitRevokeRoleStatement(RevokeRoleStmt statement, C context) {
        return visitGrantRevokeRoleStatement(statement, context);
    }

    default R visitSetRoleStatement(SetRoleStmt statement, C context) {
        return visitStatement(statement, context);
    }

    default R visitSetDefaultRoleStatement(SetDefaultRoleStmt statement, C context) {
        return visitStatement(statement, context);
    }

    // ------------------------------------------- Unsupported Statement ----------------------------------------------------

    default R visitUnsupportedStatement(UnsupportedStmt statement, C context) {
        return visitStatement(statement, context);
    }

    // ------------------------------------------- Transaction Statement ----------------------------------------------------

    default R visitBeginStatement(com.starrocks.sql.ast.txn.BeginStmt statement, C context) {
        return visitStatement(statement, context);
    }

    default R visitCommitStatement(com.starrocks.sql.ast.txn.CommitStmt statement, C context) {
        return visitStatement(statement, context);
    }

    default R visitRollbackStatement(com.starrocks.sql.ast.txn.RollbackStmt statement, C context) {
        return visitStatement(statement, context);
    }

    // ------------------------------------------- Alter Clause ----------------------------------------------------

    default R visitCleanTabletSchedQClause(CleanTabletSchedQClause clause, C context) {
        return visitNode(clause, context);
    }

    default R visitRefreshSchemeClause(RefreshSchemeClause clause, C context) {
        return visitNode(clause, context);
    }

    // ------------------------------------------- Plan Advisor Statement ----------------------------------------------------

    default R visitClearPlanAdvisorStatement(com.starrocks.sql.ast.feedback.ClearPlanAdvisorStmt statement, C context) {
        return visitStatement(statement, context);
    }

    default R visitDelPlanAdvisorStatement(com.starrocks.sql.ast.feedback.DelPlanAdvisorStmt statement, C context) {
        return visitStatement(statement, context);
    }

    default R visitShowPlanAdvisorStatement(com.starrocks.sql.ast.feedback.ShowPlanAdvisorStmt statement, C context) {
        return visitStatement(statement, context);
    }

    // ------------------------------------------- Frontend Management Clause ----------------------------------------------------

    default R visitCreateImageClause(CreateImageClause clause, C context) {
        return visitNode(clause, context);
    }

    default R visitFrontendClause(FrontendClause clause, C context) {
        return visitNode(clause, context);
    }

    default R visitAddFollowerClause(AddFollowerClause clause, C context) {
        return visitNode(clause, context);
    }

    default R visitDropFollowerClause(DropFollowerClause clause, C context) {
        return visitNode(clause, context);
    }

    default R visitAddObserverClause(AddObserverClause clause, C context) {
        return visitNode(clause, context);
    }

    default R visitDropObserverClause(DropObserverClause clause, C context) {
        return visitNode(clause, context);
    }

    // ------------------------------------------- Basic Node Types ----------------------------------------------------

    default R visitIdentifier(Identifier identifier, C context) {
        return visitNode(identifier, context);
    }

    // ------------------------------------------- Admin Statement ----------------------------------------------------

    default R visitAdminSetConfigStatement(AdminSetConfigStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitAddBackendBlackListStatement(AddBackendBlackListStmt statement, C context) {
        return visitStatement(statement, context);
    }

    default R visitAddComputeNodeBlackListStatement(AddComputeNodeBlackListStmt statement, C context) {
        return visitStatement(statement, context);
    }

    default R visitAddSqlBlackListStatement(AddSqlBlackListStmt statement, C context) {
        return visitStatement(statement, context);
    }

    default R visitAdminCheckTabletsStatement(AdminCheckTabletsStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitAdminSetAutomatedSnapshotOffStatement(AdminSetAutomatedSnapshotOffStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitAdminSetAutomatedSnapshotOnStatement(AdminSetAutomatedSnapshotOnStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    // ------------------------------------------- System Management Statement ----------------------------------------------------

    default R visitAlterSystemStatement(AlterSystemStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitAlterResourceStatement(AlterResourceStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitAlterStorageVolumeStatement(AlterStorageVolumeStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    // ------------------------------------------- Warehouse Statement ----------------------------------------------------

    default R visitCreateWarehouseStatement(CreateWarehouseStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitAlterWarehouseStatement(AlterWarehouseStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitDropWarehouseStatement(DropWarehouseStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }


    // ------------------------------------------- Drop Statement ----------------------------------------------------

    default R visitDropAnalyzeStatement(DropAnalyzeJobStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitDropDataCacheRuleStatement(DropDataCacheRuleStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitDropDictionaryStatement(DropDictionaryStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitDropRepositoryStatement(DropRepositoryStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitDropUserStatement(DropUserStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitExecuteAsStatement(ExecuteAsStmt statement, C context) {
        return visitStatement(statement, context);
    }

    // ------------------------------------------- Alter Clause ----------------------------------------------------

    default R visitAddRollupClause(AddRollupClause clause, C context) {
        return visitNode(clause, context);
    }

    default R visitRollupRenameClause(RollupRenameClause clause, C context) {
        return visitNode(clause, context);
    }

    default R visitDropBranchClause(DropBranchClause clause, C context) {
        return visitNode(clause, context);
    }

    default R visitDropIndexClause(DropIndexClause clause, C context) {
        return visitNode(clause, context);
    }

    default R visitCreateIndexClause(CreateIndexClause clause, C context) {
        return visitNode(clause, context);
    }

    default R visitDropRollupClause(DropRollupClause clause, C context) {
        return visitNode(clause, context);
    }

    default R visitDropPersistentIndexClause(DropPersistentIndexClause clause, C context) {
        return visitNode(clause, context);
    }

    default R visitAlterStorageVolumeCommentClause(AlterStorageVolumeCommentClause clause, C context) {
        return visitNode(clause, context);
    }

    // ------------------------------------------- Additional Warehouse Statement ----------------------------------------------------

    default R visitSuspendWarehouseStatement(SuspendWarehouseStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitResumeWarehouseStatement(ResumeWarehouseStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitSetWarehouseStatement(SetWarehouseStmt statement, C context) {
        return visitStatement(statement, context);
    }

    // ------------------------------------------- CN Group Statement ----------------------------------------------------

    default R visitCreateCNGroupStatement(CreateCnGroupStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitAlterCNGroupStatement(AlterCnGroupStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitEnableDisableCNGroupStatement(EnableDisableCnGroupStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    // ------------------------------------------- User Statement ----------------------------------------------------

    default R visitBaseCreateAlterUserStmt(BaseCreateAlterUserStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    default R visitCreateUserStatement(CreateUserStmt statement, C context) {
        return visitBaseCreateAlterUserStmt(statement, context);
    }

    default R visitAlterUserStatement(AlterUserStmt statement, C context) {
        return visitBaseCreateAlterUserStmt(statement, context);
    }
}