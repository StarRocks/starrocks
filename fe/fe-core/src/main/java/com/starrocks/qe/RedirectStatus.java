// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.qe;

import com.starrocks.sql.ast.AddBackendBlackListStmt;
import com.starrocks.sql.ast.AddComputeNodeBlackListStmt;
import com.starrocks.sql.ast.AddSqlBlackListStmt;
import com.starrocks.sql.ast.AdminCancelRepairTableStmt;
import com.starrocks.sql.ast.AdminCheckTabletsStmt;
import com.starrocks.sql.ast.AdminRepairTableStmt;
import com.starrocks.sql.ast.AdminSetAutomatedSnapshotOffStmt;
import com.starrocks.sql.ast.AdminSetAutomatedSnapshotOnStmt;
import com.starrocks.sql.ast.AdminSetConfigStmt;
import com.starrocks.sql.ast.AdminSetPartitionVersionStmt;
import com.starrocks.sql.ast.AdminSetReplicaStatusStmt;
import com.starrocks.sql.ast.AdminShowConfigStmt;
import com.starrocks.sql.ast.AdminShowReplicaDistributionStmt;
import com.starrocks.sql.ast.AdminShowReplicaStatusStmt;
import com.starrocks.sql.ast.AlterCatalogStmt;
import com.starrocks.sql.ast.AlterDatabaseQuotaStmt;
import com.starrocks.sql.ast.AlterDatabaseRenameStatement;
import com.starrocks.sql.ast.AlterLoadStmt;
import com.starrocks.sql.ast.AlterMaterializedViewStmt;
import com.starrocks.sql.ast.AlterResourceGroupStmt;
import com.starrocks.sql.ast.AlterResourceStmt;
import com.starrocks.sql.ast.AlterRoleStmt;
import com.starrocks.sql.ast.AlterRoutineLoadStmt;
import com.starrocks.sql.ast.AlterStorageVolumeStmt;
import com.starrocks.sql.ast.AlterSystemStmt;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.AlterUserStmt;
import com.starrocks.sql.ast.AlterViewStmt;
import com.starrocks.sql.ast.AnalyzeProfileStmt;
import com.starrocks.sql.ast.AnalyzeStmt;
import com.starrocks.sql.ast.AstVisitorExtendInterface;
import com.starrocks.sql.ast.BackupStmt;
import com.starrocks.sql.ast.BaseCreateAlterUserStmt;
import com.starrocks.sql.ast.BaseGrantRevokePrivilegeStmt;
import com.starrocks.sql.ast.BaseGrantRevokeRoleStmt;
import com.starrocks.sql.ast.CallProcedureStatement;
import com.starrocks.sql.ast.CancelAlterSystemStmt;
import com.starrocks.sql.ast.CancelAlterTableStmt;
import com.starrocks.sql.ast.CancelBackupStmt;
import com.starrocks.sql.ast.CancelCompactionStmt;
import com.starrocks.sql.ast.CancelExportStmt;
import com.starrocks.sql.ast.CancelLoadStmt;
import com.starrocks.sql.ast.CancelRefreshDictionaryStmt;
import com.starrocks.sql.ast.CancelRefreshMaterializedViewStmt;
import com.starrocks.sql.ast.CleanTemporaryTableStmt;
import com.starrocks.sql.ast.ClearDataCacheRulesStmt;
import com.starrocks.sql.ast.CreateAnalyzeJobStmt;
import com.starrocks.sql.ast.CreateCatalogStmt;
import com.starrocks.sql.ast.CreateDataCacheRuleStmt;
import com.starrocks.sql.ast.CreateDbStmt;
import com.starrocks.sql.ast.CreateDictionaryStmt;
import com.starrocks.sql.ast.CreateFileStmt;
import com.starrocks.sql.ast.CreateFunctionStmt;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.ast.CreateMaterializedViewStmt;
import com.starrocks.sql.ast.CreateRepositoryStmt;
import com.starrocks.sql.ast.CreateResourceGroupStmt;
import com.starrocks.sql.ast.CreateResourceStmt;
import com.starrocks.sql.ast.CreateRoleStmt;
import com.starrocks.sql.ast.CreateRoutineLoadStmt;
import com.starrocks.sql.ast.CreateStorageVolumeStmt;
import com.starrocks.sql.ast.CreateTableAsSelectStmt;
import com.starrocks.sql.ast.CreateTableLikeStmt;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.CreateTemporaryTableAsSelectStmt;
import com.starrocks.sql.ast.CreateTemporaryTableLikeStmt;
import com.starrocks.sql.ast.CreateTemporaryTableStmt;
import com.starrocks.sql.ast.CreateUserStmt;
import com.starrocks.sql.ast.CreateViewStmt;
import com.starrocks.sql.ast.DataCacheSelectStatement;
import com.starrocks.sql.ast.DdlStmt;
import com.starrocks.sql.ast.DeallocateStmt;
import com.starrocks.sql.ast.DelBackendBlackListStmt;
import com.starrocks.sql.ast.DelComputeNodeBlackListStmt;
import com.starrocks.sql.ast.DelSqlBlackListStmt;
import com.starrocks.sql.ast.DeleteStmt;
import com.starrocks.sql.ast.DescStorageVolumeStmt;
import com.starrocks.sql.ast.DescribeStmt;
import com.starrocks.sql.ast.DropAnalyzeJobStmt;
import com.starrocks.sql.ast.DropCatalogStmt;
import com.starrocks.sql.ast.DropDataCacheRuleStmt;
import com.starrocks.sql.ast.DropDbStmt;
import com.starrocks.sql.ast.DropDictionaryStmt;
import com.starrocks.sql.ast.DropFileStmt;
import com.starrocks.sql.ast.DropFunctionStmt;
import com.starrocks.sql.ast.DropHistogramStmt;
import com.starrocks.sql.ast.DropMaterializedViewStmt;
import com.starrocks.sql.ast.DropRepositoryStmt;
import com.starrocks.sql.ast.DropResourceGroupStmt;
import com.starrocks.sql.ast.DropResourceStmt;
import com.starrocks.sql.ast.DropRoleStmt;
import com.starrocks.sql.ast.DropStatsStmt;
import com.starrocks.sql.ast.DropStorageVolumeStmt;
import com.starrocks.sql.ast.DropTableStmt;
import com.starrocks.sql.ast.DropTaskStmt;
import com.starrocks.sql.ast.DropTemporaryTableStmt;
import com.starrocks.sql.ast.DropUserStmt;
import com.starrocks.sql.ast.ExecuteAsStmt;
import com.starrocks.sql.ast.ExecuteScriptStmt;
import com.starrocks.sql.ast.ExecuteStmt;
import com.starrocks.sql.ast.ExportStmt;
import com.starrocks.sql.ast.GrantRoleStmt;
import com.starrocks.sql.ast.HelpStmt;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.InstallPluginStmt;
import com.starrocks.sql.ast.KillAnalyzeStmt;
import com.starrocks.sql.ast.KillStmt;
import com.starrocks.sql.ast.LoadStmt;
import com.starrocks.sql.ast.PauseRoutineLoadStmt;
import com.starrocks.sql.ast.PrepareStmt;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.RecoverDbStmt;
import com.starrocks.sql.ast.RecoverPartitionStmt;
import com.starrocks.sql.ast.RecoverTableStmt;
import com.starrocks.sql.ast.RefreshDictionaryStmt;
import com.starrocks.sql.ast.RefreshMaterializedViewStatement;
import com.starrocks.sql.ast.RefreshTableStmt;
import com.starrocks.sql.ast.RestoreStmt;
import com.starrocks.sql.ast.ResumeRoutineLoadStmt;
import com.starrocks.sql.ast.RevokeRoleStmt;
import com.starrocks.sql.ast.SetCatalogStmt;
import com.starrocks.sql.ast.SetDefaultRoleStmt;
import com.starrocks.sql.ast.SetDefaultStorageVolumeStmt;
import com.starrocks.sql.ast.SetListItem;
import com.starrocks.sql.ast.SetPassVar;
import com.starrocks.sql.ast.SetRoleStmt;
import com.starrocks.sql.ast.SetStmt;
import com.starrocks.sql.ast.SetType;
import com.starrocks.sql.ast.SetUserPropertyStmt;
import com.starrocks.sql.ast.ShowAlterStmt;
import com.starrocks.sql.ast.ShowAnalyzeJobStmt;
import com.starrocks.sql.ast.ShowAnalyzeStatusStmt;
import com.starrocks.sql.ast.ShowAuthenticationStmt;
import com.starrocks.sql.ast.ShowAuthorStmt;
import com.starrocks.sql.ast.ShowBackendBlackListStmt;
import com.starrocks.sql.ast.ShowBackendsStmt;
import com.starrocks.sql.ast.ShowBackupStmt;
import com.starrocks.sql.ast.ShowBasicStatsMetaStmt;
import com.starrocks.sql.ast.ShowBrokerStmt;
import com.starrocks.sql.ast.ShowCatalogsStmt;
import com.starrocks.sql.ast.ShowCharsetStmt;
import com.starrocks.sql.ast.ShowCollationStmt;
import com.starrocks.sql.ast.ShowColumnStmt;
import com.starrocks.sql.ast.ShowComputeNodeBlackListStmt;
import com.starrocks.sql.ast.ShowComputeNodesStmt;
import com.starrocks.sql.ast.ShowCreateDbStmt;
import com.starrocks.sql.ast.ShowCreateExternalCatalogStmt;
import com.starrocks.sql.ast.ShowCreateRoutineLoadStmt;
import com.starrocks.sql.ast.ShowCreateTableStmt;
import com.starrocks.sql.ast.ShowDataCacheRulesStmt;
import com.starrocks.sql.ast.ShowDataDistributionStmt;
import com.starrocks.sql.ast.ShowDataStmt;
import com.starrocks.sql.ast.ShowDbStmt;
import com.starrocks.sql.ast.ShowDeleteStmt;
import com.starrocks.sql.ast.ShowDictionaryStmt;
import com.starrocks.sql.ast.ShowDynamicPartitionStmt;
import com.starrocks.sql.ast.ShowEnginesStmt;
import com.starrocks.sql.ast.ShowEventsStmt;
import com.starrocks.sql.ast.ShowExportStmt;
import com.starrocks.sql.ast.ShowFailPointStatement;
import com.starrocks.sql.ast.ShowFrontendsStmt;
import com.starrocks.sql.ast.ShowFunctionsStmt;
import com.starrocks.sql.ast.ShowGrantsStmt;
import com.starrocks.sql.ast.ShowHistogramStatsMetaStmt;
import com.starrocks.sql.ast.ShowIndexStmt;
import com.starrocks.sql.ast.ShowLoadStmt;
import com.starrocks.sql.ast.ShowLoadWarningsStmt;
import com.starrocks.sql.ast.ShowMaterializedViewsStmt;
import com.starrocks.sql.ast.ShowMultiColumnStatsMetaStmt;
import com.starrocks.sql.ast.ShowOpenTableStmt;
import com.starrocks.sql.ast.ShowPartitionsStmt;
import com.starrocks.sql.ast.ShowPluginsStmt;
import com.starrocks.sql.ast.ShowPrivilegesStmt;
import com.starrocks.sql.ast.ShowProcStmt;
import com.starrocks.sql.ast.ShowProcedureStmt;
import com.starrocks.sql.ast.ShowProcesslistStmt;
import com.starrocks.sql.ast.ShowProfilelistStmt;
import com.starrocks.sql.ast.ShowRepositoriesStmt;
import com.starrocks.sql.ast.ShowResourceGroupStmt;
import com.starrocks.sql.ast.ShowResourceGroupUsageStmt;
import com.starrocks.sql.ast.ShowResourcesStmt;
import com.starrocks.sql.ast.ShowRestoreStmt;
import com.starrocks.sql.ast.ShowRolesStmt;
import com.starrocks.sql.ast.ShowRoutineLoadStmt;
import com.starrocks.sql.ast.ShowRoutineLoadTaskStmt;
import com.starrocks.sql.ast.ShowRunningQueriesStmt;
import com.starrocks.sql.ast.ShowSmallFilesStmt;
import com.starrocks.sql.ast.ShowSnapshotStmt;
import com.starrocks.sql.ast.ShowSqlBlackListStmt;
import com.starrocks.sql.ast.ShowStatusStmt;
import com.starrocks.sql.ast.ShowStmt;
import com.starrocks.sql.ast.ShowStorageVolumesStmt;
import com.starrocks.sql.ast.ShowStreamLoadStmt;
import com.starrocks.sql.ast.ShowTableStatusStmt;
import com.starrocks.sql.ast.ShowTableStmt;
import com.starrocks.sql.ast.ShowTabletStmt;
import com.starrocks.sql.ast.ShowTemporaryTableStmt;
import com.starrocks.sql.ast.ShowTransactionStmt;
import com.starrocks.sql.ast.ShowTriggersStmt;
import com.starrocks.sql.ast.ShowUserPropertyStmt;
import com.starrocks.sql.ast.ShowUserStmt;
import com.starrocks.sql.ast.ShowVariablesStmt;
import com.starrocks.sql.ast.ShowWarningStmt;
import com.starrocks.sql.ast.ShowWhiteListStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.StopRoutineLoadStmt;
import com.starrocks.sql.ast.SubmitTaskStmt;
import com.starrocks.sql.ast.SyncStmt;
import com.starrocks.sql.ast.SystemVariable;
import com.starrocks.sql.ast.TruncateTableStmt;
import com.starrocks.sql.ast.UninstallPluginStmt;
import com.starrocks.sql.ast.UnsupportedStmt;
import com.starrocks.sql.ast.UpdateFailPointStatusStatement;
import com.starrocks.sql.ast.UpdateStmt;
import com.starrocks.sql.ast.UseCatalogStmt;
import com.starrocks.sql.ast.UseDbStmt;
import com.starrocks.sql.ast.feedback.AddPlanAdvisorStmt;
import com.starrocks.sql.ast.feedback.ClearPlanAdvisorStmt;
import com.starrocks.sql.ast.feedback.DelPlanAdvisorStmt;
import com.starrocks.sql.ast.feedback.ShowPlanAdvisorStmt;
import com.starrocks.sql.ast.group.CreateGroupProviderStmt;
import com.starrocks.sql.ast.group.DropGroupProviderStmt;
import com.starrocks.sql.ast.group.ShowCreateGroupProviderStmt;
import com.starrocks.sql.ast.group.ShowGroupProvidersStmt;
import com.starrocks.sql.ast.integration.AlterSecurityIntegrationStatement;
import com.starrocks.sql.ast.integration.CreateSecurityIntegrationStatement;
import com.starrocks.sql.ast.integration.DropSecurityIntegrationStatement;
import com.starrocks.sql.ast.integration.ShowCreateSecurityIntegrationStatement;
import com.starrocks.sql.ast.integration.ShowSecurityIntegrationStatement;
import com.starrocks.sql.ast.pipe.AlterPipeStmt;
import com.starrocks.sql.ast.pipe.CreatePipeStmt;
import com.starrocks.sql.ast.pipe.DescPipeStmt;
import com.starrocks.sql.ast.pipe.DropPipeStmt;
import com.starrocks.sql.ast.pipe.PipeName;
import com.starrocks.sql.ast.pipe.ShowPipeStmt;
import com.starrocks.sql.ast.spm.ControlBaselinePlanStmt;
import com.starrocks.sql.ast.spm.CreateBaselinePlanStmt;
import com.starrocks.sql.ast.spm.DropBaselinePlanStmt;
import com.starrocks.sql.ast.spm.ShowBaselinePlanStmt;
import com.starrocks.sql.ast.translate.TranslateStmt;
import com.starrocks.sql.ast.txn.BeginStmt;
import com.starrocks.sql.ast.txn.CommitStmt;
import com.starrocks.sql.ast.txn.RollbackStmt;
import com.starrocks.sql.ast.warehouse.AlterWarehouseStmt;
import com.starrocks.sql.ast.warehouse.CreateWarehouseStmt;
import com.starrocks.sql.ast.warehouse.DropWarehouseStmt;
import com.starrocks.sql.ast.warehouse.ResumeWarehouseStmt;
import com.starrocks.sql.ast.warehouse.SetWarehouseStmt;
import com.starrocks.sql.ast.warehouse.ShowClustersStmt;
import com.starrocks.sql.ast.warehouse.ShowNodesStmt;
import com.starrocks.sql.ast.warehouse.ShowWarehousesStmt;
import com.starrocks.sql.ast.warehouse.SuspendWarehouseStmt;
import com.starrocks.sql.ast.warehouse.cngroup.AlterCnGroupStmt;
import com.starrocks.sql.ast.warehouse.cngroup.CreateCnGroupStmt;
import com.starrocks.sql.ast.warehouse.cngroup.DropCnGroupStmt;
import com.starrocks.sql.ast.warehouse.cngroup.EnableDisableCnGroupStmt;

import java.util.List;

import static com.starrocks.sql.ast.ShowProcStmt.NEED_FORWARD_PATH_ROOT;

public class RedirectStatus {
    private final boolean isForwardToLeader;
    private boolean needToWaitJournalSync;

    public RedirectStatus() {
        isForwardToLeader = true;
        needToWaitJournalSync = true;
    }

    public RedirectStatus(boolean isForwardToLeader, boolean needToWaitJournalSync) {
        this.isForwardToLeader = isForwardToLeader;
        this.needToWaitJournalSync = needToWaitJournalSync;
    }

    public boolean isForwardToLeader() {
        return isForwardToLeader;
    }

    public boolean isNeedToWaitJournalSync() {
        return needToWaitJournalSync;
    }

    public void setNeedToWaitJournalSync(boolean needToWaitJournalSync) {
        this.needToWaitJournalSync = needToWaitJournalSync;
    }

    public static RedirectStatus FORWARD_NO_SYNC = new RedirectStatus(true, false);
    public static RedirectStatus FORWARD_WITH_SYNC = new RedirectStatus(true, true);
    public static RedirectStatus NO_FORWARD = new RedirectStatus(false, false);

    public static RedirectStatus getRedirectStatus(StatementBase stmt) {
        return new RedirectStatusVisitor().getRedirectStatus(stmt);
    }

    public static class RedirectStatusVisitor implements AstVisitorExtendInterface<RedirectStatus, Void> {
        public RedirectStatus getRedirectStatus(StatementBase stmt) {
            return stmt.accept(this, null);
        }

        @Override
        public RedirectStatus visitStatement(StatementBase statement, Void context) {
            throw new UnsupportedOperationException("Not implemented for " + statement.getClass().getSimpleName());
        }

        @Override
        public RedirectStatus visitDDLStatement(DdlStmt statement, Void context) {
            return RedirectStatus.FORWARD_WITH_SYNC;
        }

        // ---------------------------------------- Query Statement --------------------------------------------------------------

        @Override
        public RedirectStatus visitQueryStatement(QueryStatement statement, Void context) {
            return RedirectStatus.NO_FORWARD;
        }

        @Override
        public RedirectStatus visitPrepareStatement(PrepareStmt statement, Void context) {
            return RedirectStatus.NO_FORWARD;
        }

        @Override
        public RedirectStatus visitExecuteStatement(ExecuteStmt statement, Void context) {
            return null;
        }

        @Override
        public RedirectStatus visitDeallocatePrepareStatement(DeallocateStmt statement, Void context) {
            return null;
        }

        // ---------------------------------------- Database Statement -----------------------------------------------------

        @Override
        public RedirectStatus visitUseDbStatement(UseDbStmt statement, Void context) {
            return RedirectStatus.NO_FORWARD;
        }

        @Override
        public RedirectStatus visitShowDatabasesStatement(ShowDbStmt statement, Void context) {
            return visitShowStatement(statement, context);
        }

        @Override
        public RedirectStatus visitAlterDatabaseQuotaStatement(AlterDatabaseQuotaStmt statement, Void context) {
            return visitDDLStatement(statement, context);
        }

        @Override
        public RedirectStatus visitCreateDbStatement(CreateDbStmt statement, Void context) {
            return visitDDLStatement(statement, context);
        }

        @Override
        public RedirectStatus visitDropDbStatement(DropDbStmt statement, Void context) {
            return visitDDLStatement(statement, context);
        }

        @Override
        public RedirectStatus visitShowCreateDbStatement(ShowCreateDbStmt statement, Void context) {
            return visitShowStatement(statement, context);
        }

        @Override
        public RedirectStatus visitAlterDatabaseRenameStatement(AlterDatabaseRenameStatement statement, Void context) {
            return visitDDLStatement(statement, context);
        }

        @Override
        public RedirectStatus visitRecoverDbStatement(RecoverDbStmt statement, Void context) {
            return visitDDLStatement(statement, context);
        }

        @Override
        public RedirectStatus visitShowDataStatement(ShowDataStmt statement, Void context) {
            return visitShowStatement(statement, context);
        }

        @Override
        public RedirectStatus visitShowDataDistributionStatement(ShowDataDistributionStmt statement, Void context) {
            return visitShowStatement(statement, context);
        }

        // ---------------------------------------- Table Statement --------------------------------------------------------

        @Override
        public RedirectStatus visitCreateTableStatement(CreateTableStmt statement, Void context) {
            return visitDDLStatement(statement, context);
        }

        @Override
        public RedirectStatus visitCreateTableAsSelectStatement(CreateTableAsSelectStmt statement, Void context) {
            return RedirectStatus.FORWARD_WITH_SYNC;
        }

        @Override
        public RedirectStatus visitCreateTableLikeStatement(CreateTableLikeStmt statement, Void context) {
            return visitDDLStatement(statement, context);
        }

        @Override
        public RedirectStatus visitCreateTemporaryTableStatement(CreateTemporaryTableStmt statement, Void context) {
            return visitDDLStatement(statement, context);
        }

        @Override
        public RedirectStatus visitCreateTemporaryTableAsSelectStatement(CreateTemporaryTableAsSelectStmt statement,
                                                                         Void context) {
            return visitCreateTableAsSelectStatement(statement, context);
        }

        @Override
        public RedirectStatus visitCreateTemporaryTableLikeStatement(CreateTemporaryTableLikeStmt statement, Void context) {
            return visitDDLStatement(statement, context);
        }

        @Override
        public RedirectStatus visitDropTableStatement(DropTableStmt statement, Void context) {
            return visitDDLStatement(statement, context);
        }

        @Override
        public RedirectStatus visitDropTemporaryTableStatement(DropTemporaryTableStmt statement, Void context) {
            return visitDropTableStatement(statement, context);
        }

        @Override
        public RedirectStatus visitCleanTemporaryTableStatement(CleanTemporaryTableStmt statement, Void context) {
            return RedirectStatus.FORWARD_NO_SYNC;
        }

        @Override
        public RedirectStatus visitRecoverTableStatement(RecoverTableStmt statement, Void context) {
            return visitDDLStatement(statement, context);
        }

        @Override
        public RedirectStatus visitTruncateTableStatement(TruncateTableStmt statement, Void context) {
            return visitDDLStatement(statement, context);
        }

        @Override
        public RedirectStatus visitRefreshTableStatement(RefreshTableStmt statement, Void context) {
            return RedirectStatus.NO_FORWARD;
        }

        @Override
        public RedirectStatus visitAlterTableStatement(AlterTableStmt statement, Void context) {
            return visitDDLStatement(statement, context);
        }

        @Override
        public RedirectStatus visitCancelAlterTableStatement(CancelAlterTableStmt statement, Void context) {
            return visitDDLStatement(statement, context);
        }

        @Override
        public RedirectStatus visitShowTableStatement(ShowTableStmt statement, Void context) {
            return visitShowStatement(statement, context);
        }

        @Override
        public RedirectStatus visitShowTemporaryTablesStatement(ShowTemporaryTableStmt statement, Void context) {
            return visitShowStatement(statement, context);
        }

        @Override
        public RedirectStatus visitDescTableStmt(DescribeStmt statement, Void context) {
            return visitShowStatement(statement, context);
        }

        @Override
        public RedirectStatus visitShowTabletStatement(ShowTabletStmt statement, Void context) {
            if (ConnectContext.get().getSessionVariable().getForwardToLeader()) {
                return RedirectStatus.FORWARD_NO_SYNC;
            } else {
                return RedirectStatus.NO_FORWARD;
            }
        }

        @Override
        public RedirectStatus visitShowCreateTableStatement(ShowCreateTableStmt statement, Void context) {
            return visitShowStatement(statement, context);
        }

        // ---------------------------------------- View Statement ---------------------------------------------------------

        @Override
        public RedirectStatus visitCreateViewStatement(CreateViewStmt statement, Void context) {
            return visitDDLStatement(statement, context);
        }

        @Override
        public RedirectStatus visitAlterViewStatement(AlterViewStmt statement, Void context) {
            return visitDDLStatement(statement, context);
        }

        // ---------------------------------------- Task Statement ---------------------------------------------------------

        @Override
        public RedirectStatus visitSubmitTaskStatement(SubmitTaskStmt statement, Void context) {
            return visitDDLStatement(statement, context);
        }

        @Override
        public RedirectStatus visitDropTaskStmt(DropTaskStmt statement, Void context) {
            return visitDDLStatement(statement, context);
        }

        // ---------------------------------------- Partition Statement ----------------------------------------------------

        @Override
        public RedirectStatus visitRecoverPartitionStatement(RecoverPartitionStmt statement, Void context) {
            return visitDDLStatement(statement, context);
        }

        @Override
        public RedirectStatus visitShowPartitionsStatement(ShowPartitionsStmt statement, Void context) {
            return visitShowStatement(statement, context);
        }

        // ---------------------------------------- Materialized View Statement --------------------------------------------

        @Override
        public RedirectStatus visitCreateMaterializedViewStatement(CreateMaterializedViewStatement statement, Void context) {
            return visitDDLStatement(statement, context);
        }

        @Override
        public RedirectStatus visitCreateMaterializedViewStmt(CreateMaterializedViewStmt statement, Void context) {
            return visitDDLStatement(statement, context);
        }

        @Override
        public RedirectStatus visitAlterMaterializedViewStatement(AlterMaterializedViewStmt statement, Void context) {
            return visitDDLStatement(statement, context);
        }

        @Override
        public RedirectStatus visitRefreshMaterializedViewStatement(RefreshMaterializedViewStatement statement, Void context) {
            return visitDDLStatement(statement, context);
        }

        @Override
        public RedirectStatus visitCancelRefreshMaterializedViewStatement(CancelRefreshMaterializedViewStmt statement,
                                                                          Void context) {
            return visitDDLStatement(statement, context);
        }

        @Override
        public RedirectStatus visitShowMaterializedViewStatement(ShowMaterializedViewsStmt statement, Void context) {
            return RedirectStatus.FORWARD_NO_SYNC;
        }

        @Override
        public RedirectStatus visitDropMaterializedViewStatement(DropMaterializedViewStmt statement, Void context) {
            return visitDDLStatement(statement, context);
        }

        // ---------------------------------------- Catalog Statement ------------------------------------------------------

        @Override
        public RedirectStatus visitCreateCatalogStatement(CreateCatalogStmt statement, Void context) {
            return visitDDLStatement(statement, context);
        }

        @Override
        public RedirectStatus visitDropCatalogStatement(DropCatalogStmt statement, Void context) {
            return visitDDLStatement(statement, context);
        }

        @Override
        public RedirectStatus visitShowCatalogsStatement(ShowCatalogsStmt statement, Void context) {
            return visitShowStatement(statement, context);
        }

        @Override
        public RedirectStatus visitShowCreateExternalCatalogStatement(ShowCreateExternalCatalogStmt statement, Void context) {
            return visitShowStatement(statement, context);
        }

        @Override
        public RedirectStatus visitUseCatalogStatement(UseCatalogStmt statement, Void context) {
            return RedirectStatus.NO_FORWARD;
        }

        @Override
        public RedirectStatus visitSetCatalogStatement(SetCatalogStmt statement, Void context) {
            return RedirectStatus.NO_FORWARD;
        }

        @Override
        public RedirectStatus visitAlterCatalogStatement(AlterCatalogStmt statement, Void context) {
            return visitDDLStatement(statement, context);
        }

        // ------------------------------------------- DML Statement -------------------------------------------------------

        @Override
        public RedirectStatus visitInsertStatement(InsertStmt statement, Void context) {
            if (statement.isExplain() && !StatementBase.ExplainLevel.ANALYZE.equals(statement.getExplainLevel())) {
                return RedirectStatus.NO_FORWARD;
            } else {
                return RedirectStatus.FORWARD_WITH_SYNC;
            }
        }

        @Override
        public RedirectStatus visitUpdateStatement(UpdateStmt statement, Void context) {
            return RedirectStatus.FORWARD_WITH_SYNC;
        }

        @Override
        public RedirectStatus visitDeleteStatement(DeleteStmt statement, Void context) {
            return RedirectStatus.FORWARD_WITH_SYNC;
        }

        // ------------------------------------------- Routine Statement ---------------------------------------------------

        @Override
        public RedirectStatus visitCreateRoutineLoadStatement(CreateRoutineLoadStmt statement, Void context) {
            return visitDDLStatement(statement, context);
        }

        @Override
        public RedirectStatus visitAlterRoutineLoadStatement(AlterRoutineLoadStmt statement, Void context) {
            return visitDDLStatement(statement, context);
        }

        @Override
        public RedirectStatus visitAlterLoadStatement(AlterLoadStmt statement, Void context) {
            return visitDDLStatement(statement, context);
        }

        @Override
        public RedirectStatus visitStopRoutineLoadStatement(StopRoutineLoadStmt statement, Void context) {
            return visitDDLStatement(statement, context);
        }

        @Override
        public RedirectStatus visitResumeRoutineLoadStatement(ResumeRoutineLoadStmt statement, Void context) {
            return visitDDLStatement(statement, context);
        }

        @Override
        public RedirectStatus visitPauseRoutineLoadStatement(PauseRoutineLoadStmt statement, Void context) {
            return visitDDLStatement(statement, context);
        }

        @Override
        public RedirectStatus visitShowRoutineLoadStatement(ShowRoutineLoadStmt statement, Void context) {
            return RedirectStatus.FORWARD_NO_SYNC;
        }

        @Override
        public RedirectStatus visitShowCreateRoutineLoadStatement(ShowCreateRoutineLoadStmt statement, Void context) {
            return visitShowStatement(statement, context);
        }

        @Override
        public RedirectStatus visitShowRoutineLoadTaskStatement(ShowRoutineLoadTaskStmt statement, Void context) {
            return RedirectStatus.FORWARD_WITH_SYNC;
        }

        @Override
        public RedirectStatus visitShowStreamLoadStatement(ShowStreamLoadStmt statement, Void context) {
            return RedirectStatus.FORWARD_NO_SYNC;
        }

        // ------------------------------------------- Admin Statement -----------------------------------------------------

        @Override
        public RedirectStatus visitAdminSetConfigStatement(AdminSetConfigStmt statement, Void context) {
            return RedirectStatus.FORWARD_NO_SYNC;
        }

        @Override
        public RedirectStatus visitAdminSetReplicaStatusStatement(AdminSetReplicaStatusStmt statement, Void context) {
            return RedirectStatus.FORWARD_WITH_SYNC;
        }

        @Override
        public RedirectStatus visitAdminShowConfigStatement(AdminShowConfigStmt statement, Void context) {
            return visitShowStatement(statement, context);
        }

        @Override
        public RedirectStatus visitAdminShowReplicaDistributionStatement(AdminShowReplicaDistributionStmt statement,
                                                                         Void context) {
            if (ConnectContext.get().getSessionVariable().getForwardToLeader()) {
                return RedirectStatus.FORWARD_NO_SYNC;
            } else {
                return RedirectStatus.NO_FORWARD;
            }
        }

        @Override
        public RedirectStatus visitAdminShowReplicaStatusStatement(AdminShowReplicaStatusStmt statement, Void context) {
            if (ConnectContext.get().getSessionVariable().getForwardToLeader()) {
                return RedirectStatus.FORWARD_NO_SYNC;
            } else {
                return RedirectStatus.NO_FORWARD;
            }
        }

        @Override
        public RedirectStatus visitAdminRepairTableStatement(AdminRepairTableStmt statement, Void context) {
            return visitDDLStatement(statement, context);
        }

        @Override
        public RedirectStatus visitAdminCancelRepairTableStatement(AdminCancelRepairTableStmt statement, Void context) {
            return visitDDLStatement(statement, context);
        }

        @Override
        public RedirectStatus visitAdminCheckTabletsStatement(AdminCheckTabletsStmt statement, Void context) {
            return RedirectStatus.FORWARD_NO_SYNC;
        }

        @Override
        public RedirectStatus visitKillStatement(KillStmt statement, Void context) {
            return RedirectStatus.NO_FORWARD;
        }

        @Override
        public RedirectStatus visitAdminSetPartitionVersionStmt(AdminSetPartitionVersionStmt statement, Void context) {
            return RedirectStatus.FORWARD_NO_SYNC;
        }

        @Override
        public RedirectStatus visitSyncStatement(SyncStmt statement, Void context) {
            return RedirectStatus.FORWARD_WITH_SYNC;
        }

        @Override
        public RedirectStatus visitAdminSetAutomatedSnapshotOnStatement(AdminSetAutomatedSnapshotOnStmt clause, Void context) {
            return visitDDLStatement(clause, context);
        }

        @Override
        public RedirectStatus visitAdminSetAutomatedSnapshotOffStatement(AdminSetAutomatedSnapshotOffStmt clause, Void context) {
            return visitDDLStatement(clause, context);
        }

        // ---------------------------------------- Cluster Management Statement -------------------------------------------

        @Override
        public RedirectStatus visitAlterSystemStatement(AlterSystemStmt statement, Void context) {
            return visitDDLStatement(statement, context);
        }

        @Override
        public RedirectStatus visitCancelAlterSystemStatement(CancelAlterSystemStmt statement, Void context) {
            return visitDDLStatement(statement, context);
        }

        @Override
        public RedirectStatus visitShowComputeNodes(ShowComputeNodesStmt statement, Void context) {
            return RedirectStatus.FORWARD_NO_SYNC;
        }

        // ------------------------------------------- Analyze Statement ---------------------------------------------------

        @Override
        public RedirectStatus visitAnalyzeStatement(AnalyzeStmt statement, Void context) {
            return RedirectStatus.FORWARD_WITH_SYNC;
        }

        @Override
        public RedirectStatus visitCreateAnalyzeJobStatement(CreateAnalyzeJobStmt statement, Void context) {
            return visitDDLStatement(statement, context);
        }

        @Override
        public RedirectStatus visitDropHistogramStatement(DropHistogramStmt statement, Void context) {
            return RedirectStatus.FORWARD_WITH_SYNC;
        }

        @Override
        public RedirectStatus visitDropStatsStatement(DropStatsStmt statsStmt, Void context) {
            return RedirectStatus.FORWARD_WITH_SYNC;
        }

        @Override
        public RedirectStatus visitShowAnalyzeJobStatement(ShowAnalyzeJobStmt statement, Void context) {
            return RedirectStatus.FORWARD_NO_SYNC;
        }

        @Override
        public RedirectStatus visitShowAnalyzeStatusStatement(ShowAnalyzeStatusStmt statement, Void context) {
            return RedirectStatus.FORWARD_NO_SYNC;
        }

        @Override
        public RedirectStatus visitShowBasicStatsMetaStatement(ShowBasicStatsMetaStmt statement, Void context) {
            return RedirectStatus.FORWARD_NO_SYNC;
        }

        @Override
        public RedirectStatus visitShowHistogramStatsMetaStatement(ShowHistogramStatsMetaStmt statement, Void context) {
            return RedirectStatus.FORWARD_NO_SYNC;
        }

        @Override
        public RedirectStatus visitShowMultiColumnsStatsMetaStatement(ShowMultiColumnStatsMetaStmt statement, Void context) {
            return RedirectStatus.FORWARD_NO_SYNC;
        }

        @Override
        public RedirectStatus visitKillAnalyzeStatement(KillAnalyzeStmt statement, Void context) {
            return RedirectStatus.FORWARD_WITH_SYNC;
        }

        @Override
        public RedirectStatus visitDropAnalyzeStatement(DropAnalyzeJobStmt statement, Void context) {
            return visitDDLStatement(statement, context);
        }

        // ---------------------------------------- Analyze Profile Statement ----------------------------------------------

        @Override
        public RedirectStatus visitAnalyzeProfileStatement(AnalyzeProfileStmt statement, Void context) {
            return RedirectStatus.NO_FORWARD;
        }

        // ---------------------------------------- Resource Group Statement -----------------------------------------------

        @Override
        public RedirectStatus visitCreateResourceGroupStatement(CreateResourceGroupStmt statement, Void context) {
            return visitDDLStatement(statement, context);
        }

        @Override
        public RedirectStatus visitDropResourceGroupStatement(DropResourceGroupStmt statement, Void context) {
            return visitDDLStatement(statement, context);
        }

        @Override
        public RedirectStatus visitAlterResourceGroupStatement(AlterResourceGroupStmt statement, Void context) {
            return visitDDLStatement(statement, context);
        }

        @Override
        public RedirectStatus visitShowResourceGroupStatement(ShowResourceGroupStmt statement, Void context) {
            return visitShowStatement(statement, context);
        }

        // ---------------------------------------- External Resource Statement---------------------------------------------

        @Override
        public RedirectStatus visitCreateResourceStatement(CreateResourceStmt statement, Void context) {
            return visitDDLStatement(statement, context);
        }

        @Override
        public RedirectStatus visitDropResourceStatement(DropResourceStmt statement, Void context) {
            return visitDDLStatement(statement, context);
        }

        @Override
        public RedirectStatus visitAlterResourceStatement(AlterResourceStmt statement, Void context) {
            return visitDDLStatement(statement, context);
        }

        @Override
        public RedirectStatus visitShowResourceStatement(ShowResourcesStmt statement, Void context) {
            if (ConnectContext.get().getSessionVariable().getForwardToLeader()) {
                return RedirectStatus.FORWARD_NO_SYNC;
            } else {
                return RedirectStatus.NO_FORWARD;
            }
        }

        // ---------------------------------------- UDF Statement-----------------------------------------------------------

        @Override
        public RedirectStatus visitShowFunctionsStatement(ShowFunctionsStmt statement, Void context) {
            return visitShowStatement(statement, context);
        }

        @Override
        public RedirectStatus visitDropFunctionStatement(DropFunctionStmt statement, Void context) {
            return RedirectStatus.FORWARD_WITH_SYNC;
        }

        @Override
        public RedirectStatus visitCreateFunctionStatement(CreateFunctionStmt statement, Void context) {
            return RedirectStatus.FORWARD_WITH_SYNC;
        }

        // ---------------------------------------- LOAD Statement----------------------------------------------------------

        @Override
        public RedirectStatus visitLoadStatement(LoadStmt statement, Void context) {
            return visitDDLStatement(statement, context);
        }

        @Override
        public RedirectStatus visitShowLoadStatement(ShowLoadStmt statement, Void context) {
            return RedirectStatus.FORWARD_WITH_SYNC;
        }

        @Override
        public RedirectStatus visitShowLoadWarningsStatement(ShowLoadWarningsStmt statement, Void context) {
            return visitShowStatement(statement, context);
        }

        @Override
        public RedirectStatus visitCancelLoadStatement(CancelLoadStmt statement, Void context) {
            return visitDDLStatement(statement, context);
        }

        @Override
        public RedirectStatus visitCancelCompactionStatement(CancelCompactionStmt statement, Void context) {
            return visitDDLStatement(statement, context);
        }

        // ---------------------------------------- Show Statement ---------------------------------------------------------

        @Override
        public RedirectStatus visitShowStatement(ShowStmt statement, Void context) {
            return RedirectStatus.NO_FORWARD;
        }

        @Override
        public RedirectStatus visitShowAuthorStatement(ShowAuthorStmt statement, Void context) {
            return visitShowStatement(statement, context);
        }

        @Override
        public RedirectStatus visitShowBackendsStatement(ShowBackendsStmt statement, Void context) {
            return RedirectStatus.FORWARD_NO_SYNC;
        }

        @Override
        public RedirectStatus visitShowBrokerStatement(ShowBrokerStmt statement, Void context) {
            if (ConnectContext.get().getSessionVariable().getForwardToLeader()) {
                return RedirectStatus.FORWARD_NO_SYNC;
            } else {
                return RedirectStatus.NO_FORWARD;
            }
        }

        @Override
        public RedirectStatus visitShowDeleteStatement(ShowDeleteStmt statement, Void context) {
            return visitShowStatement(statement, context);
        }

        @Override
        public RedirectStatus visitShowDynamicPartitionStatement(ShowDynamicPartitionStmt statement, Void context) {
            return visitShowStatement(statement, context);
        }

        @Override
        public RedirectStatus visitShowFrontendsStatement(ShowFrontendsStmt statement, Void context) {
            return RedirectStatus.FORWARD_NO_SYNC;
        }

        @Override
        public RedirectStatus visitShowTransactionStatement(ShowTransactionStmt statement, Void context) {
            return RedirectStatus.FORWARD_NO_SYNC;
        }

        @Override
        public RedirectStatus visitHelpStatement(HelpStmt statement, Void context) {
            return visitShowStatement(statement, context);
        }

        @Override
        public RedirectStatus visitShowEnginesStatement(ShowEnginesStmt statement, Void context) {
            return visitShowStatement(statement, context);
        }

        @Override
        public RedirectStatus visitShowWarningStatement(ShowWarningStmt statement, Void context) {
            return visitShowStatement(statement, context);
        }

        @Override
        public RedirectStatus visitShowVariablesStatement(ShowVariablesStmt statement, Void context) {
            return visitShowStatement(statement, context);
        }

        @Override
        public RedirectStatus visitShowProcStmt(ShowProcStmt statement, Void context) {
            String path = statement.getPath();
            if (ConnectContext.get().getSessionVariable().getForwardToLeader()) {
                return RedirectStatus.FORWARD_NO_SYNC;
            } else {
                if (path.equals("/") || !path.contains("/")) {
                    return RedirectStatus.NO_FORWARD;
                }
                String[] pathGroup = path.split("/");
                if (NEED_FORWARD_PATH_ROOT.contains(pathGroup[1])) {
                    return RedirectStatus.FORWARD_NO_SYNC;
                }
                return RedirectStatus.NO_FORWARD;
            }
        }

        @Override
        public RedirectStatus visitShowProcesslistStatement(ShowProcesslistStmt statement, Void context) {
            return visitShowStatement(statement, context);
        }

        @Override
        public RedirectStatus visitShowProfilelistStatement(ShowProfilelistStmt statement, Void context) {
            return visitShowStatement(statement, context);
        }

        @Override
        public RedirectStatus visitShowRunningQueriesStatement(ShowRunningQueriesStmt statement, Void context) {
            return RedirectStatus.FORWARD_NO_SYNC;
        }

        @Override
        public RedirectStatus visitShowResourceGroupUsageStatement(ShowResourceGroupUsageStmt statement, Void context) {
            return RedirectStatus.FORWARD_NO_SYNC;
        }

        @Override
        public RedirectStatus visitShowColumnStatement(ShowColumnStmt statement, Void context) {
            return visitShowStatement(statement, context);
        }

        @Override
        public RedirectStatus visitShowTableStatusStatement(ShowTableStatusStmt statement, Void context) {
            return visitShowStatement(statement, context);
        }

        @Override
        public RedirectStatus visitShowIndexStatement(ShowIndexStmt statement, Void context) {
            return visitShowStatement(statement, context);
        }

        @Override
        public RedirectStatus visitShowAlterStatement(ShowAlterStmt statement, Void context) {
            return RedirectStatus.FORWARD_NO_SYNC;
        }

        @Override
        public RedirectStatus visitShowUserPropertyStatement(ShowUserPropertyStmt statement, Void context) {
            return visitShowStatement(statement, context);
        }

        @Override
        public RedirectStatus visitShowOpenTableStatement(ShowOpenTableStmt statement, Void context) {
            return visitShowStatement(statement, context);
        }

        @Override
        public RedirectStatus visitShowCollationStatement(ShowCollationStmt statement, Void context) {
            return visitShowStatement(statement, context);
        }

        @Override
        public RedirectStatus visitShowRepositoriesStatement(ShowRepositoriesStmt statement, Void context) {
            return visitShowStatement(statement, context);
        }

        @Override
        public RedirectStatus visitShowCharsetStatement(ShowCharsetStmt statement, Void context) {
            return visitShowStatement(statement, context);
        }

        @Override
        public RedirectStatus visitShowFailPointStatement(ShowFailPointStatement statement, Void context) {
            return visitShowStatement(statement, context);
        }

        @Override
        public RedirectStatus visitShowEventStatement(ShowEventsStmt statement, Void context) {
            return visitShowStatement(statement, context);
        }

        @Override
        public RedirectStatus visitShowPrivilegeStatement(ShowPrivilegesStmt statement, Void context) {
            return visitShowStatement(statement, context);
        }

        @Override
        public RedirectStatus visitShowProcedureStatement(ShowProcedureStmt statement, Void context) {
            return visitShowStatement(statement, context);
        }

        @Override
        public RedirectStatus visitShowStatusStatement(ShowStatusStmt statement, Void context) {
            return visitShowStatement(statement, context);
        }

        @Override
        public RedirectStatus visitShowTriggersStatement(ShowTriggersStmt statement, Void context) {
            return visitShowStatement(statement, context);
        }

        // ---------------------------------------- Authz Statement ----------------------------------------------------

        @Override
        public RedirectStatus visitBaseCreateAlterUserStmt(BaseCreateAlterUserStmt statement, Void context) {
            return visitDDLStatement(statement, context);
        }

        @Override
        public RedirectStatus visitCreateUserStatement(CreateUserStmt statement, Void context) {
            return visitBaseCreateAlterUserStmt(statement, context);
        }

        @Override
        public RedirectStatus visitDropUserStatement(DropUserStmt statement, Void context) {
            return visitDDLStatement(statement, context);
        }

        @Override
        public RedirectStatus visitAlterUserStatement(AlterUserStmt statement, Void context) {
            return visitBaseCreateAlterUserStmt(statement, context);
        }

        @Override
        public RedirectStatus visitShowUserStatement(ShowUserStmt statement, Void context) {
            return visitShowStatement(statement, context);
        }

        @Override
        public RedirectStatus visitShowAuthenticationStatement(ShowAuthenticationStmt statement, Void context) {
            return visitShowStatement(statement, context);
        }

        @Override
        public RedirectStatus visitCreateRoleStatement(CreateRoleStmt statement, Void context) {
            return visitDDLStatement(statement, context);
        }

        @Override
        public RedirectStatus visitAlterRoleStatement(AlterRoleStmt statement, Void context) {
            return visitDDLStatement(statement, context);
        }

        @Override
        public RedirectStatus visitDropRoleStatement(DropRoleStmt statement, Void context) {
            return visitDDLStatement(statement, context);
        }

        @Override
        public RedirectStatus visitShowRolesStatement(ShowRolesStmt statement, Void context) {
            return visitShowStatement(statement, context);
        }

        @Override
        public RedirectStatus visitGrantRevokeRoleStatement(BaseGrantRevokeRoleStmt statement, Void context) {
            return visitDDLStatement(statement, context);
        }

        @Override
        public RedirectStatus visitGrantRoleStatement(GrantRoleStmt statement, Void context) {
            return visitGrantRevokeRoleStatement(statement, context);
        }

        @Override
        public RedirectStatus visitRevokeRoleStatement(RevokeRoleStmt statement, Void context) {
            return visitGrantRevokeRoleStatement(statement, context);
        }

        @Override
        public RedirectStatus visitSetRoleStatement(SetRoleStmt statement, Void context) {
            return RedirectStatus.NO_FORWARD;
        }

        @Override
        public RedirectStatus visitSetDefaultRoleStatement(SetDefaultRoleStmt statement, Void context) {
            return RedirectStatus.FORWARD_WITH_SYNC;
        }

        @Override
        public RedirectStatus visitGrantRevokePrivilegeStatement(BaseGrantRevokePrivilegeStmt statement, Void context) {
            return visitDDLStatement(statement, context);
        }

        @Override
        public RedirectStatus visitShowGrantsStatement(ShowGrantsStmt statement, Void context) {
            return visitShowStatement(statement, context);
        }

        // ------------------------------------------- Security Integration Statement
        // ----------------------------------------------------

        @Override
        public RedirectStatus visitCreateSecurityIntegrationStatement(CreateSecurityIntegrationStatement statement,
                                                                      Void context) {
            return visitDDLStatement(statement, context);
        }

        @Override
        public RedirectStatus visitDropSecurityIntegrationStatement(DropSecurityIntegrationStatement statement, Void context) {
            return visitDDLStatement(statement, context);
        }

        @Override
        public RedirectStatus visitAlterSecurityIntegrationStatement(AlterSecurityIntegrationStatement statement, Void context) {
            return visitDDLStatement(statement, context);
        }

        @Override
        public RedirectStatus visitShowCreateSecurityIntegrationStatement(ShowCreateSecurityIntegrationStatement statement,
                                                                          Void context) {
            return visitShowStatement(statement, context);
        }

        @Override
        public RedirectStatus visitShowSecurityIntegrationStatement(ShowSecurityIntegrationStatement statement, Void context) {
            return visitShowStatement(statement, context);
        }

        // ------------------------------------------- Group Provider Statement
        // ----------------------------------------------------

        @Override
        public RedirectStatus visitCreateGroupProviderStatement(CreateGroupProviderStmt statement, Void context) {
            return visitDDLStatement(statement, context);
        }

        @Override
        public RedirectStatus visitDropGroupProviderStatement(DropGroupProviderStmt statement, Void context) {
            return visitDDLStatement(statement, context);
        }

        @Override
        public RedirectStatus visitShowCreateGroupProviderStatement(ShowCreateGroupProviderStmt statement, Void context) {
            return visitShowStatement(statement, context);
        }

        @Override
        public RedirectStatus visitShowGroupProvidersStatement(ShowGroupProvidersStmt statement, Void context) {
            return visitShowStatement(statement, context);
        }

        // ---------------------------------------- Backup Restore Statement -----------------------------------------------

        @Override
        public RedirectStatus visitBackupStatement(BackupStmt statement, Void context) {
            return visitDDLStatement(statement, context);
        }

        @Override
        public RedirectStatus visitRestoreStatement(RestoreStmt statement, Void context) {
            return visitDDLStatement(statement, context);
        }

        @Override
        public RedirectStatus visitShowBackupStatement(ShowBackupStmt statement, Void context) {
            return RedirectStatus.FORWARD_NO_SYNC;
        }

        @Override
        public RedirectStatus visitShowRestoreStatement(ShowRestoreStmt statement, Void context) {
            return visitShowStatement(statement, context);
        }

        @Override
        public RedirectStatus visitCancelBackupStatement(CancelBackupStmt statement, Void context) {
            return visitDDLStatement(statement, context);
        }

        @Override
        public RedirectStatus visitShowSnapshotStatement(ShowSnapshotStmt statement, Void context) {
            return visitShowStatement(statement, context);
        }

        @Override
        public RedirectStatus visitCreateRepositoryStatement(CreateRepositoryStmt statement, Void context) {
            return visitDDLStatement(statement, context);
        }

        @Override
        public RedirectStatus visitDropRepositoryStatement(DropRepositoryStmt statement, Void context) {
            return visitDDLStatement(statement, context);
        }

        // --------------------------------------- Sql BlackList And WhiteList Statement -----------------------------------

        @Override
        public RedirectStatus visitAddSqlBlackListStatement(AddSqlBlackListStmt statement, Void context) {
            return RedirectStatus.FORWARD_NO_SYNC;
        }

        @Override
        public RedirectStatus visitDelSqlBlackListStatement(DelSqlBlackListStmt statement, Void context) {
            return RedirectStatus.FORWARD_NO_SYNC;
        }

        @Override
        public RedirectStatus visitShowSqlBlackListStatement(ShowSqlBlackListStmt statement, Void context) {
            return visitShowStatement(statement, context);
        }

        @Override
        public RedirectStatus visitShowWhiteListStatement(ShowWhiteListStmt statement, Void context) {
            return visitShowStatement(statement, context);
        }

        // --------------------------------------- Backend BlackList -------------------------------------
        @Override
        public RedirectStatus visitAddBackendBlackListStatement(AddBackendBlackListStmt statement, Void context) {
            return RedirectStatus.NO_FORWARD;
        }

        @Override
        public RedirectStatus visitDelBackendBlackListStatement(DelBackendBlackListStmt statement, Void context) {
            return RedirectStatus.NO_FORWARD;
        }

        @Override
        public RedirectStatus visitShowBackendBlackListStatement(ShowBackendBlackListStmt statement, Void context) {
            return visitShowStatement(statement, context);
        }

        // --------------------------------------- Compute Node BlackList -------------------------------------
        @Override
        public RedirectStatus visitAddComputeNodeBlackListStatement(AddComputeNodeBlackListStmt statement, Void context) {
            return RedirectStatus.NO_FORWARD;
        }

        @Override
        public RedirectStatus visitDelComputeNodeBlackListStatement(DelComputeNodeBlackListStmt statement, Void context) {
            return RedirectStatus.NO_FORWARD;
        }

        @Override
        public RedirectStatus visitShowComputeNodeBlackListStatement(ShowComputeNodeBlackListStmt statement, Void context) {
            return visitShowStatement(statement, context);
        }

        @Override
        public RedirectStatus visitExecuteAsStatement(ExecuteAsStmt statement, Void context) {
            return RedirectStatus.NO_FORWARD;
        }

        @Override
        public RedirectStatus visitExecuteScriptStatement(ExecuteScriptStmt statement, Void context) {
            return RedirectStatus.NO_FORWARD;
        }

        // ------------------------------- DataCache Management Statement -------------------------------------------------
        @Override
        public RedirectStatus visitCreateDataCacheRuleStatement(CreateDataCacheRuleStmt statement, Void context) {
            return visitDDLStatement(statement, context);
        }

        @Override
        public RedirectStatus visitShowDataCacheRulesStatement(ShowDataCacheRulesStmt statement, Void context) {
            return visitShowStatement(statement, context);
        }

        @Override
        public RedirectStatus visitDropDataCacheRuleStatement(DropDataCacheRuleStmt statement, Void context) {
            return visitDDLStatement(statement, context);
        }

        @Override
        public RedirectStatus visitClearDataCacheRulesStatement(ClearDataCacheRulesStmt statement, Void context) {
            return visitDDLStatement(statement, context);
        }

        @Override
        public RedirectStatus visitDataCacheSelectStatement(DataCacheSelectStatement statement, Void context) {
            return visitDDLStatement(statement, context);
        }

        // --------------------------------------- Export Statement --------------------------------------------------------

        @Override
        public RedirectStatus visitExportStatement(ExportStmt statement, Void context) {
            return RedirectStatus.FORWARD_WITH_SYNC;
        }

        @Override
        public RedirectStatus visitCancelExportStatement(CancelExportStmt statement, Void context) {
            return visitDDLStatement(statement, context);
        }

        @Override
        public RedirectStatus visitShowExportStatement(ShowExportStmt statement, Void context) {
            return RedirectStatus.FORWARD_NO_SYNC;
        }

        // --------------------------------------- Plugin Statement --------------------------------------------------------

        @Override
        public RedirectStatus visitInstallPluginStatement(InstallPluginStmt statement, Void context) {
            return visitDDLStatement(statement, context);
        }

        @Override
        public RedirectStatus visitUninstallPluginStatement(UninstallPluginStmt statement, Void context) {
            return visitDDLStatement(statement, context);
        }

        @Override
        public RedirectStatus visitShowPluginsStatement(ShowPluginsStmt statement, Void context) {
            return visitShowStatement(statement, context);
        }

        // --------------------------------------- File Statement ----------------------------------------------------------

        @Override
        public RedirectStatus visitCreateFileStatement(CreateFileStmt statement, Void context) {
            return visitDDLStatement(statement, context);
        }

        @Override
        public RedirectStatus visitDropFileStatement(DropFileStmt statement, Void context) {
            return visitDDLStatement(statement, context);
        }

        @Override
        public RedirectStatus visitShowSmallFilesStatement(ShowSmallFilesStmt statement, Void context) {
            return visitShowStatement(statement, context);
        }

        // ------------------------------------------  Set Statement
        // -----------------------------------------------------------------

        @Override
        public RedirectStatus visitSetStatement(SetStmt statement, Void context) {
            List<SetListItem> setListItems = statement.getSetListItems();
            if (setListItems != null) {
                for (SetListItem var : setListItems) {
                    if (var instanceof SetPassVar) {
                        return RedirectStatus.FORWARD_WITH_SYNC;
                    } else if (var instanceof SystemVariable && ((SystemVariable) var).getType() == SetType.GLOBAL) {
                        return RedirectStatus.FORWARD_WITH_SYNC;
                    }
                }
            }
            return RedirectStatus.NO_FORWARD;
        }

        @Override
        public RedirectStatus visitSetUserPropertyStatement(SetUserPropertyStmt statement, Void context) {
            return visitDDLStatement(statement, context);
        }

        // ---------------------------------------- Storage Volume Statement ----------------------------------------------------

        @Override
        public RedirectStatus visitCreateStorageVolumeStatement(CreateStorageVolumeStmt statement, Void context) {
            return visitDDLStatement(statement, context);
        }

        @Override
        public RedirectStatus visitShowStorageVolumesStatement(ShowStorageVolumesStmt statement, Void context) {
            return visitShowStatement(statement, context);
        }

        @Override
        public RedirectStatus visitAlterStorageVolumeStatement(AlterStorageVolumeStmt statement, Void context) {
            return visitDDLStatement(statement, context);
        }

        @Override
        public RedirectStatus visitDropStorageVolumeStatement(DropStorageVolumeStmt statement, Void context) {
            return visitDDLStatement(statement, context);
        }

        @Override
        public RedirectStatus visitDescStorageVolumeStatement(DescStorageVolumeStmt statement, Void context) {
            return visitShowStatement(statement, context);
        }

        @Override
        public RedirectStatus visitSetDefaultStorageVolumeStatement(SetDefaultStorageVolumeStmt statement, Void context) {
            return visitDDLStatement(statement, context);
        }

        // -------------------------------------------- Pipe Statement -----------------------------------------------------
        @Override
        public RedirectStatus visitPipeName(PipeName statement, Void context) {
            return RedirectStatus.NO_FORWARD;
        }

        @Override
        public RedirectStatus visitCreatePipeStatement(CreatePipeStmt statement, Void context) {
            return visitDDLStatement(statement, context);
        }

        @Override
        public RedirectStatus visitDropPipeStatement(DropPipeStmt statement, Void context) {
            return visitDDLStatement(statement, context);
        }

        @Override
        public RedirectStatus visitAlterPipeStatement(AlterPipeStmt statement, Void context) {
            return visitDDLStatement(statement, context);
        }

        @Override
        public RedirectStatus visitShowPipeStatement(ShowPipeStmt statement, Void context) {
            return RedirectStatus.FORWARD_NO_SYNC;
        }

        @Override
        public RedirectStatus visitDescPipeStatement(DescPipeStmt statement, Void context) {
            return RedirectStatus.FORWARD_NO_SYNC;
        }

        // ---------------------------------------- FailPoint Statement ----------------------------------------------------
        @Override
        public RedirectStatus visitUpdateFailPointStatusStatement(UpdateFailPointStatusStatement statement, Void context) {
            return RedirectStatus.NO_FORWARD;
        }

        // ------------------------------------------- Dictionary Statement
        // ---------------------------------------------------------
        @Override
        public RedirectStatus visitCreateDictionaryStatement(CreateDictionaryStmt clause, Void context) {
            return visitDDLStatement(clause, context);
        }

        @Override
        public RedirectStatus visitDropDictionaryStatement(DropDictionaryStmt clause, Void context) {
            return visitDDLStatement(clause, context);
        }

        @Override
        public RedirectStatus visitRefreshDictionaryStatement(RefreshDictionaryStmt clause, Void context) {
            return visitDDLStatement(clause, context);
        }

        @Override
        public RedirectStatus visitShowDictionaryStatement(ShowDictionaryStmt clause, Void context) {
            return visitShowStatement(clause, context);
        }

        @Override
        public RedirectStatus visitCancelRefreshDictionaryStatement(CancelRefreshDictionaryStmt clause, Void context) {
            return visitDDLStatement(clause, context);
        }

        // ---------------------------------------- Warehouse Statement ----------------------------------------------------

        @Override
        public RedirectStatus visitShowWarehousesStatement(ShowWarehousesStmt statement, Void context) {
            return visitShowStatement(statement, context);
        }

        @Override
        public RedirectStatus visitShowClusterStatement(ShowClustersStmt statement, Void context) {
            return visitShowStatement(statement, context);
        }

        @Override
        public RedirectStatus visitCreateWarehouseStatement(CreateWarehouseStmt statement, Void context) {
            return visitDDLStatement(statement, context);
        }

        @Override
        public RedirectStatus visitDropWarehouseStatement(DropWarehouseStmt statement, Void context) {
            return visitDDLStatement(statement, context);
        }

        @Override
        public RedirectStatus visitSuspendWarehouseStatement(SuspendWarehouseStmt statement, Void context) {
            return visitDDLStatement(statement, context);
        }

        @Override
        public RedirectStatus visitResumeWarehouseStatement(ResumeWarehouseStmt statement, Void context) {
            return visitDDLStatement(statement, context);
        }

        @Override
        public RedirectStatus visitSetWarehouseStatement(SetWarehouseStmt statement, Void context) {
            return RedirectStatus.NO_FORWARD;
        }

        @Override
        public RedirectStatus visitShowNodesStatement(ShowNodesStmt statement, Void context) {
            return visitShowStatement(statement, context);
        }

        @Override
        public RedirectStatus visitAlterWarehouseStatement(AlterWarehouseStmt statement, Void context) {
            return visitDDLStatement(statement, context);
        }

        // ------------------------------------------- CNGroup statement ---------------------------------------------------
        @Override
        public RedirectStatus visitCreateCNGroupStatement(CreateCnGroupStmt statement, Void context) {
            return visitDDLStatement(statement, context);
        }

        @Override
        public RedirectStatus visitDropCNGroupStatement(DropCnGroupStmt statement, Void context) {
            return visitDDLStatement(statement, context);
        }

        @Override
        public RedirectStatus visitEnableDisableCNGroupStatement(EnableDisableCnGroupStmt statement, Void context) {
            return visitDDLStatement(statement, context);
        }

        @Override
        public RedirectStatus visitAlterCNGroupStatement(AlterCnGroupStmt statement, Void context) {
            return visitDDLStatement(statement, context);
        }

        // ------------------------------------------- Unsupported statement -----------------------------------------------------

        @Override
        public RedirectStatus visitUnsupportedStatement(UnsupportedStmt statement, Void context) {
            return RedirectStatus.NO_FORWARD;
        }

        // ------------------------------------------- Plan Tuning Statement -----------------------------------------------
        @Override
        public RedirectStatus visitAddPlanAdvisorStatement(AddPlanAdvisorStmt statement, Void context) {
            return RedirectStatus.NO_FORWARD;
        }

        @Override
        public RedirectStatus visitClearPlanAdvisorStatement(ClearPlanAdvisorStmt statement, Void context) {
            return RedirectStatus.NO_FORWARD;
        }

        @Override
        public RedirectStatus visitDelPlanAdvisorStatement(DelPlanAdvisorStmt statement, Void context) {
            return RedirectStatus.NO_FORWARD;
        }

        @Override
        public RedirectStatus visitShowPlanAdvisorStatement(ShowPlanAdvisorStmt statement, Void context) {
            return RedirectStatus.NO_FORWARD;
        }

        // ---------------------------------------- Transaction Statement --------------------------------------------------

        @Override
        public RedirectStatus visitBeginStatement(BeginStmt statement, Void context) {
            return RedirectStatus.FORWARD_WITH_SYNC;
        }

        @Override
        public RedirectStatus visitCommitStatement(CommitStmt statement, Void context) {
            return RedirectStatus.FORWARD_WITH_SYNC;
        }

        @Override
        public RedirectStatus visitRollbackStatement(RollbackStmt statement, Void context) {
            return RedirectStatus.FORWARD_WITH_SYNC;
        }

        // ---------------------------------------- Translate Statement --------------------------------------------------
        @Override
        public RedirectStatus visitTranslateStatement(TranslateStmt statement, Void context) {
            return RedirectStatus.NO_FORWARD;
        }

        // -------------------------------------------BaselinePlan -------------------------------------------------------

        @Override
        public RedirectStatus visitCreateBaselinePlanStatement(CreateBaselinePlanStmt statement, Void context) {
            return RedirectStatus.NO_FORWARD;
        }

        @Override
        public RedirectStatus visitDropBaselinePlanStatement(DropBaselinePlanStmt statement, Void context) {
            return visitDDLStatement(statement, context);
        }

        @Override
        public RedirectStatus visitShowBaselinePlanStatement(ShowBaselinePlanStmt statement, Void context) {
            return visitShowStatement(statement, context);
        }

        @Override
        public RedirectStatus visitControlBaselinePlanStatement(ControlBaselinePlanStmt statement, Void context) {
            return visitDDLStatement(statement, context);
        }

        @Override
        public RedirectStatus visitCallProcedureStatement(CallProcedureStatement statement, Void context) {
            return RedirectStatus.NO_FORWARD;
        }
    }
}
