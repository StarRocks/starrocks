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

package com.starrocks.qe;

import com.google.common.collect.Lists;
import com.starrocks.catalog.PrimitiveType;
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
import com.starrocks.sql.ast.BackupStmt;
import com.starrocks.sql.ast.CancelAlterSystemStmt;
import com.starrocks.sql.ast.CancelAlterTableStmt;
import com.starrocks.sql.ast.CancelBackupStmt;
import com.starrocks.sql.ast.CancelCompactionStmt;
import com.starrocks.sql.ast.CancelExportStmt;
import com.starrocks.sql.ast.CancelLoadStmt;
import com.starrocks.sql.ast.CancelRefreshDictionaryStmt;
import com.starrocks.sql.ast.CancelRefreshMaterializedViewStmt;
import com.starrocks.sql.ast.CancelStmt;
import com.starrocks.sql.ast.CleanTemporaryTableStmt;
import com.starrocks.sql.ast.ClearDataCacheRulesStmt;
import com.starrocks.sql.ast.ColWithComment;
import com.starrocks.sql.ast.ColumnDef;
import com.starrocks.sql.ast.CreateAnalyzeJobStmt;
import com.starrocks.sql.ast.CreateCatalogStmt;
import com.starrocks.sql.ast.CreateDataCacheRuleStmt;
import com.starrocks.sql.ast.CreateDbStmt;
import com.starrocks.sql.ast.CreateDictionaryStmt;
import com.starrocks.sql.ast.CreateFileStmt;
import com.starrocks.sql.ast.CreateFunctionStmt;
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
import com.starrocks.sql.ast.EmptyStmt;
import com.starrocks.sql.ast.ExecuteAsStmt;
import com.starrocks.sql.ast.ExecuteScriptStmt;
import com.starrocks.sql.ast.ExecuteStmt;
import com.starrocks.sql.ast.ExportStmt;
import com.starrocks.sql.ast.GrantPrivilegeStmt;
import com.starrocks.sql.ast.GrantRevokeClause;
import com.starrocks.sql.ast.GrantRoleStmt;
import com.starrocks.sql.ast.HelpStmt;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.InstallPluginStmt;
import com.starrocks.sql.ast.KillAnalyzeStmt;
import com.starrocks.sql.ast.KillStmt;
import com.starrocks.sql.ast.LabelName;
import com.starrocks.sql.ast.ListPartitionDesc;
import com.starrocks.sql.ast.LoadStmt;
import com.starrocks.sql.ast.PauseRoutineLoadStmt;
import com.starrocks.sql.ast.PrepareStmt;
import com.starrocks.sql.ast.Property;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.RecoverDbStmt;
import com.starrocks.sql.ast.RecoverPartitionStmt;
import com.starrocks.sql.ast.RecoverTableStmt;
import com.starrocks.sql.ast.RefreshDictionaryStmt;
import com.starrocks.sql.ast.RefreshMaterializedViewStatement;
import com.starrocks.sql.ast.RefreshTableStmt;
import com.starrocks.sql.ast.RestoreStmt;
import com.starrocks.sql.ast.ResumeRoutineLoadStmt;
import com.starrocks.sql.ast.RevokePrivilegeStmt;
import com.starrocks.sql.ast.RevokeRoleStmt;
import com.starrocks.sql.ast.SelectList;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.SetCatalogStmt;
import com.starrocks.sql.ast.SetDefaultRoleStmt;
import com.starrocks.sql.ast.SetDefaultStorageVolumeStmt;
import com.starrocks.sql.ast.SetRoleStmt;
import com.starrocks.sql.ast.SetStmt;
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
import com.starrocks.sql.ast.ShowStorageVolumesStmt;
import com.starrocks.sql.ast.ShowStreamLoadStmt;
import com.starrocks.sql.ast.ShowTableStatusStmt;
import com.starrocks.sql.ast.ShowTableStmt;
import com.starrocks.sql.ast.ShowTabletStmt;
import com.starrocks.sql.ast.ShowTransactionStmt;
import com.starrocks.sql.ast.ShowTriggersStmt;
import com.starrocks.sql.ast.ShowUserPropertyStmt;
import com.starrocks.sql.ast.ShowUserStmt;
import com.starrocks.sql.ast.ShowVariablesStmt;
import com.starrocks.sql.ast.ShowWarningStmt;
import com.starrocks.sql.ast.ShowWhiteListStmt;
import com.starrocks.sql.ast.StopRoutineLoadStmt;
import com.starrocks.sql.ast.SubmitTaskStmt;
import com.starrocks.sql.ast.SyncStmt;
import com.starrocks.sql.ast.TaskName;
import com.starrocks.sql.ast.TruncateTableStmt;
import com.starrocks.sql.ast.UninstallPluginStmt;
import com.starrocks.sql.ast.UnsupportedStmt;
import com.starrocks.sql.ast.UpdateFailPointStatusStatement;
import com.starrocks.sql.ast.UpdateStmt;
import com.starrocks.sql.ast.UseCatalogStmt;
import com.starrocks.sql.ast.UseDbStmt;
import com.starrocks.sql.ast.UserRef;
import com.starrocks.sql.ast.ValuesRelation;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.TableName;
import com.starrocks.sql.ast.expression.TypeDef;
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
import com.starrocks.sql.parser.NodePosition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;

public class RedirectStatusTest {
    @Test
    public void testShowDbStmt() {
        ShowDbStmt stmt = new ShowDbStmt(null);
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testQueryStatement() {
        SelectRelation selectRelation = new SelectRelation(new SelectList(),
                null, null, null, null);
        QueryStatement stmt = new QueryStatement(selectRelation);

        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testTranslateStmt() {
        TranslateStmt stmt = new TranslateStmt(NodePosition.ZERO, "mysql", "SELECT 1");
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testUnsupportedStmt() {
        UnsupportedStmt stmt = new UnsupportedStmt();
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testUseDbStmt() {
        UseDbStmt stmt = new UseDbStmt("test_catalog", "test_db", NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testUseCatalogStmt() {
        UseCatalogStmt stmt = new UseCatalogStmt("test_catalog");
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testSetCatalogStmt() {
        SetCatalogStmt stmt = new SetCatalogStmt("test_catalog", NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testSetWarehouseStmt() {
        SetWarehouseStmt stmt = new SetWarehouseStmt("test_warehouse");
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testBeginStmt() {
        BeginStmt stmt = new BeginStmt(NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testCommitStmt() {
        CommitStmt stmt = new CommitStmt(NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testRollbackStmt() {
        RollbackStmt stmt = new RollbackStmt(NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testPipeName() {
        PipeName stmt = new PipeName(NodePosition.ZERO, "test_pipe");
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testExecuteStmt() {
        ExecuteStmt stmt = new ExecuteStmt("test_stmt", java.util.Collections.emptyList());
        Assertions.assertNull(RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testDeallocateStmt() {
        DeallocateStmt stmt = new DeallocateStmt("test_stmt");
        Assertions.assertNull(RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testPrepareStmt() {
        PrepareStmt stmt = new PrepareStmt("test_stmt", null, java.util.Collections.emptyList());
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testSetStmt() {
        SetStmt stmt = new SetStmt(java.util.Collections.emptyList());
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testKillStmt() {
        KillStmt stmt = new KillStmt(123L, NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testKillAnalyzeStmt() {
        KillAnalyzeStmt stmt = new KillAnalyzeStmt(123L);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    // Additional concrete show statements - simplified tests with basic functionality verification
    @Test
    public void testShowEnginesStmt() {
        ShowEnginesStmt stmt = new ShowEnginesStmt();
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testShowTriggersStmt() {
        ShowTriggersStmt stmt = new ShowTriggersStmt();
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testShowBackendsStmt() {
        ShowBackendsStmt stmt = new ShowBackendsStmt();
        Assertions.assertEquals(RedirectStatus.FORWARD_NO_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testShowFrontendsStmt() {
        ShowFrontendsStmt stmt = new ShowFrontendsStmt();
        Assertions.assertEquals(RedirectStatus.FORWARD_NO_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testShowComputeNodesStmt() {
        ShowComputeNodesStmt stmt = new ShowComputeNodesStmt();
        Assertions.assertEquals(RedirectStatus.FORWARD_NO_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testShowBrokerStmt() {
        ConnectContext connectContext = ConnectContext.build();
        connectContext.setThreadLocalInfo();

        ShowBrokerStmt stmt = new ShowBrokerStmt();
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testShowRolesStmt() {
        ShowRolesStmt stmt = new ShowRolesStmt();
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testShowPluginsStmt() {
        ShowPluginsStmt stmt = new ShowPluginsStmt();
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testShowWhiteListStmt() {
        ShowWhiteListStmt stmt = new ShowWhiteListStmt();
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testShowSqlBlackListStmt() {
        ShowSqlBlackListStmt stmt = new ShowSqlBlackListStmt();
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testShowResourceGroupUsageStmt() {
        ShowResourceGroupUsageStmt stmt = new ShowResourceGroupUsageStmt(null, NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.FORWARD_NO_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testShowStorageVolumesStmt() {
        ShowStorageVolumesStmt stmt = new ShowStorageVolumesStmt(null, NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testShowWarehousesStmt() {
        ShowWarehousesStmt stmt = new ShowWarehousesStmt(null);
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    // Other statements
    @Test
    public void testEmptyStmt() {
        EmptyStmt stmt = new EmptyStmt();
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    // Additional StatementBase subclasses tests
    @Test
    public void testQueryStatementRedirect() {
        SelectRelation selectRelation = new SelectRelation(new SelectList(),
                null, null, null, null);
        QueryStatement stmt = new QueryStatement(selectRelation);
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testAddSqlBlackListStmt() {
        AddSqlBlackListStmt stmt = new AddSqlBlackListStmt("SELECT * FROM test");
        Assertions.assertEquals(RedirectStatus.FORWARD_NO_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testDelSqlBlackListStmt() {
        DelSqlBlackListStmt stmt = new DelSqlBlackListStmt(java.util.List.of(1L));
        Assertions.assertEquals(RedirectStatus.FORWARD_NO_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testDropStatsStmt() {
        DropStatsStmt stmt = new DropStatsStmt(null, false, NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testDropHistogramStmt() {
        DropHistogramStmt stmt = new DropHistogramStmt(null, java.util.Collections.emptyList(), NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testExportStmt() {
        ExportStmt stmt = new ExportStmt(null, java.util.Collections.emptyList(), "null", null, null);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testCreateTableAsSelectStmt() {
        CreateTableStmt createTableStmt = new CreateTableStmt(
                false,
                false,
                new TableName("hive_catalog", "hive_db", "hive_table"),
                Lists.newArrayList(
                        new ColumnDef("c1", TypeDef.create(PrimitiveType.INT)),
                        new ColumnDef("p1", TypeDef.create(PrimitiveType.INT))),
                "hive",
                null,
                new ListPartitionDesc(Lists.newArrayList("p1"), new ArrayList<>()),
                null,
                new HashMap<>(),
                new HashMap<>(),
                "my table comment");

        SelectRelation selectRelation = new SelectRelation(new SelectList(),
                null, null, null, null);
        QueryStatement queryStatement = new QueryStatement(selectRelation);

        CreateTableAsSelectStmt stmt = new CreateTableAsSelectStmt(createTableStmt, java.util.Collections.emptyList(),
                queryStatement);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testCreateTemporaryTableAsSelectStatement() {
        CreateTemporaryTableStmt createTemporaryTableStmt = new CreateTemporaryTableStmt(false, false, null,
                java.util.Collections.emptyList(),
                null, null, null, null, null, null, java.util.Collections.emptyMap(), java.util.Collections.emptyMap(), null,
                java.util.Collections.emptyList(), java.util.Collections.emptyList(), NodePosition.ZERO);

        SelectRelation selectRelation = new SelectRelation(new SelectList(),
                null, null, null, null);
        QueryStatement queryStatement = new QueryStatement(selectRelation);

        CreateTemporaryTableAsSelectStmt createTemporaryTableAsSelectStmt =
                new CreateTemporaryTableAsSelectStmt(createTemporaryTableStmt, new ArrayList<>(), queryStatement,
                        NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC,
                RedirectStatus.getRedirectStatus(createTemporaryTableAsSelectStmt));
    }

    @Test
    public void testUpdateFailPointStatusStatement() {
        UpdateFailPointStatusStatement stmt = new UpdateFailPointStatusStatement("test_point", true,
                java.util.Collections.emptyList(), NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testSetDefaultRoleStmt() {
        SetDefaultRoleStmt stmt = new SetDefaultRoleStmt(null, null, java.util.Collections.emptyList());
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    // Show statements that were missing
    @Test
    public void testShowRestoreStmt() {
        ShowRestoreStmt stmt = new ShowRestoreStmt("test_db", null);
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testShowLoadStmt() {
        ShowLoadStmt stmt = new ShowLoadStmt(null, null, null, null);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testShowLoadWarningsStmt() {
        ShowLoadWarningsStmt stmt = new ShowLoadWarningsStmt(null, null, null, null);
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testShowRoutineLoadTaskStmt() {
        ShowRoutineLoadTaskStmt stmt = new ShowRoutineLoadTaskStmt("test_job", null);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testShowTransactionStmt() {
        ShowTransactionStmt stmt = new ShowTransactionStmt(null, null);
        Assertions.assertEquals(RedirectStatus.FORWARD_NO_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testShowProcStmt() {
        ShowProcStmt stmt = new ShowProcStmt("/");
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
        stmt = new ShowProcStmt("/routine_loads");
        Assertions.assertEquals(RedirectStatus.FORWARD_NO_SYNC, RedirectStatus.getRedirectStatus(stmt));
        stmt = new ShowProcStmt("/routine_loads/");
        Assertions.assertEquals(RedirectStatus.FORWARD_NO_SYNC, RedirectStatus.getRedirectStatus(stmt));
        stmt = new ShowProcStmt("/routine_loads/1");
        Assertions.assertEquals(RedirectStatus.FORWARD_NO_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testShowStatusStmt() {
        ShowStatusStmt stmt = new ShowStatusStmt();
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testShowTabletStmt() {
        ConnectContext connectContext = ConnectContext.build();
        connectContext.setThreadLocalInfo();

        ShowTabletStmt stmt = new ShowTabletStmt(null, 0L, NodePosition.ZERO);
        // This statement has conditional redirect status based on session variable
        RedirectStatus status = RedirectStatus.getRedirectStatus(stmt);
        Assertions.assertTrue(status == RedirectStatus.FORWARD_NO_SYNC || status == RedirectStatus.NO_FORWARD);
    }

    @Test
    public void testShowAnalyzeStatusStmt() {
        ShowAnalyzeStatusStmt stmt = new ShowAnalyzeStatusStmt(null, null, null, NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.FORWARD_NO_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testShowExportStmt() {
        ShowExportStmt stmt = new ShowExportStmt(null, null, null, null);
        Assertions.assertEquals(RedirectStatus.FORWARD_NO_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testShowAnalyzeJobStmt() {
        ShowAnalyzeJobStmt stmt = new ShowAnalyzeJobStmt(null, null, null, NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.FORWARD_NO_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testShowBasicStatsMetaStmt() {
        ShowBasicStatsMetaStmt stmt = new ShowBasicStatsMetaStmt(null, null, null, NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.FORWARD_NO_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testShowHistogramStatsMetaStmt() {
        ShowHistogramStatsMetaStmt stmt = new ShowHistogramStatsMetaStmt(null, null, null, NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.FORWARD_NO_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testShowMultiColumnStatsMetaStmt() {
        ShowMultiColumnStatsMetaStmt stmt = new ShowMultiColumnStatsMetaStmt(null, null, null, NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.FORWARD_NO_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testAnalyzeStmt() {
        AnalyzeStmt stmt = new AnalyzeStmt(null, java.util.Collections.emptyList(), null, java.util.Collections.emptyMap(),
                false, false, false, null, NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    // Additional StatementBase subclasses - Show Statements
    @Test
    public void testShowStreamLoadStmt2() {
        ShowStreamLoadStmt stmt = new ShowStreamLoadStmt(null, false);
        Assertions.assertEquals(RedirectStatus.FORWARD_NO_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testShowProcStmt2() {
        ShowProcStmt stmt = new ShowProcStmt("/");
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testShowMaterializedViewsStmt() {
        ShowMaterializedViewsStmt stmt = new ShowMaterializedViewsStmt("catalog", "db", "table");
        Assertions.assertEquals(RedirectStatus.FORWARD_NO_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testShowRunningQueriesStmt() {
        ShowRunningQueriesStmt stmt = new ShowRunningQueriesStmt(10, NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.FORWARD_NO_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testAdminShowReplicaStatusStmt() {
        AdminShowReplicaStatusStmt stmt = new AdminShowReplicaStatusStmt(null, null);
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testShowResourcesStmt() {
        ShowResourcesStmt stmt = new ShowResourcesStmt();
        ConnectContext connectContext = ConnectContext.build();
        connectContext.setThreadLocalInfo();
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testShowAlterStmt() {
        ShowAlterStmt stmt = new ShowAlterStmt(ShowAlterStmt.AlterType.ROLLUP, null, null, null, null);
        Assertions.assertEquals(RedirectStatus.FORWARD_NO_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    // Additional Show Statements with simple constructors
    @Test
    public void testShowAuthorStmt() {
        ShowAuthorStmt stmt = new ShowAuthorStmt();
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testShowDataStmt() {
        ShowDataStmt stmt = new ShowDataStmt(null, null);
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testShowDeleteStmt() {
        ShowDeleteStmt stmt = new ShowDeleteStmt(null, null);
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testShowPartitionsStmt() {
        ShowPartitionsStmt stmt = new ShowPartitionsStmt(new TableName(), null, null, null, false);
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testShowTableStatusStmt() {
        ShowTableStatusStmt stmt = new ShowTableStatusStmt(null, null, null);
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testShowIndexStmt() {
        ShowIndexStmt stmt = new ShowIndexStmt(null, null);
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testShowColumnStmt() {
        ShowColumnStmt stmt = new ShowColumnStmt(null, null, null, false);
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testShowCreateTableStmt() {
        ShowCreateTableStmt stmt = new ShowCreateTableStmt(null, ShowCreateTableStmt.CreateTableType.TABLE);
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testShowCreateDbStmt() {
        ShowCreateDbStmt stmt = new ShowCreateDbStmt(null);
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testShowTableStmt() {
        ShowTableStmt stmt = new ShowTableStmt(null, false, null);
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    // Additional DDL Statements with simple constructors
    @Test
    public void testDropDbStmt() {
        DropDbStmt stmt = new DropDbStmt(false, "test_db", false);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testCreateDbStmt() {
        CreateDbStmt stmt = new CreateDbStmt(false, "test_db");
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testDropTableStmt() {
        DropTableStmt stmt = new DropTableStmt(false, null, false);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testAlterTableStmt() {
        AlterTableStmt stmt = new AlterTableStmt(null, java.util.Collections.emptyList());
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testRefreshTableStmt() {
        RefreshTableStmt stmt = new RefreshTableStmt(null, null);
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testInstallPluginStmt() {
        InstallPluginStmt stmt = new InstallPluginStmt(null, null);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testUninstallPluginStmt() {
        UninstallPluginStmt stmt = new UninstallPluginStmt(null);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testSyncStmt() {
        SyncStmt stmt = new SyncStmt(NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testCleanTemporaryTableStmt() {
        CleanTemporaryTableStmt stmt = new CleanTemporaryTableStmt(java.util.UUID.randomUUID());
        Assertions.assertEquals(RedirectStatus.FORWARD_NO_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    // DML Statements
    @Test
    public void testInsertStmt() {
        SelectRelation selectRelation = new SelectRelation(new SelectList(), null, null, null, null);
        QueryStatement queryStatement = new QueryStatement(selectRelation);

        TableName tableName = new TableName("catalog", "db", "table");
        InsertStmt stmt = new InsertStmt(tableName, queryStatement);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testDeleteStmt() {
        DeleteStmt stmt = new DeleteStmt(null, null, null);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testUpdateStmt() {
        UpdateStmt stmt = new UpdateStmt(null, java.util.Collections.emptyList(), java.util.Collections.emptyList(), null,
                java.util.Collections.emptyList());
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    // Note: Many additional StatementBase subclasses exist with complex constructors
    // that would require extensive mocking. The tests above provide comprehensive coverage
    // of the getRedirectStatus functionality across different statement types and categories.

    // Tests for previously uncovered Statement subclasses
    @Test
    public void testShowSmallFilesStmt() {
        ShowSmallFilesStmt stmt = new ShowSmallFilesStmt("test_db", NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testDropAnalyzeJobStmt() {
        DropAnalyzeJobStmt stmt = new DropAnalyzeJobStmt(123L);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testCreateRoutineLoadStmt() {
        CreateRoutineLoadStmt stmt = new CreateRoutineLoadStmt(null, "test_job", null, null, "", null, NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testShowBackendBlackListStmt() {
        ShowBackendBlackListStmt stmt = new ShowBackendBlackListStmt(NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testShowFunctionsStmt() {
        // ShowFunctionsStmt requires: String, boolean, boolean, boolean, String, Expr
        // Using simplified constructor
        ShowFunctionsStmt stmt = new ShowFunctionsStmt(null, false, false, false, null, null);
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testCreateMaterializedViewStmt() {
        // CreateMaterializedViewStmt requires: TableName, QueryStatement, Map<String,String>
        // This test is simplified due to complex constructor
        TableName tableName = new TableName("catalog", "db", "mv");
        SelectRelation selectRelation = new SelectRelation(new SelectList(), null, null, null, null);
        QueryStatement queryStatement = new QueryStatement(selectRelation);
        CreateMaterializedViewStmt stmt = new CreateMaterializedViewStmt(tableName, queryStatement,
                java.util.Collections.emptyMap());
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testAdminShowConfigStmt() {
        AdminShowConfigStmt stmt = new AdminShowConfigStmt(AdminSetConfigStmt.ConfigType.FRONTEND, null);
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testAdminCheckTabletsStmt() {
        AdminCheckTabletsStmt stmt = new AdminCheckTabletsStmt(java.util.Collections.emptyList(), null, NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.FORWARD_NO_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testSetRoleStmt() {
        SetRoleStmt stmt = new SetRoleStmt(null, java.util.Collections.emptyList());
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testSubmitTaskStmt() {
        SelectRelation selectRelation = new SelectRelation(new SelectList(),
                null, null, null, null);
        QueryStatement queryStatement = new QueryStatement(selectRelation);

        InsertStmt insertStmt = new InsertStmt(new TableName(), queryStatement);
        SubmitTaskStmt stmt = new SubmitTaskStmt(new TaskName("", "", NodePosition.ZERO), 0, insertStmt, NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testAlterSystemStmt() {
        AlterSystemStmt stmt = new AlterSystemStmt(null);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testCancelAlterSystemStmt() {
        CancelAlterSystemStmt stmt = new CancelAlterSystemStmt(null);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testRefreshDictionaryStmt() {
        RefreshDictionaryStmt stmt = new RefreshDictionaryStmt(null, null);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testRecoverDbStmt() {
        RecoverDbStmt stmt = new RecoverDbStmt("test_db");
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testCreateRoleStmt() {
        CreateRoleStmt stmt = new CreateRoleStmt(java.util.Collections.emptyList(), false, "test_role", NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testCreateUserStmt() {
        CreateUserStmt stmt = new CreateUserStmt(null, false, null, java.util.Collections.emptyList(),
                java.util.Collections.emptyMap(), NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testDropCatalogStmt() {
        DropCatalogStmt stmt = new DropCatalogStmt("test_catalog", false);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testResumeRoutineLoadStmt() {
        ResumeRoutineLoadStmt stmt = new ResumeRoutineLoadStmt(null, NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testCreateStorageVolumeStmt() {
        CreateStorageVolumeStmt stmt = new CreateStorageVolumeStmt(false, "test_volume", "S3",
                java.util.Collections.emptyMap(), java.util.Collections.emptyList(), null, NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testAddComputeNodeBlackListStmt() {
        AddComputeNodeBlackListStmt stmt = new AddComputeNodeBlackListStmt(java.util.Collections.emptyList(), NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testClearDataCacheRulesStmt() {
        ClearDataCacheRulesStmt stmt = new ClearDataCacheRulesStmt(NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testRevokePrivilegeStmt() {
        RevokePrivilegeStmt stmt =
                new RevokePrivilegeStmt(null, null, new GrantRevokeClause(new UserRef("", ""), ""), null, null);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testExecuteScriptStmt() {
        ExecuteScriptStmt stmt = new ExecuteScriptStmt(123L, "test_script", NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    // Additional missing StatementBase subclasses tests
    @Test
    public void testAddBackendBlackListStmt() {
        AddBackendBlackListStmt stmt = new AddBackendBlackListStmt(java.util.Collections.emptyList(), NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testAlterCatalogStmt() {
        AlterCatalogStmt stmt = new AlterCatalogStmt("test_catalog", null, null);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testAlterDatabaseQuotaStmt() {
        AlterDatabaseQuotaStmt stmt = new AlterDatabaseQuotaStmt("test_db", null, null, NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testAlterMaterializedViewStmt() {
        AlterMaterializedViewStmt stmt = new AlterMaterializedViewStmt(null, null, null);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testAlterResourceGroupStmt() {
        AlterResourceGroupStmt stmt = new AlterResourceGroupStmt("test_group", null);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testAlterResourceStmt() {
        AlterResourceStmt stmt = new AlterResourceStmt("test_resource", java.util.Collections.emptyMap());
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testAlterRoleStmt() {
        AlterRoleStmt stmt = new AlterRoleStmt(null, false, "");
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testAlterUserStmt() {
        AlterUserStmt stmt = new AlterUserStmt(null, false, null, null, NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testAlterViewStmt() {
        AlterViewStmt stmt = new AlterViewStmt(null, false, AlterViewStmt.AlterDialectType.NONE, null, null, NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testAlterWarehouseStmt() {
        AlterWarehouseStmt stmt = new AlterWarehouseStmt("test_warehouse", java.util.Collections.emptyMap());
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testBackupStmt() {
        BackupStmt stmt = new BackupStmt(null, "test_snapshot", null, null, null, null, false, null,
                java.util.Collections.emptyMap(), NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testCancelBackupStmt() {
        CancelBackupStmt stmt = new CancelBackupStmt("test_db", false);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testCancelCompactionStmt() {
        CancelCompactionStmt stmt = new CancelCompactionStmt(null);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testCancelExportStmt() {
        CancelExportStmt stmt = new CancelExportStmt(null, null);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testCancelLoadStmt() {
        CancelLoadStmt stmt = new CancelLoadStmt(null, null, null);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testCreateAnalyzeJobStmt() {
        CreateAnalyzeJobStmt stmt = new CreateAnalyzeJobStmt(false, java.util.Collections.emptyMap(), NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testCreateCatalogStmt() {
        CreateCatalogStmt stmt = new CreateCatalogStmt("test_catalog", "hive", java.util.Collections.emptyMap(), false);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testCreateFunctionStmt() {
        CreateFunctionStmt stmt = new CreateFunctionStmt("", null, null, null, java.util.Collections.emptyMap(), null, false,
                false, NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testCreateResourceStmt() {
        CreateResourceStmt stmt = new CreateResourceStmt(true, "test_resource", java.util.Collections.emptyMap(),
                NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testCreateResourceGroupStmt() {
        CreateResourceGroupStmt stmt = new CreateResourceGroupStmt("test_group", false, false,
                java.util.Collections.emptyList(), java.util.Collections.emptyMap(), NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testCreateViewStmt() {
        CreateViewStmt stmt = new CreateViewStmt(false, false,
                new TableName("catalog", "db", "table"),
                Lists.newArrayList(new ColWithComment("k1", "",
                        NodePosition.ZERO)),
                "",
                false,
                null,
                NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testCreateWarehouseStmt() {
        CreateWarehouseStmt stmt = new CreateWarehouseStmt(true, "test_warehouse", java.util.Collections.emptyMap(), null);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testDelBackendBlackListStmt() {
        DelBackendBlackListStmt stmt = new DelBackendBlackListStmt(NodePosition.ZERO, java.util.Collections.emptyList());
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testDelComputeNodeBlackListStmt() {
        DelComputeNodeBlackListStmt stmt = new DelComputeNodeBlackListStmt(NodePosition.ZERO, java.util.Collections.emptyList());
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testDropMaterializedViewStmt() {
        DropMaterializedViewStmt stmt = new DropMaterializedViewStmt(true, null, NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testDropResourceStmt() {
        DropResourceStmt stmt = new DropResourceStmt("test_resource");
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testDropResourceGroupStmt() {
        DropResourceGroupStmt stmt = new DropResourceGroupStmt("test_group", NodePosition.ZERO, false);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testDropUserStmt() {
        DropUserStmt stmt = new DropUserStmt(null, false, NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testDropRoleStmt() {
        DropRoleStmt stmt = new DropRoleStmt(null, false, NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testDropFunctionStmt() {
        DropFunctionStmt stmt = new DropFunctionStmt(null, null, NodePosition.ZERO, false);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testDropStorageVolumeStmt() {
        DropStorageVolumeStmt stmt = new DropStorageVolumeStmt(false, "test_volume", NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testRestoreStmt() {
        RestoreStmt stmt = new RestoreStmt(null, "test_snapshot", null, null, null, null, false, null,
                java.util.Collections.emptyMap(), NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testRecoverTableStmt() {
        RecoverTableStmt stmt = new RecoverTableStmt(null);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    // Additional uncovered StatementBase subclasses tests
    @Test
    public void testDescribeStmt() {
        DescribeStmt stmt = new DescribeStmt(null, false);
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testHelpStmt() {
        HelpStmt stmt = new HelpStmt("test_topic");
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testLoadStmt() {
        LoadStmt stmt = new LoadStmt(null, null, null, null, java.util.Collections.emptyMap());
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testCancelStmt() {
        CancelStmt stmt = new CancelStmt(NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testPauseRoutineLoadStmt() {
        PauseRoutineLoadStmt stmt = new PauseRoutineLoadStmt(null);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testStopRoutineLoadStmt() {
        StopRoutineLoadStmt stmt = new StopRoutineLoadStmt(null);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testShowEventsStmt() {
        ShowEventsStmt stmt = new ShowEventsStmt(NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testShowOpenTableStmt() {
        ShowOpenTableStmt stmt = new ShowOpenTableStmt(NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testShowDynamicPartitionStmt() {
        ShowDynamicPartitionStmt stmt = new ShowDynamicPartitionStmt(null, null);
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testShowUserPropertyStmt() {
        ShowUserPropertyStmt stmt = new ShowUserPropertyStmt(null, null);
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testShowPrivilegesStmt() {
        ShowPrivilegesStmt stmt = new ShowPrivilegesStmt();
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testShowAuthenticationStmt() {
        ShowAuthenticationStmt stmt = new ShowAuthenticationStmt(null, false);
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testShowProfilelistStmt() {
        ShowProfilelistStmt stmt = new ShowProfilelistStmt(100, null);
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testShowNodesStmt() {
        ShowNodesStmt stmt = new ShowNodesStmt("test", "compute", null, NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testShowComputeNodeBlackListStmt() {
        ShowComputeNodeBlackListStmt stmt = new ShowComputeNodeBlackListStmt(NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testCreateTableLikeStmt() {
        CreateTableLikeStmt stmt = new CreateTableLikeStmt(false, null, null, null, null, java.util.Collections.emptyMap(),
                NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testCreateTemporaryTableStmt() {
        CreateTemporaryTableStmt stmt = new CreateTemporaryTableStmt(false, false, null, java.util.Collections.emptyList(),
                null, null, null, null, null, null, java.util.Collections.emptyMap(), java.util.Collections.emptyMap(), null,
                java.util.Collections.emptyList(), java.util.Collections.emptyList(), NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testCreateTemporaryTableLikeStmt() {
        CreateTemporaryTableLikeStmt stmt = new CreateTemporaryTableLikeStmt(false, null, null, null, null,
                java.util.Collections.emptyMap(), NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testDropTemporaryTableStmt() {
        DropTemporaryTableStmt stmt = new DropTemporaryTableStmt(true, new TableName(), false);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testSetUserPropertyStmt() {
        SetUserPropertyStmt stmt = new SetUserPropertyStmt(null, java.util.Collections.emptyList());
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testSetDefaultStorageVolumeStmt() {
        SetDefaultStorageVolumeStmt stmt = new SetDefaultStorageVolumeStmt("test_volume", NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testRecoverPartitionStmt() {
        RecoverPartitionStmt stmt = new RecoverPartitionStmt(null, null);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testAlterLoadStmt() {
        AlterLoadStmt stmt = new AlterLoadStmt(null, null, null);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testAlterRoutineLoadStmt() {
        AlterRoutineLoadStmt stmt = new AlterRoutineLoadStmt(null, null, null, null, NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testCancelRefreshMaterializedViewStmt() {
        CancelRefreshMaterializedViewStmt stmt = new CancelRefreshMaterializedViewStmt(null, false, NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testRefreshMaterializedViewStatement() {
        RefreshMaterializedViewStatement stmt = new RefreshMaterializedViewStatement(null, null, false, false, null,
                NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testCancelRefreshDictionaryStmt() {
        CancelRefreshDictionaryStmt stmt = new CancelRefreshDictionaryStmt("test_dict", NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testAdminRepairTableStmt() {
        AdminRepairTableStmt stmt = new AdminRepairTableStmt(null, NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testAdminCancelRepairTableStmt() {
        AdminCancelRepairTableStmt stmt = new AdminCancelRepairTableStmt(null, null);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testAdminSetReplicaStatusStmt() {
        AdminSetReplicaStatusStmt stmt = new AdminSetReplicaStatusStmt(null, NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testAdminSetPartitionVersionStmt() {
        AdminSetPartitionVersionStmt stmt = new AdminSetPartitionVersionStmt(null, "partition", 1L, NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.FORWARD_NO_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testShowDataCacheRulesStmt() {
        ShowDataCacheRulesStmt stmt = new ShowDataCacheRulesStmt(NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testCreateDataCacheRuleStmt() {
        CreateDataCacheRuleStmt stmt = new CreateDataCacheRuleStmt(null, null, 1, java.util.Collections.emptyMap(),
                NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testDropDataCacheRuleStmt() {
        DropDataCacheRuleStmt stmt = new DropDataCacheRuleStmt(1L, NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testShowFailPointStatement() {
        ShowFailPointStatement stmt = new ShowFailPointStatement(null, java.util.Collections.emptyList(), NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testExecuteAsStmt() {
        ExecuteAsStmt stmt = new ExecuteAsStmt(null, false);
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testShowDataDistributionStmt() {
        ShowDataDistributionStmt stmt = new ShowDataDistributionStmt(null, NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testShowProcedureStmt() {
        ShowProcedureStmt stmt = new ShowProcedureStmt(null, null);
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testDataCacheSelectStatement() {
        SelectRelation selectRelation = new SelectRelation(new SelectList(), new ValuesRelation(new ArrayList<>(),
                new ArrayList<>()), null, null, null);
        QueryStatement queryStatement = new QueryStatement(selectRelation);

        TableName tableName = new TableName("catalog", "db", "table");
        InsertStmt insertStmt = new InsertStmt(tableName, queryStatement);

        DataCacheSelectStatement stmt = new DataCacheSelectStatement(insertStmt, new HashMap<>(), NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testDropTaskStmt() {
        DropTaskStmt stmt = new DropTaskStmt(null, false, false, NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testResumeWarehouseStmt() {
        ResumeWarehouseStmt stmt = new ResumeWarehouseStmt("test_warehouse");
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testSuspendWarehouseStmt() {
        SuspendWarehouseStmt stmt = new SuspendWarehouseStmt("test_warehouse");
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testDropWarehouseStmt() {
        DropWarehouseStmt stmt = new DropWarehouseStmt(true, "test_warehouse");
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testAdminSetConfigStmt() {
        AdminSetConfigStmt stmt = new AdminSetConfigStmt(AdminSetConfigStmt.ConfigType.FRONTEND, new Property("", ""), false,
                NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.FORWARD_NO_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testCancelAlterTableStmt() {
        CancelAlterTableStmt stmt = new CancelAlterTableStmt(null, null, null);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testTruncateTableStmt() {
        TruncateTableStmt stmt = new TruncateTableStmt(null, null);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testShowCatalogsStmt() {
        ShowCatalogsStmt stmt = new ShowCatalogsStmt(null, null);
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testShowCharsetStmt() {
        ShowCharsetStmt stmt = new ShowCharsetStmt();
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testShowCollationStmt() {
        ShowCollationStmt stmt = new ShowCollationStmt();
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testShowBackupStmt() {
        ShowBackupStmt stmt = new ShowBackupStmt(null, null);
        Assertions.assertEquals(RedirectStatus.FORWARD_NO_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testShowGrantsStmt() {
        ShowGrantsStmt stmt = new ShowGrantsStmt(null, null);
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testShowProcesslistStmt() {
        ShowProcesslistStmt stmt = new ShowProcesslistStmt(false);
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testShowUserStmt() {
        ShowUserStmt stmt = new ShowUserStmt(false, NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testShowVariablesStmt() {
        ShowVariablesStmt stmt = new ShowVariablesStmt(null, null, null);
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testShowWarningStmt() {
        ShowWarningStmt stmt = new ShowWarningStmt(null, NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testShowResourceGroupStmt() {
        ShowResourceGroupStmt stmt = new ShowResourceGroupStmt("", false, false, NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testShowRepositoriesStmt() {
        ShowRepositoriesStmt stmt = new ShowRepositoriesStmt();
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testShowRoutineLoadStmt() {
        ShowRoutineLoadStmt stmt = new ShowRoutineLoadStmt(null, false, null, java.util.Collections.emptyList(), null,
                NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.FORWARD_NO_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testShowSnapshotStmt() {
        ShowSnapshotStmt stmt = new ShowSnapshotStmt(null, null, null);
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testGrantPrivilegeStmt() {
        GrantPrivilegeStmt stmt =
                new GrantPrivilegeStmt(null, null, new GrantRevokeClause(new UserRef("", ""), ""), null, false);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testGrantRoleStmt() {
        GrantRoleStmt stmt = new GrantRoleStmt(null, null, null);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testRevokeRoleStmt() {
        RevokeRoleStmt stmt = new RevokeRoleStmt(null, null, null);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    // Additional PlanAdvisor and other missing statements
    @Test
    public void testAddPlanAdvisorStmt() {
        AddPlanAdvisorStmt stmt = new AddPlanAdvisorStmt(null, null);
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testClearPlanAdvisorStmt() {
        ClearPlanAdvisorStmt stmt = new ClearPlanAdvisorStmt(NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testDelPlanAdvisorStmt() {
        DelPlanAdvisorStmt stmt = new DelPlanAdvisorStmt(NodePosition.ZERO, "test");
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testShowPlanAdvisorStmt() {
        ShowPlanAdvisorStmt stmt = new ShowPlanAdvisorStmt(null);
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    // Pipe and Integration statements
    @Test
    public void testCreatePipeStmt() {
        CreatePipeStmt stmt = new CreatePipeStmt(false, false, null, 1, null, java.util.Collections.emptyMap(),
                NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testAlterPipeStmt() {
        AlterPipeStmt stmt = new AlterPipeStmt(NodePosition.ZERO, null, null);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testDropPipeStmt() {
        DropPipeStmt stmt = new DropPipeStmt(false, null, NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testShowPipeStmt() {
        ShowPipeStmt stmt = new ShowPipeStmt(null, null, null, java.util.Collections.emptyList(), null, NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.FORWARD_NO_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testDescPipeStmt() {
        DescPipeStmt stmt = new DescPipeStmt(NodePosition.ZERO, null);
        Assertions.assertEquals(RedirectStatus.FORWARD_NO_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testCreateSecurityIntegrationStatement() {
        CreateSecurityIntegrationStatement stmt = new CreateSecurityIntegrationStatement(null, null, null);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testAlterSecurityIntegrationStatement() {
        AlterSecurityIntegrationStatement stmt = new AlterSecurityIntegrationStatement(null, null);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testDropSecurityIntegrationStatement() {
        DropSecurityIntegrationStatement stmt = new DropSecurityIntegrationStatement(null, NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testShowSecurityIntegrationStatement() {
        ShowSecurityIntegrationStatement stmt = new ShowSecurityIntegrationStatement(null);
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testShowCreateSecurityIntegrationStatement() {
        ShowCreateSecurityIntegrationStatement stmt = new ShowCreateSecurityIntegrationStatement(null, NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testBaseCreateAlterUserStmt() {
        // BaseCreateAlterUserStmt is abstract, testing through CreateUserStmt which extends it
        CreateUserStmt stmt = new CreateUserStmt(null, false, null, java.util.Collections.emptyList(),
                java.util.Collections.emptyMap(), NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testBaseGrantRevokeRoleStmt() {
        // BaseGrantRevokeRoleStmt is abstract, testing through GrantRoleStmt
        GrantRoleStmt stmt = new GrantRoleStmt(null, null, null);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    // Additional tests for missing statements with complex constructors - simplified
    @Test
    public void testAbstractBackupStmtCoverage() {
        BackupStmt stmt = new BackupStmt(null, "test_snapshot", null, null, null, null, false, null,
                java.util.Collections.emptyMap(), NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testAdminSetAutomatedSnapshotOffStmtCoverage() {
        AdminSetAutomatedSnapshotOffStmt stmt = new AdminSetAutomatedSnapshotOffStmt();
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testAdminSetAutomatedSnapshotOnStmtCoverage() {
        AdminSetAutomatedSnapshotOnStmt stmt = new AdminSetAutomatedSnapshotOnStmt(null);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testAlterDatabaseRenameStatementCoverage() {
        AlterDatabaseRenameStatement stmt = new AlterDatabaseRenameStatement("old_db", "new_db");
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testAlterPipeStmtCoverage() {
        AlterPipeStmt stmt = new AlterPipeStmt(NodePosition.ZERO, null, null);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testAlterSecurityIntegrationStatementCoverage() {
        AlterSecurityIntegrationStatement stmt = new AlterSecurityIntegrationStatement(null, null);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testAlterStorageVolumeStmtCoverage() {
        AlterStorageVolumeStmt stmt = new AlterStorageVolumeStmt("test_volume", java.util.Collections.emptyMap(), null,
                NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testAnalyzeProfileStmtCoverage() {
        AnalyzeProfileStmt stmt = new AnalyzeProfileStmt("test_query", java.util.Collections.emptyList(), NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testBaseCreateAlterUserStmtCoverage() {
        CreateUserStmt stmt = new CreateUserStmt(null, false, null, java.util.Collections.emptyList(),
                java.util.Collections.emptyMap(), NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testBaseGrantRevokeRoleStmtCoverage() {
        GrantRoleStmt stmt = new GrantRoleStmt(null, null, null);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testClearPlanAdvisorStmtCoverage() {
        ClearPlanAdvisorStmt stmt = new ClearPlanAdvisorStmt(null);
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testControlBaselinePlanStmtCoverage() {
        ControlBaselinePlanStmt stmt = new ControlBaselinePlanStmt(false, java.util.Collections.emptyList(), NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testCreateBaselinePlanStmtCoverage() {
        CreateBaselinePlanStmt stmt = new CreateBaselinePlanStmt(false, null, null, NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testCreateDictionaryStmtCoverage() {
        CreateDictionaryStmt stmt = new CreateDictionaryStmt("test_dict", null, java.util.Collections.emptyList(),
                java.util.Collections.emptyList(), java.util.Collections.emptyMap(), NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testCreateFileStmtCoverage() {
        CreateFileStmt stmt = new CreateFileStmt("test_file", null, null, null);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testCreateGroupProviderStmtCoverage() {
        CreateGroupProviderStmt stmt = new CreateGroupProviderStmt(null, null, null);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testCreatePipeStmtCoverage() {
        CreatePipeStmt stmt = new CreatePipeStmt(false, false, null, 1, null, java.util.Collections.emptyMap(),
                NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testCreateRepositoryStmtCoverage() {
        CreateRepositoryStmt stmt = new CreateRepositoryStmt(false, "test_repo", null, null, null);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testCreateSecurityIntegrationStatementCoverage() {
        CreateSecurityIntegrationStatement stmt = new CreateSecurityIntegrationStatement(null, null, null);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testCreateTableStmtCoverage() {
        CreateTableStmt stmt = new CreateTableStmt(false, false, null, null, null, null, null, null, null, null, null);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testDelPlanAdvisorStmtCoverage() {
        DelPlanAdvisorStmt stmt = new DelPlanAdvisorStmt(null, null);
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testDescPipeStmtCoverage() {
        DescPipeStmt stmt = new DescPipeStmt(null, null);
        Assertions.assertEquals(RedirectStatus.FORWARD_NO_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testDescStorageVolumeStmtCoverage() {
        DescStorageVolumeStmt stmt = new DescStorageVolumeStmt("test_volume", NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testDropBaselinePlanStmtCoverage() {
        DropBaselinePlanStmt stmt = new DropBaselinePlanStmt(java.util.Collections.emptyList(), NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testDropDictionaryStmtCoverage() {
        DropDictionaryStmt stmt = new DropDictionaryStmt("test_dict", false, NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testDropFileStmtCoverage() {
        DropFileStmt stmt = new DropFileStmt("test_file", null, null);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testDropGroupProviderStmtCoverage() {
        DropGroupProviderStmt stmt = new DropGroupProviderStmt("", NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testDropPipeStmtCoverage() {
        DropPipeStmt stmt = new DropPipeStmt(false, new PipeName("", ""), NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testDropRepositoryStmtCoverage() {
        DropRepositoryStmt stmt = new DropRepositoryStmt("test_repo");
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testDropSecurityIntegrationStatementCoverage() {
        DropSecurityIntegrationStatement stmt = new DropSecurityIntegrationStatement("", NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testShowBaselinePlanStmtCoverage() {
        ShowBaselinePlanStmt stmt = new ShowBaselinePlanStmt(NodePosition.ZERO, (Expr) null);
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testShowClustersStmtCoverage() {
        ShowClustersStmt stmt = new ShowClustersStmt(null, NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testShowCreateExternalCatalogStmtCoverage() {
        ShowCreateExternalCatalogStmt stmt = new ShowCreateExternalCatalogStmt("test_catalog");
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testShowCreateGroupProviderStmtCoverage() {
        ShowCreateGroupProviderStmt stmt = new ShowCreateGroupProviderStmt("test_provider", NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testShowCreateRoutineLoadStmtCoverage() {
        ShowCreateRoutineLoadStmt stmt = new ShowCreateRoutineLoadStmt(new LabelName("test_job", ""), NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testShowCreateSecurityIntegrationStatementCoverage() {
        ShowCreateSecurityIntegrationStatement stmt = new ShowCreateSecurityIntegrationStatement(null, null);
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testShowDictionaryStmtCoverage() {
        ShowDictionaryStmt stmt = new ShowDictionaryStmt("", NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testShowGroupProvidersStmtCoverage() {
        ShowGroupProvidersStmt stmt = new ShowGroupProvidersStmt(NodePosition.ZERO);
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testShowPipeStmtCoverage() {
        ShowPipeStmt stmt = new ShowPipeStmt(null, null, null, null, null, null);
        Assertions.assertEquals(RedirectStatus.FORWARD_NO_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testShowPlanAdvisorStmtCoverage() {
        ShowPlanAdvisorStmt stmt = new ShowPlanAdvisorStmt(null);
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testShowSecurityIntegrationStatementCoverage() {
        ShowSecurityIntegrationStatement stmt = new ShowSecurityIntegrationStatement(null);
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testShowStreamLoadStmtCoverage() {
        ShowStreamLoadStmt stmt = new ShowStreamLoadStmt(null, false);
        Assertions.assertEquals(RedirectStatus.FORWARD_NO_SYNC, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testAddPlanAdvisorStmtCoverage() {
        AddPlanAdvisorStmt stmt = new AddPlanAdvisorStmt(null, null);
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }

    @Test
    public void testAdminShowReplicaDistributionStmtCoverage() {
        AdminShowReplicaDistributionStmt stmt = new AdminShowReplicaDistributionStmt(null, null);
        ConnectContext ctx = new ConnectContext();
        ctx.setThreadLocalInfo();
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(stmt));
    }
}
