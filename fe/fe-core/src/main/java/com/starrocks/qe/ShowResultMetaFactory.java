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

import com.google.api.client.util.Strings;
import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ResourceGroup;
import com.starrocks.catalog.ResourceMgr;
import com.starrocks.catalog.ScalarType;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.proc.BackendsProcDir;
import com.starrocks.common.proc.BrokerProcNode;
import com.starrocks.common.proc.ComputeNodeProcDir;
import com.starrocks.common.proc.DeleteInfoProcDir;
import com.starrocks.common.proc.ExportProcNode;
import com.starrocks.common.proc.FrontendsProcNode;
import com.starrocks.common.proc.LoadProcDir;
import com.starrocks.common.proc.OptimizeProcDir;
import com.starrocks.common.proc.ProcResult;
import com.starrocks.common.proc.RollupProcDir;
import com.starrocks.common.proc.SchemaChangeProcDir;
import com.starrocks.common.proc.TransProcDir;
import com.starrocks.sql.ShowTemporaryTableStmt;
import com.starrocks.sql.ast.AdminShowConfigStmt;
import com.starrocks.sql.ast.AdminShowReplicaDistributionStmt;
import com.starrocks.sql.ast.AdminShowReplicaStatusStmt;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.DescStorageVolumeStmt;
import com.starrocks.sql.ast.DescribeStmt;
import com.starrocks.sql.ast.HelpStmt;
import com.starrocks.sql.ast.RefreshMaterializedViewStatement;
import com.starrocks.sql.ast.SetType;
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
import com.starrocks.sql.ast.ShowTransactionStmt;
import com.starrocks.sql.ast.ShowTriggersStmt;
import com.starrocks.sql.ast.ShowUserPropertyStmt;
import com.starrocks.sql.ast.ShowUserStmt;
import com.starrocks.sql.ast.ShowVariablesStmt;
import com.starrocks.sql.ast.ShowWarningStmt;
import com.starrocks.sql.ast.ShowWhiteListStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.group.ShowCreateGroupProviderStmt;
import com.starrocks.sql.ast.group.ShowGroupProvidersStmt;
import com.starrocks.sql.ast.integration.ShowSecurityIntegrationStatement;
import com.starrocks.sql.ast.pipe.DescPipeStmt;
import com.starrocks.sql.ast.pipe.ShowPipeStmt;
import com.starrocks.sql.ast.spm.ShowBaselinePlanStmt;
import com.starrocks.sql.ast.warehouse.ShowClustersStmt;
import com.starrocks.sql.ast.warehouse.ShowNodesStmt;
import com.starrocks.sql.ast.warehouse.ShowWarehousesStmt;

import java.util.List;

public class ShowResultMetaFactory implements AstVisitor<ShowResultSetMetaData, Void> {
    public ShowResultSetMetaData getMetadata(StatementBase stmt) {
        return stmt.accept(this, null);
    }

    @Override
    public ShowResultSetMetaData visitShowStatement(ShowStmt statement, Void context) {
        throw new UnsupportedOperationException("Not implemented for " + statement.getClass().getSimpleName());
    }

    @Override
    public ShowResultSetMetaData visitShowLoadStatement(ShowLoadStmt statement, Void context) {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String title : LoadProcDir.TITLE_NAMES) {
            builder.addColumn(new Column(title, ScalarType.createVarchar(30)));
        }
        return builder.build();
    }

    @Override
    public ShowResultSetMetaData visitShowAnalyzeJobStatement(ShowAnalyzeJobStmt statement, Void context) {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder()
                .addColumn(new Column("Id", ScalarType.createVarchar(60)))
                .addColumn(new Column("Catalog", ScalarType.createVarchar(60)))
                .addColumn(new Column("Database", ScalarType.createVarchar(60)))
                .addColumn(new Column("Table", ScalarType.createVarchar(60)))
                .addColumn(new Column("Columns", ScalarType.createVarchar(200)))
                .addColumn(new Column("Type", ScalarType.createVarchar(20)))
                .addColumn(new Column("Schedule", ScalarType.createVarchar(20)))
                .addColumn(new Column("Properties", ScalarType.createVarchar(200)))
                .addColumn(new Column("Status", ScalarType.createVarchar(20)))
                .addColumn(new Column("LastWorkTime", ScalarType.createVarchar(60)))
                .addColumn(new Column("Reason", ScalarType.createVarchar(100)));
        return builder.build();
    }

    @Override
    public ShowResultSetMetaData visitShowDeleteStatement(ShowDeleteStmt statement, Void context) {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String title : DeleteInfoProcDir.TITLE_NAMES) {
            builder.addColumn(new Column(title, ScalarType.createVarchar(30)));
        }
        return builder.build();
    }

    @Override
    public ShowResultSetMetaData visitShowEnginesStatement(ShowEnginesStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Engine", ScalarType.createVarchar(64)))
                .addColumn(new Column("Support", ScalarType.createVarchar(8)))
                .addColumn(new Column("Comment", ScalarType.createVarchar(80)))
                .addColumn(new Column("Transactions", ScalarType.createVarchar(3)))
                .addColumn(new Column("XA", ScalarType.createVarchar(3)))
                .addColumn(new Column("Savepoints", ScalarType.createVarchar(3)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowEventStatement(ShowEventsStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Db", ScalarType.createVarchar(20)))
                .addColumn(new Column("Name", ScalarType.createVarchar(30)))
                .addColumn(new Column("Definer", ScalarType.createVarchar(20)))
                .addColumn(new Column("Time", ScalarType.createVarchar(20)))
                .addColumn(new Column("Type", ScalarType.createVarchar(20)))
                .addColumn(new Column("Execute at", ScalarType.createVarchar(20)))
                .addColumn(new Column("Interval value", ScalarType.createVarchar(30)))
                .addColumn(new Column("Interval field", ScalarType.createVarchar(30)))
                .addColumn(new Column("Status", ScalarType.createVarchar(30)))
                .addColumn(new Column("Ends", ScalarType.createVarchar(30)))
                .addColumn(new Column("Status", ScalarType.createVarchar(30)))
                .addColumn(new Column("Originator", ScalarType.createVarchar(30)))
                .addColumn(new Column("character_set_client", ScalarType.createVarchar(30)))
                .addColumn(new Column("collation_connection", ScalarType.createVarchar(30)))
                .addColumn(new Column("Database Collation", ScalarType.createVarchar(30)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowHistogramStatsMetaStatement(ShowHistogramStatsMetaStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Database", ScalarType.createVarchar(60)))
                .addColumn(new Column("Table", ScalarType.createVarchar(60)))
                .addColumn(new Column("Column", ScalarType.createVarchar(60)))
                .addColumn(new Column("Type", ScalarType.createVarchar(20)))
                .addColumn(new Column("UpdateTime", ScalarType.createVarchar(60)))
                .addColumn(new Column("Properties", ScalarType.createVarchar(200)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowPartitionsStatement(ShowPartitionsStmt statement, Void context) {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();

        ProcResult result = null;
        try {
            result = statement.getNode().fetchResult();
        } catch (AnalysisException e) {
            return builder.build();
        }

        for (String col : result.getColumnNames()) {
            builder.addColumn(new Column(col, ScalarType.createVarchar(30)));
        }
        return builder.build();
    }

    @Override
    public ShowResultSetMetaData visitShowCreateDbStatement(ShowCreateDbStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Database", ScalarType.createVarchar(20)))
                .addColumn(new Column("Create Database", ScalarType.createVarchar(30)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowBasicStatsMetaStatement(ShowBasicStatsMetaStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Database", ScalarType.createVarchar(60)))
                .addColumn(new Column("Table", ScalarType.createVarchar(60)))
                .addColumn(new Column("Columns", ScalarType.createVarchar(200)))
                .addColumn(new Column("Type", ScalarType.createVarchar(20)))
                .addColumn(new Column("UpdateTime", ScalarType.createVarchar(60)))
                .addColumn(new Column("Properties", ScalarType.createVarchar(200)))
                .addColumn(new Column("Healthy", ScalarType.createVarchar(5)))
                .addColumn(new Column("ColumnStats", ScalarType.createVarcharType(128)))
                .addColumn(new Column("TabletStatsReportTime", ScalarType.createVarcharType(60)))
                .addColumn(new Column("TableHealthyMetrics", ScalarType.createVarcharType(128)))
                .addColumn(new Column("TableUpdateTime", ScalarType.createVarcharType(60)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowAuthorStatement(ShowAuthorStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Name", ScalarType.createVarchar(30)))
                .addColumn(new Column("Location", ScalarType.createVarchar(30)))
                .addColumn(new Column("Comment", ScalarType.createVarchar(30)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowDictionaryStatement(ShowDictionaryStmt statement, Void context) {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String title : ShowDictionaryStmt.TITLE_NAMES) {
            builder.addColumn(new Column(title, ScalarType.createVarchar(30)));
        }
        return builder.build();
    }

    @Override
    public ShowResultSetMetaData visitShowWhiteListStatement(ShowWhiteListStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("user_name", ScalarType.createVarchar(20)))
                .addColumn(new Column("white_list", ScalarType.createVarchar(1000)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowSmallFilesStatement(ShowSmallFilesStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Id", ScalarType.createVarchar(32)))
                .addColumn(new Column("DbName", ScalarType.createVarchar(256)))
                .addColumn(new Column("GlobalStateMgr", ScalarType.createVarchar(32)))
                .addColumn(new Column("FileName", ScalarType.createVarchar(16)))
                .addColumn(new Column("FileSize", ScalarType.createVarchar(16)))
                .addColumn(new Column("IsContent", ScalarType.createVarchar(16)))
                .addColumn(new Column("MD5", ScalarType.createVarchar(16)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowRoutineLoadTaskStatement(ShowRoutineLoadTaskStmt statement, Void context) {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String title : ShowRoutineLoadTaskStmt.TITLE_NAMES) {
            builder.addColumn(new Column(title, ScalarType.createVarchar(30)));
        }
        return builder.build();
    }

    @Override
    public ShowResultSetMetaData visitShowSnapshotStatement(ShowSnapshotStmt statement, Void context) {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        if (!Strings.isNullOrEmpty(statement.getSnapshotName()) && !Strings.isNullOrEmpty(statement.getTimestamp())) {
            for (String title : ShowSnapshotStmt.SNAPSHOT_DETAIL) {
                builder.addColumn(new Column(title, ScalarType.createVarchar(30)));
            }
        } else {
            for (String title : ShowSnapshotStmt.SNAPSHOT_ALL) {
                builder.addColumn(new Column(title, ScalarType.createVarchar(30)));
            }
        }
        return builder.build();
    }

    @Override
    public ShowResultSetMetaData visitShowAuthenticationStatement(ShowAuthenticationStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("UserIdentity", ScalarType.createVarchar(100)))
                .addColumn(new Column("Password", ScalarType.createVarchar(20)))
                .addColumn(new Column("AuthPlugin", ScalarType.createVarchar(100)))
                .addColumn(new Column("UserForAuthPlugin", ScalarType.createVarchar(100)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowProfilelistStatement(ShowProfilelistStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("QueryId", ScalarType.createVarchar(48)))
                .addColumn(new Column("StartTime", ScalarType.createVarchar(16)))
                .addColumn(new Column("Time", ScalarType.createVarchar(16)))
                .addColumn(new Column("State", ScalarType.createVarchar(16)))
                .addColumn(new Column("Statement", ScalarType.createVarchar(128)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowSqlBlackListStatement(ShowSqlBlackListStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Id", ScalarType.createVarchar(20)))
                .addColumn(new Column("Forbidden SQL", ScalarType.createVarchar(100)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitAdminShowReplicaStatusStatement(AdminShowReplicaStatusStmt statement, Void context) {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String title : AdminShowReplicaStatusStmt.TITLE_NAMES) {
            builder.addColumn(new Column(title, ScalarType.createVarchar(30)));
        }
        return builder.build();
    }

    @Override
    public ShowResultSetMetaData visitHelpStatement(HelpStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("name", ScalarType.createVarchar(64)))
                .addColumn(new Column("description", ScalarType.createVarchar(1000)))
                .addColumn(new Column("example", ScalarType.createVarchar(1000)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowBackendBlackListStatement(ShowBackendBlackListStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("BackendId", ScalarType.createVarchar(20)))
                .addColumn(new Column("AddBlackListType", ScalarType.createVarchar(20)))
                .addColumn(new Column("LostConnectionTime", ScalarType.createVarchar(20)))
                .addColumn(new Column("LostConnectionNumberInPeriod", ScalarType.createVarchar(10)))
                .addColumn(new Column("CheckTimePeriod(s)", ScalarType.createVarchar(10)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowTableStatement(ShowTableStmt statement, Void context) {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        builder.addColumn(
                new Column("Tables_in_" + statement.getDb(), ScalarType.createVarchar(20)));
        if (statement.isVerbose()) {
            builder.addColumn(new Column("Table_type", ScalarType.createVarchar(20)));
        }
        return builder.build();
    }

    @Override
    public ShowResultSetMetaData visitShowTemporaryTablesStatement(ShowTemporaryTableStmt statement, Void context) {
        return visitShowTableStatement(statement, context);
    }

    @Override
    public ShowResultSetMetaData visitShowComputeNodeBlackListStatement(ShowComputeNodeBlackListStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("ComputeNodeId", ScalarType.createVarchar(20)))
                .addColumn(new Column("AddBlackListType", ScalarType.createVarchar(20)))
                .addColumn(new Column("LostConnectionTime", ScalarType.createVarchar(20)))
                .addColumn(new Column("LostConnectionNumberInPeriod", ScalarType.createVarchar(10)))
                .addColumn(new Column("CheckTimePeriod(s)", ScalarType.createVarchar(10)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowFrontendsStatement(ShowFrontendsStmt statement, Void context) {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String title : FrontendsProcNode.TITLE_NAMES) {
            builder.addColumn(new Column(title, ScalarType.createVarchar(30)));
        }
        return builder.build();
    }

    @Override
    public ShowResultSetMetaData visitShowLoadWarningsStatement(ShowLoadWarningsStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("JobId", ScalarType.createVarchar(15)))
                .addColumn(new Column("Label", ScalarType.createVarchar(15)))
                .addColumn(new Column("ErrorMsgDetail", ScalarType.createVarchar(100)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowBackendsStatement(ShowBackendsStmt statement, Void context) {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String title : BackendsProcDir.getMetadata()) {
            builder.addColumn(new Column(title, ScalarType.createVarchar(30)));
        }
        return builder.build();
    }

    @Override
    public ShowResultSetMetaData visitShowDataCacheRulesStatement(ShowDataCacheRulesStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Rule Id", ScalarType.createVarchar(20)))
                .addColumn(new Column("Catalog", ScalarType.createVarchar(30)))
                .addColumn(new Column("Database", ScalarType.createVarchar(30)))
                .addColumn(new Column("Table", ScalarType.createVarchar(30)))
                .addColumn(new Column("Priority", ScalarType.createVarchar(20)))
                .addColumn(new Column("Predicates", ScalarType.createVarchar(30)))
                .addColumn(new Column("Properties", ScalarType.createVarchar(30)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowCreateTableStatement(ShowCreateTableStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Table", ScalarType.createVarchar(20)))
                .addColumn(new Column("Create Table", ScalarType.createVarchar(30)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowRepositoriesStatement(ShowRepositoriesStmt statement, Void context) {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String title : ShowRepositoriesStmt.TITLE_NAMES) {
            builder.addColumn(new Column(title, ScalarType.createVarchar(30)));
        }
        return builder.build();
    }

    @Override
    public ShowResultSetMetaData visitShowExportStatement(ShowExportStmt statement, Void context) {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String title : ExportProcNode.TITLE_NAMES) {
            builder.addColumn(new Column(title, ScalarType.createVarchar(30)));
        }
        return builder.build();
    }

    @Override
    public ShowResultSetMetaData visitShowColumnStatement(ShowColumnStmt statement, Void context) {
        if (statement.isVerbose()) {
            return ShowResultSetMetaData.builder()
                    .addColumn(new Column("Field", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Type", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Collation", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Null", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Key", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Default", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Extra", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Privileges", ScalarType.createVarchar(80)))
                    .addColumn(new Column("Comment", ScalarType.createVarchar(20)))
                    .build();
        } else {
            return ShowResultSetMetaData.builder()
                    .addColumn(new Column("Field", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Type", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Null", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Key", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Default", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Extra", ScalarType.createVarchar(20)))
                    .build();
        }
    }

    @Override
    public ShowResultSetMetaData visitShowDynamicPartitionStatement(ShowDynamicPartitionStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("TableName", ScalarType.createVarchar(20)))
                .addColumn(new Column("Enable", ScalarType.createVarchar(20)))
                .addColumn(new Column("TimeUnit", ScalarType.createVarchar(20)))
                .addColumn(new Column("Start", ScalarType.createVarchar(20)))
                .addColumn(new Column("End", ScalarType.createVarchar(20)))
                .addColumn(new Column("Prefix", ScalarType.createVarchar(20)))
                .addColumn(new Column("Buckets", ScalarType.createVarchar(20)))
                .addColumn(new Column("ReplicationNum", ScalarType.createVarchar(20)))
                .addColumn(new Column("StartOf", ScalarType.createVarchar(20)))
                .addColumn(new Column("LastUpdateTime", ScalarType.createVarchar(20)))
                .addColumn(new Column("LastSchedulerTime", ScalarType.createVarchar(20)))
                .addColumn(new Column("State", ScalarType.createVarchar(20)))
                .addColumn(new Column("LastCreatePartitionMsg", ScalarType.createVarchar(20)))
                .addColumn(new Column("LastDropPartitionMsg", ScalarType.createVarchar(20)))
                .addColumn(new Column("InScheduler", ScalarType.createVarchar(20)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowAnalyzeStatusStatement(ShowAnalyzeStatusStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Id", ScalarType.createVarchar(60)))
                .addColumn(new Column("Database", ScalarType.createVarchar(60)))
                .addColumn(new Column("Table", ScalarType.createVarchar(60)))
                .addColumn(new Column("Columns", ScalarType.createVarchar(200)))
                .addColumn(new Column("Type", ScalarType.createVarchar(20)))
                .addColumn(new Column("Schedule", ScalarType.createVarchar(20)))
                .addColumn(new Column("Status", ScalarType.createVarchar(20)))
                .addColumn(new Column("StartTime", ScalarType.createVarchar(60)))
                .addColumn(new Column("EndTime", ScalarType.createVarchar(60)))
                .addColumn(new Column("Properties", ScalarType.createVarchar(200)))
                .addColumn(new Column("Reason", ScalarType.createVarchar(100)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowProcedureStatement(ShowProcedureStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Db", ScalarType.createVarchar(64)))
                .addColumn(new Column("Name", ScalarType.createVarchar(10)))
                .addColumn(new Column("Type", ScalarType.createVarchar(80)))
                .addColumn(new Column("Definer", ScalarType.createVarchar(64)))
                .addColumn(new Column("Modified", ScalarType.createVarchar(80)))
                .addColumn(new Column("Created", ScalarType.createVarchar(80)))
                .addColumn(new Column("Security_type", ScalarType.createVarchar(80)))
                .addColumn(new Column("Comment", ScalarType.createVarchar(80)))
                .addColumn(new Column("character_set_client", ScalarType.createVarchar(80)))
                .addColumn(new Column("collation_connection", ScalarType.createVarchar(80)))
                .addColumn(new Column("Database Collation", ScalarType.createVarchar(80)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowResourceStatement(ShowResourcesStmt statement, Void context) {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String title : ResourceMgr.RESOURCE_PROC_NODE_TITLE_NAMES) {
            builder.addColumn(new Column(title, ScalarType.createVarchar(30)));
        }
        return builder.build();
    }

    @Override
    public ShowResultSetMetaData visitShowVariablesStatement(ShowVariablesStmt statement, Void context) {
        if (statement.getType() != SetType.VERBOSE) {
            return ShowResultSetMetaData.builder()
                    .addColumn(new Column("Variable_name", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Value", ScalarType.createVarchar(20)))
                    .build();
        } else {
            return ShowResultSetMetaData.builder()
                    .addColumn(new Column("Variable_name", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Value", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Default_value", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Is_changed", ScalarType.createType(PrimitiveType.BOOLEAN)))
                    .build();
        }
    }

    @Override
    public ShowResultSetMetaData visitShowCharsetStatement(ShowCharsetStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Charset", ScalarType.createVarchar(20)))
                .addColumn(new Column("Description", ScalarType.createVarchar(20)))
                .addColumn(new Column("Default collation", ScalarType.createVarchar(20)))
                .addColumn(new Column("Maxlen", ScalarType.createVarchar(20)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitDescPipeStatement(DescPipeStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("DATABASE_ID", ScalarType.BIGINT))
                .addColumn(new Column("ID", ScalarType.BIGINT))
                .addColumn(new Column("NAME", ScalarType.createVarchar(64)))
                .addColumn(new Column("TYPE", ScalarType.createVarchar(8)))
                .addColumn(new Column("TABLE_NAME", ScalarType.createVarchar(64)))
                .addColumn(new Column("SOURCE", ScalarType.createVarcharType(128)))
                .addColumn(new Column("SQL", ScalarType.createVarcharType(128)))
                .addColumn(new Column("PROPERTIES", ScalarType.createVarchar(512)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowPipeStatement(ShowPipeStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("DATABASE_NAME", ScalarType.createVarchar(64)))
                .addColumn(new Column("PIPE_ID", ScalarType.BIGINT))
                .addColumn(new Column("PIPE_NAME", ScalarType.createVarchar(64)))
                .addColumn(new Column("STATE", ScalarType.createVarcharType(8)))
                .addColumn(new Column("TABLE_NAME", ScalarType.createVarchar(64)))
                .addColumn(new Column("LOAD_STATUS", ScalarType.createVarchar(512)))
                .addColumn(new Column("LAST_ERROR", ScalarType.createVarchar(1024)))
                .addColumn(new Column("CREATED_TIME", ScalarType.DATETIME))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowWarningStatement(ShowWarningStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Level", ScalarType.createVarchar(20)))
                .addColumn(new Column("Code", ScalarType.createVarchar(20)))
                .addColumn(new Column("Message", ScalarType.createVarchar(20)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowDataStatement(ShowDataStmt statement, Void context) {
        if (statement.getTableName() != null) {
            return ShowResultSetMetaData.builder()
                    .addColumn(new Column("TableName", ScalarType.createVarchar(20)))
                    .addColumn(new Column("IndexName", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Size", ScalarType.createVarchar(30)))
                    .addColumn(new Column("ReplicaCount", ScalarType.createVarchar(20)))
                    .addColumn(new Column("RowCount", ScalarType.createVarchar(20)))
                    .build();
        } else {
            return ShowResultSetMetaData.builder()
                    .addColumn(new Column("TableName", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Size", ScalarType.createVarchar(30)))
                    .addColumn(new Column("ReplicaCount", ScalarType.createVarchar(20)))
                    .build();
        }
    }

    @Override
    public ShowResultSetMetaData visitShowResourceGroupUsageStatement(ShowResourceGroupUsageStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Name", ScalarType.createVarchar(64)))
                .addColumn(new Column("Id", ScalarType.createVarchar(64)))
                .addColumn(new Column("Backend", ScalarType.createVarchar(64)))
                .addColumn(new Column("BEInUseCpuCores", ScalarType.createVarchar(64)))
                .addColumn(new Column("BEInUseMemBytes", ScalarType.createVarchar(64)))
                .addColumn(new Column("BERunningQueries", ScalarType.createVarchar(64)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowUserStatement(ShowUserStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("User", ScalarType.createVarchar(50)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowWarehousesStatement(ShowWarehousesStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Id", ScalarType.createVarchar(20)))
                .addColumn(new Column("Name", ScalarType.createVarchar(256)))
                .addColumn(new Column("State", ScalarType.createVarchar(20)))
                .addColumn(new Column("NodeCount", ScalarType.createVarchar(20)))
                .addColumn(new Column("CurrentClusterCount", ScalarType.createVarchar(20)))
                .addColumn(new Column("MaxClusterCount", ScalarType.createVarchar(20)))
                .addColumn(new Column("StartedClusters", ScalarType.createVarchar(20)))
                .addColumn(new Column("RunningSql", ScalarType.createVarchar(20)))
                .addColumn(new Column("QueuedSql", ScalarType.createVarchar(20)))
                .addColumn(new Column("CreatedOn", ScalarType.createVarchar(20)))
                .addColumn(new Column("ResumedOn", ScalarType.createVarchar(20)))
                .addColumn(new Column("UpdatedOn", ScalarType.createVarchar(20)))
                .addColumn(new Column("Property", ScalarType.createVarchar(256)))
                .addColumn(new Column("Comment", ScalarType.createVarchar(256)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowBrokerStatement(ShowBrokerStmt statement, Void context) {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String title : BrokerProcNode.BROKER_PROC_NODE_TITLE_NAMES) {
            builder.addColumn(new Column(title, ScalarType.createVarchar(30)));
        }
        return builder.build();
    }

    @Override
    public ShowResultSetMetaData visitShowRunningQueriesStatement(ShowRunningQueriesStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("QueryId", ScalarType.createVarchar(64)))
                .addColumn(new Column("WarehouseId", ScalarType.createVarchar(64)))
                .addColumn(new Column("ResourceGroupId", ScalarType.createVarchar(64)))
                .addColumn(new Column("StartTime", ScalarType.createVarchar(64)))
                .addColumn(new Column("PendingTimeout", ScalarType.createVarchar(64)))
                .addColumn(new Column("QueryTimeout", ScalarType.createVarchar(64)))
                .addColumn(new Column("State", ScalarType.createVarchar(64)))
                .addColumn(new Column("Slots", ScalarType.createVarchar(64)))
                .addColumn(new Column("Fragments", ScalarType.createVarchar(64)))
                .addColumn(new Column("DOP", ScalarType.createVarchar(64)))
                .addColumn(new Column("Frontend", ScalarType.createVarchar(64)))
                .addColumn(new Column("FeStartTime", ScalarType.createVarchar(64)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowCreateRoutineLoadStatement(ShowCreateRoutineLoadStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Job", ScalarType.createVarchar(20)))
                .addColumn(new Column("Create Job", ScalarType.createVarchar(30)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitAdminShowConfigStatement(AdminShowConfigStmt statement, Void context) {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String title : AdminShowConfigStmt.TITLE_NAMES) {
            builder.addColumn(new Column(title, ScalarType.createVarchar(30)));
        }
        return builder.build();
    }

    @Override
    public ShowResultSetMetaData visitShowPluginsStatement(ShowPluginsStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Name", ScalarType.createVarchar(64)))
                .addColumn(new Column("Type", ScalarType.createVarchar(10)))
                .addColumn(new Column("Description", ScalarType.createVarchar(200)))
                .addColumn(new Column("Version", ScalarType.createVarchar(20)))
                .addColumn(new Column("JavaVersion", ScalarType.createVarchar(20)))
                .addColumn(new Column("ClassName", ScalarType.createVarchar(64)))
                .addColumn(new Column("SoName", ScalarType.createVarchar(64)))
                .addColumn(new Column("Sources", ScalarType.createVarchar(200)))
                .addColumn(new Column("Status", ScalarType.createVarchar(20)))
                .addColumn(new Column("Properties", ScalarType.createVarchar(250)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitDescStorageVolumeStatement(DescStorageVolumeStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Name", ScalarType.createVarchar(256)))
                .addColumn(new Column("Type", ScalarType.createVarchar(20)))
                .addColumn(new Column("IsDefault", ScalarType.createVarchar(20)))
                .addColumn(new Column("Location", ScalarType.createVarchar(20)))
                .addColumn(new Column("Params", ScalarType.createVarchar(256)))
                .addColumn(new Column("Enabled", ScalarType.createVarchar(256)))
                .addColumn(new Column("Comment", ScalarType.createVarchar(20)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowMaterializedViewStatement(ShowMaterializedViewsStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .column("id", ScalarType.createType(PrimitiveType.BIGINT))
                .column("database_name", ScalarType.createVarchar(20))
                .column("name", ScalarType.createVarchar(50))
                .column("refresh_type", ScalarType.createVarchar(10))
                .column("is_active", ScalarType.createVarchar(10))
                .column("inactive_reason", ScalarType.createVarcharType(64))
                .column("partition_type", ScalarType.createVarchar(16))
                .column("task_id", ScalarType.createType(PrimitiveType.BIGINT))
                .column("task_name", ScalarType.createVarchar(50))
                .column("last_refresh_start_time", ScalarType.createType(PrimitiveType.DATETIME))
                .column("last_refresh_finished_time", ScalarType.createType(PrimitiveType.DATETIME))
                .column("last_refresh_duration", ScalarType.createType(PrimitiveType.DOUBLE))
                .column("last_refresh_state", ScalarType.createVarchar(20))
                .column("last_refresh_force_refresh", ScalarType.createVarchar(8))
                .column("last_refresh_start_partition", ScalarType.createVarchar(1024))
                .column("last_refresh_end_partition", ScalarType.createVarchar(1024))
                .column("last_refresh_base_refresh_partitions", ScalarType.createVarchar(1024))
                .column("last_refresh_mv_refresh_partitions", ScalarType.createVarchar(1024))
                .column("last_refresh_error_code", ScalarType.createVarchar(20))
                .column("last_refresh_error_message", ScalarType.createVarchar(1024))
                .column("rows", ScalarType.createType(PrimitiveType.BIGINT))
                .column("text", ScalarType.createVarchar(1024))
                .column("extra_message", ScalarType.createVarchar(1024))
                .column("query_rewrite_status", ScalarType.createVarchar(64))
                .column("creator", ScalarType.createVarchar(64))
                .column("last_refresh_process_time", ScalarType.createType(PrimitiveType.DATETIME))
                .column("last_refresh_job_id", ScalarType.createVarchar(64))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowProcStmt(ShowProcStmt statement, Void context) {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        try {
            ProcResult result = statement.getNode().fetchResult();
            for (String col : result.getColumnNames()) {
                builder.addColumn(new Column(col, ScalarType.createVarchar(30)));
            }
        } catch (AnalysisException e) {
            // Return empty builder if fetch fails
        }
        return builder.build();
    }

    @Override
    public ShowResultSetMetaData visitShowDatabasesStatement(ShowDbStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Database", ScalarType.createVarchar(64)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowTableStatusStatement(ShowTableStatusStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Name", ScalarType.createVarchar(64)))
                .addColumn(new Column("Engine", ScalarType.createVarchar(10)))
                .addColumn(new Column("Version", ScalarType.createType(PrimitiveType.BIGINT)))
                .addColumn(new Column("Row_format", ScalarType.createVarchar(64)))
                .addColumn(new Column("Rows", ScalarType.createType(PrimitiveType.BIGINT)))
                .addColumn(new Column("Avg_row_length", ScalarType.createType(PrimitiveType.BIGINT)))
                .addColumn(new Column("Data_length", ScalarType.createType(PrimitiveType.BIGINT)))
                .addColumn(new Column("Max_data_length", ScalarType.createType(PrimitiveType.BIGINT)))
                .addColumn(new Column("Index_length", ScalarType.createType(PrimitiveType.BIGINT)))
                .addColumn(new Column("Data_free", ScalarType.createType(PrimitiveType.BIGINT)))
                .addColumn(new Column("Auto_increment", ScalarType.createType(PrimitiveType.BIGINT)))
                .addColumn(new Column("Create_time", ScalarType.createType(PrimitiveType.DATETIME)))
                .addColumn(new Column("Update_time", ScalarType.createType(PrimitiveType.DATETIME)))
                .addColumn(new Column("Check_time", ScalarType.createType(PrimitiveType.DATETIME)))
                .addColumn(new Column("Collation", ScalarType.createVarchar(64)))
                .addColumn(new Column("Checksum", ScalarType.createType(PrimitiveType.BIGINT)))
                .addColumn(new Column("Create_options", ScalarType.createVarchar(64)))
                .addColumn(new Column("Comment", ScalarType.createVarchar(64)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitDescTableStmt(DescribeStmt statement, Void context) {
        if (statement.isTableFunctionTable()) {
            return ShowResultSetMetaData.builder()
                    .addColumn(new Column("Field", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Type", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Null", ScalarType.createVarchar(10)))
                    .build();
        }

        if (!statement.isAllTables()) {
            if (statement.isMaterializedView()) {
                return ShowResultSetMetaData.builder()
                        .addColumn(new Column("Field", ScalarType.createVarchar(20)))
                        .addColumn(new Column("Type", ScalarType.createVarchar(20)))
                        .addColumn(new Column("Null", ScalarType.createVarchar(10)))
                        .addColumn(new Column("Key", ScalarType.createVarchar(10)))
                        .addColumn(new Column("Default", ScalarType.createVarchar(30)))
                        .addColumn(new Column("Extra", ScalarType.createVarchar(30)))
                        .build();
            } else {
                ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
                try {
                    ProcResult result = statement.getNode().fetchResult();
                    for (String col : result.getColumnNames()) {
                        builder.addColumn(new Column(col, ScalarType.createVarchar(30)));
                    }
                } catch (AnalysisException e) {
                    // Return empty builder if fetch fails
                }
                return builder.build();
            }
        } else {
            if (statement.isOlapTable()) {
                return ShowResultSetMetaData.builder()
                        .addColumn(new Column("IndexName", ScalarType.createVarchar(20)))
                        .addColumn(new Column("IndexKeysType", ScalarType.createVarchar(20)))
                        .addColumn(new Column("Field", ScalarType.createVarchar(20)))
                        .addColumn(new Column("Type", ScalarType.createVarchar(20)))
                        .addColumn(new Column("Null", ScalarType.createVarchar(10)))
                        .addColumn(new Column("Key", ScalarType.createVarchar(10)))
                        .addColumn(new Column("Default", ScalarType.createVarchar(30)))
                        .addColumn(new Column("Extra", ScalarType.createVarchar(30)))
                        .build();
            } else if (statement.isMaterializedView()) {
                return ShowResultSetMetaData.builder()
                        .addColumn(new Column("Field", ScalarType.createVarchar(20)))
                        .addColumn(new Column("Type", ScalarType.createVarchar(20)))
                        .addColumn(new Column("Null", ScalarType.createVarchar(10)))
                        .addColumn(new Column("Key", ScalarType.createVarchar(10)))
                        .addColumn(new Column("Default", ScalarType.createVarchar(30)))
                        .addColumn(new Column("Extra", ScalarType.createVarchar(30)))
                        .build();
            } else {
                return ShowResultSetMetaData.builder()
                        .addColumn(new Column("Host", ScalarType.createVarchar(30)))
                        .addColumn(new Column("Port", ScalarType.createVarchar(10)))
                        .addColumn(new Column("User", ScalarType.createVarchar(30)))
                        .addColumn(new Column("Password", ScalarType.createVarchar(30)))
                        .addColumn(new Column("Database", ScalarType.createVarchar(30)))
                        .addColumn(new Column("Table", ScalarType.createVarchar(30)))
                        .build();
            }
        }
    }

    @Override
    public ShowResultSetMetaData visitShowProcesslistStatement(ShowProcesslistStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("ServerName", ScalarType.createVarchar(64)))
                .addColumn(new Column("Id", ScalarType.createType(PrimitiveType.BIGINT)))
                .addColumn(new Column("User", ScalarType.createVarchar(16)))
                .addColumn(new Column("Host", ScalarType.createVarchar(16)))
                .addColumn(new Column("Db", ScalarType.createVarchar(16)))
                .addColumn(new Column("Command", ScalarType.createVarchar(16)))
                .addColumn(new Column("ConnectionStartTime", ScalarType.createVarchar(16)))
                .addColumn(new Column("Time", ScalarType.createType(PrimitiveType.INT)))
                .addColumn(new Column("State", ScalarType.createVarchar(64)))
                .addColumn(new Column("Info", ScalarType.createVarchar(32 * 1024)))
                .addColumn(new Column("IsPending", ScalarType.createVarchar(16)))
                .addColumn(new Column("Warehouse", ScalarType.createVarchar(20)))
                .addColumn(new Column("CNGroup", ScalarType.createVarchar(64)))
                .addColumn(new Column("Catalog", ScalarType.createVarchar(64)))
                .addColumn(new Column("QueryId", ScalarType.createVarchar(64)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowFunctionsStatement(ShowFunctionsStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Signature", ScalarType.createVarchar(256)))
                .addColumn(new Column("Return Type", ScalarType.createVarchar(32)))
                .addColumn(new Column("Function Type", ScalarType.createVarchar(16)))
                .addColumn(new Column("Intermediate Type", ScalarType.createVarchar(16)))
                .addColumn(new Column("Properties", ScalarType.createVarchar(16)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowRoutineLoadStatement(ShowRoutineLoadStmt statement, Void context) {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();

        for (String title : ShowRoutineLoadStmt.TITLE_NAMES) {
            builder.addColumn(new Column(title, ScalarType.createVarchar(30)));
        }
        return builder.build();
    }

    @Override
    public ShowResultSetMetaData visitShowStreamLoadStatement(ShowStreamLoadStmt statement, Void context) {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();

        for (String title : ShowStreamLoadStmt.TITLE_NAMES) {
            builder.addColumn(new Column(title, ScalarType.createVarchar(30)));
        }
        return builder.build();
    }

    @Override
    public ShowResultSetMetaData visitShowAlterStatement(ShowAlterStmt statement, Void context) {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();

        ShowAlterStmt.AlterType type = statement.getType();
        ImmutableList<String> titleNames = null;
        if (type == ShowAlterStmt.AlterType.ROLLUP || type == ShowAlterStmt.AlterType.MATERIALIZED_VIEW) {
            titleNames = RollupProcDir.TITLE_NAMES;
        } else if (type == ShowAlterStmt.AlterType.COLUMN) {
            titleNames = SchemaChangeProcDir.TITLE_NAMES;
        } else if (type == ShowAlterStmt.AlterType.OPTIMIZE) {
            titleNames = OptimizeProcDir.TITLE_NAMES;
        }

        for (String title : titleNames) {
            builder.addColumn(new Column(title, ScalarType.createVarchar(30)));
        }

        return builder.build();
    }

    @Override
    public ShowResultSetMetaData visitShowUserPropertyStatement(ShowUserPropertyStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Key", ScalarType.createVarchar(64)))
                .addColumn(new Column("Value", ScalarType.createVarchar(64)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowDataDistributionStatement(ShowDataDistributionStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("PartitionName", ScalarType.createVarchar(30)))
                .addColumn(new Column("SubPartitionId", ScalarType.createVarchar(30)))
                .addColumn(new Column("MaterializedIndexName", ScalarType.createVarchar(30)))
                .addColumn(new Column("VirtualBuckets", ScalarType.createVarchar(30)))
                .addColumn(new Column("RowCount", ScalarType.createVarchar(30)))
                .addColumn(new Column("RowCount%", ScalarType.createVarchar(10)))
                .addColumn(new Column("DataSize", ScalarType.createVarchar(30)))
                .addColumn(new Column("DataSize%", ScalarType.createVarchar(10)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowCollationStatement(ShowCollationStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Collation", ScalarType.createVarchar(20)))
                .addColumn(new Column("Charset", ScalarType.createVarchar(20)))
                .addColumn(new Column("Id", ScalarType.createType(PrimitiveType.BIGINT)))
                .addColumn(new Column("Default", ScalarType.createVarchar(20)))
                .addColumn(new Column("Compiled", ScalarType.createVarchar(20)))
                .addColumn(new Column("Sortlen", ScalarType.createType(PrimitiveType.BIGINT)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowTabletStatement(ShowTabletStmt statement, Void context) {
        List<String> titleNames = statement.getTitleNames();

        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String title : titleNames) {
            builder.addColumn(new Column(title, ScalarType.createVarchar(30)));
        }
        return builder.build();
    }

    @Override
    public ShowResultSetMetaData visitShowBackupStatement(ShowBackupStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("JobId", ScalarType.createVarchar(30)))
                .addColumn(new Column("SnapshotName", ScalarType.createVarchar(30)))
                .addColumn(new Column("DbName", ScalarType.createVarchar(30)))
                .addColumn(new Column("State", ScalarType.createVarchar(30)))
                .addColumn(new Column("BackupObjs", ScalarType.createVarchar(30)))
                .addColumn(new Column("CreateTime", ScalarType.createVarchar(30)))
                .addColumn(new Column("SnapshotFinishedTime", ScalarType.createVarchar(30)))
                .addColumn(new Column("UploadFinishedTime", ScalarType.createVarchar(30)))
                .addColumn(new Column("FinishedTime", ScalarType.createVarchar(30)))
                .addColumn(new Column("UnfinishedTasks", ScalarType.createVarchar(30)))
                .addColumn(new Column("Progress", ScalarType.createVarchar(30)))
                .addColumn(new Column("TaskErrMsg", ScalarType.createVarchar(30)))
                .addColumn(new Column("Status", ScalarType.createVarchar(30)))
                .addColumn(new Column("Timeout", ScalarType.createVarchar(30)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowRestoreStatement(ShowRestoreStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("JobId", ScalarType.createVarchar(30)))
                .addColumn(new Column("Label", ScalarType.createVarchar(30)))
                .addColumn(new Column("Timestamp", ScalarType.createVarchar(30)))
                .addColumn(new Column("DbName", ScalarType.createVarchar(30)))
                .addColumn(new Column("State", ScalarType.createVarchar(30)))
                .addColumn(new Column("AllowLoad", ScalarType.createVarchar(30)))
                .addColumn(new Column("ReplicationNum", ScalarType.createVarchar(30)))
                .addColumn(new Column("RestoreObjs", ScalarType.createVarchar(30)))
                .addColumn(new Column("CreateTime", ScalarType.createVarchar(30)))
                .addColumn(new Column("MetaPreparedTime", ScalarType.createVarchar(30)))
                .addColumn(new Column("SnapshotFinishedTime", ScalarType.createVarchar(30)))
                .addColumn(new Column("DownloadFinishedTime", ScalarType.createVarchar(30)))
                .addColumn(new Column("FinishedTime", ScalarType.createVarchar(30)))
                .addColumn(new Column("UnfinishedTasks", ScalarType.createVarchar(30)))
                .addColumn(new Column("Progress", ScalarType.createVarchar(30)))
                .addColumn(new Column("TaskErrMsg", ScalarType.createVarchar(30)))
                .addColumn(new Column("Status", ScalarType.createVarchar(30)))
                .addColumn(new Column("Timeout", ScalarType.createVarchar(30)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowGrantsStatement(ShowGrantsStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("UserIdentity", ScalarType.createVarchar(100)))
                .addColumn(new Column("Catalog", ScalarType.createVarchar(400)))
                .addColumn(new Column("Grants", ScalarType.createVarchar(400)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowRolesStatement(ShowRolesStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Name", ScalarType.createVarchar(100)))
                .addColumn(new Column("Builtin", ScalarType.createVarchar(30)))
                .addColumn(new Column("Comment", ScalarType.createVarchar(300)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowSecurityIntegrationStatement(ShowSecurityIntegrationStatement statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Name", ScalarType.createVarchar(100)))
                .addColumn(new Column("Type", ScalarType.createVarchar(100)))
                .addColumn(new Column("Comment", ScalarType.createVarchar(300)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowCreateSecurityIntegrationStatement(
            com.starrocks.sql.ast.integration.ShowCreateSecurityIntegrationStatement statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Security Integration", ScalarType.createVarchar(60)))
                .addColumn(new Column("Create Security Integration", ScalarType.createVarchar(500)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowGroupProvidersStatement(ShowGroupProvidersStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Name", ScalarType.createVarchar(100)))
                .addColumn(new Column("Type", ScalarType.createVarchar(100)))
                .addColumn(new Column("Comment", ScalarType.createVarchar(300)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowCreateGroupProviderStatement(ShowCreateGroupProviderStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Group Provider", ScalarType.createVarchar(60)))
                .addColumn(new Column("Create Group Provider", ScalarType.createVarchar(500)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitAdminShowReplicaDistributionStatement(AdminShowReplicaDistributionStmt statement,
                                                                            Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("BackendId", ScalarType.createVarchar(30)))
                .addColumn(new Column("ReplicaNum", ScalarType.createVarchar(30)))
                .addColumn(new Column("Graph", ScalarType.createVarchar(30)))
                .addColumn(new Column("Percent", ScalarType.createVarchar(30)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowIndexStatement(ShowIndexStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Table", ScalarType.createVarchar(64)))
                .addColumn(new Column("Non_unique", ScalarType.createVarchar(10)))
                .addColumn(new Column("Key_name", ScalarType.createVarchar(80)))
                .addColumn(new Column("Seq_in_index", ScalarType.createVarchar(64)))
                .addColumn(new Column("Column_name", ScalarType.createVarchar(80)))
                .addColumn(new Column("Collation", ScalarType.createVarchar(80)))
                .addColumn(new Column("Cardinality", ScalarType.createVarchar(80)))
                .addColumn(new Column("Sub_part", ScalarType.createVarchar(80)))
                .addColumn(new Column("Packed", ScalarType.createVarchar(80)))
                .addColumn(new Column("Null", ScalarType.createVarchar(80)))
                .addColumn(new Column("Index_type", ScalarType.createVarchar(80)))
                .addColumn(new Column("Comment", ScalarType.createVarchar(80)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowTransactionStatement(ShowTransactionStmt statement, Void context) {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String title : TransProcDir.TITLE_NAMES) {
            builder.addColumn(new Column(title, ScalarType.createVarchar(30)));
        }
        return builder.build();
    }

    @Override
    public ShowResultSetMetaData visitShowMultiColumnsStatsMetaStatement(ShowMultiColumnStatsMetaStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Database", ScalarType.createVarchar(60)))
                .addColumn(new Column("Table", ScalarType.createVarchar(60)))
                .addColumn(new Column("Columns", ScalarType.createVarchar(200)))
                .addColumn(new Column("Type", ScalarType.createVarchar(20)))
                .addColumn(new Column("StatisticsTypes", ScalarType.createVarchar(200)))
                .addColumn(new Column("UpdateTime", ScalarType.createVarchar(60)))
                .addColumn(new Column("Properties", ScalarType.createVarchar(200)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowBaselinePlanStatement(ShowBaselinePlanStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Id", ScalarType.createVarchar(60)))
                .addColumn(new Column("global", ScalarType.createVarchar(10)))
                .addColumn(new Column("enable", ScalarType.createVarchar(10)))
                .addColumn(new Column("bindSQLDigest", ScalarType.createVarchar(65535)))
                .addColumn(new Column("bindSQLHash", ScalarType.createVarchar(60)))
                .addColumn(new Column("bindSQL", ScalarType.createVarchar(65535)))
                .addColumn(new Column("planSQL", ScalarType.createVarchar(65535)))
                .addColumn(new Column("costs", ScalarType.createVarchar(60)))
                .addColumn(new Column("queryMs", ScalarType.createVarchar(60)))
                .addColumn(new Column("source", ScalarType.createVarchar(60)))
                .addColumn(new Column("updateTime", ScalarType.createVarchar(60)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowResourceGroupStatement(ShowResourceGroupStmt statement, Void context) {
        if (statement.isVerbose()) {
            return ResourceGroup.VERBOSE_META_DATA;
        }
        return ResourceGroup.META_DATA;
    }

    @Override
    public ShowResultSetMetaData visitShowCatalogsStatement(ShowCatalogsStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Catalog", ScalarType.createVarchar(256)))
                .addColumn(new Column("Type", ScalarType.createVarchar(20)))
                .addColumn(new Column("Comment", ScalarType.createVarchar(30)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowCreateExternalCatalogStatement(ShowCreateExternalCatalogStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Catalog", ScalarType.createVarchar(20)))
                .addColumn(new Column("Create Catalog", ScalarType.createVarchar(30)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowStorageVolumesStatement(ShowStorageVolumesStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Storage Volume", ScalarType.createVarchar(256)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowFailPointStatement(ShowFailPointStatement statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Name", ScalarType.createVarchar(256)))
                .addColumn(new Column("TriggerMode", ScalarType.createVarchar(32)))
                .addColumn(new Column("Times/Probability", ScalarType.createVarchar(16)))
                .addColumn(new Column("Host", ScalarType.createVarchar(64)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowNodesStatement(ShowNodesStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("WarehouseName", ScalarType.createVarchar(256)))
                .addColumn(new Column("CNGroupId", ScalarType.createVarchar(20)))
                .addColumn(new Column("WorkerGroupId", ScalarType.createVarchar(20)))
                .addColumn(new Column("NodeId", ScalarType.createVarchar(20)))
                .addColumn(new Column("WorkerId", ScalarType.createVarchar(20)))
                .addColumn(new Column("IP", ScalarType.createVarchar(256)))
                .addColumn(new Column("HeartbeatPort", ScalarType.createVarchar(20)))
                .addColumn(new Column("BePort", ScalarType.createVarchar(20)))
                .addColumn(new Column("HttpPort", ScalarType.createVarchar(20)))
                .addColumn(new Column("BrpcPort", ScalarType.createVarchar(20)))
                .addColumn(new Column("StarletPort", ScalarType.createVarchar(20)))
                .addColumn(new Column("LastStartTime", ScalarType.createVarchar(256)))
                .addColumn(new Column("LastUpdateMs", ScalarType.createVarchar(256)))
                .addColumn(new Column("Alive", ScalarType.createVarchar(20)))
                .addColumn(new Column("ErrMsg", ScalarType.createVarchar(256)))
                .addColumn(new Column("Version", ScalarType.createVarchar(20)))
                .addColumn(new Column("NumRunningQueries", ScalarType.createVarchar(20)))
                .addColumn(new Column("CpuCores", ScalarType.createVarchar(20)))
                .addColumn(new Column("MemUsedPct", ScalarType.createVarchar(20)))
                .addColumn(new Column("CpuUsedPct", ScalarType.createVarchar(20)))
                .addColumn(new Column("CNGroupName", ScalarType.createVarchar(256)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowClusterStatement(ShowClustersStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("CNGroupId", ScalarType.createVarchar(20)))
                .addColumn(new Column("CNGroupName", ScalarType.createVarchar(256)))
                .addColumn(new Column("WorkerGroupId", ScalarType.createVarchar(20)))
                .addColumn(new Column("ComputeNodeIds", ScalarType.createVarchar(4096)))
                .addColumn(new Column("Pending", ScalarType.createVarchar(20)))
                .addColumn(new Column("Running", ScalarType.createVarchar(20)))
                .addColumn(new Column("Enabled", ScalarType.createVarchar(10)))
                .addColumn(new Column("Properties", ScalarType.createVarchar(1024)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowOpenTableStatement(ShowOpenTableStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Database", ScalarType.createVarchar(64)))
                .addColumn(new Column("Table", ScalarType.createVarchar(10)))
                .addColumn(new Column("In_use", ScalarType.createVarchar(80)))
                .addColumn(new Column("Name_locked", ScalarType.createVarchar(64)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowPrivilegeStatement(ShowPrivilegesStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Privilege", ScalarType.createVarchar(64)))
                .addColumn(new Column("Context", ScalarType.createVarchar(64)))
                .addColumn(new Column("Comment", ScalarType.createVarchar(200)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowStatusStatement(ShowStatusStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Variable_name", ScalarType.createVarchar(20)))
                .addColumn(new Column("Value", ScalarType.createVarchar(20)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowTriggersStatement(ShowTriggersStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Trigger", ScalarType.createVarchar(64)))
                .addColumn(new Column("Event", ScalarType.createVarchar(10)))
                .addColumn(new Column("Table", ScalarType.createVarchar(80)))
                .addColumn(new Column("Statement", ScalarType.createVarchar(64)))
                .addColumn(new Column("Timing", ScalarType.createVarchar(80)))
                .addColumn(new Column("Created", ScalarType.createVarchar(80)))
                .addColumn(new Column("sql_mode", ScalarType.createVarchar(80)))
                .addColumn(new Column("Definer", ScalarType.createVarchar(80)))
                .addColumn(new Column("character_set_client", ScalarType.createVarchar(80)))
                .addColumn(new Column("collation_connection", ScalarType.createVarchar(80)))
                .addColumn(new Column("Database Collation", ScalarType.createVarchar(80)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitRefreshMaterializedViewStatement(RefreshMaterializedViewStatement statement, Void context) {
        return ShowResultSetMetaData.builder().addColumn(new Column("QUERY_ID", ScalarType.createVarchar(60))).build();
    }

    @Override
    public ShowResultSetMetaData visitShowComputeNodes(ShowComputeNodesStmt statement, Void context) {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String title : ComputeNodeProcDir.getMetadata()) {
            builder.addColumn(new Column(title, ScalarType.createVarchar(30)));
        }
        return builder.build();
    }
}
