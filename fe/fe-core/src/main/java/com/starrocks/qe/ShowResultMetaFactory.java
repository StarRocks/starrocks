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
import com.starrocks.catalog.ResourceGroup;
import com.starrocks.catalog.ResourceMgr;
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
import com.starrocks.common.proc.ProcService;
import com.starrocks.common.proc.RollupProcDir;
import com.starrocks.common.proc.SchemaChangeProcDir;
import com.starrocks.common.proc.TransProcDir;
import com.starrocks.sql.ast.AdminShowConfigStmt;
import com.starrocks.sql.ast.AdminShowReplicaDistributionStmt;
import com.starrocks.sql.ast.AdminShowReplicaStatusStmt;
import com.starrocks.sql.ast.AstVisitorExtendInterface;
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
import com.starrocks.sql.ast.ShowTemporaryTableStmt;
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
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.TypeFactory;

import java.util.List;

import static com.starrocks.type.BooleanType.BOOLEAN;
import static com.starrocks.type.DateType.DATETIME;
import static com.starrocks.type.FloatType.DOUBLE;
import static com.starrocks.type.IntegerType.BIGINT;
import static com.starrocks.type.IntegerType.INT;

public class ShowResultMetaFactory implements AstVisitorExtendInterface<ShowResultSetMetaData, Void> {
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
            builder.addColumn(new Column(title, TypeFactory.createVarcharType(30)));
        }
        return builder.build();
    }

    @Override
    public ShowResultSetMetaData visitShowAnalyzeJobStatement(ShowAnalyzeJobStmt statement, Void context) {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder()
                .addColumn(new Column("Id", TypeFactory.createVarcharType(60)))
                .addColumn(new Column("Catalog", TypeFactory.createVarcharType(60)))
                .addColumn(new Column("Database", TypeFactory.createVarcharType(60)))
                .addColumn(new Column("Table", TypeFactory.createVarcharType(60)))
                .addColumn(new Column("Columns", TypeFactory.createVarcharType(200)))
                .addColumn(new Column("Type", TypeFactory.createVarcharType(20)))
                .addColumn(new Column("Schedule", TypeFactory.createVarcharType(20)))
                .addColumn(new Column("Properties", TypeFactory.createVarcharType(200)))
                .addColumn(new Column("Status", TypeFactory.createVarcharType(20)))
                .addColumn(new Column("LastWorkTime", TypeFactory.createVarcharType(60)))
                .addColumn(new Column("Reason", TypeFactory.createVarcharType(100)));
        return builder.build();
    }

    @Override
    public ShowResultSetMetaData visitShowDeleteStatement(ShowDeleteStmt statement, Void context) {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String title : DeleteInfoProcDir.TITLE_NAMES) {
            builder.addColumn(new Column(title, TypeFactory.createVarcharType(30)));
        }
        return builder.build();
    }

    @Override
    public ShowResultSetMetaData visitShowEnginesStatement(ShowEnginesStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Engine", TypeFactory.createVarcharType(64)))
                .addColumn(new Column("Support", TypeFactory.createVarcharType(8)))
                .addColumn(new Column("Comment", TypeFactory.createVarcharType(80)))
                .addColumn(new Column("Transactions", TypeFactory.createVarcharType(3)))
                .addColumn(new Column("XA", TypeFactory.createVarcharType(3)))
                .addColumn(new Column("Savepoints", TypeFactory.createVarcharType(3)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowEventStatement(ShowEventsStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Db", TypeFactory.createVarcharType(20)))
                .addColumn(new Column("Name", TypeFactory.createVarcharType(30)))
                .addColumn(new Column("Definer", TypeFactory.createVarcharType(20)))
                .addColumn(new Column("Time", TypeFactory.createVarcharType(20)))
                .addColumn(new Column("Type", TypeFactory.createVarcharType(20)))
                .addColumn(new Column("Execute at", TypeFactory.createVarcharType(20)))
                .addColumn(new Column("Interval value", TypeFactory.createVarcharType(30)))
                .addColumn(new Column("Interval field", TypeFactory.createVarcharType(30)))
                .addColumn(new Column("Status", TypeFactory.createVarcharType(30)))
                .addColumn(new Column("Ends", TypeFactory.createVarcharType(30)))
                .addColumn(new Column("Status", TypeFactory.createVarcharType(30)))
                .addColumn(new Column("Originator", TypeFactory.createVarcharType(30)))
                .addColumn(new Column("character_set_client", TypeFactory.createVarcharType(30)))
                .addColumn(new Column("collation_connection", TypeFactory.createVarcharType(30)))
                .addColumn(new Column("Database Collation", TypeFactory.createVarcharType(30)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowHistogramStatsMetaStatement(ShowHistogramStatsMetaStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Database", TypeFactory.createVarcharType(60)))
                .addColumn(new Column("Table", TypeFactory.createVarcharType(60)))
                .addColumn(new Column("Column", TypeFactory.createVarcharType(60)))
                .addColumn(new Column("Type", TypeFactory.createVarcharType(20)))
                .addColumn(new Column("UpdateTime", TypeFactory.createVarcharType(60)))
                .addColumn(new Column("Properties", TypeFactory.createVarcharType(200)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowPartitionsStatement(ShowPartitionsStmt statement, Void context) {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();

        if (Strings.isNullOrEmpty(statement.getProcPath())) {
            return builder.build();
        }

        ProcResult result = null;
        try {
            result = ProcService.getInstance().open(statement.getProcPath()).fetchResult();
        } catch (AnalysisException e) {
            return builder.build();
        }

        for (String col : result.getColumnNames()) {
            builder.addColumn(new Column(col, TypeFactory.createVarcharType(30)));
        }
        return builder.build();
    }

    @Override
    public ShowResultSetMetaData visitShowCreateDbStatement(ShowCreateDbStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Database", TypeFactory.createVarcharType(20)))
                .addColumn(new Column("Create Database", TypeFactory.createVarcharType(30)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowBasicStatsMetaStatement(ShowBasicStatsMetaStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Database", TypeFactory.createVarcharType(60)))
                .addColumn(new Column("Table", TypeFactory.createVarcharType(60)))
                .addColumn(new Column("Columns", TypeFactory.createVarcharType(200)))
                .addColumn(new Column("Type", TypeFactory.createVarcharType(20)))
                .addColumn(new Column("UpdateTime", TypeFactory.createVarcharType(60)))
                .addColumn(new Column("Properties", TypeFactory.createVarcharType(200)))
                .addColumn(new Column("Healthy", TypeFactory.createVarcharType(5)))
                .addColumn(new Column("ColumnStats", TypeFactory.createVarcharType(128)))
                .addColumn(new Column("TabletStatsReportTime", TypeFactory.createVarcharType(60)))
                .addColumn(new Column("TableHealthyMetrics", TypeFactory.createVarcharType(128)))
                .addColumn(new Column("TableUpdateTime", TypeFactory.createVarcharType(60)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowAuthorStatement(ShowAuthorStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Name", TypeFactory.createVarcharType(30)))
                .addColumn(new Column("Location", TypeFactory.createVarcharType(30)))
                .addColumn(new Column("Comment", TypeFactory.createVarcharType(30)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowDictionaryStatement(ShowDictionaryStmt statement, Void context) {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String title : ShowDictionaryStmt.TITLE_NAMES) {
            builder.addColumn(new Column(title, TypeFactory.createVarcharType(30)));
        }
        return builder.build();
    }

    @Override
    public ShowResultSetMetaData visitShowWhiteListStatement(ShowWhiteListStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("user_name", TypeFactory.createVarcharType(20)))
                .addColumn(new Column("white_list", TypeFactory.createVarcharType(1000)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowSmallFilesStatement(ShowSmallFilesStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Id", TypeFactory.createVarcharType(32)))
                .addColumn(new Column("DbName", TypeFactory.createVarcharType(256)))
                .addColumn(new Column("GlobalStateMgr", TypeFactory.createVarcharType(32)))
                .addColumn(new Column("FileName", TypeFactory.createVarcharType(16)))
                .addColumn(new Column("FileSize", TypeFactory.createVarcharType(16)))
                .addColumn(new Column("IsContent", TypeFactory.createVarcharType(16)))
                .addColumn(new Column("MD5", TypeFactory.createVarcharType(16)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowRoutineLoadTaskStatement(ShowRoutineLoadTaskStmt statement, Void context) {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String title : ShowRoutineLoadTaskStmt.TITLE_NAMES) {
            builder.addColumn(new Column(title, TypeFactory.createVarcharType(30)));
        }
        return builder.build();
    }

    @Override
    public ShowResultSetMetaData visitShowSnapshotStatement(ShowSnapshotStmt statement, Void context) {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        if (!Strings.isNullOrEmpty(statement.getSnapshotName()) && !Strings.isNullOrEmpty(statement.getTimestamp())) {
            for (String title : ShowSnapshotStmt.SNAPSHOT_DETAIL) {
                builder.addColumn(new Column(title, TypeFactory.createVarcharType(30)));
            }
        } else {
            for (String title : ShowSnapshotStmt.SNAPSHOT_ALL) {
                builder.addColumn(new Column(title, TypeFactory.createVarcharType(30)));
            }
        }
        return builder.build();
    }

    @Override
    public ShowResultSetMetaData visitShowAuthenticationStatement(ShowAuthenticationStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("UserIdentity", TypeFactory.createVarcharType(100)))
                .addColumn(new Column("Password", TypeFactory.createVarcharType(20)))
                .addColumn(new Column("AuthPlugin", TypeFactory.createVarcharType(100)))
                .addColumn(new Column("UserForAuthPlugin", TypeFactory.createVarcharType(100)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowProfilelistStatement(ShowProfilelistStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("QueryId", TypeFactory.createVarcharType(48)))
                .addColumn(new Column("StartTime", TypeFactory.createVarcharType(16)))
                .addColumn(new Column("Time", TypeFactory.createVarcharType(16)))
                .addColumn(new Column("State", TypeFactory.createVarcharType(16)))
                .addColumn(new Column("Statement", TypeFactory.createVarcharType(128)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowSqlBlackListStatement(ShowSqlBlackListStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Id", TypeFactory.createVarcharType(20)))
                .addColumn(new Column("Forbidden SQL", TypeFactory.createVarcharType(100)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitAdminShowReplicaStatusStatement(AdminShowReplicaStatusStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("TabletId", TypeFactory.createVarcharType(30)))
                .addColumn(new Column("ReplicaId", TypeFactory.createVarcharType(30)))
                .addColumn(new Column("BackendId", TypeFactory.createVarcharType(30)))
                .addColumn(new Column("Version", TypeFactory.createVarcharType(30)))
                .addColumn(new Column("LastFailedVersion", TypeFactory.createVarcharType(30)))
                .addColumn(new Column("LastSuccessVersion", TypeFactory.createVarcharType(30)))
                .addColumn(new Column("CommittedVersion", TypeFactory.createVarcharType(30)))
                .addColumn(new Column("SchemaHash", TypeFactory.createVarcharType(30)))
                .addColumn(new Column("VersionNum", TypeFactory.createVarcharType(30)))
                .addColumn(new Column("IsBad", TypeFactory.createVarcharType(30)))
                .addColumn(new Column("IsSetBadForce", TypeFactory.createVarcharType(30)))
                .addColumn(new Column("State", TypeFactory.createVarcharType(30)))
                .addColumn(new Column("Status", TypeFactory.createVarcharType(30)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitHelpStatement(HelpStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("name", TypeFactory.createVarcharType(64)))
                .addColumn(new Column("description", TypeFactory.createVarcharType(1000)))
                .addColumn(new Column("example", TypeFactory.createVarcharType(1000)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowBackendBlackListStatement(ShowBackendBlackListStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("BackendId", TypeFactory.createVarcharType(20)))
                .addColumn(new Column("AddBlackListType", TypeFactory.createVarcharType(20)))
                .addColumn(new Column("LostConnectionTime", TypeFactory.createVarcharType(20)))
                .addColumn(new Column("LostConnectionNumberInPeriod", TypeFactory.createVarcharType(10)))
                .addColumn(new Column("CheckTimePeriod(s)", TypeFactory.createVarcharType(10)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowTableStatement(ShowTableStmt statement, Void context) {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        builder.addColumn(
                new Column("Tables_in_" + statement.getDb(), TypeFactory.createVarcharType(20)));
        if (statement.isVerbose()) {
            builder.addColumn(new Column("Table_type", TypeFactory.createVarcharType(20)));
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
                .addColumn(new Column("ComputeNodeId", TypeFactory.createVarcharType(20)))
                .addColumn(new Column("AddBlackListType", TypeFactory.createVarcharType(20)))
                .addColumn(new Column("LostConnectionTime", TypeFactory.createVarcharType(20)))
                .addColumn(new Column("LostConnectionNumberInPeriod", TypeFactory.createVarcharType(10)))
                .addColumn(new Column("CheckTimePeriod(s)", TypeFactory.createVarcharType(10)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowFrontendsStatement(ShowFrontendsStmt statement, Void context) {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String title : FrontendsProcNode.TITLE_NAMES) {
            builder.addColumn(new Column(title, TypeFactory.createVarcharType(30)));
        }
        return builder.build();
    }

    @Override
    public ShowResultSetMetaData visitShowLoadWarningsStatement(ShowLoadWarningsStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("JobId", TypeFactory.createVarcharType(15)))
                .addColumn(new Column("Label", TypeFactory.createVarcharType(15)))
                .addColumn(new Column("ErrorMsgDetail", TypeFactory.createVarcharType(100)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowBackendsStatement(ShowBackendsStmt statement, Void context) {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String title : BackendsProcDir.getMetadata()) {
            builder.addColumn(new Column(title, TypeFactory.createVarcharType(30)));
        }
        return builder.build();
    }

    @Override
    public ShowResultSetMetaData visitShowDataCacheRulesStatement(ShowDataCacheRulesStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Rule Id", TypeFactory.createVarcharType(20)))
                .addColumn(new Column("Catalog", TypeFactory.createVarcharType(30)))
                .addColumn(new Column("Database", TypeFactory.createVarcharType(30)))
                .addColumn(new Column("Table", TypeFactory.createVarcharType(30)))
                .addColumn(new Column("Priority", TypeFactory.createVarcharType(20)))
                .addColumn(new Column("Predicates", TypeFactory.createVarcharType(30)))
                .addColumn(new Column("Properties", TypeFactory.createVarcharType(30)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowCreateTableStatement(ShowCreateTableStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Table", TypeFactory.createVarcharType(20)))
                .addColumn(new Column("Create Table", TypeFactory.createVarcharType(30)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowRepositoriesStatement(ShowRepositoriesStmt statement, Void context) {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String title : ShowRepositoriesStmt.TITLE_NAMES) {
            builder.addColumn(new Column(title, TypeFactory.createVarcharType(30)));
        }
        return builder.build();
    }

    @Override
    public ShowResultSetMetaData visitShowExportStatement(ShowExportStmt statement, Void context) {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String title : ExportProcNode.TITLE_NAMES) {
            builder.addColumn(new Column(title, TypeFactory.createVarcharType(30)));
        }
        return builder.build();
    }

    @Override
    public ShowResultSetMetaData visitShowColumnStatement(ShowColumnStmt statement, Void context) {
        if (statement.isVerbose()) {
            return ShowResultSetMetaData.builder()
                    .addColumn(new Column("Field", TypeFactory.createVarcharType(20)))
                    .addColumn(new Column("Type", TypeFactory.createVarcharType(20)))
                    .addColumn(new Column("Collation", TypeFactory.createVarcharType(20)))
                    .addColumn(new Column("Null", TypeFactory.createVarcharType(20)))
                    .addColumn(new Column("Key", TypeFactory.createVarcharType(20)))
                    .addColumn(new Column("Default", TypeFactory.createVarcharType(20)))
                    .addColumn(new Column("Extra", TypeFactory.createVarcharType(20)))
                    .addColumn(new Column("Privileges", TypeFactory.createVarcharType(80)))
                    .addColumn(new Column("Comment", TypeFactory.createVarcharType(20)))
                    .build();
        } else {
            return ShowResultSetMetaData.builder()
                    .addColumn(new Column("Field", TypeFactory.createVarcharType(20)))
                    .addColumn(new Column("Type", TypeFactory.createVarcharType(20)))
                    .addColumn(new Column("Null", TypeFactory.createVarcharType(20)))
                    .addColumn(new Column("Key", TypeFactory.createVarcharType(20)))
                    .addColumn(new Column("Default", TypeFactory.createVarcharType(20)))
                    .addColumn(new Column("Extra", TypeFactory.createVarcharType(20)))
                    .build();
        }
    }

    @Override
    public ShowResultSetMetaData visitShowDynamicPartitionStatement(ShowDynamicPartitionStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("TableName", TypeFactory.createVarcharType(20)))
                .addColumn(new Column("Enable", TypeFactory.createVarcharType(20)))
                .addColumn(new Column("TimeUnit", TypeFactory.createVarcharType(20)))
                .addColumn(new Column("Start", TypeFactory.createVarcharType(20)))
                .addColumn(new Column("End", TypeFactory.createVarcharType(20)))
                .addColumn(new Column("Prefix", TypeFactory.createVarcharType(20)))
                .addColumn(new Column("Buckets", TypeFactory.createVarcharType(20)))
                .addColumn(new Column("ReplicationNum", TypeFactory.createVarcharType(20)))
                .addColumn(new Column("StartOf", TypeFactory.createVarcharType(20)))
                .addColumn(new Column("LastUpdateTime", TypeFactory.createVarcharType(20)))
                .addColumn(new Column("LastSchedulerTime", TypeFactory.createVarcharType(20)))
                .addColumn(new Column("State", TypeFactory.createVarcharType(20)))
                .addColumn(new Column("LastCreatePartitionMsg", TypeFactory.createVarcharType(20)))
                .addColumn(new Column("LastDropPartitionMsg", TypeFactory.createVarcharType(20)))
                .addColumn(new Column("InScheduler", TypeFactory.createVarcharType(20)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowAnalyzeStatusStatement(ShowAnalyzeStatusStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Id", TypeFactory.createVarcharType(60)))
                .addColumn(new Column("Database", TypeFactory.createVarcharType(60)))
                .addColumn(new Column("Table", TypeFactory.createVarcharType(60)))
                .addColumn(new Column("Columns", TypeFactory.createVarcharType(200)))
                .addColumn(new Column("Type", TypeFactory.createVarcharType(20)))
                .addColumn(new Column("Schedule", TypeFactory.createVarcharType(20)))
                .addColumn(new Column("Status", TypeFactory.createVarcharType(20)))
                .addColumn(new Column("StartTime", TypeFactory.createVarcharType(60)))
                .addColumn(new Column("EndTime", TypeFactory.createVarcharType(60)))
                .addColumn(new Column("Properties", TypeFactory.createVarcharType(200)))
                .addColumn(new Column("Reason", TypeFactory.createVarcharType(100)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowProcedureStatement(ShowProcedureStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Db", TypeFactory.createVarcharType(64)))
                .addColumn(new Column("Name", TypeFactory.createVarcharType(10)))
                .addColumn(new Column("Type", TypeFactory.createVarcharType(80)))
                .addColumn(new Column("Definer", TypeFactory.createVarcharType(64)))
                .addColumn(new Column("Modified", TypeFactory.createVarcharType(80)))
                .addColumn(new Column("Created", TypeFactory.createVarcharType(80)))
                .addColumn(new Column("Security_type", TypeFactory.createVarcharType(80)))
                .addColumn(new Column("Comment", TypeFactory.createVarcharType(80)))
                .addColumn(new Column("character_set_client", TypeFactory.createVarcharType(80)))
                .addColumn(new Column("collation_connection", TypeFactory.createVarcharType(80)))
                .addColumn(new Column("Database Collation", TypeFactory.createVarcharType(80)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowResourceStatement(ShowResourcesStmt statement, Void context) {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String title : ResourceMgr.RESOURCE_PROC_NODE_TITLE_NAMES) {
            builder.addColumn(new Column(title, TypeFactory.createVarcharType(30)));
        }
        return builder.build();
    }

    @Override
    public ShowResultSetMetaData visitShowVariablesStatement(ShowVariablesStmt statement, Void context) {
        if (statement.getType() != SetType.VERBOSE) {
            return ShowResultSetMetaData.builder()
                    .addColumn(new Column("Variable_name", TypeFactory.createVarcharType(20)))
                    .addColumn(new Column("Value", TypeFactory.createVarcharType(20)))
                    .build();
        } else {
            return ShowResultSetMetaData.builder()
                    .addColumn(new Column("Variable_name", TypeFactory.createVarcharType(20)))
                    .addColumn(new Column("Value", TypeFactory.createVarcharType(20)))
                    .addColumn(new Column("Default_value", TypeFactory.createVarcharType(20)))
                    .addColumn(new Column("Is_changed", BOOLEAN))
                    .build();
        }
    }

    @Override
    public ShowResultSetMetaData visitShowCharsetStatement(ShowCharsetStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Charset", TypeFactory.createVarcharType(20)))
                .addColumn(new Column("Description", TypeFactory.createVarcharType(20)))
                .addColumn(new Column("Default collation", TypeFactory.createVarcharType(20)))
                .addColumn(new Column("Maxlen", TypeFactory.createVarcharType(20)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitDescPipeStatement(DescPipeStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("DATABASE_ID", IntegerType.BIGINT))
                .addColumn(new Column("ID", IntegerType.BIGINT))
                .addColumn(new Column("NAME", TypeFactory.createVarcharType(64)))
                .addColumn(new Column("TYPE", TypeFactory.createVarcharType(8)))
                .addColumn(new Column("TABLE_NAME", TypeFactory.createVarcharType(64)))
                .addColumn(new Column("SOURCE", TypeFactory.createVarcharType(128)))
                .addColumn(new Column("SQL", TypeFactory.createVarcharType(128)))
                .addColumn(new Column("PROPERTIES", TypeFactory.createVarcharType(512)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowPipeStatement(ShowPipeStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("DATABASE_NAME", TypeFactory.createVarcharType(64)))
                .addColumn(new Column("PIPE_ID", IntegerType.BIGINT))
                .addColumn(new Column("PIPE_NAME", TypeFactory.createVarcharType(64)))
                .addColumn(new Column("STATE", TypeFactory.createVarcharType(8)))
                .addColumn(new Column("TABLE_NAME", TypeFactory.createVarcharType(64)))
                .addColumn(new Column("LOAD_STATUS", TypeFactory.createVarcharType(512)))
                .addColumn(new Column("LAST_ERROR", TypeFactory.createVarcharType(1024)))
                .addColumn(new Column("CREATED_TIME", DateType.DATETIME))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowWarningStatement(ShowWarningStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Level", TypeFactory.createVarcharType(20)))
                .addColumn(new Column("Code", TypeFactory.createVarcharType(20)))
                .addColumn(new Column("Message", TypeFactory.createVarcharType(20)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowDataStatement(ShowDataStmt statement, Void context) {
        if (statement.getTableName() != null) {
            return ShowResultSetMetaData.builder()
                    .addColumn(new Column("TableName", TypeFactory.createVarcharType(20)))
                    .addColumn(new Column("IndexName", TypeFactory.createVarcharType(20)))
                    .addColumn(new Column("Size", TypeFactory.createVarcharType(30)))
                    .addColumn(new Column("ReplicaCount", TypeFactory.createVarcharType(20)))
                    .addColumn(new Column("RowCount", TypeFactory.createVarcharType(20)))
                    .build();
        } else {
            return ShowResultSetMetaData.builder()
                    .addColumn(new Column("TableName", TypeFactory.createVarcharType(20)))
                    .addColumn(new Column("Size", TypeFactory.createVarcharType(30)))
                    .addColumn(new Column("ReplicaCount", TypeFactory.createVarcharType(20)))
                    .build();
        }
    }

    @Override
    public ShowResultSetMetaData visitShowResourceGroupUsageStatement(ShowResourceGroupUsageStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Name", TypeFactory.createVarcharType(64)))
                .addColumn(new Column("Id", TypeFactory.createVarcharType(64)))
                .addColumn(new Column("Backend", TypeFactory.createVarcharType(64)))
                .addColumn(new Column("BEInUseCpuCores", TypeFactory.createVarcharType(64)))
                .addColumn(new Column("BEInUseMemBytes", TypeFactory.createVarcharType(64)))
                .addColumn(new Column("BERunningQueries", TypeFactory.createVarcharType(64)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowUserStatement(ShowUserStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("User", TypeFactory.createVarcharType(50)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowWarehousesStatement(ShowWarehousesStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Id", TypeFactory.createVarcharType(20)))
                .addColumn(new Column("Name", TypeFactory.createVarcharType(256)))
                .addColumn(new Column("State", TypeFactory.createVarcharType(20)))
                .addColumn(new Column("NodeCount", TypeFactory.createVarcharType(20)))
                .addColumn(new Column("CurrentClusterCount", TypeFactory.createVarcharType(20)))
                .addColumn(new Column("MaxClusterCount", TypeFactory.createVarcharType(20)))
                .addColumn(new Column("StartedClusters", TypeFactory.createVarcharType(20)))
                .addColumn(new Column("RunningSql", TypeFactory.createVarcharType(20)))
                .addColumn(new Column("QueuedSql", TypeFactory.createVarcharType(20)))
                .addColumn(new Column("CreatedOn", TypeFactory.createVarcharType(20)))
                .addColumn(new Column("ResumedOn", TypeFactory.createVarcharType(20)))
                .addColumn(new Column("UpdatedOn", TypeFactory.createVarcharType(20)))
                .addColumn(new Column("Property", TypeFactory.createVarcharType(256)))
                .addColumn(new Column("Comment", TypeFactory.createVarcharType(256)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowBrokerStatement(ShowBrokerStmt statement, Void context) {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String title : BrokerProcNode.BROKER_PROC_NODE_TITLE_NAMES) {
            builder.addColumn(new Column(title, TypeFactory.createVarcharType(30)));
        }
        return builder.build();
    }

    @Override
    public ShowResultSetMetaData visitShowRunningQueriesStatement(ShowRunningQueriesStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("QueryId", TypeFactory.createVarcharType(64)))
                .addColumn(new Column("WarehouseId", TypeFactory.createVarcharType(64)))
                .addColumn(new Column("ResourceGroupId", TypeFactory.createVarcharType(64)))
                .addColumn(new Column("StartTime", TypeFactory.createVarcharType(64)))
                .addColumn(new Column("PendingTimeout", TypeFactory.createVarcharType(64)))
                .addColumn(new Column("QueryTimeout", TypeFactory.createVarcharType(64)))
                .addColumn(new Column("State", TypeFactory.createVarcharType(64)))
                .addColumn(new Column("Slots", TypeFactory.createVarcharType(64)))
                .addColumn(new Column("Fragments", TypeFactory.createVarcharType(64)))
                .addColumn(new Column("DOP", TypeFactory.createVarcharType(64)))
                .addColumn(new Column("Frontend", TypeFactory.createVarcharType(64)))
                .addColumn(new Column("FeStartTime", TypeFactory.createVarcharType(64)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowCreateRoutineLoadStatement(ShowCreateRoutineLoadStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Job", TypeFactory.createVarcharType(20)))
                .addColumn(new Column("Create Job", TypeFactory.createVarcharType(30)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitAdminShowConfigStatement(AdminShowConfigStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Key", TypeFactory.createVarcharType(30)))
                .addColumn(new Column("AliasNames", TypeFactory.createVarcharType(30)))
                .addColumn(new Column("Value", TypeFactory.createVarcharType(30)))
                .addColumn(new Column("Type", TypeFactory.createVarcharType(30)))
                .addColumn(new Column("IsMutable", TypeFactory.createVarcharType(30)))
                .addColumn(new Column("Comment", TypeFactory.createVarcharType(30)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowPluginsStatement(ShowPluginsStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Name", TypeFactory.createVarcharType(64)))
                .addColumn(new Column("Type", TypeFactory.createVarcharType(10)))
                .addColumn(new Column("Description", TypeFactory.createVarcharType(200)))
                .addColumn(new Column("Version", TypeFactory.createVarcharType(20)))
                .addColumn(new Column("JavaVersion", TypeFactory.createVarcharType(20)))
                .addColumn(new Column("ClassName", TypeFactory.createVarcharType(64)))
                .addColumn(new Column("SoName", TypeFactory.createVarcharType(64)))
                .addColumn(new Column("Sources", TypeFactory.createVarcharType(200)))
                .addColumn(new Column("Status", TypeFactory.createVarcharType(20)))
                .addColumn(new Column("Properties", TypeFactory.createVarcharType(250)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitDescStorageVolumeStatement(DescStorageVolumeStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Name", TypeFactory.createVarcharType(256)))
                .addColumn(new Column("Type", TypeFactory.createVarcharType(20)))
                .addColumn(new Column("IsDefault", TypeFactory.createVarcharType(20)))
                .addColumn(new Column("Location", TypeFactory.createVarcharType(20)))
                .addColumn(new Column("Params", TypeFactory.createVarcharType(256)))
                .addColumn(new Column("Enabled", TypeFactory.createVarcharType(256)))
                .addColumn(new Column("Comment", TypeFactory.createVarcharType(20)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowMaterializedViewStatement(ShowMaterializedViewsStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .column("id", BIGINT)
                .column("database_name", TypeFactory.createVarcharType(20))
                .column("name", TypeFactory.createVarcharType(50))
                .column("refresh_type", TypeFactory.createVarcharType(10))
                .column("is_active", TypeFactory.createVarcharType(10))
                .column("inactive_reason", TypeFactory.createVarcharType(64))
                .column("partition_type", TypeFactory.createVarcharType(16))
                .column("task_id", BIGINT)
                .column("task_name", TypeFactory.createVarcharType(50))
                .column("last_refresh_start_time", DATETIME)
                .column("last_refresh_finished_time", DATETIME)
                .column("last_refresh_duration", DOUBLE)
                .column("last_refresh_state", TypeFactory.createVarcharType(20))
                .column("last_refresh_force_refresh", TypeFactory.createVarcharType(8))
                .column("last_refresh_start_partition", TypeFactory.createVarcharType(1024))
                .column("last_refresh_end_partition", TypeFactory.createVarcharType(1024))
                .column("last_refresh_base_refresh_partitions", TypeFactory.createVarcharType(1024))
                .column("last_refresh_mv_refresh_partitions", TypeFactory.createVarcharType(1024))
                .column("last_refresh_error_code", TypeFactory.createVarcharType(20))
                .column("last_refresh_error_message", TypeFactory.createVarcharType(1024))
                .column("rows", BIGINT)
                .column("text", TypeFactory.createVarcharType(1024))
                .column("extra_message", TypeFactory.createVarcharType(1024))
                .column("query_rewrite_status", TypeFactory.createVarcharType(64))
                .column("creator", TypeFactory.createVarcharType(64))
                .column("last_refresh_process_time", DATETIME)
                .column("last_refresh_job_id", TypeFactory.createVarcharType(64))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowProcStmt(ShowProcStmt statement, Void context) {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        if (Strings.isNullOrEmpty(statement.getPath())) {
            return builder.build();
        }
        try {
            ProcResult result = ProcService.getInstance().open(statement.getPath()).fetchResult();
            for (String col : result.getColumnNames()) {
                builder.addColumn(new Column(col, TypeFactory.createVarcharType(30)));
            }
        } catch (AnalysisException e) {
            // Return empty builder if fetch fails
        }
        return builder.build();
    }

    @Override
    public ShowResultSetMetaData visitShowDatabasesStatement(ShowDbStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Database", TypeFactory.createVarcharType(64)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowTableStatusStatement(ShowTableStatusStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Name", TypeFactory.createVarcharType(64)))
                .addColumn(new Column("Engine", TypeFactory.createVarcharType(10)))
                .addColumn(new Column("Version", BIGINT))
                .addColumn(new Column("Row_format", TypeFactory.createVarcharType(64)))
                .addColumn(new Column("Rows", BIGINT))
                .addColumn(new Column("Avg_row_length", BIGINT))
                .addColumn(new Column("Data_length", BIGINT))
                .addColumn(new Column("Max_data_length", BIGINT))
                .addColumn(new Column("Index_length", BIGINT))
                .addColumn(new Column("Data_free", BIGINT))
                .addColumn(new Column("Auto_increment", BIGINT))
                .addColumn(new Column("Create_time", DATETIME))
                .addColumn(new Column("Update_time", DATETIME))
                .addColumn(new Column("Check_time", DATETIME))
                .addColumn(new Column("Collation", TypeFactory.createVarcharType(64)))
                .addColumn(new Column("Checksum", BIGINT))
                .addColumn(new Column("Create_options", TypeFactory.createVarcharType(64)))
                .addColumn(new Column("Comment", TypeFactory.createVarcharType(64)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitDescTableStmt(DescribeStmt statement, Void context) {
        if (statement.isTableFunctionTable()) {
            return ShowResultSetMetaData.builder()
                    .addColumn(new Column("Field", TypeFactory.createVarcharType(20)))
                    .addColumn(new Column("Type", TypeFactory.createVarcharType(20)))
                    .addColumn(new Column("Null", TypeFactory.createVarcharType(10)))
                    .build();
        }

        if (!statement.isAllTables()) {
            if (statement.isMaterializedView()) {
                return ShowResultSetMetaData.builder()
                        .addColumn(new Column("Field", TypeFactory.createVarcharType(20)))
                        .addColumn(new Column("Type", TypeFactory.createVarcharType(20)))
                        .addColumn(new Column("Null", TypeFactory.createVarcharType(10)))
                        .addColumn(new Column("Key", TypeFactory.createVarcharType(10)))
                        .addColumn(new Column("Default", TypeFactory.createVarcharType(30)))
                        .addColumn(new Column("Extra", TypeFactory.createVarcharType(30)))
                        .build();
            } else {
                ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
                if (Strings.isNullOrEmpty(statement.getProcPath())) {
                    return builder.build();
                }
                try {
                    ProcResult result = ProcService.getInstance().open(statement.getProcPath()).fetchResult();
                    for (String col : result.getColumnNames()) {
                        builder.addColumn(new Column(col, TypeFactory.createVarcharType(30)));
                    }
                } catch (AnalysisException e) {
                    // Return empty builder if fetch fails
                }
                return builder.build();
            }
        } else {
            if (statement.isOlapTable()) {
                return ShowResultSetMetaData.builder()
                        .addColumn(new Column("IndexName", TypeFactory.createVarcharType(20)))
                        .addColumn(new Column("IndexKeysType", TypeFactory.createVarcharType(20)))
                        .addColumn(new Column("Field", TypeFactory.createVarcharType(20)))
                        .addColumn(new Column("Type", TypeFactory.createVarcharType(20)))
                        .addColumn(new Column("Null", TypeFactory.createVarcharType(10)))
                        .addColumn(new Column("Key", TypeFactory.createVarcharType(10)))
                        .addColumn(new Column("Default", TypeFactory.createVarcharType(30)))
                        .addColumn(new Column("Extra", TypeFactory.createVarcharType(30)))
                        .build();
            } else if (statement.isMaterializedView()) {
                return ShowResultSetMetaData.builder()
                        .addColumn(new Column("Field", TypeFactory.createVarcharType(20)))
                        .addColumn(new Column("Type", TypeFactory.createVarcharType(20)))
                        .addColumn(new Column("Null", TypeFactory.createVarcharType(10)))
                        .addColumn(new Column("Key", TypeFactory.createVarcharType(10)))
                        .addColumn(new Column("Default", TypeFactory.createVarcharType(30)))
                        .addColumn(new Column("Extra", TypeFactory.createVarcharType(30)))
                        .build();
            } else {
                return ShowResultSetMetaData.builder()
                        .addColumn(new Column("Host", TypeFactory.createVarcharType(30)))
                        .addColumn(new Column("Port", TypeFactory.createVarcharType(10)))
                        .addColumn(new Column("User", TypeFactory.createVarcharType(30)))
                        .addColumn(new Column("Password", TypeFactory.createVarcharType(30)))
                        .addColumn(new Column("Database", TypeFactory.createVarcharType(30)))
                        .addColumn(new Column("Table", TypeFactory.createVarcharType(30)))
                        .build();
            }
        }
    }

    @Override
    public ShowResultSetMetaData visitShowProcesslistStatement(ShowProcesslistStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("ServerName", TypeFactory.createVarcharType(64)))
                .addColumn(new Column("Id", BIGINT))
                .addColumn(new Column("User", TypeFactory.createVarcharType(16)))
                .addColumn(new Column("Host", TypeFactory.createVarcharType(16)))
                .addColumn(new Column("Db", TypeFactory.createVarcharType(16)))
                .addColumn(new Column("Command", TypeFactory.createVarcharType(16)))
                .addColumn(new Column("ConnectionStartTime", TypeFactory.createVarcharType(16)))
                .addColumn(new Column("Time", INT))
                .addColumn(new Column("State", TypeFactory.createVarcharType(64)))
                .addColumn(new Column("Info", TypeFactory.createVarcharType(32 * 1024)))
                .addColumn(new Column("IsPending", TypeFactory.createVarcharType(16)))
                .addColumn(new Column("Warehouse", TypeFactory.createVarcharType(20)))
                .addColumn(new Column("CNGroup", TypeFactory.createVarcharType(64)))
                .addColumn(new Column("Catalog", TypeFactory.createVarcharType(64)))
                .addColumn(new Column("QueryId", TypeFactory.createVarcharType(64)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowFunctionsStatement(ShowFunctionsStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Signature", TypeFactory.createVarcharType(256)))
                .addColumn(new Column("Return Type", TypeFactory.createVarcharType(32)))
                .addColumn(new Column("Function Type", TypeFactory.createVarcharType(16)))
                .addColumn(new Column("Intermediate Type", TypeFactory.createVarcharType(16)))
                .addColumn(new Column("Properties", TypeFactory.createVarcharType(16)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowRoutineLoadStatement(ShowRoutineLoadStmt statement, Void context) {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();

        for (String title : ShowRoutineLoadStmt.TITLE_NAMES) {
            builder.addColumn(new Column(title, TypeFactory.createVarcharType(30)));
        }
        return builder.build();
    }

    @Override
    public ShowResultSetMetaData visitShowStreamLoadStatement(ShowStreamLoadStmt statement, Void context) {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();

        for (String title : ShowStreamLoadStmt.TITLE_NAMES) {
            builder.addColumn(new Column(title, TypeFactory.createVarcharType(30)));
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
            builder.addColumn(new Column(title, TypeFactory.createVarcharType(30)));
        }

        return builder.build();
    }

    @Override
    public ShowResultSetMetaData visitShowUserPropertyStatement(ShowUserPropertyStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Key", TypeFactory.createVarcharType(64)))
                .addColumn(new Column("Value", TypeFactory.createVarcharType(64)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowDataDistributionStatement(ShowDataDistributionStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("PartitionName", TypeFactory.createVarcharType(30)))
                .addColumn(new Column("SubPartitionId", TypeFactory.createVarcharType(30)))
                .addColumn(new Column("MaterializedIndexName", TypeFactory.createVarcharType(30)))
                .addColumn(new Column("RowCount", TypeFactory.createVarcharType(30)))
                .addColumn(new Column("RowCount%", TypeFactory.createVarcharType(10)))
                .addColumn(new Column("DataSize", TypeFactory.createVarcharType(30)))
                .addColumn(new Column("DataSize%", TypeFactory.createVarcharType(10)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowCollationStatement(ShowCollationStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Collation", TypeFactory.createVarcharType(20)))
                .addColumn(new Column("Charset", TypeFactory.createVarcharType(20)))
                .addColumn(new Column("Id", BIGINT))
                .addColumn(new Column("Default", TypeFactory.createVarcharType(20)))
                .addColumn(new Column("Compiled", TypeFactory.createVarcharType(20)))
                .addColumn(new Column("Sortlen", BIGINT))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowTabletStatement(ShowTabletStmt statement, Void context) {
        List<String> titleNames = statement.getTitleNames();

        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String title : titleNames) {
            builder.addColumn(new Column(title, TypeFactory.createVarcharType(30)));
        }
        return builder.build();
    }

    @Override
    public ShowResultSetMetaData visitShowBackupStatement(ShowBackupStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("JobId", TypeFactory.createVarcharType(30)))
                .addColumn(new Column("SnapshotName", TypeFactory.createVarcharType(30)))
                .addColumn(new Column("DbName", TypeFactory.createVarcharType(30)))
                .addColumn(new Column("State", TypeFactory.createVarcharType(30)))
                .addColumn(new Column("BackupObjs", TypeFactory.createVarcharType(30)))
                .addColumn(new Column("CreateTime", TypeFactory.createVarcharType(30)))
                .addColumn(new Column("SnapshotFinishedTime", TypeFactory.createVarcharType(30)))
                .addColumn(new Column("UploadFinishedTime", TypeFactory.createVarcharType(30)))
                .addColumn(new Column("FinishedTime", TypeFactory.createVarcharType(30)))
                .addColumn(new Column("UnfinishedTasks", TypeFactory.createVarcharType(30)))
                .addColumn(new Column("Progress", TypeFactory.createVarcharType(30)))
                .addColumn(new Column("TaskErrMsg", TypeFactory.createVarcharType(30)))
                .addColumn(new Column("Status", TypeFactory.createVarcharType(30)))
                .addColumn(new Column("Timeout", TypeFactory.createVarcharType(30)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowRestoreStatement(ShowRestoreStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("JobId", TypeFactory.createVarcharType(30)))
                .addColumn(new Column("Label", TypeFactory.createVarcharType(30)))
                .addColumn(new Column("Timestamp", TypeFactory.createVarcharType(30)))
                .addColumn(new Column("DbName", TypeFactory.createVarcharType(30)))
                .addColumn(new Column("State", TypeFactory.createVarcharType(30)))
                .addColumn(new Column("AllowLoad", TypeFactory.createVarcharType(30)))
                .addColumn(new Column("ReplicationNum", TypeFactory.createVarcharType(30)))
                .addColumn(new Column("RestoreObjs", TypeFactory.createVarcharType(30)))
                .addColumn(new Column("CreateTime", TypeFactory.createVarcharType(30)))
                .addColumn(new Column("MetaPreparedTime", TypeFactory.createVarcharType(30)))
                .addColumn(new Column("SnapshotFinishedTime", TypeFactory.createVarcharType(30)))
                .addColumn(new Column("DownloadFinishedTime", TypeFactory.createVarcharType(30)))
                .addColumn(new Column("FinishedTime", TypeFactory.createVarcharType(30)))
                .addColumn(new Column("UnfinishedTasks", TypeFactory.createVarcharType(30)))
                .addColumn(new Column("Progress", TypeFactory.createVarcharType(30)))
                .addColumn(new Column("TaskErrMsg", TypeFactory.createVarcharType(30)))
                .addColumn(new Column("Status", TypeFactory.createVarcharType(30)))
                .addColumn(new Column("Timeout", TypeFactory.createVarcharType(30)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowGrantsStatement(ShowGrantsStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("UserIdentity", TypeFactory.createVarcharType(100)))
                .addColumn(new Column("Catalog", TypeFactory.createVarcharType(400)))
                .addColumn(new Column("Grants", TypeFactory.createVarcharType(400)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowRolesStatement(ShowRolesStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Name", TypeFactory.createVarcharType(100)))
                .addColumn(new Column("Builtin", TypeFactory.createVarcharType(30)))
                .addColumn(new Column("Comment", TypeFactory.createVarcharType(300)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowSecurityIntegrationStatement(ShowSecurityIntegrationStatement statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Name", TypeFactory.createVarcharType(100)))
                .addColumn(new Column("Type", TypeFactory.createVarcharType(100)))
                .addColumn(new Column("Comment", TypeFactory.createVarcharType(300)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowCreateSecurityIntegrationStatement(
            com.starrocks.sql.ast.integration.ShowCreateSecurityIntegrationStatement statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Security Integration", TypeFactory.createVarcharType(60)))
                .addColumn(new Column("Create Security Integration", TypeFactory.createVarcharType(500)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowGroupProvidersStatement(ShowGroupProvidersStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Name", TypeFactory.createVarcharType(100)))
                .addColumn(new Column("Type", TypeFactory.createVarcharType(100)))
                .addColumn(new Column("Comment", TypeFactory.createVarcharType(300)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowCreateGroupProviderStatement(ShowCreateGroupProviderStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Group Provider", TypeFactory.createVarcharType(60)))
                .addColumn(new Column("Create Group Provider", TypeFactory.createVarcharType(500)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitAdminShowReplicaDistributionStatement(AdminShowReplicaDistributionStmt statement,
                                                                            Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("BackendId", TypeFactory.createVarcharType(30)))
                .addColumn(new Column("ReplicaNum", TypeFactory.createVarcharType(30)))
                .addColumn(new Column("Graph", TypeFactory.createVarcharType(30)))
                .addColumn(new Column("Percent", TypeFactory.createVarcharType(30)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowIndexStatement(ShowIndexStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Table", TypeFactory.createVarcharType(64)))
                .addColumn(new Column("Non_unique", TypeFactory.createVarcharType(10)))
                .addColumn(new Column("Key_name", TypeFactory.createVarcharType(80)))
                .addColumn(new Column("Seq_in_index", TypeFactory.createVarcharType(64)))
                .addColumn(new Column("Column_name", TypeFactory.createVarcharType(80)))
                .addColumn(new Column("Collation", TypeFactory.createVarcharType(80)))
                .addColumn(new Column("Cardinality", TypeFactory.createVarcharType(80)))
                .addColumn(new Column("Sub_part", TypeFactory.createVarcharType(80)))
                .addColumn(new Column("Packed", TypeFactory.createVarcharType(80)))
                .addColumn(new Column("Null", TypeFactory.createVarcharType(80)))
                .addColumn(new Column("Index_type", TypeFactory.createVarcharType(80)))
                .addColumn(new Column("Comment", TypeFactory.createVarcharType(80)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowTransactionStatement(ShowTransactionStmt statement, Void context) {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String title : TransProcDir.TITLE_NAMES) {
            builder.addColumn(new Column(title, TypeFactory.createVarcharType(30)));
        }
        return builder.build();
    }

    @Override
    public ShowResultSetMetaData visitShowMultiColumnsStatsMetaStatement(ShowMultiColumnStatsMetaStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Database", TypeFactory.createVarcharType(60)))
                .addColumn(new Column("Table", TypeFactory.createVarcharType(60)))
                .addColumn(new Column("Columns", TypeFactory.createVarcharType(200)))
                .addColumn(new Column("Type", TypeFactory.createVarcharType(20)))
                .addColumn(new Column("StatisticsTypes", TypeFactory.createVarcharType(200)))
                .addColumn(new Column("UpdateTime", TypeFactory.createVarcharType(60)))
                .addColumn(new Column("Properties", TypeFactory.createVarcharType(200)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowBaselinePlanStatement(ShowBaselinePlanStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Id", TypeFactory.createVarcharType(60)))
                .addColumn(new Column("global", TypeFactory.createVarcharType(10)))
                .addColumn(new Column("enable", TypeFactory.createVarcharType(10)))
                .addColumn(new Column("bindSQLDigest", TypeFactory.createVarcharType(65535)))
                .addColumn(new Column("bindSQLHash", TypeFactory.createVarcharType(60)))
                .addColumn(new Column("bindSQL", TypeFactory.createVarcharType(65535)))
                .addColumn(new Column("planSQL", TypeFactory.createVarcharType(65535)))
                .addColumn(new Column("costs", TypeFactory.createVarcharType(60)))
                .addColumn(new Column("queryMs", TypeFactory.createVarcharType(60)))
                .addColumn(new Column("source", TypeFactory.createVarcharType(60)))
                .addColumn(new Column("updateTime", TypeFactory.createVarcharType(60)))
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
                .addColumn(new Column("Catalog", TypeFactory.createVarcharType(256)))
                .addColumn(new Column("Type", TypeFactory.createVarcharType(20)))
                .addColumn(new Column("Comment", TypeFactory.createVarcharType(30)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowCreateExternalCatalogStatement(ShowCreateExternalCatalogStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Catalog", TypeFactory.createVarcharType(20)))
                .addColumn(new Column("Create Catalog", TypeFactory.createVarcharType(30)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowStorageVolumesStatement(ShowStorageVolumesStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Storage Volume", TypeFactory.createVarcharType(256)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowFailPointStatement(ShowFailPointStatement statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Name", TypeFactory.createVarcharType(256)))
                .addColumn(new Column("TriggerMode", TypeFactory.createVarcharType(32)))
                .addColumn(new Column("Times/Probability", TypeFactory.createVarcharType(16)))
                .addColumn(new Column("Host", TypeFactory.createVarcharType(64)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowNodesStatement(ShowNodesStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("WarehouseName", TypeFactory.createVarcharType(256)))
                .addColumn(new Column("CNGroupId", TypeFactory.createVarcharType(20)))
                .addColumn(new Column("WorkerGroupId", TypeFactory.createVarcharType(20)))
                .addColumn(new Column("NodeId", TypeFactory.createVarcharType(20)))
                .addColumn(new Column("WorkerId", TypeFactory.createVarcharType(20)))
                .addColumn(new Column("IP", TypeFactory.createVarcharType(256)))
                .addColumn(new Column("HeartbeatPort", TypeFactory.createVarcharType(20)))
                .addColumn(new Column("BePort", TypeFactory.createVarcharType(20)))
                .addColumn(new Column("HttpPort", TypeFactory.createVarcharType(20)))
                .addColumn(new Column("BrpcPort", TypeFactory.createVarcharType(20)))
                .addColumn(new Column("StarletPort", TypeFactory.createVarcharType(20)))
                .addColumn(new Column("LastStartTime", TypeFactory.createVarcharType(256)))
                .addColumn(new Column("LastUpdateMs", TypeFactory.createVarcharType(256)))
                .addColumn(new Column("Alive", TypeFactory.createVarcharType(20)))
                .addColumn(new Column("ErrMsg", TypeFactory.createVarcharType(256)))
                .addColumn(new Column("Version", TypeFactory.createVarcharType(20)))
                .addColumn(new Column("NumRunningQueries", TypeFactory.createVarcharType(20)))
                .addColumn(new Column("CpuCores", TypeFactory.createVarcharType(20)))
                .addColumn(new Column("MemUsedPct", TypeFactory.createVarcharType(20)))
                .addColumn(new Column("CpuUsedPct", TypeFactory.createVarcharType(20)))
                .addColumn(new Column("CNGroupName", TypeFactory.createVarcharType(256)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowClusterStatement(ShowClustersStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("CNGroupId", TypeFactory.createVarcharType(20)))
                .addColumn(new Column("CNGroupName", TypeFactory.createVarcharType(256)))
                .addColumn(new Column("WorkerGroupId", TypeFactory.createVarcharType(20)))
                .addColumn(new Column("ComputeNodeIds", TypeFactory.createVarcharType(4096)))
                .addColumn(new Column("Pending", TypeFactory.createVarcharType(20)))
                .addColumn(new Column("Running", TypeFactory.createVarcharType(20)))
                .addColumn(new Column("Enabled", TypeFactory.createVarcharType(10)))
                .addColumn(new Column("Properties", TypeFactory.createVarcharType(1024)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowOpenTableStatement(ShowOpenTableStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Database", TypeFactory.createVarcharType(64)))
                .addColumn(new Column("Table", TypeFactory.createVarcharType(10)))
                .addColumn(new Column("In_use", TypeFactory.createVarcharType(80)))
                .addColumn(new Column("Name_locked", TypeFactory.createVarcharType(64)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowPrivilegeStatement(ShowPrivilegesStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Privilege", TypeFactory.createVarcharType(64)))
                .addColumn(new Column("Context", TypeFactory.createVarcharType(64)))
                .addColumn(new Column("Comment", TypeFactory.createVarcharType(200)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowStatusStatement(ShowStatusStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Variable_name", TypeFactory.createVarcharType(20)))
                .addColumn(new Column("Value", TypeFactory.createVarcharType(20)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowTriggersStatement(ShowTriggersStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Trigger", TypeFactory.createVarcharType(64)))
                .addColumn(new Column("Event", TypeFactory.createVarcharType(10)))
                .addColumn(new Column("Table", TypeFactory.createVarcharType(80)))
                .addColumn(new Column("Statement", TypeFactory.createVarcharType(64)))
                .addColumn(new Column("Timing", TypeFactory.createVarcharType(80)))
                .addColumn(new Column("Created", TypeFactory.createVarcharType(80)))
                .addColumn(new Column("sql_mode", TypeFactory.createVarcharType(80)))
                .addColumn(new Column("Definer", TypeFactory.createVarcharType(80)))
                .addColumn(new Column("character_set_client", TypeFactory.createVarcharType(80)))
                .addColumn(new Column("collation_connection", TypeFactory.createVarcharType(80)))
                .addColumn(new Column("Database Collation", TypeFactory.createVarcharType(80)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitRefreshMaterializedViewStatement(RefreshMaterializedViewStatement statement, Void context) {
        return ShowResultSetMetaData.builder().addColumn(new Column("QUERY_ID", TypeFactory.createVarcharType(60))).build();
    }

    @Override
    public ShowResultSetMetaData visitShowComputeNodes(ShowComputeNodesStmt statement, Void context) {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String title : ComputeNodeProcDir.getMetadata()) {
            builder.addColumn(new Column(title, TypeFactory.createVarcharType(30)));
        }
        return builder.build();
    }
}
