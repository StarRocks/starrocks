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
            builder.addColumn(new Column(title, TypeFactory.createVarchar(30)));
        }
        return builder.build();
    }

    @Override
    public ShowResultSetMetaData visitShowAnalyzeJobStatement(ShowAnalyzeJobStmt statement, Void context) {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder()
                .addColumn(new Column("Id", TypeFactory.createVarchar(60)))
                .addColumn(new Column("Catalog", TypeFactory.createVarchar(60)))
                .addColumn(new Column("Database", TypeFactory.createVarchar(60)))
                .addColumn(new Column("Table", TypeFactory.createVarchar(60)))
                .addColumn(new Column("Columns", TypeFactory.createVarchar(200)))
                .addColumn(new Column("Type", TypeFactory.createVarchar(20)))
                .addColumn(new Column("Schedule", TypeFactory.createVarchar(20)))
                .addColumn(new Column("Properties", TypeFactory.createVarchar(200)))
                .addColumn(new Column("Status", TypeFactory.createVarchar(20)))
                .addColumn(new Column("LastWorkTime", TypeFactory.createVarchar(60)))
                .addColumn(new Column("Reason", TypeFactory.createVarchar(100)));
        return builder.build();
    }

    @Override
    public ShowResultSetMetaData visitShowDeleteStatement(ShowDeleteStmt statement, Void context) {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String title : DeleteInfoProcDir.TITLE_NAMES) {
            builder.addColumn(new Column(title, TypeFactory.createVarchar(30)));
        }
        return builder.build();
    }

    @Override
    public ShowResultSetMetaData visitShowEnginesStatement(ShowEnginesStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Engine", TypeFactory.createVarchar(64)))
                .addColumn(new Column("Support", TypeFactory.createVarchar(8)))
                .addColumn(new Column("Comment", TypeFactory.createVarchar(80)))
                .addColumn(new Column("Transactions", TypeFactory.createVarchar(3)))
                .addColumn(new Column("XA", TypeFactory.createVarchar(3)))
                .addColumn(new Column("Savepoints", TypeFactory.createVarchar(3)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowEventStatement(ShowEventsStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Db", TypeFactory.createVarchar(20)))
                .addColumn(new Column("Name", TypeFactory.createVarchar(30)))
                .addColumn(new Column("Definer", TypeFactory.createVarchar(20)))
                .addColumn(new Column("Time", TypeFactory.createVarchar(20)))
                .addColumn(new Column("Type", TypeFactory.createVarchar(20)))
                .addColumn(new Column("Execute at", TypeFactory.createVarchar(20)))
                .addColumn(new Column("Interval value", TypeFactory.createVarchar(30)))
                .addColumn(new Column("Interval field", TypeFactory.createVarchar(30)))
                .addColumn(new Column("Status", TypeFactory.createVarchar(30)))
                .addColumn(new Column("Ends", TypeFactory.createVarchar(30)))
                .addColumn(new Column("Status", TypeFactory.createVarchar(30)))
                .addColumn(new Column("Originator", TypeFactory.createVarchar(30)))
                .addColumn(new Column("character_set_client", TypeFactory.createVarchar(30)))
                .addColumn(new Column("collation_connection", TypeFactory.createVarchar(30)))
                .addColumn(new Column("Database Collation", TypeFactory.createVarchar(30)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowHistogramStatsMetaStatement(ShowHistogramStatsMetaStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Database", TypeFactory.createVarchar(60)))
                .addColumn(new Column("Table", TypeFactory.createVarchar(60)))
                .addColumn(new Column("Column", TypeFactory.createVarchar(60)))
                .addColumn(new Column("Type", TypeFactory.createVarchar(20)))
                .addColumn(new Column("UpdateTime", TypeFactory.createVarchar(60)))
                .addColumn(new Column("Properties", TypeFactory.createVarchar(200)))
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
            builder.addColumn(new Column(col, TypeFactory.createVarchar(30)));
        }
        return builder.build();
    }

    @Override
    public ShowResultSetMetaData visitShowCreateDbStatement(ShowCreateDbStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Database", TypeFactory.createVarchar(20)))
                .addColumn(new Column("Create Database", TypeFactory.createVarchar(30)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowBasicStatsMetaStatement(ShowBasicStatsMetaStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Database", TypeFactory.createVarchar(60)))
                .addColumn(new Column("Table", TypeFactory.createVarchar(60)))
                .addColumn(new Column("Columns", TypeFactory.createVarchar(200)))
                .addColumn(new Column("Type", TypeFactory.createVarchar(20)))
                .addColumn(new Column("UpdateTime", TypeFactory.createVarchar(60)))
                .addColumn(new Column("Properties", TypeFactory.createVarchar(200)))
                .addColumn(new Column("Healthy", TypeFactory.createVarchar(5)))
                .addColumn(new Column("ColumnStats", TypeFactory.createVarcharType(128)))
                .addColumn(new Column("TabletStatsReportTime", TypeFactory.createVarcharType(60)))
                .addColumn(new Column("TableHealthyMetrics", TypeFactory.createVarcharType(128)))
                .addColumn(new Column("TableUpdateTime", TypeFactory.createVarcharType(60)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowAuthorStatement(ShowAuthorStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Name", TypeFactory.createVarchar(30)))
                .addColumn(new Column("Location", TypeFactory.createVarchar(30)))
                .addColumn(new Column("Comment", TypeFactory.createVarchar(30)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowDictionaryStatement(ShowDictionaryStmt statement, Void context) {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String title : ShowDictionaryStmt.TITLE_NAMES) {
            builder.addColumn(new Column(title, TypeFactory.createVarchar(30)));
        }
        return builder.build();
    }

    @Override
    public ShowResultSetMetaData visitShowWhiteListStatement(ShowWhiteListStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("user_name", TypeFactory.createVarchar(20)))
                .addColumn(new Column("white_list", TypeFactory.createVarchar(1000)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowSmallFilesStatement(ShowSmallFilesStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Id", TypeFactory.createVarchar(32)))
                .addColumn(new Column("DbName", TypeFactory.createVarchar(256)))
                .addColumn(new Column("GlobalStateMgr", TypeFactory.createVarchar(32)))
                .addColumn(new Column("FileName", TypeFactory.createVarchar(16)))
                .addColumn(new Column("FileSize", TypeFactory.createVarchar(16)))
                .addColumn(new Column("IsContent", TypeFactory.createVarchar(16)))
                .addColumn(new Column("MD5", TypeFactory.createVarchar(16)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowRoutineLoadTaskStatement(ShowRoutineLoadTaskStmt statement, Void context) {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String title : ShowRoutineLoadTaskStmt.TITLE_NAMES) {
            builder.addColumn(new Column(title, TypeFactory.createVarchar(30)));
        }
        return builder.build();
    }

    @Override
    public ShowResultSetMetaData visitShowSnapshotStatement(ShowSnapshotStmt statement, Void context) {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        if (!Strings.isNullOrEmpty(statement.getSnapshotName()) && !Strings.isNullOrEmpty(statement.getTimestamp())) {
            for (String title : ShowSnapshotStmt.SNAPSHOT_DETAIL) {
                builder.addColumn(new Column(title, TypeFactory.createVarchar(30)));
            }
        } else {
            for (String title : ShowSnapshotStmt.SNAPSHOT_ALL) {
                builder.addColumn(new Column(title, TypeFactory.createVarchar(30)));
            }
        }
        return builder.build();
    }

    @Override
    public ShowResultSetMetaData visitShowAuthenticationStatement(ShowAuthenticationStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("UserIdentity", TypeFactory.createVarchar(100)))
                .addColumn(new Column("Password", TypeFactory.createVarchar(20)))
                .addColumn(new Column("AuthPlugin", TypeFactory.createVarchar(100)))
                .addColumn(new Column("UserForAuthPlugin", TypeFactory.createVarchar(100)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowProfilelistStatement(ShowProfilelistStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("QueryId", TypeFactory.createVarchar(48)))
                .addColumn(new Column("StartTime", TypeFactory.createVarchar(16)))
                .addColumn(new Column("Time", TypeFactory.createVarchar(16)))
                .addColumn(new Column("State", TypeFactory.createVarchar(16)))
                .addColumn(new Column("Statement", TypeFactory.createVarchar(128)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowSqlBlackListStatement(ShowSqlBlackListStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Id", TypeFactory.createVarchar(20)))
                .addColumn(new Column("Forbidden SQL", TypeFactory.createVarchar(100)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitAdminShowReplicaStatusStatement(AdminShowReplicaStatusStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("TabletId", TypeFactory.createVarchar(30)))
                .addColumn(new Column("ReplicaId", TypeFactory.createVarchar(30)))
                .addColumn(new Column("BackendId", TypeFactory.createVarchar(30)))
                .addColumn(new Column("Version", TypeFactory.createVarchar(30)))
                .addColumn(new Column("LastFailedVersion", TypeFactory.createVarchar(30)))
                .addColumn(new Column("LastSuccessVersion", TypeFactory.createVarchar(30)))
                .addColumn(new Column("CommittedVersion", TypeFactory.createVarchar(30)))
                .addColumn(new Column("SchemaHash", TypeFactory.createVarchar(30)))
                .addColumn(new Column("VersionNum", TypeFactory.createVarchar(30)))
                .addColumn(new Column("IsBad", TypeFactory.createVarchar(30)))
                .addColumn(new Column("IsSetBadForce", TypeFactory.createVarchar(30)))
                .addColumn(new Column("State", TypeFactory.createVarchar(30)))
                .addColumn(new Column("Status", TypeFactory.createVarchar(30)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitHelpStatement(HelpStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("name", TypeFactory.createVarchar(64)))
                .addColumn(new Column("description", TypeFactory.createVarchar(1000)))
                .addColumn(new Column("example", TypeFactory.createVarchar(1000)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowBackendBlackListStatement(ShowBackendBlackListStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("BackendId", TypeFactory.createVarchar(20)))
                .addColumn(new Column("AddBlackListType", TypeFactory.createVarchar(20)))
                .addColumn(new Column("LostConnectionTime", TypeFactory.createVarchar(20)))
                .addColumn(new Column("LostConnectionNumberInPeriod", TypeFactory.createVarchar(10)))
                .addColumn(new Column("CheckTimePeriod(s)", TypeFactory.createVarchar(10)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowTableStatement(ShowTableStmt statement, Void context) {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        builder.addColumn(
                new Column("Tables_in_" + statement.getDb(), TypeFactory.createVarchar(20)));
        if (statement.isVerbose()) {
            builder.addColumn(new Column("Table_type", TypeFactory.createVarchar(20)));
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
                .addColumn(new Column("ComputeNodeId", TypeFactory.createVarchar(20)))
                .addColumn(new Column("AddBlackListType", TypeFactory.createVarchar(20)))
                .addColumn(new Column("LostConnectionTime", TypeFactory.createVarchar(20)))
                .addColumn(new Column("LostConnectionNumberInPeriod", TypeFactory.createVarchar(10)))
                .addColumn(new Column("CheckTimePeriod(s)", TypeFactory.createVarchar(10)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowFrontendsStatement(ShowFrontendsStmt statement, Void context) {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String title : FrontendsProcNode.TITLE_NAMES) {
            builder.addColumn(new Column(title, TypeFactory.createVarchar(30)));
        }
        return builder.build();
    }

    @Override
    public ShowResultSetMetaData visitShowLoadWarningsStatement(ShowLoadWarningsStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("JobId", TypeFactory.createVarchar(15)))
                .addColumn(new Column("Label", TypeFactory.createVarchar(15)))
                .addColumn(new Column("ErrorMsgDetail", TypeFactory.createVarchar(100)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowBackendsStatement(ShowBackendsStmt statement, Void context) {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String title : BackendsProcDir.getMetadata()) {
            builder.addColumn(new Column(title, TypeFactory.createVarchar(30)));
        }
        return builder.build();
    }

    @Override
    public ShowResultSetMetaData visitShowDataCacheRulesStatement(ShowDataCacheRulesStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Rule Id", TypeFactory.createVarchar(20)))
                .addColumn(new Column("Catalog", TypeFactory.createVarchar(30)))
                .addColumn(new Column("Database", TypeFactory.createVarchar(30)))
                .addColumn(new Column("Table", TypeFactory.createVarchar(30)))
                .addColumn(new Column("Priority", TypeFactory.createVarchar(20)))
                .addColumn(new Column("Predicates", TypeFactory.createVarchar(30)))
                .addColumn(new Column("Properties", TypeFactory.createVarchar(30)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowCreateTableStatement(ShowCreateTableStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Table", TypeFactory.createVarchar(20)))
                .addColumn(new Column("Create Table", TypeFactory.createVarchar(30)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowRepositoriesStatement(ShowRepositoriesStmt statement, Void context) {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String title : ShowRepositoriesStmt.TITLE_NAMES) {
            builder.addColumn(new Column(title, TypeFactory.createVarchar(30)));
        }
        return builder.build();
    }

    @Override
    public ShowResultSetMetaData visitShowExportStatement(ShowExportStmt statement, Void context) {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String title : ExportProcNode.TITLE_NAMES) {
            builder.addColumn(new Column(title, TypeFactory.createVarchar(30)));
        }
        return builder.build();
    }

    @Override
    public ShowResultSetMetaData visitShowColumnStatement(ShowColumnStmt statement, Void context) {
        if (statement.isVerbose()) {
            return ShowResultSetMetaData.builder()
                    .addColumn(new Column("Field", TypeFactory.createVarchar(20)))
                    .addColumn(new Column("Type", TypeFactory.createVarchar(20)))
                    .addColumn(new Column("Collation", TypeFactory.createVarchar(20)))
                    .addColumn(new Column("Null", TypeFactory.createVarchar(20)))
                    .addColumn(new Column("Key", TypeFactory.createVarchar(20)))
                    .addColumn(new Column("Default", TypeFactory.createVarchar(20)))
                    .addColumn(new Column("Extra", TypeFactory.createVarchar(20)))
                    .addColumn(new Column("Privileges", TypeFactory.createVarchar(80)))
                    .addColumn(new Column("Comment", TypeFactory.createVarchar(20)))
                    .build();
        } else {
            return ShowResultSetMetaData.builder()
                    .addColumn(new Column("Field", TypeFactory.createVarchar(20)))
                    .addColumn(new Column("Type", TypeFactory.createVarchar(20)))
                    .addColumn(new Column("Null", TypeFactory.createVarchar(20)))
                    .addColumn(new Column("Key", TypeFactory.createVarchar(20)))
                    .addColumn(new Column("Default", TypeFactory.createVarchar(20)))
                    .addColumn(new Column("Extra", TypeFactory.createVarchar(20)))
                    .build();
        }
    }

    @Override
    public ShowResultSetMetaData visitShowDynamicPartitionStatement(ShowDynamicPartitionStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("TableName", TypeFactory.createVarchar(20)))
                .addColumn(new Column("Enable", TypeFactory.createVarchar(20)))
                .addColumn(new Column("TimeUnit", TypeFactory.createVarchar(20)))
                .addColumn(new Column("Start", TypeFactory.createVarchar(20)))
                .addColumn(new Column("End", TypeFactory.createVarchar(20)))
                .addColumn(new Column("Prefix", TypeFactory.createVarchar(20)))
                .addColumn(new Column("Buckets", TypeFactory.createVarchar(20)))
                .addColumn(new Column("ReplicationNum", TypeFactory.createVarchar(20)))
                .addColumn(new Column("StartOf", TypeFactory.createVarchar(20)))
                .addColumn(new Column("LastUpdateTime", TypeFactory.createVarchar(20)))
                .addColumn(new Column("LastSchedulerTime", TypeFactory.createVarchar(20)))
                .addColumn(new Column("State", TypeFactory.createVarchar(20)))
                .addColumn(new Column("LastCreatePartitionMsg", TypeFactory.createVarchar(20)))
                .addColumn(new Column("LastDropPartitionMsg", TypeFactory.createVarchar(20)))
                .addColumn(new Column("InScheduler", TypeFactory.createVarchar(20)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowAnalyzeStatusStatement(ShowAnalyzeStatusStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Id", TypeFactory.createVarchar(60)))
                .addColumn(new Column("Database", TypeFactory.createVarchar(60)))
                .addColumn(new Column("Table", TypeFactory.createVarchar(60)))
                .addColumn(new Column("Columns", TypeFactory.createVarchar(200)))
                .addColumn(new Column("Type", TypeFactory.createVarchar(20)))
                .addColumn(new Column("Schedule", TypeFactory.createVarchar(20)))
                .addColumn(new Column("Status", TypeFactory.createVarchar(20)))
                .addColumn(new Column("StartTime", TypeFactory.createVarchar(60)))
                .addColumn(new Column("EndTime", TypeFactory.createVarchar(60)))
                .addColumn(new Column("Properties", TypeFactory.createVarchar(200)))
                .addColumn(new Column("Reason", TypeFactory.createVarchar(100)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowProcedureStatement(ShowProcedureStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Db", TypeFactory.createVarchar(64)))
                .addColumn(new Column("Name", TypeFactory.createVarchar(10)))
                .addColumn(new Column("Type", TypeFactory.createVarchar(80)))
                .addColumn(new Column("Definer", TypeFactory.createVarchar(64)))
                .addColumn(new Column("Modified", TypeFactory.createVarchar(80)))
                .addColumn(new Column("Created", TypeFactory.createVarchar(80)))
                .addColumn(new Column("Security_type", TypeFactory.createVarchar(80)))
                .addColumn(new Column("Comment", TypeFactory.createVarchar(80)))
                .addColumn(new Column("character_set_client", TypeFactory.createVarchar(80)))
                .addColumn(new Column("collation_connection", TypeFactory.createVarchar(80)))
                .addColumn(new Column("Database Collation", TypeFactory.createVarchar(80)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowResourceStatement(ShowResourcesStmt statement, Void context) {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String title : ResourceMgr.RESOURCE_PROC_NODE_TITLE_NAMES) {
            builder.addColumn(new Column(title, TypeFactory.createVarchar(30)));
        }
        return builder.build();
    }

    @Override
    public ShowResultSetMetaData visitShowVariablesStatement(ShowVariablesStmt statement, Void context) {
        if (statement.getType() != SetType.VERBOSE) {
            return ShowResultSetMetaData.builder()
                    .addColumn(new Column("Variable_name", TypeFactory.createVarchar(20)))
                    .addColumn(new Column("Value", TypeFactory.createVarchar(20)))
                    .build();
        } else {
            return ShowResultSetMetaData.builder()
                    .addColumn(new Column("Variable_name", TypeFactory.createVarchar(20)))
                    .addColumn(new Column("Value", TypeFactory.createVarchar(20)))
                    .addColumn(new Column("Default_value", TypeFactory.createVarchar(20)))
                    .addColumn(new Column("Is_changed", BOOLEAN))
                    .build();
        }
    }

    @Override
    public ShowResultSetMetaData visitShowCharsetStatement(ShowCharsetStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Charset", TypeFactory.createVarchar(20)))
                .addColumn(new Column("Description", TypeFactory.createVarchar(20)))
                .addColumn(new Column("Default collation", TypeFactory.createVarchar(20)))
                .addColumn(new Column("Maxlen", TypeFactory.createVarchar(20)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitDescPipeStatement(DescPipeStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("DATABASE_ID", IntegerType.BIGINT))
                .addColumn(new Column("ID", IntegerType.BIGINT))
                .addColumn(new Column("NAME", TypeFactory.createVarchar(64)))
                .addColumn(new Column("TYPE", TypeFactory.createVarchar(8)))
                .addColumn(new Column("TABLE_NAME", TypeFactory.createVarchar(64)))
                .addColumn(new Column("SOURCE", TypeFactory.createVarcharType(128)))
                .addColumn(new Column("SQL", TypeFactory.createVarcharType(128)))
                .addColumn(new Column("PROPERTIES", TypeFactory.createVarchar(512)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowPipeStatement(ShowPipeStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("DATABASE_NAME", TypeFactory.createVarchar(64)))
                .addColumn(new Column("PIPE_ID", IntegerType.BIGINT))
                .addColumn(new Column("PIPE_NAME", TypeFactory.createVarchar(64)))
                .addColumn(new Column("STATE", TypeFactory.createVarcharType(8)))
                .addColumn(new Column("TABLE_NAME", TypeFactory.createVarchar(64)))
                .addColumn(new Column("LOAD_STATUS", TypeFactory.createVarchar(512)))
                .addColumn(new Column("LAST_ERROR", TypeFactory.createVarchar(1024)))
                .addColumn(new Column("CREATED_TIME", DateType.DATETIME))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowWarningStatement(ShowWarningStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Level", TypeFactory.createVarchar(20)))
                .addColumn(new Column("Code", TypeFactory.createVarchar(20)))
                .addColumn(new Column("Message", TypeFactory.createVarchar(20)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowDataStatement(ShowDataStmt statement, Void context) {
        if (statement.getTableName() != null) {
            return ShowResultSetMetaData.builder()
                    .addColumn(new Column("TableName", TypeFactory.createVarchar(20)))
                    .addColumn(new Column("IndexName", TypeFactory.createVarchar(20)))
                    .addColumn(new Column("Size", TypeFactory.createVarchar(30)))
                    .addColumn(new Column("ReplicaCount", TypeFactory.createVarchar(20)))
                    .addColumn(new Column("RowCount", TypeFactory.createVarchar(20)))
                    .build();
        } else {
            return ShowResultSetMetaData.builder()
                    .addColumn(new Column("TableName", TypeFactory.createVarchar(20)))
                    .addColumn(new Column("Size", TypeFactory.createVarchar(30)))
                    .addColumn(new Column("ReplicaCount", TypeFactory.createVarchar(20)))
                    .build();
        }
    }

    @Override
    public ShowResultSetMetaData visitShowResourceGroupUsageStatement(ShowResourceGroupUsageStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Name", TypeFactory.createVarchar(64)))
                .addColumn(new Column("Id", TypeFactory.createVarchar(64)))
                .addColumn(new Column("Backend", TypeFactory.createVarchar(64)))
                .addColumn(new Column("BEInUseCpuCores", TypeFactory.createVarchar(64)))
                .addColumn(new Column("BEInUseMemBytes", TypeFactory.createVarchar(64)))
                .addColumn(new Column("BERunningQueries", TypeFactory.createVarchar(64)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowUserStatement(ShowUserStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("User", TypeFactory.createVarchar(50)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowWarehousesStatement(ShowWarehousesStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Id", TypeFactory.createVarchar(20)))
                .addColumn(new Column("Name", TypeFactory.createVarchar(256)))
                .addColumn(new Column("State", TypeFactory.createVarchar(20)))
                .addColumn(new Column("NodeCount", TypeFactory.createVarchar(20)))
                .addColumn(new Column("CurrentClusterCount", TypeFactory.createVarchar(20)))
                .addColumn(new Column("MaxClusterCount", TypeFactory.createVarchar(20)))
                .addColumn(new Column("StartedClusters", TypeFactory.createVarchar(20)))
                .addColumn(new Column("RunningSql", TypeFactory.createVarchar(20)))
                .addColumn(new Column("QueuedSql", TypeFactory.createVarchar(20)))
                .addColumn(new Column("CreatedOn", TypeFactory.createVarchar(20)))
                .addColumn(new Column("ResumedOn", TypeFactory.createVarchar(20)))
                .addColumn(new Column("UpdatedOn", TypeFactory.createVarchar(20)))
                .addColumn(new Column("Property", TypeFactory.createVarchar(256)))
                .addColumn(new Column("Comment", TypeFactory.createVarchar(256)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowBrokerStatement(ShowBrokerStmt statement, Void context) {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String title : BrokerProcNode.BROKER_PROC_NODE_TITLE_NAMES) {
            builder.addColumn(new Column(title, TypeFactory.createVarchar(30)));
        }
        return builder.build();
    }

    @Override
    public ShowResultSetMetaData visitShowRunningQueriesStatement(ShowRunningQueriesStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("QueryId", TypeFactory.createVarchar(64)))
                .addColumn(new Column("WarehouseId", TypeFactory.createVarchar(64)))
                .addColumn(new Column("ResourceGroupId", TypeFactory.createVarchar(64)))
                .addColumn(new Column("StartTime", TypeFactory.createVarchar(64)))
                .addColumn(new Column("PendingTimeout", TypeFactory.createVarchar(64)))
                .addColumn(new Column("QueryTimeout", TypeFactory.createVarchar(64)))
                .addColumn(new Column("State", TypeFactory.createVarchar(64)))
                .addColumn(new Column("Slots", TypeFactory.createVarchar(64)))
                .addColumn(new Column("Fragments", TypeFactory.createVarchar(64)))
                .addColumn(new Column("DOP", TypeFactory.createVarchar(64)))
                .addColumn(new Column("Frontend", TypeFactory.createVarchar(64)))
                .addColumn(new Column("FeStartTime", TypeFactory.createVarchar(64)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowCreateRoutineLoadStatement(ShowCreateRoutineLoadStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Job", TypeFactory.createVarchar(20)))
                .addColumn(new Column("Create Job", TypeFactory.createVarchar(30)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitAdminShowConfigStatement(AdminShowConfigStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Key", TypeFactory.createVarchar(30)))
                .addColumn(new Column("AliasNames", TypeFactory.createVarchar(30)))
                .addColumn(new Column("Value", TypeFactory.createVarchar(30)))
                .addColumn(new Column("Type", TypeFactory.createVarchar(30)))
                .addColumn(new Column("IsMutable", TypeFactory.createVarchar(30)))
                .addColumn(new Column("Comment", TypeFactory.createVarchar(30)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowPluginsStatement(ShowPluginsStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Name", TypeFactory.createVarchar(64)))
                .addColumn(new Column("Type", TypeFactory.createVarchar(10)))
                .addColumn(new Column("Description", TypeFactory.createVarchar(200)))
                .addColumn(new Column("Version", TypeFactory.createVarchar(20)))
                .addColumn(new Column("JavaVersion", TypeFactory.createVarchar(20)))
                .addColumn(new Column("ClassName", TypeFactory.createVarchar(64)))
                .addColumn(new Column("SoName", TypeFactory.createVarchar(64)))
                .addColumn(new Column("Sources", TypeFactory.createVarchar(200)))
                .addColumn(new Column("Status", TypeFactory.createVarchar(20)))
                .addColumn(new Column("Properties", TypeFactory.createVarchar(250)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitDescStorageVolumeStatement(DescStorageVolumeStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Name", TypeFactory.createVarchar(256)))
                .addColumn(new Column("Type", TypeFactory.createVarchar(20)))
                .addColumn(new Column("IsDefault", TypeFactory.createVarchar(20)))
                .addColumn(new Column("Location", TypeFactory.createVarchar(20)))
                .addColumn(new Column("Params", TypeFactory.createVarchar(256)))
                .addColumn(new Column("Enabled", TypeFactory.createVarchar(256)))
                .addColumn(new Column("Comment", TypeFactory.createVarchar(20)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowMaterializedViewStatement(ShowMaterializedViewsStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .column("id", BIGINT)
                .column("database_name", TypeFactory.createVarchar(20))
                .column("name", TypeFactory.createVarchar(50))
                .column("refresh_type", TypeFactory.createVarchar(10))
                .column("is_active", TypeFactory.createVarchar(10))
                .column("inactive_reason", TypeFactory.createVarcharType(64))
                .column("partition_type", TypeFactory.createVarchar(16))
                .column("task_id", BIGINT)
                .column("task_name", TypeFactory.createVarchar(50))
                .column("last_refresh_start_time", DATETIME)
                .column("last_refresh_finished_time", DATETIME)
                .column("last_refresh_duration", DOUBLE)
                .column("last_refresh_state", TypeFactory.createVarchar(20))
                .column("last_refresh_force_refresh", TypeFactory.createVarchar(8))
                .column("last_refresh_start_partition", TypeFactory.createVarchar(1024))
                .column("last_refresh_end_partition", TypeFactory.createVarchar(1024))
                .column("last_refresh_base_refresh_partitions", TypeFactory.createVarchar(1024))
                .column("last_refresh_mv_refresh_partitions", TypeFactory.createVarchar(1024))
                .column("last_refresh_error_code", TypeFactory.createVarchar(20))
                .column("last_refresh_error_message", TypeFactory.createVarchar(1024))
                .column("rows", BIGINT)
                .column("text", TypeFactory.createVarchar(1024))
                .column("extra_message", TypeFactory.createVarchar(1024))
                .column("query_rewrite_status", TypeFactory.createVarchar(64))
                .column("creator", TypeFactory.createVarchar(64))
                .column("last_refresh_process_time", DATETIME)
                .column("last_refresh_job_id", TypeFactory.createVarchar(64))
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
                builder.addColumn(new Column(col, TypeFactory.createVarchar(30)));
            }
        } catch (AnalysisException e) {
            // Return empty builder if fetch fails
        }
        return builder.build();
    }

    @Override
    public ShowResultSetMetaData visitShowDatabasesStatement(ShowDbStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Database", TypeFactory.createVarchar(64)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowTableStatusStatement(ShowTableStatusStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Name", TypeFactory.createVarchar(64)))
                .addColumn(new Column("Engine", TypeFactory.createVarchar(10)))
                .addColumn(new Column("Version", BIGINT))
                .addColumn(new Column("Row_format", TypeFactory.createVarchar(64)))
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
                .addColumn(new Column("Collation", TypeFactory.createVarchar(64)))
                .addColumn(new Column("Checksum", BIGINT))
                .addColumn(new Column("Create_options", TypeFactory.createVarchar(64)))
                .addColumn(new Column("Comment", TypeFactory.createVarchar(64)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitDescTableStmt(DescribeStmt statement, Void context) {
        if (statement.isTableFunctionTable()) {
            return ShowResultSetMetaData.builder()
                    .addColumn(new Column("Field", TypeFactory.createVarchar(20)))
                    .addColumn(new Column("Type", TypeFactory.createVarchar(20)))
                    .addColumn(new Column("Null", TypeFactory.createVarchar(10)))
                    .build();
        }

        if (!statement.isAllTables()) {
            if (statement.isMaterializedView()) {
                return ShowResultSetMetaData.builder()
                        .addColumn(new Column("Field", TypeFactory.createVarchar(20)))
                        .addColumn(new Column("Type", TypeFactory.createVarchar(20)))
                        .addColumn(new Column("Null", TypeFactory.createVarchar(10)))
                        .addColumn(new Column("Key", TypeFactory.createVarchar(10)))
                        .addColumn(new Column("Default", TypeFactory.createVarchar(30)))
                        .addColumn(new Column("Extra", TypeFactory.createVarchar(30)))
                        .build();
            } else {
                ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
                if (Strings.isNullOrEmpty(statement.getProcPath())) {
                    return builder.build();
                }
                try {
                    ProcResult result = ProcService.getInstance().open(statement.getProcPath()).fetchResult();
                    for (String col : result.getColumnNames()) {
                        builder.addColumn(new Column(col, TypeFactory.createVarchar(30)));
                    }
                } catch (AnalysisException e) {
                    // Return empty builder if fetch fails
                }
                return builder.build();
            }
        } else {
            if (statement.isOlapTable()) {
                return ShowResultSetMetaData.builder()
                        .addColumn(new Column("IndexName", TypeFactory.createVarchar(20)))
                        .addColumn(new Column("IndexKeysType", TypeFactory.createVarchar(20)))
                        .addColumn(new Column("Field", TypeFactory.createVarchar(20)))
                        .addColumn(new Column("Type", TypeFactory.createVarchar(20)))
                        .addColumn(new Column("Null", TypeFactory.createVarchar(10)))
                        .addColumn(new Column("Key", TypeFactory.createVarchar(10)))
                        .addColumn(new Column("Default", TypeFactory.createVarchar(30)))
                        .addColumn(new Column("Extra", TypeFactory.createVarchar(30)))
                        .build();
            } else if (statement.isMaterializedView()) {
                return ShowResultSetMetaData.builder()
                        .addColumn(new Column("Field", TypeFactory.createVarchar(20)))
                        .addColumn(new Column("Type", TypeFactory.createVarchar(20)))
                        .addColumn(new Column("Null", TypeFactory.createVarchar(10)))
                        .addColumn(new Column("Key", TypeFactory.createVarchar(10)))
                        .addColumn(new Column("Default", TypeFactory.createVarchar(30)))
                        .addColumn(new Column("Extra", TypeFactory.createVarchar(30)))
                        .build();
            } else {
                return ShowResultSetMetaData.builder()
                        .addColumn(new Column("Host", TypeFactory.createVarchar(30)))
                        .addColumn(new Column("Port", TypeFactory.createVarchar(10)))
                        .addColumn(new Column("User", TypeFactory.createVarchar(30)))
                        .addColumn(new Column("Password", TypeFactory.createVarchar(30)))
                        .addColumn(new Column("Database", TypeFactory.createVarchar(30)))
                        .addColumn(new Column("Table", TypeFactory.createVarchar(30)))
                        .build();
            }
        }
    }

    @Override
    public ShowResultSetMetaData visitShowProcesslistStatement(ShowProcesslistStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("ServerName", TypeFactory.createVarchar(64)))
                .addColumn(new Column("Id", BIGINT))
                .addColumn(new Column("User", TypeFactory.createVarchar(16)))
                .addColumn(new Column("Host", TypeFactory.createVarchar(16)))
                .addColumn(new Column("Db", TypeFactory.createVarchar(16)))
                .addColumn(new Column("Command", TypeFactory.createVarchar(16)))
                .addColumn(new Column("ConnectionStartTime", TypeFactory.createVarchar(16)))
                .addColumn(new Column("Time", INT))
                .addColumn(new Column("State", TypeFactory.createVarchar(64)))
                .addColumn(new Column("Info", TypeFactory.createVarchar(32 * 1024)))
                .addColumn(new Column("IsPending", TypeFactory.createVarchar(16)))
                .addColumn(new Column("Warehouse", TypeFactory.createVarchar(20)))
                .addColumn(new Column("CNGroup", TypeFactory.createVarchar(64)))
                .addColumn(new Column("Catalog", TypeFactory.createVarchar(64)))
                .addColumn(new Column("QueryId", TypeFactory.createVarchar(64)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowFunctionsStatement(ShowFunctionsStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Signature", TypeFactory.createVarchar(256)))
                .addColumn(new Column("Return Type", TypeFactory.createVarchar(32)))
                .addColumn(new Column("Function Type", TypeFactory.createVarchar(16)))
                .addColumn(new Column("Intermediate Type", TypeFactory.createVarchar(16)))
                .addColumn(new Column("Properties", TypeFactory.createVarchar(16)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowRoutineLoadStatement(ShowRoutineLoadStmt statement, Void context) {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();

        for (String title : ShowRoutineLoadStmt.TITLE_NAMES) {
            builder.addColumn(new Column(title, TypeFactory.createVarchar(30)));
        }
        return builder.build();
    }

    @Override
    public ShowResultSetMetaData visitShowStreamLoadStatement(ShowStreamLoadStmt statement, Void context) {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();

        for (String title : ShowStreamLoadStmt.TITLE_NAMES) {
            builder.addColumn(new Column(title, TypeFactory.createVarchar(30)));
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
            builder.addColumn(new Column(title, TypeFactory.createVarchar(30)));
        }

        return builder.build();
    }

    @Override
    public ShowResultSetMetaData visitShowUserPropertyStatement(ShowUserPropertyStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Key", TypeFactory.createVarchar(64)))
                .addColumn(new Column("Value", TypeFactory.createVarchar(64)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowDataDistributionStatement(ShowDataDistributionStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("PartitionName", TypeFactory.createVarchar(30)))
                .addColumn(new Column("SubPartitionId", TypeFactory.createVarchar(30)))
                .addColumn(new Column("MaterializedIndexName", TypeFactory.createVarchar(30)))
                .addColumn(new Column("RowCount", TypeFactory.createVarchar(30)))
                .addColumn(new Column("RowCount%", TypeFactory.createVarchar(10)))
                .addColumn(new Column("DataSize", TypeFactory.createVarchar(30)))
                .addColumn(new Column("DataSize%", TypeFactory.createVarchar(10)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowCollationStatement(ShowCollationStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Collation", TypeFactory.createVarchar(20)))
                .addColumn(new Column("Charset", TypeFactory.createVarchar(20)))
                .addColumn(new Column("Id", BIGINT))
                .addColumn(new Column("Default", TypeFactory.createVarchar(20)))
                .addColumn(new Column("Compiled", TypeFactory.createVarchar(20)))
                .addColumn(new Column("Sortlen", BIGINT))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowTabletStatement(ShowTabletStmt statement, Void context) {
        List<String> titleNames = statement.getTitleNames();

        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String title : titleNames) {
            builder.addColumn(new Column(title, TypeFactory.createVarchar(30)));
        }
        return builder.build();
    }

    @Override
    public ShowResultSetMetaData visitShowBackupStatement(ShowBackupStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("JobId", TypeFactory.createVarchar(30)))
                .addColumn(new Column("SnapshotName", TypeFactory.createVarchar(30)))
                .addColumn(new Column("DbName", TypeFactory.createVarchar(30)))
                .addColumn(new Column("State", TypeFactory.createVarchar(30)))
                .addColumn(new Column("BackupObjs", TypeFactory.createVarchar(30)))
                .addColumn(new Column("CreateTime", TypeFactory.createVarchar(30)))
                .addColumn(new Column("SnapshotFinishedTime", TypeFactory.createVarchar(30)))
                .addColumn(new Column("UploadFinishedTime", TypeFactory.createVarchar(30)))
                .addColumn(new Column("FinishedTime", TypeFactory.createVarchar(30)))
                .addColumn(new Column("UnfinishedTasks", TypeFactory.createVarchar(30)))
                .addColumn(new Column("Progress", TypeFactory.createVarchar(30)))
                .addColumn(new Column("TaskErrMsg", TypeFactory.createVarchar(30)))
                .addColumn(new Column("Status", TypeFactory.createVarchar(30)))
                .addColumn(new Column("Timeout", TypeFactory.createVarchar(30)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowRestoreStatement(ShowRestoreStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("JobId", TypeFactory.createVarchar(30)))
                .addColumn(new Column("Label", TypeFactory.createVarchar(30)))
                .addColumn(new Column("Timestamp", TypeFactory.createVarchar(30)))
                .addColumn(new Column("DbName", TypeFactory.createVarchar(30)))
                .addColumn(new Column("State", TypeFactory.createVarchar(30)))
                .addColumn(new Column("AllowLoad", TypeFactory.createVarchar(30)))
                .addColumn(new Column("ReplicationNum", TypeFactory.createVarchar(30)))
                .addColumn(new Column("RestoreObjs", TypeFactory.createVarchar(30)))
                .addColumn(new Column("CreateTime", TypeFactory.createVarchar(30)))
                .addColumn(new Column("MetaPreparedTime", TypeFactory.createVarchar(30)))
                .addColumn(new Column("SnapshotFinishedTime", TypeFactory.createVarchar(30)))
                .addColumn(new Column("DownloadFinishedTime", TypeFactory.createVarchar(30)))
                .addColumn(new Column("FinishedTime", TypeFactory.createVarchar(30)))
                .addColumn(new Column("UnfinishedTasks", TypeFactory.createVarchar(30)))
                .addColumn(new Column("Progress", TypeFactory.createVarchar(30)))
                .addColumn(new Column("TaskErrMsg", TypeFactory.createVarchar(30)))
                .addColumn(new Column("Status", TypeFactory.createVarchar(30)))
                .addColumn(new Column("Timeout", TypeFactory.createVarchar(30)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowGrantsStatement(ShowGrantsStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("UserIdentity", TypeFactory.createVarchar(100)))
                .addColumn(new Column("Catalog", TypeFactory.createVarchar(400)))
                .addColumn(new Column("Grants", TypeFactory.createVarchar(400)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowRolesStatement(ShowRolesStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Name", TypeFactory.createVarchar(100)))
                .addColumn(new Column("Builtin", TypeFactory.createVarchar(30)))
                .addColumn(new Column("Comment", TypeFactory.createVarchar(300)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowSecurityIntegrationStatement(ShowSecurityIntegrationStatement statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Name", TypeFactory.createVarchar(100)))
                .addColumn(new Column("Type", TypeFactory.createVarchar(100)))
                .addColumn(new Column("Comment", TypeFactory.createVarchar(300)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowCreateSecurityIntegrationStatement(
            com.starrocks.sql.ast.integration.ShowCreateSecurityIntegrationStatement statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Security Integration", TypeFactory.createVarchar(60)))
                .addColumn(new Column("Create Security Integration", TypeFactory.createVarchar(500)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowGroupProvidersStatement(ShowGroupProvidersStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Name", TypeFactory.createVarchar(100)))
                .addColumn(new Column("Type", TypeFactory.createVarchar(100)))
                .addColumn(new Column("Comment", TypeFactory.createVarchar(300)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowCreateGroupProviderStatement(ShowCreateGroupProviderStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Group Provider", TypeFactory.createVarchar(60)))
                .addColumn(new Column("Create Group Provider", TypeFactory.createVarchar(500)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitAdminShowReplicaDistributionStatement(AdminShowReplicaDistributionStmt statement,
                                                                            Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("BackendId", TypeFactory.createVarchar(30)))
                .addColumn(new Column("ReplicaNum", TypeFactory.createVarchar(30)))
                .addColumn(new Column("Graph", TypeFactory.createVarchar(30)))
                .addColumn(new Column("Percent", TypeFactory.createVarchar(30)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowIndexStatement(ShowIndexStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Table", TypeFactory.createVarchar(64)))
                .addColumn(new Column("Non_unique", TypeFactory.createVarchar(10)))
                .addColumn(new Column("Key_name", TypeFactory.createVarchar(80)))
                .addColumn(new Column("Seq_in_index", TypeFactory.createVarchar(64)))
                .addColumn(new Column("Column_name", TypeFactory.createVarchar(80)))
                .addColumn(new Column("Collation", TypeFactory.createVarchar(80)))
                .addColumn(new Column("Cardinality", TypeFactory.createVarchar(80)))
                .addColumn(new Column("Sub_part", TypeFactory.createVarchar(80)))
                .addColumn(new Column("Packed", TypeFactory.createVarchar(80)))
                .addColumn(new Column("Null", TypeFactory.createVarchar(80)))
                .addColumn(new Column("Index_type", TypeFactory.createVarchar(80)))
                .addColumn(new Column("Comment", TypeFactory.createVarchar(80)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowTransactionStatement(ShowTransactionStmt statement, Void context) {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String title : TransProcDir.TITLE_NAMES) {
            builder.addColumn(new Column(title, TypeFactory.createVarchar(30)));
        }
        return builder.build();
    }

    @Override
    public ShowResultSetMetaData visitShowMultiColumnsStatsMetaStatement(ShowMultiColumnStatsMetaStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Database", TypeFactory.createVarchar(60)))
                .addColumn(new Column("Table", TypeFactory.createVarchar(60)))
                .addColumn(new Column("Columns", TypeFactory.createVarchar(200)))
                .addColumn(new Column("Type", TypeFactory.createVarchar(20)))
                .addColumn(new Column("StatisticsTypes", TypeFactory.createVarchar(200)))
                .addColumn(new Column("UpdateTime", TypeFactory.createVarchar(60)))
                .addColumn(new Column("Properties", TypeFactory.createVarchar(200)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowBaselinePlanStatement(ShowBaselinePlanStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Id", TypeFactory.createVarchar(60)))
                .addColumn(new Column("global", TypeFactory.createVarchar(10)))
                .addColumn(new Column("enable", TypeFactory.createVarchar(10)))
                .addColumn(new Column("bindSQLDigest", TypeFactory.createVarchar(65535)))
                .addColumn(new Column("bindSQLHash", TypeFactory.createVarchar(60)))
                .addColumn(new Column("bindSQL", TypeFactory.createVarchar(65535)))
                .addColumn(new Column("planSQL", TypeFactory.createVarchar(65535)))
                .addColumn(new Column("costs", TypeFactory.createVarchar(60)))
                .addColumn(new Column("queryMs", TypeFactory.createVarchar(60)))
                .addColumn(new Column("source", TypeFactory.createVarchar(60)))
                .addColumn(new Column("updateTime", TypeFactory.createVarchar(60)))
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
                .addColumn(new Column("Catalog", TypeFactory.createVarchar(256)))
                .addColumn(new Column("Type", TypeFactory.createVarchar(20)))
                .addColumn(new Column("Comment", TypeFactory.createVarchar(30)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowCreateExternalCatalogStatement(ShowCreateExternalCatalogStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Catalog", TypeFactory.createVarchar(20)))
                .addColumn(new Column("Create Catalog", TypeFactory.createVarchar(30)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowStorageVolumesStatement(ShowStorageVolumesStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Storage Volume", TypeFactory.createVarchar(256)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowFailPointStatement(ShowFailPointStatement statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Name", TypeFactory.createVarchar(256)))
                .addColumn(new Column("TriggerMode", TypeFactory.createVarchar(32)))
                .addColumn(new Column("Times/Probability", TypeFactory.createVarchar(16)))
                .addColumn(new Column("Host", TypeFactory.createVarchar(64)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowNodesStatement(ShowNodesStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("WarehouseName", TypeFactory.createVarchar(256)))
                .addColumn(new Column("CNGroupId", TypeFactory.createVarchar(20)))
                .addColumn(new Column("WorkerGroupId", TypeFactory.createVarchar(20)))
                .addColumn(new Column("NodeId", TypeFactory.createVarchar(20)))
                .addColumn(new Column("WorkerId", TypeFactory.createVarchar(20)))
                .addColumn(new Column("IP", TypeFactory.createVarchar(256)))
                .addColumn(new Column("HeartbeatPort", TypeFactory.createVarchar(20)))
                .addColumn(new Column("BePort", TypeFactory.createVarchar(20)))
                .addColumn(new Column("HttpPort", TypeFactory.createVarchar(20)))
                .addColumn(new Column("BrpcPort", TypeFactory.createVarchar(20)))
                .addColumn(new Column("StarletPort", TypeFactory.createVarchar(20)))
                .addColumn(new Column("LastStartTime", TypeFactory.createVarchar(256)))
                .addColumn(new Column("LastUpdateMs", TypeFactory.createVarchar(256)))
                .addColumn(new Column("Alive", TypeFactory.createVarchar(20)))
                .addColumn(new Column("ErrMsg", TypeFactory.createVarchar(256)))
                .addColumn(new Column("Version", TypeFactory.createVarchar(20)))
                .addColumn(new Column("NumRunningQueries", TypeFactory.createVarchar(20)))
                .addColumn(new Column("CpuCores", TypeFactory.createVarchar(20)))
                .addColumn(new Column("MemUsedPct", TypeFactory.createVarchar(20)))
                .addColumn(new Column("CpuUsedPct", TypeFactory.createVarchar(20)))
                .addColumn(new Column("CNGroupName", TypeFactory.createVarchar(256)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowClusterStatement(ShowClustersStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("CNGroupId", TypeFactory.createVarchar(20)))
                .addColumn(new Column("CNGroupName", TypeFactory.createVarchar(256)))
                .addColumn(new Column("WorkerGroupId", TypeFactory.createVarchar(20)))
                .addColumn(new Column("ComputeNodeIds", TypeFactory.createVarchar(4096)))
                .addColumn(new Column("Pending", TypeFactory.createVarchar(20)))
                .addColumn(new Column("Running", TypeFactory.createVarchar(20)))
                .addColumn(new Column("Enabled", TypeFactory.createVarchar(10)))
                .addColumn(new Column("Properties", TypeFactory.createVarchar(1024)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowOpenTableStatement(ShowOpenTableStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Database", TypeFactory.createVarchar(64)))
                .addColumn(new Column("Table", TypeFactory.createVarchar(10)))
                .addColumn(new Column("In_use", TypeFactory.createVarchar(80)))
                .addColumn(new Column("Name_locked", TypeFactory.createVarchar(64)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowPrivilegeStatement(ShowPrivilegesStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Privilege", TypeFactory.createVarchar(64)))
                .addColumn(new Column("Context", TypeFactory.createVarchar(64)))
                .addColumn(new Column("Comment", TypeFactory.createVarchar(200)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowStatusStatement(ShowStatusStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Variable_name", TypeFactory.createVarchar(20)))
                .addColumn(new Column("Value", TypeFactory.createVarchar(20)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitShowTriggersStatement(ShowTriggersStmt statement, Void context) {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("Trigger", TypeFactory.createVarchar(64)))
                .addColumn(new Column("Event", TypeFactory.createVarchar(10)))
                .addColumn(new Column("Table", TypeFactory.createVarchar(80)))
                .addColumn(new Column("Statement", TypeFactory.createVarchar(64)))
                .addColumn(new Column("Timing", TypeFactory.createVarchar(80)))
                .addColumn(new Column("Created", TypeFactory.createVarchar(80)))
                .addColumn(new Column("sql_mode", TypeFactory.createVarchar(80)))
                .addColumn(new Column("Definer", TypeFactory.createVarchar(80)))
                .addColumn(new Column("character_set_client", TypeFactory.createVarchar(80)))
                .addColumn(new Column("collation_connection", TypeFactory.createVarchar(80)))
                .addColumn(new Column("Database Collation", TypeFactory.createVarchar(80)))
                .build();
    }

    @Override
    public ShowResultSetMetaData visitRefreshMaterializedViewStatement(RefreshMaterializedViewStatement statement, Void context) {
        return ShowResultSetMetaData.builder().addColumn(new Column("QUERY_ID", TypeFactory.createVarchar(60))).build();
    }

    @Override
    public ShowResultSetMetaData visitShowComputeNodes(ShowComputeNodesStmt statement, Void context) {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String title : ComputeNodeProcDir.getMetadata()) {
            builder.addColumn(new Column(title, TypeFactory.createVarchar(30)));
        }
        return builder.build();
    }
}
