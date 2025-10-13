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

import com.starrocks.common.proc.BaseProcResult;
import com.starrocks.common.proc.ProcNodeInterface;
import com.starrocks.common.proc.ProcResult;
import com.starrocks.sql.ast.AdminShowConfigStmt;
import com.starrocks.sql.ast.AdminShowReplicaDistributionStmt;
import com.starrocks.sql.ast.AdminShowReplicaStatusStmt;
import com.starrocks.sql.ast.LabelName;
import com.starrocks.sql.ast.QualifiedName;
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
import com.starrocks.sql.ast.TableRef;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.TableName;
import com.starrocks.sql.ast.expression.TableRefPersist;
import com.starrocks.sql.ast.group.ShowCreateGroupProviderStmt;
import com.starrocks.sql.ast.group.ShowGroupProvidersStmt;
import com.starrocks.sql.ast.integration.ShowCreateSecurityIntegrationStatement;
import com.starrocks.sql.ast.integration.ShowSecurityIntegrationStatement;
import com.starrocks.sql.ast.pipe.ShowPipeStmt;
import com.starrocks.sql.ast.spm.ShowBaselinePlanStmt;
import com.starrocks.sql.ast.warehouse.ShowClustersStmt;
import com.starrocks.sql.ast.warehouse.ShowNodesStmt;
import com.starrocks.sql.ast.warehouse.ShowWarehousesStmt;
import com.starrocks.sql.parser.NodePosition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

public class ShowStmtMetaTest {
    @Test
    public void testShowRolesStmt() {
        ShowRolesStmt stmt = new ShowRolesStmt();
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        Assertions.assertEquals(3, metaData.getColumnCount());
        Assertions.assertEquals("Name", metaData.getColumn(0).getName());
        Assertions.assertEquals("Builtin", metaData.getColumn(1).getName());
        Assertions.assertEquals("Comment", metaData.getColumn(2).getName());
    }

    @Test
    public void testShowDbStmt() {
        ShowDbStmt stmt = new ShowDbStmt(null);
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        Assertions.assertEquals(1, metaData.getColumnCount());
        Assertions.assertEquals("Database", metaData.getColumn(0).getName());
    }

    @Test
    public void testShowTableStmt() {
        ShowTableStmt stmt = new ShowTableStmt("test_db", false, null);
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        Assertions.assertEquals(1, metaData.getColumnCount());
        Assertions.assertEquals("Tables_in_test_db", metaData.getColumn(0).getName());
    }

    @Test
    public void testShowTableStmtVerbose() {
        ShowTableStmt stmt = new ShowTableStmt("test_db", true, null);
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        Assertions.assertEquals(2, metaData.getColumnCount());
        Assertions.assertEquals("Tables_in_test_db", metaData.getColumn(0).getName());
        Assertions.assertEquals("Table_type", metaData.getColumn(1).getName());
    }

    @Test
    public void testShowVariablesStmt() {
        ShowVariablesStmt stmt = new ShowVariablesStmt(SetType.SESSION, null);
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        Assertions.assertEquals(2, metaData.getColumnCount());
        Assertions.assertEquals("Variable_name", metaData.getColumn(0).getName());
        Assertions.assertEquals("Value", metaData.getColumn(1).getName());
    }

    @Test
    public void testShowVariablesStmtVerbose() {
        ShowVariablesStmt stmt = new ShowVariablesStmt(SetType.VERBOSE, null);
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        Assertions.assertEquals(4, metaData.getColumnCount());
        Assertions.assertEquals("Variable_name", metaData.getColumn(0).getName());
        Assertions.assertEquals("Value", metaData.getColumn(1).getName());
        Assertions.assertEquals("Default_value", metaData.getColumn(2).getName());
        Assertions.assertEquals("Is_changed", metaData.getColumn(3).getName());
    }

    @Test
    public void testShowColumnStmt() {
        TableName tableName = new TableName("test_db", "test_table");
        ShowColumnStmt stmt = new ShowColumnStmt(tableName, "test_db", null, false);
        stmt.init();
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        Assertions.assertEquals(6, metaData.getColumnCount());
        Assertions.assertEquals("Field", metaData.getColumn(0).getName());
        Assertions.assertEquals("Type", metaData.getColumn(1).getName());
        Assertions.assertEquals("Null", metaData.getColumn(2).getName());
        Assertions.assertEquals("Key", metaData.getColumn(3).getName());
        Assertions.assertEquals("Default", metaData.getColumn(4).getName());
        Assertions.assertEquals("Extra", metaData.getColumn(5).getName());
    }

    @Test
    public void testShowColumnStmtVerbose() {
        TableName tableName = new TableName("test_db", "test_table");
        ShowColumnStmt stmt = new ShowColumnStmt(tableName, "test_db", null, true);
        stmt.init();
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        Assertions.assertEquals(9, metaData.getColumnCount());
        Assertions.assertEquals("Field", metaData.getColumn(0).getName());
        Assertions.assertEquals("Type", metaData.getColumn(1).getName());
        Assertions.assertEquals("Collation", metaData.getColumn(2).getName());
        Assertions.assertEquals("Null", metaData.getColumn(3).getName());
        Assertions.assertEquals("Key", metaData.getColumn(4).getName());
        Assertions.assertEquals("Default", metaData.getColumn(5).getName());
        Assertions.assertEquals("Extra", metaData.getColumn(6).getName());
        Assertions.assertEquals("Privileges", metaData.getColumn(7).getName());
        Assertions.assertEquals("Comment", metaData.getColumn(8).getName());
    }

    @Test
    public void testShowEnginesStmt() {
        ShowEnginesStmt stmt = new ShowEnginesStmt();
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        Assertions.assertEquals(6, metaData.getColumnCount());
        Assertions.assertEquals("Engine", metaData.getColumn(0).getName());
        Assertions.assertEquals("Support", metaData.getColumn(1).getName());
        Assertions.assertEquals("Comment", metaData.getColumn(2).getName());
        Assertions.assertEquals("Transactions", metaData.getColumn(3).getName());
        Assertions.assertEquals("XA", metaData.getColumn(4).getName());
        Assertions.assertEquals("Savepoints", metaData.getColumn(5).getName());
    }

    @Test
    public void testShowPrivilegesStmt() {
        ShowPrivilegesStmt stmt = new ShowPrivilegesStmt();
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        Assertions.assertEquals(3, metaData.getColumnCount());
        Assertions.assertEquals("Privilege", metaData.getColumn(0).getName());
        Assertions.assertEquals("Context", metaData.getColumn(1).getName());
        Assertions.assertEquals("Comment", metaData.getColumn(2).getName());
    }

    @Test
    public void testShowBackendsStmt() {
        ShowBackendsStmt stmt = new ShowBackendsStmt();
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        // BackendsProcDir.getMetadata() returns different column counts based on RunMode
        // For now, we'll just check that it has columns
        Assertions.assertEquals(30, metaData.getColumnCount());
        // Check some common column names
        Assertions.assertEquals("BackendId", metaData.getColumn(0).getName());
        Assertions.assertEquals("IP", metaData.getColumn(1).getName());
        Assertions.assertEquals("HeartbeatPort", metaData.getColumn(2).getName());
        Assertions.assertEquals("BePort", metaData.getColumn(3).getName());
        Assertions.assertEquals("HttpPort", metaData.getColumn(4).getName());
        Assertions.assertEquals("BrpcPort", metaData.getColumn(5).getName());
    }

    @Test
    public void testShowFrontendsStmt() {
        ShowFrontendsStmt stmt = new ShowFrontendsStmt();
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        Assertions.assertEquals(17, metaData.getColumnCount());
        Assertions.assertEquals("Id", metaData.getColumn(0).getName());
        Assertions.assertEquals("Name", metaData.getColumn(1).getName());
        Assertions.assertEquals("IP", metaData.getColumn(2).getName());
        Assertions.assertEquals("EditLogPort", metaData.getColumn(3).getName());
        Assertions.assertEquals("HttpPort", metaData.getColumn(4).getName());
        Assertions.assertEquals("QueryPort", metaData.getColumn(5).getName());
        Assertions.assertEquals("RpcPort", metaData.getColumn(6).getName());
        Assertions.assertEquals("Role", metaData.getColumn(7).getName());
        Assertions.assertEquals("ClusterId", metaData.getColumn(8).getName());
        Assertions.assertEquals("Join", metaData.getColumn(9).getName());
        Assertions.assertEquals("Alive", metaData.getColumn(10).getName());
        Assertions.assertEquals("ReplayedJournalId", metaData.getColumn(11).getName());
        Assertions.assertEquals("LastHeartbeat", metaData.getColumn(12).getName());
        Assertions.assertEquals("IsHelper", metaData.getColumn(13).getName());
        Assertions.assertEquals("ErrMsg", metaData.getColumn(14).getName());
        Assertions.assertEquals("StartTime", metaData.getColumn(15).getName());
        Assertions.assertEquals("Version", metaData.getColumn(16).getName());
    }

    @Test
    public void testShowBrokerStmt() {
        ShowBrokerStmt stmt = new ShowBrokerStmt();
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        Assertions.assertEquals(7, metaData.getColumnCount());
        Assertions.assertEquals("Name", metaData.getColumn(0).getName());
        Assertions.assertEquals("IP", metaData.getColumn(1).getName());
        Assertions.assertEquals("Port", metaData.getColumn(2).getName());
        Assertions.assertEquals("Alive", metaData.getColumn(3).getName());
        Assertions.assertEquals("LastStartTime", metaData.getColumn(4).getName());
        Assertions.assertEquals("LastUpdateTime", metaData.getColumn(5).getName());
        Assertions.assertEquals("ErrMsg", metaData.getColumn(6).getName());
    }

    @Test
    public void testShowCharsetStmt() {
        ShowCharsetStmt stmt = new ShowCharsetStmt();
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        Assertions.assertEquals(4, metaData.getColumnCount());
        Assertions.assertEquals("Charset", metaData.getColumn(0).getName());
        Assertions.assertEquals("Description", metaData.getColumn(1).getName());
        Assertions.assertEquals("Default collation", metaData.getColumn(2).getName());
        Assertions.assertEquals("Maxlen", metaData.getColumn(3).getName());
    }

    @Test
    public void testShowCreateDbStmt() {
        ShowCreateDbStmt stmt = new ShowCreateDbStmt("test_db");
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        Assertions.assertEquals(2, metaData.getColumnCount());
        Assertions.assertEquals("Database", metaData.getColumn(0).getName());
        Assertions.assertEquals("Create Database", metaData.getColumn(1).getName());
    }

    @Test
    public void testShowCreateTableStmt() {
        TableName tableName = new TableName("test_db", "test_table");
        ShowCreateTableStmt stmt = new ShowCreateTableStmt(tableName, ShowCreateTableStmt.CreateTableType.TABLE);
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        Assertions.assertEquals(2, metaData.getColumnCount());
        Assertions.assertEquals("Table", metaData.getColumn(0).getName());
        Assertions.assertEquals("Create Table", metaData.getColumn(1).getName());
    }

    @Test
    public void testShowCreateTableStmtView() {
        TableName tableName = new TableName("test_db", "test_view");
        ShowCreateTableStmt stmt = new ShowCreateTableStmt(tableName, ShowCreateTableStmt.CreateTableType.VIEW);
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        Assertions.assertEquals(2, metaData.getColumnCount());
        Assertions.assertEquals("Table", metaData.getColumn(0).getName());
        Assertions.assertEquals("Create Table", metaData.getColumn(1).getName());
    }

    @Test
    public void testShowCreateTableStmtMaterializedView() {
        TableName tableName = new TableName("test_db", "test_mv");
        ShowCreateTableStmt stmt = new ShowCreateTableStmt(tableName, ShowCreateTableStmt.CreateTableType.MATERIALIZED_VIEW);
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        Assertions.assertEquals(2, metaData.getColumnCount());
        Assertions.assertEquals("Table", metaData.getColumn(0).getName());
        Assertions.assertEquals("Create Table", metaData.getColumn(1).getName());
    }

    @Test
    public void testShowCollationStmt() {
        ShowCollationStmt stmt = new ShowCollationStmt();
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        Assertions.assertEquals(6, metaData.getColumnCount());
        Assertions.assertEquals("Collation", metaData.getColumn(0).getName());
        Assertions.assertEquals("Charset", metaData.getColumn(1).getName());
        Assertions.assertEquals("Id", metaData.getColumn(2).getName());
        Assertions.assertEquals("Default", metaData.getColumn(3).getName());
        Assertions.assertEquals("Compiled", metaData.getColumn(4).getName());
        Assertions.assertEquals("Sortlen", metaData.getColumn(5).getName());
    }

    @Test
    public void testShowUserStmt() {
        ShowUserStmt stmt = new ShowUserStmt(false);
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        Assertions.assertEquals(1, metaData.getColumnCount());
        Assertions.assertEquals("User", metaData.getColumn(0).getName());
    }

    @Test
    public void testShowUserStmtAll() {
        ShowUserStmt stmt = new ShowUserStmt(true);
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        Assertions.assertEquals(1, metaData.getColumnCount());
        Assertions.assertEquals("User", metaData.getColumn(0).getName());
    }

    @Test
    public void testShowStatusStmt() {
        ShowStatusStmt stmt = new ShowStatusStmt();
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        Assertions.assertEquals(2, metaData.getColumnCount());
        Assertions.assertEquals("Variable_name", metaData.getColumn(0).getName());
        Assertions.assertEquals("Value", metaData.getColumn(1).getName());
    }

    @Test
    public void testShowProcesslistStmt() {
        ShowProcesslistStmt stmt = new ShowProcesslistStmt(false);
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        Assertions.assertEquals(15, metaData.getColumnCount());
        Assertions.assertEquals("ServerName", metaData.getColumn(0).getName());
        Assertions.assertEquals("Id", metaData.getColumn(1).getName());
        Assertions.assertEquals("User", metaData.getColumn(2).getName());
        Assertions.assertEquals("Host", metaData.getColumn(3).getName());
        Assertions.assertEquals("Db", metaData.getColumn(4).getName());
        Assertions.assertEquals("Command", metaData.getColumn(5).getName());
        Assertions.assertEquals("ConnectionStartTime", metaData.getColumn(6).getName());
        Assertions.assertEquals("Time", metaData.getColumn(7).getName());
        Assertions.assertEquals("State", metaData.getColumn(8).getName());
        Assertions.assertEquals("Info", metaData.getColumn(9).getName());
        Assertions.assertEquals("IsPending", metaData.getColumn(10).getName());
        Assertions.assertEquals("Warehouse", metaData.getColumn(11).getName());
        Assertions.assertEquals("CNGroup", metaData.getColumn(12).getName());
    }

    @Test
    public void testShowFunctionsStmt() {
        ShowFunctionsStmt stmt = new ShowFunctionsStmt("test_db", false, false, false, null, null);
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        Assertions.assertEquals(5, metaData.getColumnCount());
        Assertions.assertEquals("Signature", metaData.getColumn(0).getName());
        Assertions.assertEquals("Return Type", metaData.getColumn(1).getName());
        Assertions.assertEquals("Function Type", metaData.getColumn(2).getName());
        Assertions.assertEquals("Intermediate Type", metaData.getColumn(3).getName());
        Assertions.assertEquals("Properties", metaData.getColumn(4).getName());
    }

    @Test
    public void testShowGrantsStmt() {
        ShowGrantsStmt stmt = new ShowGrantsStmt(null, null);
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        Assertions.assertEquals(3, metaData.getColumnCount());
        Assertions.assertEquals("UserIdentity", metaData.getColumn(0).getName());
        Assertions.assertEquals("Catalog", metaData.getColumn(1).getName());
        Assertions.assertEquals("Grants", metaData.getColumn(2).getName());
    }

    @Test
    public void testShowIndexStmt() {
        TableName tableName = new TableName("test_db", "test_table");
        ShowIndexStmt stmt = new ShowIndexStmt("test_db", tableName);
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        Assertions.assertEquals(12, metaData.getColumnCount());
        Assertions.assertEquals("Table", metaData.getColumn(0).getName());
        Assertions.assertEquals("Non_unique", metaData.getColumn(1).getName());
        Assertions.assertEquals("Key_name", metaData.getColumn(2).getName());
        Assertions.assertEquals("Seq_in_index", metaData.getColumn(3).getName());
        Assertions.assertEquals("Column_name", metaData.getColumn(4).getName());
        Assertions.assertEquals("Collation", metaData.getColumn(5).getName());
        Assertions.assertEquals("Cardinality", metaData.getColumn(6).getName());
        Assertions.assertEquals("Sub_part", metaData.getColumn(7).getName());
        Assertions.assertEquals("Packed", metaData.getColumn(8).getName());
        Assertions.assertEquals("Null", metaData.getColumn(9).getName());
        Assertions.assertEquals("Index_type", metaData.getColumn(10).getName());
        Assertions.assertEquals("Comment", metaData.getColumn(11).getName());
    }

    @Test
    public void testShowTableStatusStmt() {
        ShowTableStatusStmt stmt = new ShowTableStatusStmt("test_db", null, null);
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        Assertions.assertEquals(18, metaData.getColumnCount());
        Assertions.assertEquals("Name", metaData.getColumn(0).getName());
        Assertions.assertEquals("Engine", metaData.getColumn(1).getName());
        Assertions.assertEquals("Version", metaData.getColumn(2).getName());
        Assertions.assertEquals("Row_format", metaData.getColumn(3).getName());
        Assertions.assertEquals("Rows", metaData.getColumn(4).getName());
        Assertions.assertEquals("Avg_row_length", metaData.getColumn(5).getName());
        Assertions.assertEquals("Data_length", metaData.getColumn(6).getName());
        Assertions.assertEquals("Max_data_length", metaData.getColumn(7).getName());
        Assertions.assertEquals("Index_length", metaData.getColumn(8).getName());
        Assertions.assertEquals("Data_free", metaData.getColumn(9).getName());
        Assertions.assertEquals("Auto_increment", metaData.getColumn(10).getName());
        Assertions.assertEquals("Create_time", metaData.getColumn(11).getName());
        Assertions.assertEquals("Update_time", metaData.getColumn(12).getName());
        Assertions.assertEquals("Check_time", metaData.getColumn(13).getName());
        Assertions.assertEquals("Collation", metaData.getColumn(14).getName());
        Assertions.assertEquals("Checksum", metaData.getColumn(15).getName());
        Assertions.assertEquals("Create_options", metaData.getColumn(16).getName());
        Assertions.assertEquals("Comment", metaData.getColumn(17).getName());
    }

    @Test
    public void testShowCatalogsStmt() {
        ShowCatalogsStmt stmt = new ShowCatalogsStmt(null);
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        Assertions.assertEquals(3, metaData.getColumnCount());
        Assertions.assertEquals("Catalog", metaData.getColumn(0).getName());
        Assertions.assertEquals("Type", metaData.getColumn(1).getName());
        Assertions.assertEquals("Comment", metaData.getColumn(2).getName());
    }

    @Test
    public void testShowResourcesStmt() {
        ShowResourcesStmt stmt = new ShowResourcesStmt();
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        Assertions.assertEquals(4, metaData.getColumnCount());
        Assertions.assertEquals("Name", metaData.getColumn(0).getName());
        Assertions.assertEquals("ResourceType", metaData.getColumn(1).getName());
        Assertions.assertEquals("Key", metaData.getColumn(2).getName());
        Assertions.assertEquals("Value", metaData.getColumn(3).getName());
    }

    @Test
    public void testShowDeleteStmt() {
        ShowDeleteStmt stmt = new ShowDeleteStmt("test_db");
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        Assertions.assertEquals(5, metaData.getColumnCount());
        Assertions.assertEquals("TableName", metaData.getColumn(0).getName());
        Assertions.assertEquals("PartitionName", metaData.getColumn(1).getName());
        Assertions.assertEquals("CreateTime", metaData.getColumn(2).getName());
        Assertions.assertEquals("DeleteCondition", metaData.getColumn(3).getName());
        Assertions.assertEquals("State", metaData.getColumn(4).getName());
    }

    @Test
    public void testShowLoadWarningsStmt() {
        ShowLoadWarningsStmt stmt = new ShowLoadWarningsStmt("test_db", null, null, null);
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        Assertions.assertEquals(3, metaData.getColumnCount());
        Assertions.assertEquals("JobId", metaData.getColumn(0).getName());
        Assertions.assertEquals("Label", metaData.getColumn(1).getName());
        Assertions.assertEquals("ErrorMsgDetail", metaData.getColumn(2).getName());
    }

    @Test
    public void testShowSmallFilesStmt() {
        ShowSmallFilesStmt stmt = new ShowSmallFilesStmt("test_db");
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        Assertions.assertEquals(7, metaData.getColumnCount());
        Assertions.assertEquals("Id", metaData.getColumn(0).getName());
        Assertions.assertEquals("DbName", metaData.getColumn(1).getName());
        Assertions.assertEquals("GlobalStateMgr", metaData.getColumn(2).getName());
        Assertions.assertEquals("FileName", metaData.getColumn(3).getName());
        Assertions.assertEquals("FileSize", metaData.getColumn(4).getName());
        Assertions.assertEquals("IsContent", metaData.getColumn(5).getName());
        Assertions.assertEquals("MD5", metaData.getColumn(6).getName());
    }

    @Test
    public void testShowComputeNodeBlackListStmt() {
        ShowComputeNodeBlackListStmt stmt = new ShowComputeNodeBlackListStmt(null);
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        Assertions.assertEquals(5, metaData.getColumnCount());
        Assertions.assertEquals("ComputeNodeId", metaData.getColumn(0).getName());
        Assertions.assertEquals("AddBlackListType", metaData.getColumn(1).getName());
        Assertions.assertEquals("LostConnectionTime", metaData.getColumn(2).getName());
        Assertions.assertEquals("LostConnectionNumberInPeriod", metaData.getColumn(3).getName());
        Assertions.assertEquals("CheckTimePeriod(s)", metaData.getColumn(4).getName());
    }

    @Test
    public void testAdminShowConfigStmt() {
        AdminShowConfigStmt stmt = new AdminShowConfigStmt(null, null);
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        Assertions.assertEquals(6, metaData.getColumnCount());
        Assertions.assertEquals("Key", metaData.getColumn(0).getName());
        Assertions.assertEquals("AliasNames", metaData.getColumn(1).getName());
        Assertions.assertEquals("Value", metaData.getColumn(2).getName());
        Assertions.assertEquals("Type", metaData.getColumn(3).getName());
        Assertions.assertEquals("IsMutable", metaData.getColumn(4).getName());
        Assertions.assertEquals("Comment", metaData.getColumn(5).getName());
    }

    @Test
    public void testShowWhiteListStmt() {
        ShowWhiteListStmt stmt = new ShowWhiteListStmt();
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        Assertions.assertEquals(2, metaData.getColumnCount());
        Assertions.assertEquals("user_name", metaData.getColumn(0).getName());
        Assertions.assertEquals("white_list", metaData.getColumn(1).getName());
    }

    @Test
    public void testShowAnalyzeJobStmt() {
        ShowAnalyzeJobStmt stmt = new ShowAnalyzeJobStmt(null, null, null, null);
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        Assertions.assertEquals(11, metaData.getColumnCount());
        Assertions.assertEquals("Id", metaData.getColumn(0).getName());
        Assertions.assertEquals("Catalog", metaData.getColumn(1).getName());
        Assertions.assertEquals("Database", metaData.getColumn(2).getName());
        Assertions.assertEquals("Table", metaData.getColumn(3).getName());
        Assertions.assertEquals("Columns", metaData.getColumn(4).getName());
        Assertions.assertEquals("Type", metaData.getColumn(5).getName());
        Assertions.assertEquals("Schedule", metaData.getColumn(6).getName());
        Assertions.assertEquals("Properties", metaData.getColumn(7).getName());
        Assertions.assertEquals("Status", metaData.getColumn(8).getName());
        Assertions.assertEquals("LastWorkTime", metaData.getColumn(9).getName());
        Assertions.assertEquals("Reason", metaData.getColumn(10).getName());
    }

    @Test
    public void testShowAnalyzeStatusStmt() {
        ShowAnalyzeStatusStmt stmt = new ShowAnalyzeStatusStmt(null, null, null, null);
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        Assertions.assertEquals(11, metaData.getColumnCount());
        Assertions.assertEquals("Id", metaData.getColumn(0).getName());
        Assertions.assertEquals("Database", metaData.getColumn(1).getName());
        Assertions.assertEquals("Table", metaData.getColumn(2).getName());
        Assertions.assertEquals("Columns", metaData.getColumn(3).getName());
        Assertions.assertEquals("Type", metaData.getColumn(4).getName());
        Assertions.assertEquals("Schedule", metaData.getColumn(5).getName());
        Assertions.assertEquals("Status", metaData.getColumn(6).getName());
        Assertions.assertEquals("StartTime", metaData.getColumn(7).getName());
        Assertions.assertEquals("EndTime", metaData.getColumn(8).getName());
        Assertions.assertEquals("Properties", metaData.getColumn(9).getName());
        Assertions.assertEquals("Reason", metaData.getColumn(10).getName());
    }

    @Test
    public void testShowBackupStmt() {
        ShowBackupStmt stmt = new ShowBackupStmt("test_db");
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        Assertions.assertEquals(14, metaData.getColumnCount());
        Assertions.assertEquals("JobId", metaData.getColumn(0).getName());
        Assertions.assertEquals("SnapshotName", metaData.getColumn(1).getName());
        Assertions.assertEquals("DbName", metaData.getColumn(2).getName());
        Assertions.assertEquals("State", metaData.getColumn(3).getName());
        Assertions.assertEquals("BackupObjs", metaData.getColumn(4).getName());
        Assertions.assertEquals("CreateTime", metaData.getColumn(5).getName());
        Assertions.assertEquals("SnapshotFinishedTime", metaData.getColumn(6).getName());
        Assertions.assertEquals("UploadFinishedTime", metaData.getColumn(7).getName());
        Assertions.assertEquals("FinishedTime", metaData.getColumn(8).getName());
        Assertions.assertEquals("UnfinishedTasks", metaData.getColumn(9).getName());
        Assertions.assertEquals("Progress", metaData.getColumn(10).getName());
        Assertions.assertEquals("TaskErrMsg", metaData.getColumn(11).getName());
        Assertions.assertEquals("Status", metaData.getColumn(12).getName());
        Assertions.assertEquals("Timeout", metaData.getColumn(13).getName());
    }

    @Test
    public void testShowRestoreStmt() {
        ShowRestoreStmt stmt = new ShowRestoreStmt("test_db", null);
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        Assertions.assertEquals(18, metaData.getColumnCount());
        Assertions.assertEquals("JobId", metaData.getColumn(0).getName());
        Assertions.assertEquals("Label", metaData.getColumn(1).getName());
        Assertions.assertEquals("Timestamp", metaData.getColumn(2).getName());
        Assertions.assertEquals("DbName", metaData.getColumn(3).getName());
        Assertions.assertEquals("State", metaData.getColumn(4).getName());
        Assertions.assertEquals("AllowLoad", metaData.getColumn(5).getName());
        Assertions.assertEquals("ReplicationNum", metaData.getColumn(6).getName());
        Assertions.assertEquals("RestoreObjs", metaData.getColumn(7).getName());
        Assertions.assertEquals("CreateTime", metaData.getColumn(8).getName());
        Assertions.assertEquals("MetaPreparedTime", metaData.getColumn(9).getName());
        Assertions.assertEquals("SnapshotFinishedTime", metaData.getColumn(10).getName());
        Assertions.assertEquals("DownloadFinishedTime", metaData.getColumn(11).getName());
        Assertions.assertEquals("FinishedTime", metaData.getColumn(12).getName());
        Assertions.assertEquals("UnfinishedTasks", metaData.getColumn(13).getName());
        Assertions.assertEquals("Progress", metaData.getColumn(14).getName());
        Assertions.assertEquals("TaskErrMsg", metaData.getColumn(15).getName());
        Assertions.assertEquals("Status", metaData.getColumn(16).getName());
        Assertions.assertEquals("Timeout", metaData.getColumn(17).getName());
    }

    @Test
    public void testShowDynamicPartitionStmt() {
        ShowDynamicPartitionStmt stmt = new ShowDynamicPartitionStmt("test_db");
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        Assertions.assertEquals(15, metaData.getColumnCount());
        Assertions.assertEquals("TableName", metaData.getColumn(0).getName());
        Assertions.assertEquals("Enable", metaData.getColumn(1).getName());
        Assertions.assertEquals("TimeUnit", metaData.getColumn(2).getName());
        Assertions.assertEquals("Start", metaData.getColumn(3).getName());
        Assertions.assertEquals("End", metaData.getColumn(4).getName());
        Assertions.assertEquals("Prefix", metaData.getColumn(5).getName());
        Assertions.assertEquals("Buckets", metaData.getColumn(6).getName());
        Assertions.assertEquals("ReplicationNum", metaData.getColumn(7).getName());
        Assertions.assertEquals("StartOf", metaData.getColumn(8).getName());
        Assertions.assertEquals("LastUpdateTime", metaData.getColumn(9).getName());
        Assertions.assertEquals("LastSchedulerTime", metaData.getColumn(10).getName());
        Assertions.assertEquals("State", metaData.getColumn(11).getName());
        Assertions.assertEquals("LastCreatePartitionMsg", metaData.getColumn(12).getName());
        Assertions.assertEquals("LastDropPartitionMsg", metaData.getColumn(13).getName());
        Assertions.assertEquals("InScheduler", metaData.getColumn(14).getName());
    }

    @Test
    public void testShowRoutineLoadStmt() {
        ShowRoutineLoadStmt stmt = new ShowRoutineLoadStmt(null, false);
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        // TITLE_NAMES has different lengths based on RunMode
        Assertions.assertEquals(22, metaData.getColumnCount());
        // Check some common column names
        Assertions.assertEquals("Id", metaData.getColumn(0).getName());
        Assertions.assertEquals("Name", metaData.getColumn(1).getName());
        Assertions.assertEquals("CreateTime", metaData.getColumn(2).getName());
        Assertions.assertEquals("PauseTime", metaData.getColumn(3).getName());
        Assertions.assertEquals("EndTime", metaData.getColumn(4).getName());
        Assertions.assertEquals("DbName", metaData.getColumn(5).getName());
        Assertions.assertEquals("TableName", metaData.getColumn(6).getName());
        Assertions.assertEquals("State", metaData.getColumn(7).getName());
        Assertions.assertEquals("DataSourceType", metaData.getColumn(8).getName());
        Assertions.assertEquals("CurrentTaskNum", metaData.getColumn(9).getName());
        Assertions.assertEquals("JobProperties", metaData.getColumn(10).getName());
        Assertions.assertEquals("DataSourceProperties", metaData.getColumn(11).getName());
        Assertions.assertEquals("CustomProperties", metaData.getColumn(12).getName());
        Assertions.assertEquals("Statistic", metaData.getColumn(13).getName());
        Assertions.assertEquals("Progress", metaData.getColumn(14).getName());
        Assertions.assertEquals("TimestampProgress", metaData.getColumn(15).getName());
        Assertions.assertEquals("ReasonOfStateChanged", metaData.getColumn(16).getName());
    }

    @Test
    public void testShowDataCacheRulesStmt() {
        ShowDataCacheRulesStmt stmt = new ShowDataCacheRulesStmt(null);
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        Assertions.assertEquals(7, metaData.getColumnCount());
        Assertions.assertEquals("Rule Id", metaData.getColumn(0).getName());
        Assertions.assertEquals("Catalog", metaData.getColumn(1).getName());
        Assertions.assertEquals("Database", metaData.getColumn(2).getName());
        Assertions.assertEquals("Table", metaData.getColumn(3).getName());
        Assertions.assertEquals("Priority", metaData.getColumn(4).getName());
        Assertions.assertEquals("Predicates", metaData.getColumn(5).getName());
        Assertions.assertEquals("Properties", metaData.getColumn(6).getName());
    }

    @Test
    public void testShowWarningStmt() {
        ShowWarningStmt stmt = new ShowWarningStmt(null, null);
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        Assertions.assertEquals(3, metaData.getColumnCount());
        Assertions.assertEquals("Level", metaData.getColumn(0).getName());
        Assertions.assertEquals("Code", metaData.getColumn(1).getName());
        Assertions.assertEquals("Message", metaData.getColumn(2).getName());
    }

    @Test
    public void testShowCreateRoutineLoadStmt() {
        ShowCreateRoutineLoadStmt stmt = new ShowCreateRoutineLoadStmt(null);
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        Assertions.assertEquals(2, metaData.getColumnCount());
        Assertions.assertEquals("Job", metaData.getColumn(0).getName());
        Assertions.assertEquals("Create Job", metaData.getColumn(1).getName());
    }

    @Test
    public void testShowLoadStmt() {
        ShowLoadStmt stmt = new ShowLoadStmt("test_db", null, null, null);
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        Assertions.assertEquals(21, metaData.getColumnCount());
        Assertions.assertEquals("JobId", metaData.getColumn(0).getName());
        Assertions.assertEquals("Label", metaData.getColumn(1).getName());
        Assertions.assertEquals("State", metaData.getColumn(2).getName());
        Assertions.assertEquals("Progress", metaData.getColumn(3).getName());
        Assertions.assertEquals("Type", metaData.getColumn(4).getName());
        Assertions.assertEquals("Priority", metaData.getColumn(5).getName());
        Assertions.assertEquals("ScanRows", metaData.getColumn(6).getName());
        Assertions.assertEquals("FilteredRows", metaData.getColumn(7).getName());
        Assertions.assertEquals("UnselectedRows", metaData.getColumn(8).getName());
        Assertions.assertEquals("SinkRows", metaData.getColumn(9).getName());
        Assertions.assertEquals("EtlInfo", metaData.getColumn(10).getName());
        Assertions.assertEquals("TaskInfo", metaData.getColumn(11).getName());
        Assertions.assertEquals("ErrorMsg", metaData.getColumn(12).getName());
        Assertions.assertEquals("CreateTime", metaData.getColumn(13).getName());
        Assertions.assertEquals("EtlStartTime", metaData.getColumn(14).getName());
        Assertions.assertEquals("EtlFinishTime", metaData.getColumn(15).getName());
        Assertions.assertEquals("LoadStartTime", metaData.getColumn(16).getName());
        Assertions.assertEquals("LoadFinishTime", metaData.getColumn(17).getName());
        Assertions.assertEquals("TrackingSQL", metaData.getColumn(18).getName());
        Assertions.assertEquals("JobDetails", metaData.getColumn(19).getName());
        Assertions.assertEquals("Warehouse", metaData.getColumn(20).getName());
    }

    @Test
    public void testShowPartitionsStmt() {
        TableName tableName = new TableName("test_db", "test_table");
        ShowPartitionsStmt stmt = new ShowPartitionsStmt(tableName, null, null, null, false);

        // Test basic statement creation
        Assertions.assertNotNull(stmt);
        Assertions.assertEquals("test_db", stmt.getDbName());
        Assertions.assertEquals("test_table", stmt.getTableName());

        // Test getMetaData() with a mock ProcNodeInterface
        // This simulates the behavior in ShowStmtAnalyzer.visitShowPartitionsStatement
        ProcNodeInterface mockNode = new ProcNodeInterface() {
            @Override
            public ProcResult fetchResult() {
                // Return a mock result with typical partition columns
                BaseProcResult result = new BaseProcResult();
                List<String> columnNames = Arrays.asList(
                        "PartitionId", "PartitionName", "VisibleVersion", "VisibleVersionTime",
                        "VisibleVersionHash", "State", "PartitionKey", "Range", "DistributionKey",
                        "Buckets", "ReplicationNum", "StorageMedium", "CooldownTime",
                        "LastConsistencyCheckTime", "DataSize", "StorageSize", "IsInMemory",
                        "RowCount", "DataVersion", "VersionEpoch", "VersionTxnType", "TabletBalanced"
                );
                result.setNames(columnNames);
                return result;
            }
        };

        stmt.setNode(mockNode);

        // Now test getMetaData() method
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        Assertions.assertEquals(22, metaData.getColumnCount());

        // Verify the column names match what we expect from PartitionsProcDir
        Assertions.assertEquals("PartitionId", metaData.getColumn(0).getName());
        Assertions.assertEquals("PartitionName", metaData.getColumn(1).getName());
        Assertions.assertEquals("VisibleVersion", metaData.getColumn(2).getName());
        Assertions.assertEquals("VisibleVersionTime", metaData.getColumn(3).getName());
        Assertions.assertEquals("VisibleVersionHash", metaData.getColumn(4).getName());
        Assertions.assertEquals("State", metaData.getColumn(5).getName());
        Assertions.assertEquals("PartitionKey", metaData.getColumn(6).getName());
        Assertions.assertEquals("Range", metaData.getColumn(7).getName());
        Assertions.assertEquals("DistributionKey", metaData.getColumn(8).getName());
        Assertions.assertEquals("Buckets", metaData.getColumn(9).getName());
        Assertions.assertEquals("ReplicationNum", metaData.getColumn(10).getName());
        Assertions.assertEquals("StorageMedium", metaData.getColumn(11).getName());
        Assertions.assertEquals("CooldownTime", metaData.getColumn(12).getName());
        Assertions.assertEquals("LastConsistencyCheckTime", metaData.getColumn(13).getName());
        Assertions.assertEquals("DataSize", metaData.getColumn(14).getName());
        Assertions.assertEquals("StorageSize", metaData.getColumn(15).getName());
        Assertions.assertEquals("IsInMemory", metaData.getColumn(16).getName());
        Assertions.assertEquals("RowCount", metaData.getColumn(17).getName());
        Assertions.assertEquals("DataVersion", metaData.getColumn(18).getName());
        Assertions.assertEquals("VersionEpoch", metaData.getColumn(19).getName());
        Assertions.assertEquals("VersionTxnType", metaData.getColumn(20).getName());
        Assertions.assertEquals("TabletBalanced", metaData.getColumn(21).getName());
    }

    @Test
    public void testShowAuthorStmt() {
        ShowAuthorStmt stmt = new ShowAuthorStmt();
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        Assertions.assertEquals(3, metaData.getColumnCount());
        Assertions.assertEquals("Name", metaData.getColumn(0).getName());
        Assertions.assertEquals("Location", metaData.getColumn(1).getName());
        Assertions.assertEquals("Comment", metaData.getColumn(2).getName());
    }

    @Test
    public void testShowAuthenticationStmt() {
        ShowAuthenticationStmt stmt = new ShowAuthenticationStmt(null, false);
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        Assertions.assertEquals(4, metaData.getColumnCount());
        Assertions.assertEquals("UserIdentity", metaData.getColumn(0).getName());
        Assertions.assertEquals("Password", metaData.getColumn(1).getName());
        Assertions.assertEquals("AuthPlugin", metaData.getColumn(2).getName());
        Assertions.assertEquals("UserForAuthPlugin", metaData.getColumn(3).getName());
    }

    @Test
    public void testShowRoutineLoadTaskStmt() {
        ShowRoutineLoadTaskStmt stmt = new ShowRoutineLoadTaskStmt("test_db", null);
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        // TITLE_NAMES has different lengths based on RunMode
        Assertions.assertEquals(11, metaData.getColumnCount());
        // Check some common column names
        Assertions.assertEquals("TaskId", metaData.getColumn(0).getName());
        Assertions.assertEquals("TxnId", metaData.getColumn(1).getName());
        Assertions.assertEquals("TxnStatus", metaData.getColumn(2).getName());
        Assertions.assertEquals("JobId", metaData.getColumn(3).getName());
        Assertions.assertEquals("CreateTime", metaData.getColumn(4).getName());
        Assertions.assertEquals("LastScheduledTime", metaData.getColumn(5).getName());
        Assertions.assertEquals("ExecuteStartTime", metaData.getColumn(6).getName());
        Assertions.assertEquals("Timeout", metaData.getColumn(7).getName());
        Assertions.assertEquals("BeId", metaData.getColumn(8).getName());
        Assertions.assertEquals("DataSourceProperties", metaData.getColumn(9).getName());
        Assertions.assertEquals("Message", metaData.getColumn(10).getName());
    }

    @Test
    public void testShowResourceGroupUsageStmt() {
        ShowResourceGroupUsageStmt stmt = new ShowResourceGroupUsageStmt("test_group", null);
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        Assertions.assertEquals(6, metaData.getColumnCount());
        Assertions.assertEquals("Name", metaData.getColumn(0).getName());
        Assertions.assertEquals("Id", metaData.getColumn(1).getName());
        Assertions.assertEquals("Backend", metaData.getColumn(2).getName());
        Assertions.assertEquals("BEInUseCpuCores", metaData.getColumn(3).getName());
        Assertions.assertEquals("BEInUseMemBytes", metaData.getColumn(4).getName());
        Assertions.assertEquals("BERunningQueries", metaData.getColumn(5).getName());
    }

    @Test
    public void testShowEventsStmt() {
        ShowEventsStmt stmt = new ShowEventsStmt(NodePosition.ZERO);
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        Assertions.assertEquals(15, metaData.getColumnCount());
        Assertions.assertEquals("Db", metaData.getColumn(0).getName());
        Assertions.assertEquals("Name", metaData.getColumn(1).getName());
        Assertions.assertEquals("Definer", metaData.getColumn(2).getName());
        Assertions.assertEquals("Time", metaData.getColumn(3).getName());
        Assertions.assertEquals("Type", metaData.getColumn(4).getName());
        Assertions.assertEquals("Execute at", metaData.getColumn(5).getName());
        Assertions.assertEquals("Interval value", metaData.getColumn(6).getName());
        Assertions.assertEquals("Interval field", metaData.getColumn(7).getName());
        Assertions.assertEquals("Status", metaData.getColumn(8).getName());
        Assertions.assertEquals("Ends", metaData.getColumn(9).getName());
        Assertions.assertEquals("Status", metaData.getColumn(10).getName());
        Assertions.assertEquals("Originator", metaData.getColumn(11).getName());
        Assertions.assertEquals("character_set_client", metaData.getColumn(12).getName());
        Assertions.assertEquals("collation_connection", metaData.getColumn(13).getName());
        Assertions.assertEquals("Database Collation", metaData.getColumn(14).getName());
    }

    @Test
    public void testShowBasicStatsMetaStmt() {
        ShowBasicStatsMetaStmt stmt = new ShowBasicStatsMetaStmt(null, null, null, null);
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        Assertions.assertEquals(11, metaData.getColumnCount());
        Assertions.assertEquals("Database", metaData.getColumn(0).getName());
        Assertions.assertEquals("Table", metaData.getColumn(1).getName());
        Assertions.assertEquals("Columns", metaData.getColumn(2).getName());
        Assertions.assertEquals("Type", metaData.getColumn(3).getName());
        Assertions.assertEquals("UpdateTime", metaData.getColumn(4).getName());
        Assertions.assertEquals("Properties", metaData.getColumn(5).getName());
        Assertions.assertEquals("Healthy", metaData.getColumn(6).getName());
        Assertions.assertEquals("ColumnStats", metaData.getColumn(7).getName());
        Assertions.assertEquals("TabletStatsReportTime", metaData.getColumn(8).getName());
        Assertions.assertEquals("TableHealthyMetrics", metaData.getColumn(9).getName());
        Assertions.assertEquals("TableUpdateTime", metaData.getColumn(10).getName());
    }

    @Test
    public void testShowDataStmt() {
        ShowDataStmt stmt = new ShowDataStmt("test_db", "test_table");
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        // ShowDataStmt uses different metadata based on whether it has a table name
        // For table data, it should have 5 columns
        Assertions.assertEquals(5, metaData.getColumnCount());
        Assertions.assertEquals("TableName", metaData.getColumn(0).getName());
        Assertions.assertEquals("IndexName", metaData.getColumn(1).getName());
        Assertions.assertEquals("Size", metaData.getColumn(2).getName());
        Assertions.assertEquals("ReplicaCount", metaData.getColumn(3).getName());
        Assertions.assertEquals("RowCount", metaData.getColumn(4).getName());
    }

    @Test
    public void testShowDictionaryStmt() {
        ShowDictionaryStmt stmt = new ShowDictionaryStmt("test_db", NodePosition.ZERO);
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        Assertions.assertEquals(13, metaData.getColumnCount());
        Assertions.assertEquals("DictionaryId", metaData.getColumn(0).getName());
        Assertions.assertEquals("DictionaryName", metaData.getColumn(1).getName());
        Assertions.assertEquals("CatalogName", metaData.getColumn(2).getName());
        Assertions.assertEquals("DbName", metaData.getColumn(3).getName());
        Assertions.assertEquals("dictionaryObject", metaData.getColumn(4).getName());
        Assertions.assertEquals("dictionaryKeys", metaData.getColumn(5).getName());
        Assertions.assertEquals("dictionaryValues", metaData.getColumn(6).getName());
        Assertions.assertEquals("status", metaData.getColumn(7).getName());
        Assertions.assertEquals("lastSuccessRefreshTime", metaData.getColumn(8).getName());
        Assertions.assertEquals("lastSuccessFinishedTime", metaData.getColumn(9).getName());
        Assertions.assertEquals("nextSchedulableTime", metaData.getColumn(10).getName());
        Assertions.assertEquals("ErrorMessage", metaData.getColumn(11).getName());
        Assertions.assertEquals("approximated dictionaryMemoryUsage (Bytes)", metaData.getColumn(12).getName());
    }

    @Test
    public void testShowSqlBlackListStmt() {
        ShowSqlBlackListStmt stmt = new ShowSqlBlackListStmt();
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        Assertions.assertEquals(2, metaData.getColumnCount());
        Assertions.assertEquals("Id", metaData.getColumn(0).getName());
        Assertions.assertEquals("Forbidden SQL", metaData.getColumn(1).getName());
    }

    @Test
    public void testShowDataDistributionStmt() {
        TableRef tableRef = new TableRef(QualifiedName.of(List.of("test_db", "test_table")), null, NodePosition.ZERO);
        ShowDataDistributionStmt stmt = new ShowDataDistributionStmt(tableRef);
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        Assertions.assertEquals(8, metaData.getColumnCount());
        Assertions.assertEquals("PartitionName", metaData.getColumn(0).getName());
        Assertions.assertEquals("SubPartitionId", metaData.getColumn(1).getName());
        Assertions.assertEquals("MaterializedIndexName", metaData.getColumn(2).getName());
        Assertions.assertEquals("VirtualBuckets", metaData.getColumn(3).getName());
        Assertions.assertEquals("RowCount", metaData.getColumn(4).getName());
        Assertions.assertEquals("RowCount%", metaData.getColumn(5).getName());
        Assertions.assertEquals("DataSize", metaData.getColumn(6).getName());
        Assertions.assertEquals("DataSize%", metaData.getColumn(7).getName());
    }

    @Test
    public void testAdminShowReplicaStatusStmt() {
        TableName tableName = new TableName("test_db", "test_table");
        TableRefPersist tableRef = new TableRefPersist(tableName, null, null, NodePosition.ZERO);
        AdminShowReplicaStatusStmt stmt = new AdminShowReplicaStatusStmt(tableRef, null);
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        Assertions.assertEquals(13, metaData.getColumnCount());
        Assertions.assertEquals("TabletId", metaData.getColumn(0).getName());
        Assertions.assertEquals("ReplicaId", metaData.getColumn(1).getName());
        Assertions.assertEquals("BackendId", metaData.getColumn(2).getName());
        Assertions.assertEquals("Version", metaData.getColumn(3).getName());
        Assertions.assertEquals("LastFailedVersion", metaData.getColumn(4).getName());
        Assertions.assertEquals("LastSuccessVersion", metaData.getColumn(5).getName());
        Assertions.assertEquals("CommittedVersion", metaData.getColumn(6).getName());
        Assertions.assertEquals("SchemaHash", metaData.getColumn(7).getName());
        Assertions.assertEquals("VersionNum", metaData.getColumn(8).getName());
        Assertions.assertEquals("IsBad", metaData.getColumn(9).getName());
        Assertions.assertEquals("IsSetBadForce", metaData.getColumn(10).getName());
        Assertions.assertEquals("State", metaData.getColumn(11).getName());
        Assertions.assertEquals("Status", metaData.getColumn(12).getName());
    }

    @Test
    public void testShowAlterStmt() {
        ShowAlterStmt stmt = new ShowAlterStmt(ShowAlterStmt.AlterType.COLUMN, "test_db", null, null, null, NodePosition.ZERO);
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        // SchemaChangeProcDir.TITLE_NAMES has different lengths based on RunMode
        Assertions.assertEquals(13, metaData.getColumnCount());
        // Check some common column names
        Assertions.assertEquals("JobId", metaData.getColumn(0).getName());
        Assertions.assertEquals("TableName", metaData.getColumn(1).getName());
        Assertions.assertEquals("CreateTime", metaData.getColumn(2).getName());
        Assertions.assertEquals("FinishTime", metaData.getColumn(3).getName());
        Assertions.assertEquals("IndexName", metaData.getColumn(4).getName());
    }

    @Test
    public void testShowTriggersStmt() {
        ShowTriggersStmt stmt = new ShowTriggersStmt(NodePosition.ZERO);
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        Assertions.assertEquals(11, metaData.getColumnCount());
        Assertions.assertEquals("Trigger", metaData.getColumn(0).getName());
        Assertions.assertEquals("Event", metaData.getColumn(1).getName());
        Assertions.assertEquals("Table", metaData.getColumn(2).getName());
        Assertions.assertEquals("Statement", metaData.getColumn(3).getName());
        Assertions.assertEquals("Timing", metaData.getColumn(4).getName());
        Assertions.assertEquals("Created", metaData.getColumn(5).getName());
        Assertions.assertEquals("sql_mode", metaData.getColumn(6).getName());
        Assertions.assertEquals("Definer", metaData.getColumn(7).getName());
        Assertions.assertEquals("character_set_client", metaData.getColumn(8).getName());
        Assertions.assertEquals("collation_connection", metaData.getColumn(9).getName());
        Assertions.assertEquals("Database Collation", metaData.getColumn(10).getName());
    }

    @Test
    public void testShowMaterializedViewsStmt() {
        ShowMaterializedViewsStmt stmt = new ShowMaterializedViewsStmt("test_db", null);
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        Assertions.assertEquals(27, metaData.getColumnCount());
        Assertions.assertEquals("id", metaData.getColumn(0).getName());
        Assertions.assertEquals("database_name", metaData.getColumn(1).getName());
        Assertions.assertEquals("name", metaData.getColumn(2).getName());
        Assertions.assertEquals("refresh_type", metaData.getColumn(3).getName());
        Assertions.assertEquals("is_active", metaData.getColumn(4).getName());
        Assertions.assertEquals("inactive_reason", metaData.getColumn(5).getName());
        Assertions.assertEquals("partition_type", metaData.getColumn(6).getName());
        Assertions.assertEquals("task_id", metaData.getColumn(7).getName());
        Assertions.assertEquals("task_name", metaData.getColumn(8).getName());
        Assertions.assertEquals("last_refresh_start_time", metaData.getColumn(9).getName());
        Assertions.assertEquals("last_refresh_finished_time", metaData.getColumn(10).getName());
        Assertions.assertEquals("last_refresh_duration", metaData.getColumn(11).getName());
        Assertions.assertEquals("last_refresh_state", metaData.getColumn(12).getName());
        Assertions.assertEquals("last_refresh_force_refresh", metaData.getColumn(13).getName());
        Assertions.assertEquals("last_refresh_start_partition", metaData.getColumn(14).getName());
        Assertions.assertEquals("last_refresh_end_partition", metaData.getColumn(15).getName());
        Assertions.assertEquals("last_refresh_base_refresh_partitions", metaData.getColumn(16).getName());
        Assertions.assertEquals("last_refresh_mv_refresh_partitions", metaData.getColumn(17).getName());
        Assertions.assertEquals("last_refresh_error_code", metaData.getColumn(18).getName());
        Assertions.assertEquals("last_refresh_error_message", metaData.getColumn(19).getName());
        Assertions.assertEquals("rows", metaData.getColumn(20).getName());
        Assertions.assertEquals("text", metaData.getColumn(21).getName());
        Assertions.assertEquals("extra_message", metaData.getColumn(22).getName());
        Assertions.assertEquals("query_rewrite_status", metaData.getColumn(23).getName());
        Assertions.assertEquals("creator", metaData.getColumn(24).getName());
        Assertions.assertEquals("last_refresh_process_time", metaData.getColumn(25).getName());
        Assertions.assertEquals("last_refresh_job_id", metaData.getColumn(26).getName());
    }

    @Test
    public void testShowResourceGroupStmt() {
        ShowResourceGroupStmt stmt = new ShowResourceGroupStmt("test_group", false, false, NodePosition.ZERO);
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        Assertions.assertEquals(11, metaData.getColumnCount());
        Assertions.assertEquals("name", metaData.getColumn(0).getName());
        Assertions.assertEquals("id", metaData.getColumn(1).getName());
        Assertions.assertEquals("cpu_weight", metaData.getColumn(2).getName());
        Assertions.assertEquals("exclusive_cpu_cores", metaData.getColumn(3).getName());
        Assertions.assertEquals("mem_limit", metaData.getColumn(4).getName());
        Assertions.assertEquals("big_query_cpu_second_limit", metaData.getColumn(5).getName());
        Assertions.assertEquals("big_query_scan_rows_limit", metaData.getColumn(6).getName());
        Assertions.assertEquals("big_query_mem_limit", metaData.getColumn(7).getName());
        Assertions.assertEquals("concurrency_limit", metaData.getColumn(8).getName());
        Assertions.assertEquals("spill_mem_limit_threshold", metaData.getColumn(9).getName());
        Assertions.assertEquals("classifiers", metaData.getColumn(10).getName());
    }

    @Test
    public void testShowStorageVolumesStmt() {
        ShowStorageVolumesStmt stmt = new ShowStorageVolumesStmt("test_volume", NodePosition.ZERO);
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        Assertions.assertEquals(1, metaData.getColumnCount());
        Assertions.assertEquals("Storage Volume", metaData.getColumn(0).getName());
    }

    @Test
    public void testShowExportStmt() {
        ShowExportStmt stmt = new ShowExportStmt("test_db", null, null, null);
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        Assertions.assertEquals(11, metaData.getColumnCount());
        Assertions.assertEquals("JobId", metaData.getColumn(0).getName());
        Assertions.assertEquals("QueryId", metaData.getColumn(1).getName());
        Assertions.assertEquals("State", metaData.getColumn(2).getName());
        Assertions.assertEquals("Progress", metaData.getColumn(3).getName());
        Assertions.assertEquals("TaskInfo", metaData.getColumn(4).getName());
        Assertions.assertEquals("Path", metaData.getColumn(5).getName());
        Assertions.assertEquals("CreateTime", metaData.getColumn(6).getName());
        Assertions.assertEquals("StartTime", metaData.getColumn(7).getName());
        Assertions.assertEquals("FinishTime", metaData.getColumn(8).getName());
        Assertions.assertEquals("Timeout", metaData.getColumn(9).getName());
        Assertions.assertEquals("ErrorMsg", metaData.getColumn(10).getName());
    }

    @Test
    public void testShowUserPropertyStmt() {
        ShowUserPropertyStmt stmt = new ShowUserPropertyStmt(null, null);
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        Assertions.assertEquals(2, metaData.getColumnCount());
        Assertions.assertEquals("Key", metaData.getColumn(0).getName());
        Assertions.assertEquals("Value", metaData.getColumn(1).getName());
    }

    @Test
    public void testShowRepositoriesStmt() {
        ShowRepositoriesStmt stmt = new ShowRepositoriesStmt();
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        Assertions.assertEquals(7, metaData.getColumnCount());
        Assertions.assertEquals("RepoId", metaData.getColumn(0).getName());
        Assertions.assertEquals("RepoName", metaData.getColumn(1).getName());
        Assertions.assertEquals("CreateTime", metaData.getColumn(2).getName());
        Assertions.assertEquals("IsReadOnly", metaData.getColumn(3).getName());
        Assertions.assertEquals("Location", metaData.getColumn(4).getName());
        Assertions.assertEquals("Broker", metaData.getColumn(5).getName());
        Assertions.assertEquals("ErrMsg", metaData.getColumn(6).getName());
    }

    @Test
    public void testShowCreateExternalCatalogStmt() {
        ShowCreateExternalCatalogStmt stmt = new ShowCreateExternalCatalogStmt("test_catalog");
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        Assertions.assertEquals(2, metaData.getColumnCount());
        Assertions.assertEquals("Catalog", metaData.getColumn(0).getName());
        Assertions.assertEquals("Create Catalog", metaData.getColumn(1).getName());
    }

    @Test
    public void testAdminShowReplicaDistributionStmt() {
        TableName tableName = new TableName("test_db", "test_table");
        TableRefPersist tableRef = new TableRefPersist(tableName, null, null, NodePosition.ZERO);
        AdminShowReplicaDistributionStmt stmt = new AdminShowReplicaDistributionStmt(tableRef);
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        Assertions.assertEquals(4, metaData.getColumnCount());
        Assertions.assertEquals("BackendId", metaData.getColumn(0).getName());
        Assertions.assertEquals("ReplicaNum", metaData.getColumn(1).getName());
        Assertions.assertEquals("Graph", metaData.getColumn(2).getName());
        Assertions.assertEquals("Percent", metaData.getColumn(3).getName());
    }

    @Test
    public void testShowGroupProvidersStmt() {
        ShowGroupProvidersStmt stmt = new ShowGroupProvidersStmt(NodePosition.ZERO);
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        Assertions.assertEquals(3, metaData.getColumnCount());
        Assertions.assertEquals("Name", metaData.getColumn(0).getName());
        Assertions.assertEquals("Type", metaData.getColumn(1).getName());
        Assertions.assertEquals("Comment", metaData.getColumn(2).getName());
    }

    @Test
    public void testShowTransactionStmt() {
        ShowTransactionStmt stmt = new ShowTransactionStmt("test_db", null, NodePosition.ZERO);
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        Assertions.assertEquals(16, metaData.getColumnCount());
        Assertions.assertEquals("TransactionId", metaData.getColumn(0).getName());
        Assertions.assertEquals("Label", metaData.getColumn(1).getName());
        Assertions.assertEquals("Coordinator", metaData.getColumn(2).getName());
        Assertions.assertEquals("TransactionStatus", metaData.getColumn(3).getName());
        Assertions.assertEquals("LoadJobSourceType", metaData.getColumn(4).getName());
        Assertions.assertEquals("PrepareTime", metaData.getColumn(5).getName());
        Assertions.assertEquals("PreparedTime", metaData.getColumn(6).getName());
        Assertions.assertEquals("CommitTime", metaData.getColumn(7).getName());
        Assertions.assertEquals("PublishTime", metaData.getColumn(8).getName());
        Assertions.assertEquals("FinishTime", metaData.getColumn(9).getName());
        Assertions.assertEquals("Reason", metaData.getColumn(10).getName());
        Assertions.assertEquals("ErrorReplicasCount", metaData.getColumn(11).getName());
        Assertions.assertEquals("ListenerId", metaData.getColumn(12).getName());
        Assertions.assertEquals("TimeoutMs", metaData.getColumn(13).getName());
        Assertions.assertEquals("PreparedTimeoutMs", metaData.getColumn(14).getName());
        Assertions.assertEquals("ErrMsg", metaData.getColumn(15).getName());
    }

    @Test
    public void testShowOpenTableStmt() {
        ShowOpenTableStmt stmt = new ShowOpenTableStmt(NodePosition.ZERO);
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        Assertions.assertEquals(4, metaData.getColumnCount());
        Assertions.assertEquals("Database", metaData.getColumn(0).getName());
        Assertions.assertEquals("Table", metaData.getColumn(1).getName());
        Assertions.assertEquals("In_use", metaData.getColumn(2).getName());
        Assertions.assertEquals("Name_locked", metaData.getColumn(3).getName());
    }

    @Test
    public void testShowComputeNodesStmt() {
        ShowComputeNodesStmt stmt = new ShowComputeNodesStmt();
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        // ComputeNodeProcDir.getMetadata() returns different column counts based on RunMode
        Assertions.assertEquals(21, metaData.getColumnCount());
        // Check some common column names
        Assertions.assertEquals("ComputeNodeId", metaData.getColumn(0).getName());
        Assertions.assertEquals("IP", metaData.getColumn(1).getName());
        Assertions.assertEquals("HeartbeatPort", metaData.getColumn(2).getName());
        Assertions.assertEquals("BePort", metaData.getColumn(3).getName());
        Assertions.assertEquals("HttpPort", metaData.getColumn(4).getName());
        Assertions.assertEquals("BrpcPort", metaData.getColumn(5).getName());
        Assertions.assertEquals("LastStartTime", metaData.getColumn(6).getName());
        Assertions.assertEquals("LastHeartbeat", metaData.getColumn(7).getName());
        Assertions.assertEquals("Alive", metaData.getColumn(8).getName());
        Assertions.assertEquals("SystemDecommissioned", metaData.getColumn(9).getName());
        Assertions.assertEquals("ClusterDecommissioned", metaData.getColumn(10).getName());
        Assertions.assertEquals("ErrMsg", metaData.getColumn(11).getName());
        Assertions.assertEquals("Version", metaData.getColumn(12).getName());
        Assertions.assertEquals("CpuCores", metaData.getColumn(13).getName());
        Assertions.assertEquals("MemLimit", metaData.getColumn(14).getName());
        Assertions.assertEquals("NumRunningQueries", metaData.getColumn(15).getName());
        Assertions.assertEquals("MemUsedPct", metaData.getColumn(16).getName());
        Assertions.assertEquals("CpuUsedPct", metaData.getColumn(17).getName());
        Assertions.assertEquals("DataCacheMetrics", metaData.getColumn(18).getName());
        Assertions.assertEquals("HasStoragePath", metaData.getColumn(19).getName());
        Assertions.assertEquals("StatusCode", metaData.getColumn(20).getName());
    }

    @Test
    public void testShowProcStmt() {
        ShowProcStmt stmt = new ShowProcStmt("/test/path");
        // ShowProcStmt requires a ProcNodeInterface to be set before getMetaData() can work
        // For now, we'll just test that the statement can be created without errors
        Assertions.assertNotNull(stmt);
        Assertions.assertEquals("/test/path", stmt.getPath());
    }

    @Test
    public void testShowProcedureStmt() {
        ShowProcedureStmt stmt = new ShowProcedureStmt("test_db", null, NodePosition.ZERO);
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        Assertions.assertEquals(11, metaData.getColumnCount());
        Assertions.assertEquals("Db", metaData.getColumn(0).getName());
        Assertions.assertEquals("Name", metaData.getColumn(1).getName());
        Assertions.assertEquals("Type", metaData.getColumn(2).getName());
        Assertions.assertEquals("Definer", metaData.getColumn(3).getName());
        Assertions.assertEquals("Modified", metaData.getColumn(4).getName());
        Assertions.assertEquals("Created", metaData.getColumn(5).getName());
        Assertions.assertEquals("Security_type", metaData.getColumn(6).getName());
        Assertions.assertEquals("Comment", metaData.getColumn(7).getName());
        Assertions.assertEquals("character_set_client", metaData.getColumn(8).getName());
        Assertions.assertEquals("collation_connection", metaData.getColumn(9).getName());
        Assertions.assertEquals("Database Collation", metaData.getColumn(10).getName());
    }

    @Test
    public void testShowStreamLoadStmt() {
        ShowStreamLoadStmt stmt = new ShowStreamLoadStmt(new LabelName("test_db", "test_label"), false);
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        Assertions.assertEquals(25, metaData.getColumnCount());
        Assertions.assertEquals("Label", metaData.getColumn(0).getName());
        Assertions.assertEquals("Id", metaData.getColumn(1).getName());
        Assertions.assertEquals("LoadId", metaData.getColumn(2).getName());
        Assertions.assertEquals("TxnId", metaData.getColumn(3).getName());
        Assertions.assertEquals("DbName", metaData.getColumn(4).getName());
        Assertions.assertEquals("TableName", metaData.getColumn(5).getName());
        Assertions.assertEquals("State", metaData.getColumn(6).getName());
        Assertions.assertEquals("ErrorMsg", metaData.getColumn(7).getName());
        Assertions.assertEquals("TrackingURL", metaData.getColumn(8).getName());
        Assertions.assertEquals("ChannelNum", metaData.getColumn(9).getName());
        Assertions.assertEquals("PreparedChannelNum", metaData.getColumn(10).getName());
        Assertions.assertEquals("NumRowsNormal", metaData.getColumn(11).getName());
        Assertions.assertEquals("NumRowsAbNormal", metaData.getColumn(12).getName());
        Assertions.assertEquals("NumRowsUnselected", metaData.getColumn(13).getName());
        Assertions.assertEquals("NumLoadBytes", metaData.getColumn(14).getName());
        Assertions.assertEquals("TimeoutSecond", metaData.getColumn(15).getName());
        Assertions.assertEquals("CreateTimeMs", metaData.getColumn(16).getName());
        Assertions.assertEquals("BeforeLoadTimeMs", metaData.getColumn(17).getName());
        Assertions.assertEquals("StartLoadingTimeMs", metaData.getColumn(18).getName());
        Assertions.assertEquals("StartPreparingTimeMs", metaData.getColumn(19).getName());
        Assertions.assertEquals("FinishPreparingTimeMs", metaData.getColumn(20).getName());
        Assertions.assertEquals("EndTimeMs", metaData.getColumn(21).getName());
        Assertions.assertEquals("ChannelState", metaData.getColumn(22).getName());
        Assertions.assertEquals("Type", metaData.getColumn(23).getName());
        Assertions.assertEquals("TrackingSQL", metaData.getColumn(24).getName());
    }

    @Test
    public void testShowTabletStmt() {
        // Test single tablet mode (isShowSingleTablet = true)
        ShowTabletStmt stmt = new ShowTabletStmt(null, 12345L, NodePosition.ZERO);
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        Assertions.assertEquals(10, metaData.getColumnCount());
        Assertions.assertEquals("DbName", metaData.getColumn(0).getName());
        Assertions.assertEquals("TableName", metaData.getColumn(1).getName());
        Assertions.assertEquals("PartitionName", metaData.getColumn(2).getName());
        Assertions.assertEquals("IndexName", metaData.getColumn(3).getName());
        Assertions.assertEquals("DbId", metaData.getColumn(4).getName());
        Assertions.assertEquals("TableId", metaData.getColumn(5).getName());
        Assertions.assertEquals("PartitionId", metaData.getColumn(6).getName());
        Assertions.assertEquals("IndexId", metaData.getColumn(7).getName());
        Assertions.assertEquals("IsSync", metaData.getColumn(8).getName());
        Assertions.assertEquals("DetailCmd", metaData.getColumn(9).getName());

        // Test with table name but no table set (should return empty list)
        TableName tableName = new TableName("test_db", "test_table");
        ShowTabletStmt stmt2 = new ShowTabletStmt(tableName, 12345L, NodePosition.ZERO);
        ShowResultSetMetaData metaData2 = new ShowResultMetaFactory().getMetadata(stmt2);
        Assertions.assertEquals(0, metaData2.getColumnCount());

        // Test basic statement properties
        Assertions.assertEquals("test_db", stmt2.getDbName());
        Assertions.assertEquals("test_table", stmt2.getTableName());
        Assertions.assertEquals(12345L, stmt2.getTabletId());
        Assertions.assertFalse(stmt2.isShowSingleTablet());
        Assertions.assertTrue(stmt.isShowSingleTablet());
    }

    @Test
    public void testShowPluginsStmt() {
        ShowPluginsStmt stmt = new ShowPluginsStmt();
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        Assertions.assertEquals(10, metaData.getColumnCount());
        Assertions.assertEquals("Name", metaData.getColumn(0).getName());
        Assertions.assertEquals("Type", metaData.getColumn(1).getName());
        Assertions.assertEquals("Description", metaData.getColumn(2).getName());
        Assertions.assertEquals("Version", metaData.getColumn(3).getName());
        Assertions.assertEquals("JavaVersion", metaData.getColumn(4).getName());
        Assertions.assertEquals("ClassName", metaData.getColumn(5).getName());
        Assertions.assertEquals("SoName", metaData.getColumn(6).getName());
        Assertions.assertEquals("Sources", metaData.getColumn(7).getName());
        Assertions.assertEquals("Status", metaData.getColumn(8).getName());
        Assertions.assertEquals("Properties", metaData.getColumn(9).getName());
    }

    @Test
    public void testShowMultiColumnStatsMetaStmt() {
        ShowMultiColumnStatsMetaStmt stmt = new ShowMultiColumnStatsMetaStmt(null, null, null, null);
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        Assertions.assertEquals(7, metaData.getColumnCount());
        Assertions.assertEquals("Database", metaData.getColumn(0).getName());
        Assertions.assertEquals("Table", metaData.getColumn(1).getName());
        Assertions.assertEquals("Columns", metaData.getColumn(2).getName());
        Assertions.assertEquals("Type", metaData.getColumn(3).getName());
        Assertions.assertEquals("StatisticsTypes", metaData.getColumn(4).getName());
        Assertions.assertEquals("UpdateTime", metaData.getColumn(5).getName());
        Assertions.assertEquals("Properties", metaData.getColumn(6).getName());
    }

    @Test
    public void testShowProfilelistStmt() {
        ShowProfilelistStmt stmt = new ShowProfilelistStmt(10, NodePosition.ZERO);
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        Assertions.assertEquals(5, metaData.getColumnCount());
        Assertions.assertEquals("QueryId", metaData.getColumn(0).getName());
        Assertions.assertEquals("StartTime", metaData.getColumn(1).getName());
        Assertions.assertEquals("Time", metaData.getColumn(2).getName());
        Assertions.assertEquals("State", metaData.getColumn(3).getName());
        Assertions.assertEquals("Statement", metaData.getColumn(4).getName());
    }

    @Test
    public void testShowSnapshotStmt() {
        ShowSnapshotStmt stmt = new ShowSnapshotStmt("test_db", null, NodePosition.ZERO);
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        Assertions.assertEquals(3, metaData.getColumnCount());
        Assertions.assertEquals("Snapshot", metaData.getColumn(0).getName());
        Assertions.assertEquals("Timestamp", metaData.getColumn(1).getName());
        Assertions.assertEquals("Status", metaData.getColumn(2).getName());
    }

    @Test
    public void testShowHistogramStatsMetaStmt() {
        ShowHistogramStatsMetaStmt stmt = new ShowHistogramStatsMetaStmt(null, null, null, null);
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        Assertions.assertEquals(6, metaData.getColumnCount());
        Assertions.assertEquals("Database", metaData.getColumn(0).getName());
        Assertions.assertEquals("Table", metaData.getColumn(1).getName());
        Assertions.assertEquals("Column", metaData.getColumn(2).getName());
        Assertions.assertEquals("Type", metaData.getColumn(3).getName());
        Assertions.assertEquals("UpdateTime", metaData.getColumn(4).getName());
        Assertions.assertEquals("Properties", metaData.getColumn(5).getName());
    }

    @Test
    public void testShowRunningQueriesStmt() {
        ShowRunningQueriesStmt stmt = new ShowRunningQueriesStmt(10, NodePosition.ZERO);
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        Assertions.assertEquals(12, metaData.getColumnCount());
        Assertions.assertEquals("QueryId", metaData.getColumn(0).getName());
        Assertions.assertEquals("WarehouseId", metaData.getColumn(1).getName());
        Assertions.assertEquals("ResourceGroupId", metaData.getColumn(2).getName());
        Assertions.assertEquals("StartTime", metaData.getColumn(3).getName());
        Assertions.assertEquals("PendingTimeout", metaData.getColumn(4).getName());
        Assertions.assertEquals("QueryTimeout", metaData.getColumn(5).getName());
        Assertions.assertEquals("State", metaData.getColumn(6).getName());
        Assertions.assertEquals("Slots", metaData.getColumn(7).getName());
        Assertions.assertEquals("Fragments", metaData.getColumn(8).getName());
        Assertions.assertEquals("DOP", metaData.getColumn(9).getName());
        Assertions.assertEquals("Frontend", metaData.getColumn(10).getName());
        Assertions.assertEquals("FeStartTime", metaData.getColumn(11).getName());
    }

    @Test
    public void testShowBackendBlackListStmt() {
        ShowBackendBlackListStmt stmt = new ShowBackendBlackListStmt(null);
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        Assertions.assertEquals(5, metaData.getColumnCount());
        Assertions.assertEquals("BackendId", metaData.getColumn(0).getName());
        Assertions.assertEquals("AddBlackListType", metaData.getColumn(1).getName());
        Assertions.assertEquals("LostConnectionTime", metaData.getColumn(2).getName());
        Assertions.assertEquals("LostConnectionNumberInPeriod", metaData.getColumn(3).getName());
        Assertions.assertEquals("CheckTimePeriod(s)", metaData.getColumn(4).getName());
    }

    @Test
    public void testShowClustersStmt() {
        ShowClustersStmt stmt = new ShowClustersStmt("test_cluster", NodePosition.ZERO);
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        Assertions.assertEquals(8, metaData.getColumnCount());
        Assertions.assertEquals("CNGroupId", metaData.getColumn(0).getName());
        Assertions.assertEquals("CNGroupName", metaData.getColumn(1).getName());
        Assertions.assertEquals("WorkerGroupId", metaData.getColumn(2).getName());
        Assertions.assertEquals("ComputeNodeIds", metaData.getColumn(3).getName());
        Assertions.assertEquals("Pending", metaData.getColumn(4).getName());
        Assertions.assertEquals("Running", metaData.getColumn(5).getName());
        Assertions.assertEquals("Enabled", metaData.getColumn(6).getName());
        Assertions.assertEquals("Properties", metaData.getColumn(7).getName());
    }

    @Test
    public void testShowNodesStmt() {
        ShowNodesStmt stmt = new ShowNodesStmt("test_cluster", "test_warehouse", "test_node", NodePosition.ZERO);
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        Assertions.assertEquals(21, metaData.getColumnCount());
        Assertions.assertEquals("WarehouseName", metaData.getColumn(0).getName());
        Assertions.assertEquals("CNGroupId", metaData.getColumn(1).getName());
        Assertions.assertEquals("WorkerGroupId", metaData.getColumn(2).getName());
        Assertions.assertEquals("NodeId", metaData.getColumn(3).getName());
        Assertions.assertEquals("WorkerId", metaData.getColumn(4).getName());
        Assertions.assertEquals("IP", metaData.getColumn(5).getName());
        Assertions.assertEquals("HeartbeatPort", metaData.getColumn(6).getName());
        Assertions.assertEquals("BePort", metaData.getColumn(7).getName());
        Assertions.assertEquals("HttpPort", metaData.getColumn(8).getName());
        Assertions.assertEquals("BrpcPort", metaData.getColumn(9).getName());
        Assertions.assertEquals("StarletPort", metaData.getColumn(10).getName());
        Assertions.assertEquals("LastStartTime", metaData.getColumn(11).getName());
        Assertions.assertEquals("LastUpdateMs", metaData.getColumn(12).getName());
        Assertions.assertEquals("Alive", metaData.getColumn(13).getName());
        Assertions.assertEquals("ErrMsg", metaData.getColumn(14).getName());
        Assertions.assertEquals("Version", metaData.getColumn(15).getName());
        Assertions.assertEquals("NumRunningQueries", metaData.getColumn(16).getName());
        Assertions.assertEquals("CpuCores", metaData.getColumn(17).getName());
        Assertions.assertEquals("MemUsedPct", metaData.getColumn(18).getName());
        Assertions.assertEquals("CpuUsedPct", metaData.getColumn(19).getName());
        Assertions.assertEquals("CNGroupName", metaData.getColumn(20).getName());
    }

    @Test
    public void testShowWarehousesStmt() {
        ShowWarehousesStmt stmt = new ShowWarehousesStmt("test_warehouse", NodePosition.ZERO);
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        Assertions.assertEquals(14, metaData.getColumnCount());
        Assertions.assertEquals("Id", metaData.getColumn(0).getName());
        Assertions.assertEquals("Name", metaData.getColumn(1).getName());
        Assertions.assertEquals("State", metaData.getColumn(2).getName());
        Assertions.assertEquals("NodeCount", metaData.getColumn(3).getName());
        Assertions.assertEquals("CurrentClusterCount", metaData.getColumn(4).getName());
        Assertions.assertEquals("MaxClusterCount", metaData.getColumn(5).getName());
        Assertions.assertEquals("StartedClusters", metaData.getColumn(6).getName());
        Assertions.assertEquals("RunningSql", metaData.getColumn(7).getName());
        Assertions.assertEquals("QueuedSql", metaData.getColumn(8).getName());
        Assertions.assertEquals("CreatedOn", metaData.getColumn(9).getName());
        Assertions.assertEquals("ResumedOn", metaData.getColumn(10).getName());
        Assertions.assertEquals("UpdatedOn", metaData.getColumn(11).getName());
        Assertions.assertEquals("Property", metaData.getColumn(12).getName());
        Assertions.assertEquals("Comment", metaData.getColumn(13).getName());
    }

    @Test
    public void testShowCreateGroupProviderStmt() {
        ShowCreateGroupProviderStmt stmt = new ShowCreateGroupProviderStmt("test_provider", NodePosition.ZERO);
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        Assertions.assertEquals(2, metaData.getColumnCount());
        Assertions.assertEquals("Group Provider", metaData.getColumn(0).getName());
        Assertions.assertEquals("Create Group Provider", metaData.getColumn(1).getName());
    }

    @Test
    public void testShowPipeStmt() {
        ShowPipeStmt stmt = new ShowPipeStmt("test_db", "test_pipe", null, null, null, NodePosition.ZERO);
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        Assertions.assertEquals(8, metaData.getColumnCount());
        Assertions.assertEquals("DATABASE_NAME", metaData.getColumn(0).getName());
        Assertions.assertEquals("PIPE_ID", metaData.getColumn(1).getName());
        Assertions.assertEquals("PIPE_NAME", metaData.getColumn(2).getName());
        Assertions.assertEquals("STATE", metaData.getColumn(3).getName());
        Assertions.assertEquals("TABLE_NAME", metaData.getColumn(4).getName());
        Assertions.assertEquals("LOAD_STATUS", metaData.getColumn(5).getName());
        Assertions.assertEquals("LAST_ERROR", metaData.getColumn(6).getName());
        Assertions.assertEquals("CREATED_TIME", metaData.getColumn(7).getName());
    }

    @Test
    public void testShowBaselinePlanStmt() {
        ShowBaselinePlanStmt stmt = new ShowBaselinePlanStmt(NodePosition.ZERO, (Expr) null);
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        Assertions.assertEquals(11, metaData.getColumnCount());
        Assertions.assertEquals("Id", metaData.getColumn(0).getName());
        Assertions.assertEquals("global", metaData.getColumn(1).getName());
        Assertions.assertEquals("enable", metaData.getColumn(2).getName());
        Assertions.assertEquals("bindSQLDigest", metaData.getColumn(3).getName());
        Assertions.assertEquals("bindSQLHash", metaData.getColumn(4).getName());
        Assertions.assertEquals("bindSQL", metaData.getColumn(5).getName());
        Assertions.assertEquals("planSQL", metaData.getColumn(6).getName());
        Assertions.assertEquals("costs", metaData.getColumn(7).getName());
        Assertions.assertEquals("queryMs", metaData.getColumn(8).getName());
        Assertions.assertEquals("source", metaData.getColumn(9).getName());
        Assertions.assertEquals("updateTime", metaData.getColumn(10).getName());
    }

    @Test
    public void testShowFailPointStatement() {
        ShowFailPointStatement stmt = new ShowFailPointStatement(null, null, NodePosition.ZERO);
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        Assertions.assertEquals(4, metaData.getColumnCount());
        Assertions.assertEquals("Name", metaData.getColumn(0).getName());
        Assertions.assertEquals("TriggerMode", metaData.getColumn(1).getName());
        Assertions.assertEquals("Times/Probability", metaData.getColumn(2).getName());
        Assertions.assertEquals("Host", metaData.getColumn(3).getName());
    }

    @Test
    public void testShowTemporaryTableStmt() {
        ShowTemporaryTableStmt stmt = new ShowTemporaryTableStmt("test_db", null, null, null, NodePosition.ZERO);
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        Assertions.assertEquals(1, metaData.getColumnCount());
        Assertions.assertEquals("Tables_in_test_db", metaData.getColumn(0).getName());
    }

    @Test
    public void testShowCreateSecurityIntegrationStatement() {
        ShowCreateSecurityIntegrationStatement stmt =
                new ShowCreateSecurityIntegrationStatement("test_integration", NodePosition.ZERO);
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        Assertions.assertEquals(2, metaData.getColumnCount());
        Assertions.assertEquals("Security Integration", metaData.getColumn(0).getName());
        Assertions.assertEquals("Create Security Integration", metaData.getColumn(1).getName());
    }

    @Test
    public void testShowSecurityIntegrationStatement() {
        ShowSecurityIntegrationStatement stmt = new ShowSecurityIntegrationStatement(NodePosition.ZERO);
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        Assertions.assertEquals(3, metaData.getColumnCount());
        Assertions.assertEquals("Name", metaData.getColumn(0).getName());
        Assertions.assertEquals("Type", metaData.getColumn(1).getName());
        Assertions.assertEquals("Comment", metaData.getColumn(2).getName());
    }
}