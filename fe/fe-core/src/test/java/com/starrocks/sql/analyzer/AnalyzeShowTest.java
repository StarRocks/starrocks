// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.analyzer;

import com.starrocks.analysis.UserIdentity;
import com.starrocks.common.AnalysisException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.SetType;
import com.starrocks.sql.ast.ShowAuthenticationStmt;
import com.starrocks.sql.ast.ShowColumnStmt;
import com.starrocks.sql.ast.ShowPartitionsStmt;
import com.starrocks.sql.ast.ShowStmt;
import com.starrocks.sql.ast.ShowTableStatusStmt;
import com.starrocks.sql.ast.ShowTableStmt;
import com.starrocks.sql.ast.ShowVariablesStmt;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class AnalyzeShowTest {

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
    }

    @Test
    public void testShowVariables() throws AnalysisException {
        analyzeSuccess("show variables");
        ShowStmt statement = (ShowStmt) analyzeSuccess("show variables where variables_name = 't1'");
        Assert.assertEquals("SELECT VARIABLE_NAME AS Variable_name, VARIABLE_VALUE AS Value " +
                        "FROM information_schema.SESSION_VARIABLES WHERE variables_name = 't1'",
                AST2SQL.toString(statement.toSelectStmt()));

        ShowVariablesStmt stmt = (ShowVariablesStmt) analyzeSuccess("show global variables like 'abc'");
        Assert.assertEquals("abc", stmt.getPattern());
        Assert.assertEquals(stmt.getType(), SetType.GLOBAL);
    }

    @Test
    public void testShowTableStatus() throws AnalysisException {
        analyzeSuccess("show table status;");
        ShowTableStatusStmt statement =
                (ShowTableStatusStmt) analyzeSuccess("show table status where Name = 't1';");
        Assert.assertEquals("SELECT TABLE_NAME AS Name, ENGINE AS Engine, VERSION AS Version, " +
                        "ROW_FORMAT AS Row_format, TABLE_ROWS AS Rows, AVG_ROW_LENGTH AS Avg_row_length, " +
                        "DATA_LENGTH AS Data_length, MAX_DATA_LENGTH AS Max_data_length, INDEX_LENGTH AS Index_length, " +
                        "DATA_FREE AS Data_free, AUTO_INCREMENT AS Auto_increment, CREATE_TIME AS Create_time, " +
                        "UPDATE_TIME AS Update_time, CHECK_TIME AS Check_time, " +
                        "TABLE_COLLATION AS Collation, CHECKSUM AS Checksum, CREATE_OPTIONS AS Create_options, " +
                        "TABLE_COMMENT AS Comment FROM information_schema.tables WHERE TABLE_NAME = 't1'",
                AST2SQL.toString(statement.toSelectStmt()));
    }

    @Test
    public void testShowDatabases() throws AnalysisException {
        analyzeSuccess("show databases;");
        ShowStmt statement = (ShowStmt) analyzeSuccess("show databases where `database` = 't1';");
        Assert.assertEquals("SELECT SCHEMA_NAME AS Database FROM information_schema.schemata WHERE SCHEMA_NAME = 't1'",
                AST2SQL.toString(statement.toSelectStmt()));
    }

    @Test
    public void testShowTables() throws AnalysisException {
        analyzeSuccess("show tables;");
        ShowTableStmt statement = (ShowTableStmt) analyzeSuccess("show tables where table_name = 't1';");
        Assert.assertEquals(
                "SELECT TABLE_NAME AS Tables_in_test FROM information_schema.tables"
                        + " WHERE (TABLE_SCHEMA = 'test') AND (table_name = 't1')",
                AST2SQL.toString(statement.toSelectStmt()));

        statement = (ShowTableStmt) analyzeSuccess("show tables from `test`");
        Assert.assertEquals("test", statement.getDb());
    }

    @Test
    public void testShowColumns() throws AnalysisException {
        analyzeSuccess("show columns from t1;");
        ShowColumnStmt statement = (ShowColumnStmt) analyzeSuccess("show columns from t1 where Field = 'v1';");
        Assert.assertEquals("SELECT COLUMN_NAME AS Field, DATA_TYPE AS Type, IS_NULLABLE AS Null, " +
                        "COLUMN_KEY AS Key, COLUMN_DEFAULT AS Default, EXTRA AS Extra " +
                        "FROM information_schema.COLUMNS WHERE COLUMN_NAME = 'v1'",
                AST2SQL.toString(statement.toSelectStmt()));
    }

    @Test
    public void testShowAuthentication() throws AnalysisException {
        ConnectContext connectContext = ConnectContext.get();
        connectContext.setCurrentUserIdentity(UserIdentity.ROOT);

        String sql = "SHOW AUTHENTICATION;";
        ShowAuthenticationStmt stmt = (ShowAuthenticationStmt) analyzeSuccess(sql);
        Assert.assertFalse(stmt.isAll());
        Assert.assertEquals("root", stmt.getUserIdent().getQualifiedUser());

        sql = "SHOW ALL AUTHENTICATION;";
        stmt = (ShowAuthenticationStmt) analyzeSuccess(sql);
        Assert.assertTrue(stmt.isAll());
        Assert.assertNull(stmt.getUserIdent());

        sql = "SHOW AUTHENTICATION FOR xx";
        stmt = (ShowAuthenticationStmt) analyzeSuccess(sql);
        Assert.assertFalse(stmt.isAll());
        Assert.assertEquals("xx", stmt.getUserIdent().getQualifiedUser());
    }

    @Test
    public void testShowIndex() {
        analyzeSuccess("SHOW INDEX FROM `test`.`t0`");
    }

    @Test
    public void testShowPartitions() {
        analyzeSuccess("SHOW PARTITIONS FROM `test`.`t0`");
        ShowPartitionsStmt showPartitionsStmt = (ShowPartitionsStmt) analyzeSuccess(
                "SHOW PARTITIONS FROM `test`.`t0` " +
                        "WHERE `LastConsistencyCheckTime` > '2019-12-22 10:22:11'");
        Assert.assertEquals("LastConsistencyCheckTime > '2019-12-22 10:22:11'",
                AST2SQL.toString(showPartitionsStmt.getFilterMap().get("lastconsistencychecktime")));

        showPartitionsStmt = (ShowPartitionsStmt) analyzeSuccess("SHOW PARTITIONS FROM `test`.`t0`" +
                " WHERE `PartitionName` LIKE '%p2019%'");
        Assert.assertEquals("PartitionName LIKE '%p2019%'",
                AST2SQL.toString(showPartitionsStmt.getFilterMap().get("partitionname")));

        showPartitionsStmt = (ShowPartitionsStmt) analyzeSuccess("SHOW PARTITIONS FROM `test`.`t0`" +
                " WHERE `PartitionName` = 'p1'");
        Assert.assertEquals("PartitionName = 'p1'",
                AST2SQL.toString(showPartitionsStmt.getFilterMap().get("partitionname")));

        showPartitionsStmt = (ShowPartitionsStmt) analyzeSuccess("SHOW PARTITIONS FROM " +
                "`test`.`t0` ORDER BY `PartitionId` ASC LIMIT 10\"");
        Assert.assertEquals(" LIMIT 10",
                AST2SQL.toString(showPartitionsStmt.getLimitElement()));
    }
}
