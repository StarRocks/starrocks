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
        Assert.assertEquals("SELECT information_schema.SESSION_VARIABLES.VARIABLE_NAME AS Variable_name, " +
                        "information_schema.SESSION_VARIABLES.VARIABLE_VALUE AS Value " +
                        "FROM information_schema.SESSION_VARIABLES WHERE variables_name = 't1'",
                AstToStringBuilder.toString(statement.toSelectStmt()));

        ShowVariablesStmt stmt = (ShowVariablesStmt) analyzeSuccess("show global variables like 'abc'");
        Assert.assertEquals("abc", stmt.getPattern());
        Assert.assertEquals(stmt.getType(), SetType.GLOBAL);
    }

    @Test
    public void testShowTableStatus() throws AnalysisException {
        analyzeSuccess("show table status;");
        ShowTableStatusStmt statement =
                (ShowTableStatusStmt) analyzeSuccess("show table status where Name = 't1';");
        Assert.assertEquals("SELECT information_schema.tables.TABLE_NAME AS Name, " +
                        "information_schema.tables.ENGINE AS Engine, information_schema.tables.VERSION AS Version, " +
                        "information_schema.tables.ROW_FORMAT AS Row_format, " +
                        "information_schema.tables.TABLE_ROWS AS Rows, " +
                        "information_schema.tables.AVG_ROW_LENGTH AS Avg_row_length, " +
                        "information_schema.tables.DATA_LENGTH AS Data_length, " +
                        "information_schema.tables.MAX_DATA_LENGTH AS Max_data_length, " +
                        "information_schema.tables.INDEX_LENGTH AS Index_length, " +
                        "information_schema.tables.DATA_FREE AS Data_free, " +
                        "information_schema.tables.AUTO_INCREMENT AS Auto_increment, " +
                        "information_schema.tables.CREATE_TIME AS Create_time, " +
                        "information_schema.tables.UPDATE_TIME AS Update_time, " +
                        "information_schema.tables.CHECK_TIME AS Check_time, " +
                        "information_schema.tables.TABLE_COLLATION AS Collation, " +
                        "information_schema.tables.CHECKSUM AS Checksum, " +
                        "information_schema.tables.CREATE_OPTIONS AS Create_options, " +
                        "information_schema.tables.TABLE_COMMENT AS Comment " +
                        "FROM information_schema.tables WHERE information_schema.tables.TABLE_NAME = 't1'",
                AstToStringBuilder.toString(statement.toSelectStmt()));
    }

    @Test
    public void testShowDatabases() throws AnalysisException {
        analyzeSuccess("show databases;");
        ShowStmt statement = (ShowStmt) analyzeSuccess("show databases where `database` = 't1';");
        Assert.assertEquals("SELECT information_schema.schemata.SCHEMA_NAME AS Database " +
                        "FROM information_schema.schemata WHERE information_schema.schemata.SCHEMA_NAME = 't1'",
                AstToStringBuilder.toString(statement.toSelectStmt()));
    }

    @Test
    public void testShowTables() throws AnalysisException {
        analyzeSuccess("show tables;");
        ShowTableStmt statement = (ShowTableStmt) analyzeSuccess("show tables where table_name = 't1';");
        Assert.assertEquals(
                "SELECT information_schema.tables.TABLE_NAME AS Tables_in_test " +
                        "FROM information_schema.tables " +
                        "WHERE (information_schema.tables.TABLE_SCHEMA = 'test') AND (table_name = 't1')",
                AstToStringBuilder.toString(statement.toSelectStmt()));

        statement = (ShowTableStmt) analyzeSuccess("show tables from `test`");
        Assert.assertEquals("test", statement.getDb());
    }

    @Test
    public void testShowColumns() throws AnalysisException {
        analyzeSuccess("show columns from t1;");
        ShowColumnStmt statement = (ShowColumnStmt) analyzeSuccess("show columns from t1 where Field = 'v1';");
        Assert.assertEquals("SELECT information_schema.COLUMNS.COLUMN_NAME AS Field, " +
                        "information_schema.COLUMNS.DATA_TYPE AS Type, " +
                        "information_schema.COLUMNS.IS_NULLABLE AS Null, " +
                        "information_schema.COLUMNS.COLUMN_KEY AS Key, " +
                        "information_schema.COLUMNS.COLUMN_DEFAULT AS Default, " +
                        "information_schema.COLUMNS.EXTRA AS Extra " +
                        "FROM information_schema.COLUMNS WHERE (information_schema.COLUMNS.COLUMN_NAME = 'v1') " +
                        "AND (information_schema.COLUMNS.TABLE_SCHEMA = 'test')",
                AstToStringBuilder.toString(statement.toSelectStmt()));
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
                AstToStringBuilder.toString(showPartitionsStmt.getFilterMap().get("lastconsistencychecktime")));

        showPartitionsStmt = (ShowPartitionsStmt) analyzeSuccess("SHOW PARTITIONS FROM `test`.`t0`" +
                " WHERE `PartitionName` LIKE '%p2019%'");
        Assert.assertEquals("PartitionName LIKE '%p2019%'",
                AstToStringBuilder.toString(showPartitionsStmt.getFilterMap().get("partitionname")));

        showPartitionsStmt = (ShowPartitionsStmt) analyzeSuccess("SHOW PARTITIONS FROM `test`.`t0`" +
                " WHERE `PartitionName` = 'p1'");
        Assert.assertEquals("PartitionName = 'p1'",
                AstToStringBuilder.toString(showPartitionsStmt.getFilterMap().get("partitionname")));

        showPartitionsStmt = (ShowPartitionsStmt) analyzeSuccess("SHOW PARTITIONS FROM " +
                "`test`.`t0` ORDER BY `PartitionId` ASC LIMIT 10\"");
        Assert.assertEquals(" LIMIT 10",
                AstToStringBuilder.toString(showPartitionsStmt.getLimitElement()));
    }
}
