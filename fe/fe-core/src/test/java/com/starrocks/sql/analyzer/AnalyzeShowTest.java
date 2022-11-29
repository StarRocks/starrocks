// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.analyzer;

import com.starrocks.analysis.ShowStmt;
import com.starrocks.common.AnalysisException;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.UUID;

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
                AST2SQL.toString(statement.toSelectStmt()));
    }

    @Test
    public void testShowTableStatus() throws AnalysisException {
        analyzeSuccess("show table status;");
<<<<<<< HEAD
        ShowStmt statement =
                (ShowStmt) analyzeSuccess("show table status where Name = 't1';");
        Assert.assertEquals("SELECT TABLE_NAME AS Name, ENGINE AS Engine, VERSION AS Version, " +
                        "ROW_FORMAT AS Row_format, TABLE_ROWS AS Rows, AVG_ROW_LENGTH AS Avg_row_length, " +
                        "DATA_LENGTH AS Data_length, MAX_DATA_LENGTH AS Max_data_length, INDEX_LENGTH AS Index_length, " +
                        "DATA_FREE AS Data_free, AUTO_INCREMENT AS Auto_increment, CREATE_TIME AS Create_time, " +
                        "UPDATE_TIME AS Update_time, CHECK_TIME AS Check_time, " +
                        "TABLE_COLLATION AS Collation, CHECKSUM AS Checksum, CREATE_OPTIONS AS Create_options, " +
                        "TABLE_COMMENT AS Comment FROM information_schema.tables WHERE TABLE_NAME = 't1'",
=======
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
>>>>>>> e3b6d66c4 ([BugFix] Fix when output has duplicate item, order by works on wrong column-ref (#13754))
                AST2SQL.toString(statement.toSelectStmt()));
    }

    @Test
    public void testShowDatabases() throws AnalysisException {
        analyzeSuccess("show databases;");
<<<<<<< HEAD
        ShowStmt statement = (ShowStmt) analyzeSuccess("show databases where database = 't1';");
        Assert.assertEquals("SELECT SCHEMA_NAME AS Database FROM information_schema.schemata WHERE SCHEMA_NAME = 't1'",
=======
        ShowStmt statement = (ShowStmt) analyzeSuccess("show databases where `database` = 't1';");
        Assert.assertEquals("SELECT information_schema.schemata.SCHEMA_NAME AS Database " +
                        "FROM information_schema.schemata WHERE information_schema.schemata.SCHEMA_NAME = 't1'",
>>>>>>> e3b6d66c4 ([BugFix] Fix when output has duplicate item, order by works on wrong column-ref (#13754))
                AST2SQL.toString(statement.toSelectStmt()));
    }

    @Test
    public void testShowTables() throws AnalysisException {
        analyzeSuccess("show tables;");
        ShowStmt statement = (ShowStmt) analyzeSuccess("show tables where table_name = 't1';");
        Assert.assertEquals(
                "SELECT information_schema.tables.TABLE_NAME AS Tables_in_test " +
                        "FROM information_schema.tables " +
                        "WHERE (information_schema.tables.TABLE_SCHEMA = 'test') AND (table_name = 't1')",
                AST2SQL.toString(statement.toSelectStmt()));
        analyzeSuccess("show tables from `test` like 'test'");
    }

    @Test
    public void testShowColumns() throws AnalysisException {
        analyzeSuccess("show columns from t1;");
<<<<<<< HEAD
        ShowStmt statement = (ShowStmt) analyzeSuccess("show columns from t1 where Field = 'v1';");
        Assert.assertEquals("SELECT COLUMN_NAME AS Field, DATA_TYPE AS Type, IS_NULLABLE AS Null, " +
                        "COLUMN_KEY AS Key, COLUMN_DEFAULT AS Default, EXTRA AS Extra " +
                        "FROM information_schema.COLUMNS WHERE COLUMN_NAME = 'v1'",
=======
        ShowColumnStmt statement = (ShowColumnStmt) analyzeSuccess("show columns from t1 where Field = 'v1';");
        Assert.assertEquals("SELECT information_schema.COLUMNS.COLUMN_NAME AS Field, " +
                        "information_schema.COLUMNS.DATA_TYPE AS Type, " +
                        "information_schema.COLUMNS.IS_NULLABLE AS Null, " +
                        "information_schema.COLUMNS.COLUMN_KEY AS Key, " +
                        "information_schema.COLUMNS.COLUMN_DEFAULT AS Default, " +
                        "information_schema.COLUMNS.EXTRA AS Extra " +
                        "FROM information_schema.COLUMNS WHERE information_schema.COLUMNS.COLUMN_NAME = 'v1'",
>>>>>>> e3b6d66c4 ([BugFix] Fix when output has duplicate item, order by works on wrong column-ref (#13754))
                AST2SQL.toString(statement.toSelectStmt()));
    }
}
