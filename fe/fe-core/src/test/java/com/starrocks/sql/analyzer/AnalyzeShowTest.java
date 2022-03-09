// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.analyzer;

import com.starrocks.analysis.ShowStmt;
import com.starrocks.common.AnalysisException;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.UUID;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class AnalyzeShowTest {
    // may also start a Mocked Frontend
    private static String runningDir = "fe/mocked/AnalyzeShow/" + UUID.randomUUID().toString() + "/";

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(runningDir);
        AnalyzeTestUtil.init();
    }

    @Test
    public void testShowVariables() throws AnalysisException {
        analyzeSuccess("show variables");
        ShowStmt statement = (ShowStmt) analyzeSuccess("show variables where variables_name = 't1'");
        Assert.assertEquals("SELECT VARIABLE_NAME `Variable_name`, VARIABLE_VALUE `Value` " +
                        "FROM `information_schema`.`SESSION_VARIABLES` WHERE variables_name = 't1'",
                AST2SQL.toString(statement.toSelectStmt()));
    }

    @Test
    public void testShowTableStatus() throws AnalysisException {
        analyzeSuccess("show table status;");
        ShowStmt statement =
                (ShowStmt) analyzeSuccess("show table status where Name = 't1';");
        Assert.assertEquals("SELECT TABLE_NAME `Name`, ENGINE `Engine`, VERSION `Version`, " +
                        "ROW_FORMAT `Row_format`, TABLE_ROWS `Rows`, AVG_ROW_LENGTH `Avg_row_length`, " +
                        "DATA_LENGTH `Data_length`, MAX_DATA_LENGTH `Max_data_length`, INDEX_LENGTH `Index_length`, " +
                        "DATA_FREE `Data_free`, AUTO_INCREMENT `Auto_increment`, CREATE_TIME `Create_time`, " +
                        "UPDATE_TIME `Update_time`, CHECK_TIME `Check_time`, TABLE_COLLATION `Collation`, " +
                        "CHECKSUM `Checksum`, CREATE_OPTIONS `Create_options`, TABLE_COMMENT `Comment` " +
                        "FROM `information_schema`.`tables` WHERE TABLE_NAME = 't1'",
                AST2SQL.toString(statement.toSelectStmt()));
    }

    @Test
    public void testShowDatabases() throws AnalysisException {
        analyzeSuccess("show databases;");
        ShowStmt statement = (ShowStmt) analyzeSuccess("show databases where database = 't1';");
        Assert.assertEquals("SELECT SCHEMA_NAME `Database` FROM `information_schema`.`schemata` WHERE SCHEMA_NAME = 't1'",
                AST2SQL.toString(statement.toSelectStmt()));
    }

    @Test
    public void testShowTables() throws AnalysisException {
        analyzeSuccess("show tables;");
        ShowStmt statement = (ShowStmt) analyzeSuccess("show tables where table_name = 't1';");
        Assert.assertEquals("SELECT TABLE_NAME `Tables_in_test` FROM `information_schema`.`tables` WHERE table_name = 't1'",
                AST2SQL.toString(statement.toSelectStmt()));
    }

    @Test
    public void testShowColumns() throws AnalysisException {
        analyzeSuccess("show columns from t1;");
        ShowStmt statement = (ShowStmt) analyzeSuccess("show columns from t1 where Field = 'v1';");
        Assert.assertEquals("SELECT COLUMN_NAME `Field`, DATA_TYPE `Type`, IS_NULLABLE `Null`, COLUMN_KEY `Key`, " +
                        "COLUMN_DEFAULT `Default`, EXTRA `Extra` FROM `information_schema`.`COLUMNS` WHERE COLUMN_NAME = 'v1'",
                AST2SQL.toString(statement.toSelectStmt()));
    }
}
