// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.starrocks.analysis.CreateTableStmt;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class AnalyzeCreateTableTest {

    @BeforeClass
    public static void beforeClass() throws Exception {
        AnalyzeTestUtil.init();
    }

    @Test
    public void testNormal() {
        CreateTableStmt stmt = (CreateTableStmt) analyzeSuccess(
                "create table test.table1 (col1 int, col2 varchar(10)) engine=olap duplicate key(col1, col2) distributed by hash(col1) buckets 10");
        Assert.assertEquals("default_cluster:test", stmt.getDbName());
        Assert.assertEquals("table1", stmt.getTableName());
        Assert.assertNull(stmt.getProperties());
    }

    @Test
    public void testCreateTableWithRollup() {
        String sql =
                "create table test.table1 (col1 int, col2 varchar(10)) engine=olap aggregate key(col1, col2)" +
                        " distributed by hash(col1) buckets 10 rollup ( index1(col1, col2), index2(col2, col3))";
        CreateTableStmt stmt = (CreateTableStmt) analyzeSuccess(sql);
        Assert.assertEquals("default_cluster:test", stmt.getDbName());
        Assert.assertEquals("table1", stmt.getTableName());
        Assert.assertNull(stmt.getProperties());
        Assert.assertTrue(stmt.toSql()
                .contains("rollup( `index1` (`col1`, `col2`), `index2` (`col2`, `col3`))"));
    }

    @Test
    public void testDefaultDbNormal() {
        String sql =
                "create table test.table1 (col1 int, col2 varchar(10)) engine=olap aggregate key(col1, col2)" +
                        " distributed by hash(col1) buckets 10 rollup ( index1(col1, col2), index2(col2, col3))";
        CreateTableStmt stmt = (CreateTableStmt) analyzeSuccess(sql);
        Assert.assertEquals("default_cluster:test", stmt.getDbName());
        Assert.assertEquals("table1", stmt.getTableName());
        Assert.assertNull(stmt.getPartitionDesc());
        Assert.assertNull(stmt.getProperties());
    }

    @Test
    public void testNoDb() {
        AnalyzeTestUtil.getConnectContext().setDatabase(null);
        String sql =
                "create table table1 (col1 int, col2 varchar(10)) engine=olap duplicate key(col1, col2) distributed by hash(col1) buckets 10";
        analyzeFail(sql, "No database selected");
        AnalyzeTestUtil.getConnectContext().setDatabase("test");
    }

    @Test
    public void testEmptyCol() {
        String sql =
                "create table test.table1 () engine=olap aggregate key(col1, col2)" +
                        " distributed by hash(col1) buckets 10";
        analyzeFail(sql);
    }

    @Test
    public void testDupCol() {
        String sql =
                "create table test.table1 (col1 int, col2 varchar(10), col2 varchar(10)) engine=olap aggregate key(col1, col2, col2)" +
                        " distributed by hash(col1) buckets 10";
        analyzeFail(sql);
    }

    @Test
    public void testBitmapKey() {
        String sql =
                "create table test.table1 (col1 int, col2 varchar(10), col3 BITMAP) engine=olap aggregate key(col1, col2, col3)" +
                        " distributed by hash(col1) buckets 10";
        analyzeFail(sql, "Invalid data type of key column 'col3': 'BITMAP'");
    }

    @Test
    public void testHLLKey() {
        String sql =
                "create table test.table1 (col1 int, col2 varchar(10), col3 HLL) engine=olap aggregate key(col1, col2, col3)" +
                        " distributed by hash(col1) buckets 10";
        analyzeFail(sql, "Invalid data type of key column 'col3': 'HLL'");
    }

    @Test
    public void testPercentileKey() {
        String sql =
                "create table test.table1 (col1 int, col2 varchar(10), col3 PERCENTILE) engine=olap aggregate key(col1, col2, col3)" +
                        " distributed by hash(col1) buckets 10";
        analyzeFail(sql, "Invalid data type of key column 'col3': 'PERCENTILE'");
    }

    @Test
    public void testArrayKey() {
        String sql =
                "create table test.table1 (col1 int, col2 varchar(10), col3 ARRAY<INT>) engine=olap aggregate key(col1, col2, col3)" +
                        " distributed by hash(col1) buckets 10";
        analyzeFail(sql, "Invalid data type of key column 'col3': 'ARRAY<INT>'");
    }

    @Test
    public void testBitmapWithoutAggregateMethod() {
        String sql =
                "create table test.table1 (col1 int, col2 varchar(10), col3 BITMAP) engine=olap aggregate key(col1, col2)" +
                        " distributed by hash(col1) buckets 10";
        analyzeFail(sql, "AGG_KEYS table should specify aggregate type for non-key column[col3]");
    }

    @Test
    public void testBitmapWithPrimaryKey() {
        String sql =
                "create table test.table1 (col1 int, col2 varchar(10), col3 BITMAP) engine=olap primary key(col1, col2)" +
                        " distributed by hash(col1) buckets 10";
        analyzeSuccess(sql);
    }

    @Test
    public void testBitmapWithUniqueKey() {
        String sql =
                "create table test.table1 (col1 int, col2 varchar(10), col3 BITMAP) engine=olap unique key(col1, col2)" +
                        " distributed by hash(col1) buckets 10";
        analyzeSuccess(sql);
    }

    @Test
    public void testBitmapWithDuplicateKey() {
        String sql =
                "create table test.table1 (col1 int, col2 varchar(10), col3 BITMAP) engine=olap duplicate key(col1, col2)" +
                        " distributed by hash(col1) buckets 10";
        analyzeFail(sql, "No aggregate function specified for 'col3'");
    }

    @Test
    public void testHLLWithoutAggregateMethod() {
        String sql =
                "create table test.table1 (col1 int, col2 varchar(10), col3 HLL) engine=olap aggregate key(col1, col2)" +
                        " distributed by hash(col1) buckets 10";
        analyzeFail(sql, "AGG_KEYS table should specify aggregate type for non-key column[col3]");
    }

    @Test
    public void testHLLWithPrimaryKey() throws Exception {
        String sql =
                "create table table1 (col1 int, col2 varchar(10), col3 HLL) engine=olap primary key(col1, col2)" +
                        " distributed by hash(col1) buckets 10";
        analyzeSuccess(sql);
    }

    @Test
    public void testPercentileWithoutAggregateMethod() {
        String sql =
                "create table table1 (col1 int, col2 varchar(10), col3 PERCENTILE) engine=olap aggregate key(col1, col2)" +
                        " distributed by hash(col1) buckets 10";
        analyzeFail(sql, "AGG_KEYS table should specify aggregate type for non-key column[col3]");
    }

    @Test
    public void testPercentileWithPrimaryKey() throws Exception {
        String sql =
                "create table table1 (col1 int, col2 varchar(10), col3 PERCENTILE) engine=olap primary key(col1, col2)" +
                        " distributed by hash(col1) buckets 10";
        analyzeSuccess(sql);
    }

    @Test
    public void testInvalidAggregateFuncForBitmap() {
        String sql =
                "create table table1 (col1 int, col2 varchar(10), col3 bitmap SUM) engine=olap aggregate key(col1, col2)" +
                        " distributed by hash(col1) buckets 10";
        analyzeFail(sql, "Invalid aggregate function 'SUM' for 'col3'");
    }

    @Test
    public void testInvalidAggregateFuncForHLL() {
        String sql =
                "create table table1 (col1 int, col2 varchar(10), col3 hll SUM) engine=olap aggregate key(col1, col2)" +
                        " distributed by hash(col1) buckets 10";
        analyzeFail(sql, "Invalid aggregate function 'SUM' for 'col3'");
    }

    @Test
    public void testInvalidAggregateFuncForArray() {
        String sql =
                "create table table1 (col1 int, col2 varchar(10), col3 array<int> SUM) engine=olap aggregate key(col1, col2)" +
                        " distributed by hash(col1) buckets 10";
        analyzeFail(sql, "Invalid aggregate function 'SUM' for 'col3'");
    }

    @Test
    public void testPrimaryKeyNullable() {
        String sql =
                "create table table1 (col1 int null) engine=olap primary key(col1)" +
                        " distributed by hash(col1) buckets 10";
        analyzeFail(sql);
    }

    @Test
    public void testPrimaryKeyChar() {
        String sql =
                "create table table1 (col1 char(10) not null ) engine=olap primary key(col1)" +
                        " distributed by hash(col1) buckets 10";
        analyzeFail(sql);
    }

    @Test
    public void testIndex() {
        String sql =
                "create table table1 (col1 char(10) not null, index index1 (col1) using bitmap comment \"index1\") engine=olap duplicate key(col1)" +
                        " distributed by hash(col1) buckets 10";
        analyzeSuccess(sql);
    }

    @Test
    public void testIndexColumnNotInTable() {
        String sql =
                "create table table1 (col1 char(10) not null, index index1 (col2) using bitmap comment \"index1\") engine=olap duplicate key(col1)" +
                        " distributed by hash(col1) buckets 10";
        analyzeFail(sql);
    }

    @Test
    public void testNullKey() {
        String sql = "create table table1 (col1 char(10) not null) engine=olap distributed by hash(col1) buckets 10";
        analyzeSuccess(sql);
    }

    @Test
    public void testNullDistribution() {
        String sql = "create table table1 (col1 char(10) not null) engine=olap duplicate key(col1)";
        analyzeFail(sql);
    }
}