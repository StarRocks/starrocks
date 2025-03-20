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

package com.starrocks.sql.analyzer;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.MetadataMgr;
import com.starrocks.sql.ast.CreateTableStmt;
import mockit.Expectations;
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
                "create table test.table1 (col1 int, col2 varchar(10)) engine=olap " +
                        "duplicate key(col1, col2) distributed by hash(col1) buckets 10");
        Assert.assertEquals("test", stmt.getDbName());
        Assert.assertEquals("table1", stmt.getTableName());
        Assert.assertNull(stmt.getProperties());
    }

    @Test
    public void testCreateTableWithRollup() {
        String sql =
                "create table test.table1 (col1 int, col2 varchar(10)) engine=olap aggregate key(col1, col2)" +
                        " distributed by hash(col1) buckets 10 rollup ( index1(col1, col2), index2(col2, col3))";
        CreateTableStmt stmt = (CreateTableStmt) analyzeSuccess(sql);
        Assert.assertEquals("test", stmt.getDbName());
        Assert.assertEquals("table1", stmt.getTableName());
        Assert.assertNull(stmt.getProperties());
    }

    @Test
    public void testDefaultDbNormal() {
        String sql =
                "create table test.table1 (col1 int, col2 varchar(10)) engine=olap aggregate key(col1, col2)" +
                        " distributed by hash(col1) buckets 10 rollup ( index1(col1, col2), index2(col2, col3))";
        CreateTableStmt stmt = (CreateTableStmt) analyzeSuccess(sql);
        Assert.assertEquals("test", stmt.getDbName());
        Assert.assertEquals("table1", stmt.getTableName());
        Assert.assertNull(stmt.getPartitionDesc());
        Assert.assertNull(stmt.getProperties());
    }

    @Test
    public void testNoDb() {
        AnalyzeTestUtil.getConnectContext().setDatabase(null);
        String sql =
                "create table table1 (col1 int, col2 varchar(10)) engine=olap " +
                        "duplicate key(col1, col2) distributed by hash(col1) buckets 10";
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
        String sql = "create table test.table1 (col1 int, col2 varchar(10), col2 varchar(10)) engine=olap " +
                "aggregate key(col1, col2, col2) distributed by hash(col1) buckets 10";
        analyzeFail(sql);
    }

    @Test
    public void testBitmapKey() {
        String sql =
                "create table test.table1 (col1 int, col2 varchar(10), col3 BITMAP) engine=olap " +
                        "aggregate key(col1, col2, col3) distributed by hash(col1) buckets 10";
        analyzeFail(sql, "Invalid data type of key column 'col3': 'BITMAP'");
    }

    @Test
    public void testHLLKey() {
        String sql =
                "create table test.table1 (col1 int, col2 varchar(10), col3 HLL) engine=olap " +
                        "aggregate key(col1, col2, col3) distributed by hash(col1) buckets 10";
        analyzeFail(sql, "Invalid data type of key column 'col3': 'HLL'");
    }

    @Test
    public void testPercentileKey() {
        String sql =
                "create table test.table1 (col1 int, col2 varchar(10), col3 PERCENTILE) engine=olap " +
                        "aggregate key(col1, col2, col3) distributed by hash(col1) buckets 10";
        analyzeFail(sql, "Invalid data type of key column 'col3': 'PERCENTILE'");
    }

    @Test
    public void testArrayKey() {
        String sql =
                "create table test.table1 (col1 int, col2 varchar(10), col3 ARRAY<INT>) engine=olap " +
                        "aggregate key(col1, col2, col3) distributed by hash(col1) buckets 10";
        analyzeFail(sql, "Invalid data type of key column 'col3': 'ARRAY<INT>'");
    }

    @Test
    public void testBitmapWithoutAggregateMethod() {
        String sql =
                "create table test.table1 (col1 int, col2 varchar(10), col3 BITMAP) engine=olap " +
                        "aggregate key(col1, col2) distributed by hash(col1) buckets 10";
        analyzeFail(sql, "AGG_KEYS table should specify aggregate type for non-key column[col3]");
    }

    @Test
    public void testBitmapWithPrimaryKey() {
        String sql =
                "create table test.table1 (col1 int, col2 varchar(10), col3 BITMAP) engine=olap " +
                        "primary key(col1, col2) distributed by hash(col1) buckets 10";
        analyzeSuccess(sql);
    }

    @Test
    public void testBitmapWithUniqueKey() {
        String sql =
                "create table test.table1 (col1 int, col2 varchar(10), col3 BITMAP) engine=olap " +
                        "unique key(col1, col2) distributed by hash(col1) buckets 10";
        analyzeSuccess(sql);
    }

    @Test
    public void testBitmapWithDuplicateKey() {
        String sql =
                "create table test.table1 (col1 int, col2 varchar(10), col3 BITMAP) engine=olap " +
                        "duplicate key(col1, col2) distributed by hash(col1) buckets 10";
        analyzeFail(sql, "No aggregate function specified for 'col3'");
    }

    @Test
    public void testHLLWithoutAggregateMethod() {
        String sql =
                "create table test.table1 (col1 int, col2 varchar(10), col3 HLL) engine=olap " +
                        "aggregate key(col1, col2) distributed by hash(col1) buckets 10";
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
                "create table table1 (col1 int, col2 varchar(10), col3 PERCENTILE) engine=olap " +
                        "aggregate key(col1, col2) distributed by hash(col1) buckets 10";
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
                "create table table1 (col1 char(10) not null, index index1 (col1) using bitmap comment \"index1\") " +
                        "engine=olap duplicate key(col1) distributed by hash(col1) buckets 10";
        analyzeSuccess(sql);
    }

    @Test
    public void testIndexColumnNotInTable() {
        String sql =
                "create table table1 (col1 char(10) not null, index index1 (col2) using bitmap comment \"index1\") " +
                        "engine=olap duplicate key(col1) distributed by hash(col1) buckets 10";
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
        analyzeSuccess(sql);
    }

    @Test
    public void testRandomDistribution() {
        analyzeSuccess("create table table1 (col1 char(10) not null) engine=olap duplicate key(col1) distributed by random");
        analyzeSuccess("create table table1 (col1 char(10) not null) engine=olap duplicate key(col1) " +
                "distributed by random buckets 10");
    }

    @Test
    public void testExternalTable() {
        analyzeSuccess("create external table table1 (col1 char(10) not null) engine=olap distributed by hash(col1) buckets 10");
        analyzeSuccess("create external table table1 (col1 char(10) not null) engine=olap " +
                "distributed by random buckets 10");
    }

    @Test
    public void testRandomDistributionForAggKeyDefault() {
        analyzeFail("create table table1 (col1 char(10) not null, col2 bigint sum) engine=olap aggregate key(col1)");
    }

    @Test
    public void testRandomDistributionForAggKey() {
        analyzeSuccess("create table table1 (col1 char(10) not null, col2 bigint sum) engine=olap aggregate key(col1) " +
                "distributed by random");
        analyzeSuccess("create table table1 (col1 char(10) not null, col2 bigint sum) engine=olap aggregate key(col1) " +
                "distributed by random buckets 10");
        analyzeFail("create table table1 (col1 char(10) not null, col2 bigint replace) engine=olap aggregate key(col1) " +
                "distributed by random buckets 10");
        analyzeFail("create table table1 (col1 char(10) not null, col2 bigint REPLACE_IF_NOT_NULL) " +
                "engine=olap aggregate key(col1) distributed by random buckets 10");
    }

    @Test
    public void testComplexType() {
        analyzeSuccess("create table table1 (col0 int, col1 array<array<int>>) " +
                "engine=olap distributed by hash(col0) buckets 10");
        analyzeSuccess("create external table table1 (col0 int, col1 array<array<int>>) " +
                "engine=hive properties('key' = 'value')");

        analyzeSuccess("create table table1 (col0 int, col1 array<map<int,int>>) " +
                "engine=olap distributed by hash(col0) buckets 10");
        analyzeSuccess("create external table table1 (col0 int, col1 array<map<int,int>>) " +
                "engine=hive properties('key' = 'value')");

        analyzeSuccess("create table table1 (col0 int, col1 array<struct<a int>>) " +
                "engine=olap distributed by hash(col0) buckets 10");
        analyzeSuccess("create external table table1 (col0 int, col1 array<struct<a int>>) " +
                "engine=hive properties('key' = 'value')");

        analyzeSuccess("create table table1 (col0 int, col1 map<int,int>) " +
                "engine=olap distributed by hash(col0) buckets 10");
        analyzeSuccess("create external table table1 (col0 int, col1 map<int,int>) " +
                "engine=hive properties('key' = 'value')");

        analyzeSuccess("create table table1 (col0 int, col1 struct<a int>) " +
                "engine=olap distributed by hash(col0) buckets 10");
        analyzeSuccess("create external table table1 (col0 int, col1 struct<a int>) " +
                "engine=hive properties('key' = 'value')");
    }

    @Test
    public void testIceberg() throws Exception {
        analyzeFail("create external table not_exist_catalog.iceberg_db.iceberg_table (k1 int, k2 int)");

        String createIcebergCatalogStmt = "create external catalog iceberg_catalog properties (\"type\"=\"iceberg\", " +
                "\"hive.metastore.uris\"=\"thrift://hms:9083\", \"iceberg.catalog.type\"=\"hive\")";
        AnalyzeTestUtil.getStarRocksAssert().withCatalog(createIcebergCatalogStmt);

        MetadataMgr metadata = AnalyzeTestUtil.getConnectContext().getGlobalStateMgr().getMetadataMgr();
        new Expectations(metadata) {
            {
                metadata.getDb((ConnectContext) any, "iceberg_catalog", "not_exist_db");
                result = null;
                minTimes = 0;
            }
        };
        analyzeFail("create external table iceberg_catalog.not_exist_db.iceberg_table (k1 int, k2 int)");

        new Expectations(metadata) {
            {
                metadata.getDb((ConnectContext) any, "iceberg_catalog", "iceberg_db");
                result = new Database();
                minTimes = 0;

                metadata.tableExists((ConnectContext) any, "iceberg_catalog", "iceberg_db", anyString);
                result = false;
            }
        };

        analyzeSuccess("create external table iceberg_catalog.iceberg_db.iceberg_table (k1 int, k2 int)");
        analyzeSuccess("create external table iceberg_catalog.iceberg_db.iceberg_table (k1 int, k2 int) partition by (k2)");

        analyzeFail("create external table iceberg_catalog.iceberg_db.iceberg_table (k1 int, k2 int) partition by (error_col)");
        analyzeFail("create external table iceberg_catalog.iceberg_db.iceberg_table (k1 int, dt datetime) partition by days(dt)");

        AnalyzeTestUtil.getConnectContext().setCurrentCatalog("default_catalog");
        analyzeFail("create external table iceberg_db.iceberg_table (k1 int, k2 int) partition by (k2)");

        AnalyzeTestUtil.getConnectContext().setCurrentCatalog("iceberg_catalog");
        analyzeSuccess("create external table iceberg_db.iceberg_table (k1 int, k2 int) partition by (k2)");

        AnalyzeTestUtil.getConnectContext().setDatabase("iceberg_db");
        analyzeSuccess("create external table iceberg_table (k1 int, k2 int) partition by (k2)");
        analyzeSuccess("create external table iceberg_table (k1 int, k2 int) engine=iceberg partition by (k2)");

        String createHiveCatalogStmt = "create external catalog hive_catalog properties (\"type\"=\"hive\", " +
                "\"hive.metastore.uris\"=\"thrift://hms:9083\")";
        AnalyzeTestUtil.getStarRocksAssert().withCatalog(createHiveCatalogStmt);
        new Expectations(metadata) {
            {
                metadata.getDb((ConnectContext) any, "hive_catalog", "hive_db");
                result = new Database();
                minTimes = 0;
            }
        };

        AnalyzeTestUtil.getConnectContext().setCurrentCatalog(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME);
        AnalyzeTestUtil.getConnectContext().setDatabase("test");
    }

    @Test
    public void testGeneratedColumnWithExternalTable() throws Exception {
        analyzeFail("create external table ex_hive_tbl0 (col_tinyint tinyint null comment \"column tinyint\"," +
                "col_varchar varchar(5), col_boolean boolean null comment \"column boolean\", col_new int" +
                "as col_tinyint+1) ENGINE=hive properties (\"resource\" = \"hive_resource\"," +
                "\"table\" = \"hive_hdfs_orc_nocompress\"," +
                "\"database\" = \"hive_extbl_test\");");
    }

    @Test
    public void testGeneratedColumnOnAGGTable() throws Exception {
        analyzeFail("CREATE TABLE t ( id BIGINT NOT NULL,  name BIGINT NOT NULL, v1 BIGINT SUM as id)" +
                "AGGREGATE KEY (id) DISTRIBUTED BY HASH(id) BUCKETS 1 " +
                "PROPERTIES(\"replication_num\" = \"1\", \"replicated_storage\"=\"true\");");
    }

    @Test
    public void testNgrambloomIndex() throws Exception {
        // create index with non-existent column
        String sql = "CREATE TABLE TABLE1 (COL1 INT, COL2 VARCHAR(10)," +
                "INDEX INDEX1(COL3) USING NGRAMBF ('BLOOM_FILTER_FPP' = '0.01', 'GRAM_NUM' = '2'))" +
                "AGGREGATE KEY(COL1, COL2) DISTRIBUTED BY HASH(COL1) BUCKETS 10;";
        analyzeFail(sql, "INDEX1 column does not exist in table.");

        // create index in non-string column
        sql = "CREATE TABLE TABLE1 (COL1 INT, COL2 INT," +
                "INDEX INDEX1(COL2) USING NGRAMBF ('BLOOM_FILTER_FPP' = '0.01', 'GRAM_NUM' = '2'))" +
                "AGGREGATE KEY(COL1, COL2) DISTRIBUTED BY HASH(COL1) BUCKETS 10;";
        analyzeFail(sql, "Invalid ngram bloom filter column 'COL2': unsupported type INT");

        // create index with multiple columns
        sql = "CREATE TABLE TABLE1 (COL1 VARCHAR(10), COL2 VARCHAR(10)," +
                "INDEX INDEX1(COL1, COL2) USING NGRAMBF ('BLOOM_FILTER_FPP' = '0.01', 'GRAM_NUM' = '2'))" +
                "AGGREGATE KEY(COL1, COL2) DISTRIBUTED BY HASH(COL1) BUCKETS 10;";
        analyzeFail(sql, "INDEX1 index can only apply to a single column");

        // create index with wrong fpp
        sql = "CREATE TABLE TABLE1 (COL1 INT, COL2 VARCHAR(10)," +
                "INDEX INDEX1(COL2) USING NGRAMBF ('BLOOM_FILTER_FPP' = '1', 'GRAM_NUM' = '2'))" +
                "AGGREGATE KEY(COL1, COL2) DISTRIBUTED BY HASH(COL1) BUCKETS 10;";
        analyzeFail(sql, "Bloom filter fpp should in [1.0E-4, 0.05]");

        // create index with wrong gram num
        sql = "CREATE TABLE TABLE1 (COL1 INT, COL2 VARCHAR(10)," +
                "INDEX INDEX1(COL2) USING NGRAMBF ('BLOOM_FILTER_FPP' = '0.01', 'GRAM_NUM' = '0'))" +
                "AGGREGATE KEY(COL1, COL2) DISTRIBUTED BY HASH(COL1) BUCKETS 10;";
        analyzeFail(sql, "Ngram Bloom filter's gram_num should be positive number");

        // create index with agg mode's non-key column, col3 use sum as agg function
        sql = "CREATE TABLE TABLE1 (COL1 INT, COL2 VARCHAR(10), COL3 VARCHAR(10) REPLACE," +
                "INDEX INDEX1(COL3) USING NGRAMBF ('BLOOM_FILTER_FPP' = '0.01', 'GRAM_NUM' = '2'))" +
                "AGGREGATE KEY(COL1, COL2) DISTRIBUTED BY HASH(COL1) BUCKETS 10;";
        analyzeFail(sql, "Ngram Bloom filter index only used in columns of " +
                "DUP_KEYS/PRIMARY table or key columns of UNIQUE_KEYS/AGG_KEYS table");

        // create index with invalid properties
        sql = "CREATE TABLE TABLE1 (COL1 INT, COL2 VARCHAR(10)," +
                "INDEX INDEX1(COL2) USING NGRAMBF ('BLOOM_FILTER_FPP' = '0.01', 'GRAM_NUM' = '2', 'CASE_SENSITIVE' = '2'))" +
                "AGGREGATE KEY(COL1, COL2) DISTRIBUTED BY HASH(COL1) BUCKETS 10;";
        analyzeFail(sql, "Ngram Bloom filter's case_sensitive should be true or false");

        // create index with correct fpp and gram num and case-insensitive
        sql = "CREATE TABLE TABLE1 (COL1 INT, COL2 VARCHAR(10)," +
                "INDEX INDEX1(COL2) USING NGRAMBF ('BLOOM_FILTER_FPP' = '0.01', 'GRAM_NUM' = '2', 'CASE_SENSITIVE' = 'false'))" +
                "AGGREGATE KEY(COL1, COL2) DISTRIBUTED BY HASH(COL1) BUCKETS 10;";
        analyzeSuccess(sql);

        // create index with default valus
        sql = "CREATE TABLE TABLE1 (COL1 INT, COL2 VARCHAR(10)," +
                "INDEX INDEX1(COL2) USING NGRAMBF)" +
                "AGGREGATE KEY(COL1, COL2) DISTRIBUTED BY HASH(COL1) BUCKETS 10;";
        analyzeSuccess(sql);
    }
}
