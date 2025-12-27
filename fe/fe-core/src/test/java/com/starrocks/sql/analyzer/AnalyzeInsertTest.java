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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.MysqlTable;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableName;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.type.ArrayType;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.getConnectContext;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.getStarRocksAssert;

public class AnalyzeInsertTest {

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
        StarRocksAssert starRocksAssert = getStarRocksAssert();
        String createIcebergCatalogStmt = "create external catalog iceberg_catalog properties (\"type\"=\"iceberg\", " +
                "\"hive.metastore.uris\"=\"thrift://hms:9083\", \"iceberg.catalog.type\"=\"hive\")";
        starRocksAssert.withCatalog(createIcebergCatalogStmt);

        starRocksAssert.withCatalog("create external catalog hive_catalog properties (\"type\"=\"hive\", " +
                "\"hive.metastore.uris\"=\"thrift://hms:9083\")");
    }

    @Test
    public void testInsert() {
        analyzeFail("insert into t0 select v4,v5 from t1",
                "Inserted target column count: 3 doesn't match select/value column count: 2");
        analyzeFail("insert into t0 select 1,2", "Inserted target column count: 3 doesn't match select/value column count: 2");
        analyzeFail("insert into t0 values(1,2)", "Inserted target column count: 3 doesn't match select/value column count: 2");

        analyzeFail("insert into tnotnull(v1) values(1)",
                "must be explicitly mentioned in column permutation");
        analyzeFail("insert into tnotnull(v1,v3) values(1,3)",
                "must be explicitly mentioned in column permutation");

        analyzeSuccess("insert into tarray(v1,v4) values (1,[NULL,9223372036854775808])");

        analyzeFail("insert into t0 values (170141183460469231731687303715884105728)",
                "Numeric overflow 170141183460469231731687303715884105728.");

        analyzeFail("insert into tall(ta) values(min('x'))", "Values clause cannot contain aggregations");
        analyzeFail("insert into tall(ta) values(case min('x') when 'x' then 'x' end)",
                "Values clause cannot contain aggregations");
        analyzeFail("insert into tall(ta) values(min('x') over())", "Values clause cannot contain window function");

        analyzeSuccess("INSERT INTO tp  PARTITION(p1) VALUES(1,2,3)");

        analyzeSuccess("insert into t0 with label l1 select * from t0");
        analyzeSuccess("insert into t0 with label `l1` select * from t0");

        analyzeSuccess("insert into tmc values (1,2)");
        analyzeSuccess("insert into tmc (id,name) values (1,2)");
        analyzeFail("insert into tmc values (1,2,3)",
                "Inserted target column count: 2 doesn't match select/value column count: 3");
        analyzeFail("insert into tmc (id,name,mc) values (1,2,3)", "generated column 'mc' can not be specified.");
    }

    @Test
    public void testInsertOverwriteWhenSchemaChange() throws Exception {
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState()
                .getLocalMetastore().getDb("test").getTable("t0");
        table.setState(OlapTable.OlapTableState.SCHEMA_CHANGE);
        analyzeFail("insert overwrite t0 select * from t0;",
                "table state is SCHEMA_CHANGE, please wait to insert overwrite until table state is normal");
        table.setState(OlapTable.OlapTableState.NORMAL);
    }

    @Test
    public void testInsertIcebergUnpartitionedTable(@Mocked IcebergTable icebergTable) {
        analyzeFail("insert into err_catalog.db.tbl values(1)",
                "Unknown catalog 'err_catalog'");

        MetadataMgr metadata = AnalyzeTestUtil.getConnectContext().getGlobalStateMgr().getMetadataMgr();

        new MockUp<MetadataMgr>() {
            @Mock
            public Database getDb(ConnectContext context, String catalogName, String dbName) {
                return new Database();
            }
        };

        new MockUp<MetaUtils>() {
            @Mock
            public Table getSessionAwareTable(ConnectContext context, Database database, TableName tableName) {
                return null;
            }
        };
        analyzeFail("insert into iceberg_catalog.db.err_tbl values (1)",
                "Table err_tbl is not found");

        new MockUp<MetaUtils>() {
            @Mock
            public Table getSessionAwareTable(ConnectContext context, Database database, TableName tableName) {
                return icebergTable;
            }
        };
        new Expectations(metadata) {
            {
                metadata.getDb((ConnectContext) any, anyString, anyString);
                minTimes = 0;
                result = new Database();

                icebergTable.supportInsert();
                result = true;
                minTimes = 0;
            }
        };
        analyzeFail("insert into iceberg_catalog.db.tbl values (1)",
                "Inserted target column count: 0 doesn't match select/value column count: 1");

        new Expectations(metadata) {
            {
                metadata.getDb((ConnectContext) any, anyString, anyString);
                minTimes = 0;
                result = new Database();

                icebergTable.getBaseSchema();
                result = ImmutableList.of(new Column("c1", IntegerType.INT));
                minTimes = 0;

                icebergTable.getFullSchema();
                result = ImmutableList.of(new Column("c1", IntegerType.INT));
                minTimes = 0;
            }
        };
        analyzeSuccess("insert into iceberg_catalog.db.iceberg_tbl values (1)");
    }

    @Test
    public void testPartitionedIcebergTable(@Mocked IcebergTable icebergTable) {
        MetadataMgr metadata = AnalyzeTestUtil.getConnectContext().getGlobalStateMgr().getMetadataMgr();

        new MockUp<MetadataMgr>() {
            @Mock
            public Database getDb(String catalogName, String dbName) {
                return new Database();
            }
        };

        new MockUp<MetaUtils>() {
            @Mock
            public Table getSessionAwareTable(ConnectContext context, Database database, TableName tableName) {
                return icebergTable;
            }
        };

        new Expectations(metadata) {
            {
                metadata.getDb((ConnectContext) any, anyString, anyString);
                minTimes = 0;
                result = new Database();

                icebergTable.supportInsert();
                result = true;
                minTimes = 0;

                icebergTable.getPartitionColumnNames();
                result = Lists.newArrayList("p1", "p2");
                minTimes = 0;

                icebergTable.getColumn(anyString);
                result = ImmutableList.of(new Column("p1", ArrayType.ARRAY_DATE));
                minTimes = 0;

                icebergTable.isIcebergTable();
                result = true;
                minTimes = 0;
            }
        };

        analyzeFail("insert into iceberg_catalog.db.tbl partition(p1=1) values (1)",
                "Must include all 2 partition columns in the partition clause.");

        analyzeFail("insert into iceberg_catalog.db.tbl partition(p1=1, p2=\"aaffsssaa\") values (1)",
                "Type[array<date>] not supported.");

        new Expectations() {
            {
                icebergTable.getBaseSchema();
                result = ImmutableList.of(new Column("c1", IntegerType.INT),
                        new Column("p1", IntegerType.INT), new Column("p2", IntegerType.INT));
                minTimes = 0;

                icebergTable.getFullSchema();
                result = ImmutableList.of(new Column("c1", IntegerType.INT),
                        new Column("p1", IntegerType.INT), new Column("p2", IntegerType.INT));
                minTimes = 0;

                icebergTable.getColumn(anyString);
                result = ImmutableList.of(new Column("p1", IntegerType.INT), new Column("p2", IntegerType.INT));
                minTimes = 0;

                icebergTable.getPartitionColumnNames();
                result = Lists.newArrayList("p1", "p2");
                minTimes = 1;
            }
        };

        analyzeSuccess("insert into iceberg_catalog.db.tbl partition(p1=111, p2=NULL) values (1)");
        analyzeSuccess("insert into iceberg_catalog.db.tbl partition(p1=111, p2=222) values (1)");

        new Expectations() {
            {
                icebergTable.getBaseSchema();
                result = ImmutableList.of(new Column("c1", IntegerType.INT), new Column("p1", DateType.DATETIME),
                        new Column("p2", IntegerType.INT));
                minTimes = 0;

                icebergTable.getFullSchema();
                result = ImmutableList.of(new Column("c1", IntegerType.INT), new Column("p1", DateType.DATETIME),
                        new Column("p2", IntegerType.INT));
                minTimes = 0;

                icebergTable.getColumn(anyString);
                result = ImmutableList.of(new Column("p1", IntegerType.INT), new Column("p2", DateType.DATETIME));
                minTimes = 0;

                icebergTable.getPartitionColumnNames();
                result = Lists.newArrayList("p1", "p2");
                minTimes = 1;

                icebergTable.getType();
                result = Table.TableType.ICEBERG;
                minTimes = 0;
            }
        };

        analyzeSuccess("insert into iceberg_catalog.db.tbl select 1, 2, \"2023-01-01 12:34:45\"");
    }

    @Test
    public void testInsertHiveNonManagedTable(@Mocked HiveTable hiveTable) {
        new MockUp<MetadataMgr>() {
            @Mock
            public Database getDb(ConnectContext context, String catalogName, String dbName) {
                return new Database();
            }
        };

        new MockUp<MetaUtils>() {
            @Mock
            public Table getSessionAwareTable(ConnectContext conntext, Database database, TableName tableName) {
                return hiveTable;
            }
        };

        new Expectations(hiveTable) {
            {
                hiveTable.supportInsert();
                result = true;
                minTimes = 0;

                hiveTable.isHiveTable();
                result = true;
                minTimes = 0;

                hiveTable.isUnPartitioned();
                result = false;
                minTimes = 0;

                hiveTable.getHiveTableType();
                result = HiveTable.HiveTableType.EXTERNAL_TABLE;
                minTimes = 0;
            }
        };

        analyzeFail("insert into hive_catalog.db.tbl select 1, 2, 3", 
                "Only support to write hive managed table");
    }

    @Test
    public void testTableFunctionTable() {
        analyzeSuccess("insert into files ( \n" +
                "\t\"path\" = \"s3://path/to/directory/\", \n" +
                "\t\"format\"=\"parquet\", \n" +
                "\t\"compression\" = \"uncompressed\" ) \n" +
                "select \"abc\" as k1");

        analyzeSuccess("insert into files ( \n" +
                "\t\"path\" = \"s3://path/to/directory/\", \n" +
                "\t\"format\"=\"parquet\" ) \n" +
                "select \"abc\" as k1");

        analyzeFail("insert into files ( \n" +
                "\t\"format\"=\"parquet\", \n" +
                "\t\"compression\" = \"uncompressed\" ) \n" +
                "select \"abc\" as k1",
                "path is a mandatory property. \"path\" = \"s3://path/to/your/location/\".");

        analyzeFail("insert into files ( \n" +
                        "\t\"path\" = \"s3://path/to/directory/\", \n" +
                        "\t\"compression\" = \"uncompressed\" ) \n" +
                        "select \"abc\" as k1",
                "format is a mandatory property. Use any of (parquet, orc, csv).");

        analyzeFail("insert into files ( \n" +
                "\t\"path\" = \"s3://path/to/directory/\", \n" +
                "\t\"format\"=\"unknown\", \n" +
                "\t\"compression\" = \"uncompressed\" ) \n" +
                "select \"abc\" as k1",
                "Unsupported format unknown. Use any of (parquet, orc, csv).");

        analyzeFail("insert into files ( \n" +
                        "\t\"path\" = \"s3://path/to/directory/\", \n" +
                        "\t\"format\"=\"parquet\", \n" +
                        "\t\"compression\" = \"unknown\" ) \n" +
                        "select \"abc\" as k1",
                "Unsupported compression codec unknown. Use any of (uncompressed, snappy, lz4, zstd, gzip).");

        analyzeFail("insert into files ( \n" +
                        "\t\"path\" = \"s3://path/to/directory/\", \n" +
                        "\t\"format\"=\"parquet\", \n" +
                        "\t\"compression\" = \"uncompressed\", \n" +
                        "\t\"partition_by\"=\"k1\",\n" +
                        "\t\"single\"=\"true\" ) \n" +
                        "select \"abc\" as k1",
                "cannot use partition_by and single simultaneously.");

        analyzeSuccess("insert into files ( \n" +
                        "\t\"path\" = \"s3://path/to/directory/\", \n" +
                        "\t\"format\"=\"parquet\", \n" +
                        "\t\"compression\" = \"uncompressed\", \n" +
                        "\t\"partition_by\"=\"k1\" ) \n" +
                        "select \"abc\" as k1");

        analyzeSuccess("insert into files ( \n" +
                "\t\"path\" = \"s3://path/to/directory/\", \n" +
                "\t\"format\"=\"parquet\", \n" +
                "\t\"compression\" = \"uncompressed\", \n" +
                "\t\"partition_by\"=\"k1, k2\" ) \n" +
                "select \"abc\" as k1, 123 as k2");

        analyzeFail("insert into files ( \n" +
                "\t\"path\" = \"s3://path/to/directory/\", \n" +
                "\t\"format\"=\"parquet\", \n" +
                "\t\"compression\" = \"uncompressed\", \n" +
                "\t\"partition_by\"=\"k3\" ) \n" +
                "select \"abc\" as k1, 123 as k2",
                "partition columns expected to be a subset of [k1, k2], but got extra columns: [k3]");

        analyzeSuccess("insert into files ( \n" +
                "\t\"path\" = \"s3://path/to/directory/\", \n" +
                "\t\"format\"=\"parquet\", \n" +
                "\t\"compression\" = \"uncompressed\", \n" +
                "\t\"single\"=\"true\" ) \n" +
                "select \"abc\" as k1, 123 as k2");

        analyzeFail("insert into files ( \n" +
                "\t\"path\" = \"s3://path/to/directory/\", \n" +
                "\t\"format\"=\"parquet\", \n" +
                "\t\"compression\" = \"uncompressed\", \n" +
                "\t\"single\"=\"false-false\" ) \n" +
                "select \"abc\" as k1, 123 as k2",
                "got invalid parameter \"single\" = \"false-false\", expect a boolean value (true or false).");

        analyzeFail("insert into files ( \n" +
                        "\t\"path\" = \"s3://path/to/directory/\", \n" +
                        "\t\"format\"=\"parquet\", \n" +
                        "\t\"compression\" = \"uncompressed\", \n" +
                        "\t\"parquet.use_legacy_encoding\"=\"f\" ) \n" +
                        "select \"abc\" as k1, 123 as k2",
                "got invalid parameter \"parquet.use_legacy_encoding\" = \"f\", expect a boolean value (true or false).");

        analyzeSuccess("insert into files ( \n" +
                        "\t\"path\" = \"s3://path/to/directory/\", \n" +
                        "\t\"format\"=\"parquet\", \n" +
                        "\t\"compression\" = \"uncompressed\", \n" +
                        "\t\"parquet.use_legacy_encoding\"=\"true\" ) \n" +
                        "select \"abc\" as k1, 123 as k2");

        analyzeFail("insert into files ( \n" +
                "\t\"path\" = \"s3://path/to/directory/\", \n" +
                "\t\"format\"=\"parquet\", \n" +
                "\t\"compression\" = \"uncompressed\", \n" +
                "\t\"partition_by\"=\"k1\" ) \n" +
                "select 1.23 as k1", "partition column does not support type of DECIMAL32(3,2).");

        analyzeFail("insert into files ( \n" +
                "\t\"path\" = \"s3://path/to/directory/\", \n" +
                "\t\"format\"=\"parquet\", \n" +
                "\t\"compression\" = \"uncompressed\" ) \n" +
                "select 1 as a, 2 as a", "expect column names to be distinct, but got duplicate(s): [a]");
    }

    @Test
    public void testInsertFailAbortTransaction() throws Exception {
        StarRocksAssert starRocksAssert = getStarRocksAssert();
        ConnectContext connectContext = getConnectContext();
        starRocksAssert.withDatabase("insert_fail").withTable("create table insert_fail.t1 (k1 int, k2 int) " +
                "distributed by hash(k1) buckets 1 properties ('replication_num' = '1')");

        String insertSql = "insert into insert_fail.t1 values (1)";
        connectContext.setQueryId(UUIDUtil.genUUID());
        StatementBase statement = SqlParser.parseSingleStatement(insertSql, connectContext.getSessionVariable().getSqlMode());
        try {
            new StmtExecutor(connectContext, statement).execute();
        } catch (Exception e) {
            Assertions.assertTrue(
                    e.getMessage().contains("Inserted target column count: 2 doesn't match select/value column count: 1"));
        }

        List<List<String>> results = starRocksAssert.show("show proc '/transactions/insert_fail'");
        Assertions.assertEquals(2, results.size());
        for (List<String> row : results) {
            Assertions.assertEquals(2, row.size());
            if (row.get(0).equals("running")) {
                Assertions.assertEquals("0", row.get(1));
            } else if (row.get(0).equals("finished")) {
                Assertions.assertEquals("1", row.get(1));
            }
        }
    }

    /**
     * Test for areTablesCopySafe method.
     */
    @Test
    public void testAreTablesCopySafeForInsertStmt() {
        // Test 1: INSERT INTO olap_table SELECT * FROM olap_table
        // Both target and source tables are OlapTable, should be copy safe
        InsertStmt insertStmt1 = (InsertStmt) analyzeSuccess("insert into t0 select * from t0");
        Assertions.assertTrue(AnalyzerUtils.areTablesCopySafe(insertStmt1));

        // Test 2: INSERT INTO olap_table VALUES (...)
        // Target table is OlapTable, should be copy safe
        InsertStmt insertStmt2 = (InsertStmt) analyzeSuccess("insert into t0 values(1, 2, 3)");
        Assertions.assertTrue(AnalyzerUtils.areTablesCopySafe(insertStmt2));

        // Test 3: INSERT INTO olap_table SELECT from multiple olap tables with JOIN
        InsertStmt insertStmt3 = (InsertStmt) analyzeSuccess(
                "insert into t0 select t1.v4, t1.v5, t1.v6 from t1 join t2 on t1.v4 = t2.v7");
        Assertions.assertTrue(AnalyzerUtils.areTablesCopySafe(insertStmt3));

        // Test 4: INSERT INTO olap_table SELECT with subquery
        InsertStmt insertStmt4 = (InsertStmt) analyzeSuccess(
                "insert into t0 select * from (select v4, v5, v6 from t1) sub");
        Assertions.assertTrue(AnalyzerUtils.areTablesCopySafe(insertStmt4));
    }

    /**
     * Test areTablesCopySafe for INSERT statement with Hive table.
     * Hive table is in IMMUTABLE_EXTERNAL_TABLES, so it should be copy safe.
     */
    @Test
    public void testAreTablesCopySafeForInsertIntoHiveTable(@Mocked HiveTable hiveTable) {
        new MockUp<MetadataMgr>() {
            @Mock
            public Database getDb(ConnectContext context, String catalogName, String dbName) {
                if ("hive_catalog".equals(catalogName)) {
                    return new Database();
                }
                return GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbName);
            }
        };

        new MockUp<MetaUtils>() {
            @Mock
            public Table getSessionAwareTable(ConnectContext context, Database database, TableName tableName) {
                if ("hive_catalog".equals(tableName.getCatalog())) {
                    return hiveTable;
                }
                return GlobalStateMgr.getCurrentState().getLocalMetastore().getTable("test", tableName.getTbl());
            }
        };

        new Expectations(hiveTable) {
            {
                hiveTable.supportInsert();
                result = true;
                minTimes = 0;

                hiveTable.isHiveTable();
                result = true;
                minTimes = 0;

                hiveTable.isUnPartitioned();
                result = true;
                minTimes = 0;

                hiveTable.getTableLocation();
                result = "hdfs://path/to/hive/table";
                minTimes = 0;

                hiveTable.getHiveTableType();
                result = HiveTable.HiveTableType.MANAGED_TABLE;
                minTimes = 0;

                hiveTable.getType();
                result = Table.TableType.HIVE;
                minTimes = 0;

                hiveTable.getRelatedMaterializedViews();
                result = com.google.common.collect.Sets.newHashSet();
                minTimes = 0;

                hiveTable.isOlapTableOrMaterializedView();
                result = false;
                minTimes = 0;

                hiveTable.getPartitionColumnNames();
                result = ImmutableList.of();
                minTimes = 0;

                hiveTable.getBaseSchema();
                result = ImmutableList.of(
                        new Column("v1", IntegerType.BIGINT),
                        new Column("v2", IntegerType.BIGINT),
                        new Column("v3", IntegerType.BIGINT));
                minTimes = 0;

                hiveTable.getFullSchema();
                result = ImmutableList.of(
                        new Column("v1", IntegerType.BIGINT),
                        new Column("v2", IntegerType.BIGINT),
                        new Column("v3", IntegerType.BIGINT));
                minTimes = 0;
            }
        };

        // INSERT INTO HiveTable SELECT FROM OlapTable should be copy safe
        // because Hive is in IMMUTABLE_EXTERNAL_TABLES
        InsertStmt insertStmt = (InsertStmt) analyzeSuccess(
                "insert into hive_catalog.db.tbl select v1, v2, v3 from t0");
        Assertions.assertTrue(AnalyzerUtils.areTablesCopySafe(insertStmt));
    }

    /**
     * Test areTablesCopySafe for INSERT statement with Mysql table.
     * Mysql table is not in IMMUTABLE_EXTERNAL_TABLES, so it should not be copy safe.
     */
    @Test
    public void testAreTablesCopySafeForInsertIntoMysqlTable(@Mocked MysqlTable mysqlTable) {
        new MockUp<MetaUtils>() {
            @Mock
            public Table getSessionAwareTable(ConnectContext context, Database database, TableName tableName) {
                if ("mysql_table".equals(tableName.getTbl())) {
                    return mysqlTable;
                }
                return GlobalStateMgr.getCurrentState().getLocalMetastore().getTable("test", tableName.getTbl());
            }
        };

        new Expectations(mysqlTable) {
            {
                mysqlTable.supportInsert();
                result = true;
                minTimes = 0;

                mysqlTable.getType();
                result = Table.TableType.MYSQL;
                minTimes = 0;

                mysqlTable.getRelatedMaterializedViews();
                result = com.google.common.collect.Sets.newHashSet();
                minTimes = 0;

                mysqlTable.isOlapTableOrMaterializedView();
                result = false;
                minTimes = 0;

                mysqlTable.getBaseSchema();
                result = ImmutableList.of(
                        new Column("v1", IntegerType.BIGINT),
                        new Column("v2", IntegerType.BIGINT),
                        new Column("v3", IntegerType.BIGINT));
                minTimes = 0;

                mysqlTable.getFullSchema();
                result = ImmutableList.of(
                        new Column("v1", IntegerType.BIGINT),
                        new Column("v2", IntegerType.BIGINT),
                        new Column("v3", IntegerType.BIGINT));
                minTimes = 0;
            }
        };

        // INSERT INTO MysqlTable SELECT FROM OlapTable should not be copy safe
        // because Mysql is not in IMMUTABLE_EXTERNAL_TABLES
        InsertStmt insertStmt = (InsertStmt) analyzeSuccess(
                "insert into test.mysql_table select v1, v2, v3 from t0");
        Assertions.assertFalse(AnalyzerUtils.areTablesCopySafe(insertStmt));
    }
}
