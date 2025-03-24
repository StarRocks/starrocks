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
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.getConnectContext;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.getStarRocksAssert;

public class AnalyzeInsertTest {

    @BeforeClass
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
                result = ImmutableList.of(new Column("c1", Type.INT));
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
                result = ImmutableList.of(new Column("p1", Type.ARRAY_DATE));
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
                result = ImmutableList.of(new Column("c1", Type.INT), new Column("p1", Type.INT), new Column("p2", Type.INT));
                minTimes = 0;

                icebergTable.getColumn(anyString);
                result = ImmutableList.of(new Column("p1", Type.INT), new Column("p2", Type.INT));
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
                result = ImmutableList.of(new Column("c1", Type.INT), new Column("p1", Type.DATETIME),
                        new Column("p2", Type.INT));
                minTimes = 0;

                icebergTable.getColumn(anyString);
                result = ImmutableList.of(new Column("p1", Type.INT), new Column("p2", Type.DATETIME));
                minTimes = 0;

                icebergTable.getPartitionColumnNames();
                result = Lists.newArrayList("p1", "p2");
                minTimes = 1;

                icebergTable.getType();
                result = Table.TableType.ICEBERG;
                minTimes = 1;
            }
        };

        analyzeFail("insert into iceberg_catalog.db.tbl select 1, 2, \"2023-01-01 12:34:45\"",
                "Unsupported partition column type [DATETIME] for ICEBERG table sink.");
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

                hiveTable.isHiveTable();
                result = true;

                hiveTable.isUnPartitioned();
                result = false;

                hiveTable.getHiveTableType();
                result = HiveTable.HiveTableType.EXTERNAL_TABLE;
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
            Assert.assertTrue(
                    e.getMessage().contains("Inserted target column count: 2 doesn't match select/value column count: 1"));
        }

        List<List<String>> results = starRocksAssert.show("show proc '/transactions/insert_fail'");
        Assert.assertEquals(2, results.size());
        for (List<String> row : results) {
            Assert.assertEquals(2, row.size());
            if (row.get(0).equals("running")) {
                Assert.assertEquals("0", row.get(1));
            } else if (row.get(0).equals("finished")) {
                Assert.assertEquals("1", row.get(1));
            }
        }
    }
}
