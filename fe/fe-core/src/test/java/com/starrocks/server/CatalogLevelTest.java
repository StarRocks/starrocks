// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.server;

import com.starrocks.catalog.Database;
import com.starrocks.connector.hive.HiveMetaClient;
import com.starrocks.connector.hive.HiveMetastoreApiConverter;
import com.starrocks.connector.hive.HiveMetastoreTest;
import com.starrocks.connector.iceberg.IcebergUtil;
import com.starrocks.connector.iceberg.hive.HiveTableOperations;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mocked;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.iceberg.types.Types.NestedField.optional;

public class CatalogLevelTest {
    HiveMetaClient metaClient = new HiveMetastoreTest.MockedHiveMetaClient();

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
    }

    @Test
    public void testQueryExternalCatalogInDefaultCatalog(@Mocked MetadataMgr metadataMgr) throws Exception {
        String createCatalog = "CREATE EXTERNAL CATALOG hive_catalog PROPERTIES(\"type\"=\"hive\", \"hive.metastore.uris\"=\"thrift://127.0.0.1:9083\")";
        StarRocksAssert starRocksAssert = new StarRocksAssert();
        starRocksAssert.withCatalog(createCatalog);

        Table hmsTable = metaClient.getTable("hive_db", "hive_table");
        com.starrocks.catalog.Table hiveTable = HiveMetastoreApiConverter.toHiveTable(hmsTable, "hive_catalog");
        GlobalStateMgr.getCurrentState().setMetadataMgr(metadataMgr);
        new Expectations(metadataMgr) {
            {
                metadataMgr.getDb("hive_catalog", "hive_db");
                result = new Database(111, "hive_db");
                minTimes = 0;

                metadataMgr.getTable("hive_catalog", "hive_db", "hive_table");
                result = hiveTable;
            }
        };
        String sql1 = "select col1 from hive_catalog.hive_db.hive_table";

        AnalyzeTestUtil.analyzeSuccess(sql1);
    }

    @Test
    public void testQueryIcebergCatalog(@Mocked MetadataMgr metadataMgr,
                                        @Mocked HiveTableOperations hiveTableOperations) throws Exception {
        String createCatalog = "CREATE EXTERNAL CATALOG iceberg_catalog PROPERTIES(\"type\"=\"iceberg\"," +
                " \"iceberg.catalog.hive.metastore.uris\"=\"thrift://127.0.0.1:9083\", \"iceberg.catalog.type\" = \"hive\")";
        StarRocksAssert starRocksAssert = new StarRocksAssert();
        starRocksAssert.withCatalog(createCatalog);
        Configuration conf = new Configuration();
        conf.set(HiveConf.ConfVars.METASTOREURIS.varname, "thrift://127.0.0.1:9083");

        new Expectations() {
            {
                hiveTableOperations.current().schema();
                result = new Schema(optional(1, "col1", Types.LongType.get()));
            }
        };

        org.apache.iceberg.Table tbl = new org.apache.iceberg.BaseTable(hiveTableOperations, "iceberg_table");
        com.starrocks.catalog.Table icebergTable = IcebergUtil.convertHiveCatalogToSRTable(tbl, "thrift://127.0.0.1:9083",
                "iceberg_catalog", "iceberg_db", "iceberg_table");
        GlobalStateMgr.getCurrentState().setMetadataMgr(metadataMgr);
        new Expectations(metadataMgr) {
            {
                metadataMgr.getDb("iceberg_catalog", "iceberg_db");
                result = new Database(111, "iceberg_db");
                minTimes = 0;

                metadataMgr.getTable("iceberg_catalog", "iceberg_db", "iceberg_table");
                result = icebergTable;
            }
        };
        String sql1 = "select col1 from iceberg_catalog.iceberg_db.iceberg_table";

        AnalyzeTestUtil.analyzeSuccess(sql1);

    }
}
