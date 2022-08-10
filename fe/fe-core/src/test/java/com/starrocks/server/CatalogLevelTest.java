// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.server;

import com.amazonaws.services.dynamodbv2.document.Expected;
import com.google.common.collect.Lists;
import com.starrocks.analysis.StatementBase;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Resource;
import com.starrocks.catalog.ResourceMgr;
import com.starrocks.external.HiveMetaStoreTableUtils;
import com.starrocks.external.hive.HiveRepository;
import com.starrocks.external.iceberg.IcebergUtil;
import com.starrocks.external.iceberg.hive.HiveTableOperations;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.ast.CreateCatalogStmt;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import javax.validation.constraints.AssertTrue;

import static org.apache.iceberg.types.Types.NestedField.optional;

public class CatalogLevelTest {

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
        List<FieldSchema> partKeys = Lists.newArrayList(new FieldSchema("col1", "BIGINT", ""));
        List<FieldSchema> unPartKeys = Lists.newArrayList(new FieldSchema("col2", "INT", ""));
        String hdfsPath = "hdfs://127.0.0.1:10000/hive";
        StorageDescriptor sd = new StorageDescriptor();
        sd.setCols(unPartKeys);
        sd.setLocation(hdfsPath);
        Table msTable1 = new Table();
        msTable1.setDbName("hive_db");
        msTable1.setTableName("hive_table");
        msTable1.setPartitionKeys(partKeys);
        msTable1.setSd(sd);
        msTable1.setTableType("MANAGED_TABLE");
        com.starrocks.catalog.Table hiveTable = HiveMetaStoreTableUtils.convertHiveConnTableToSRTable(msTable1, "thrift://127.0.0.1:9083");
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
        String sql_1 = "select col1 from hive_catalog.hive_db.hive_table";

        AnalyzeTestUtil.analyzeSuccess(sql_1);

    }

    @Test
    public void testQueryHudiCatalog(@Mocked MetadataMgr metadataMgr,
                                     @Mocked ResourceMgr resourceMgr,
                                     @Mocked HiveRepository hiveRepository,
                                     @Mocked HoodieTableMetaClient metaClient,
                                     @Mocked TableSchemaResolver schemaUtil) throws Exception {
        String createCatalog = "CREATE EXTERNAL CATALOG hudi_catalog PROPERTIES(\"type\"=\"hudi\", \"hive.metastore.uris\"=\"thrift://127.0.0.1:9083\")";
        StatementBase statementBase = AnalyzeTestUtil.analyzeSuccess(createCatalog);
        Assert.assertTrue(statementBase instanceof CreateCatalogStmt);
        GlobalStateMgr.getCurrentState().getCatalogMgr().createCatalog((CreateCatalogStmt) statementBase);
        List<FieldSchema> partKeys = Lists.newArrayList(new FieldSchema("col1", "BIGINT", ""));
        List<FieldSchema> unPartKeys = Lists.newArrayList(new FieldSchema("col2", "INT", ""));
        String hdfsPath = "hdfs://127.0.0.1:10000/hudi";
        StorageDescriptor sd = new StorageDescriptor();
        sd.setCols(unPartKeys);
        sd.setLocation(hdfsPath);
        Table msTable1 = new Table();
        msTable1.setDbName("hudi_db");
        msTable1.setTableName("hudi_table");
        msTable1.setPartitionKeys(partKeys);
        msTable1.setSd(sd);
        msTable1.setTableType("MANAGED_TABLE");

        List<org.apache.avro.Schema.Field> hudiFields = new ArrayList<>();
        hudiFields.add(new org.apache.avro.Schema.Field("col1",
                org.apache.avro.Schema.createUnion(
                        org.apache.avro.Schema.create(org.apache.avro.Schema.Type.NULL), org.apache.avro.Schema.create(
                                org.apache.avro.Schema.Type.LONG)), "", null));
        hudiFields.add(new org.apache.avro.Schema.Field("col2",
                org.apache.avro.Schema.createUnion(
                        org.apache.avro.Schema.create(org.apache.avro.Schema.Type.NULL), org.apache.avro.Schema.create(
                                org.apache.avro.Schema.Type.INT)), "", null));
        org.apache.avro.Schema hudiSchema = org.apache.avro.Schema.createRecord(hudiFields);

        new MockUp<ResourceMgr>(ResourceMgr.class) {
            @Mock
            public Resource getResource(String name) {
                return null;
            }
        };

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState().getResourceMgr();
                result = resourceMgr;

                GlobalStateMgr.getCurrentState().getHiveRepository();
                result = hiveRepository;

                schemaUtil.getTableAvroSchema();
                result = hudiSchema;
            }
        };

        com.starrocks.catalog.Table hudiTable = HiveMetaStoreTableUtils.convertHudiConnTableToSRTable(msTable1, "thrift://127.0.0.1:9083");

        new Expectations() {
            {
                metadataMgr.getDb("hudi_catalog", "hudi_db");
                result = new Database(111, "hudi_db");
                minTimes = 0;

                metadataMgr.getTable("hudi_catalog", "hudi_db", "hudi_table");
                result = hudiTable;
            }
        };
        String sql_1 = "select col1 from hudi_catalog.hudi_db.hudi_table";

        AnalyzeTestUtil.analyzeSuccess(sql_1);
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
                "iceberg_db", "iceberg_table");
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
        String sql_1 = "select col1 from iceberg_catalog.iceberg_db.iceberg_table";

        AnalyzeTestUtil.analyzeSuccess(sql_1);

    }
}
