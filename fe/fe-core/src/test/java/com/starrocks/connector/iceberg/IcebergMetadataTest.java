// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.connector.iceberg;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.iceberg.hive.HiveTableOperations;
import mockit.Expectations;
import mockit.Mocked;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.starrocks.catalog.IcebergTable.ICEBERG_CATALOG_TYPE;
import static com.starrocks.catalog.IcebergTable.ICEBERG_METASTORE_URIS;
import static com.starrocks.catalog.Table.TableType.ICEBERG;

public class IcebergMetadataTest {
    private static final String CATALOG_NAME = "IcebergCatalog";

    @Test
    public void testListDatabaseNames(@Mocked HiveMetaStoreClient metaStoreThriftClient) throws Exception {
        new Expectations() {
            {
                metaStoreThriftClient.getAllDatabases();
                result = Lists.newArrayList("db1", "db2");
                minTimes = 0;
            }
        };

        Map<String, String> properties = new HashMap<>();
        String metastoreUris = "thrift://127.0.0.1:9083";
        properties.put(ICEBERG_METASTORE_URIS, metastoreUris);
        properties.put(ICEBERG_CATALOG_TYPE, "hive");
        HdfsEnvironment hdfsEnvironment = new HdfsEnvironment();
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, properties, hdfsEnvironment);
        List<String> expectResult = Lists.newArrayList("db1", "db2");
        Assert.assertEquals(expectResult, metadata.listDbNames());
    }

    @Test
    public void testGetDB(@Mocked IcebergHiveCatalog icebergHiveCatalog) throws Exception {
        String db = "db";

        new Expectations() {
            {
                icebergHiveCatalog.getDB(db);
                result = new Database(0, db);
                minTimes = 0;
            }
        };

        Map<String, String> properties = new HashMap<>();
        HdfsEnvironment hdfsEnvironment = new HdfsEnvironment();
        String metastoreUris = "thrift://127.0.0.1:9083";
        properties.put(ICEBERG_METASTORE_URIS, metastoreUris);
        properties.put(ICEBERG_CATALOG_TYPE, "hive");
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, properties, hdfsEnvironment);
        Database expectResult = new Database(0, db);
        Assert.assertEquals(expectResult, metadata.getDb(db));
    }


    @Test
    public void testListTableNames(@Mocked IcebergHiveCatalog icebergHiveCatalog) throws Exception {
        String db1 = "db1";
        String tbl1 = "tbl1";
        String tbl2 = "tbl2";

        new Expectations() {
            {
                icebergHiveCatalog.listTables(Namespace.of(db1));
                result = Lists.newArrayList(TableIdentifier.of(db1, tbl1), TableIdentifier.of(db1, tbl2));
                minTimes = 0;
            }
        };

        Map<String, String> properties = new HashMap<>();
        HdfsEnvironment hdfsEnvironment = new HdfsEnvironment();
        String metastoreUris = "thrift://127.0.0.1:9083";
        properties.put(ICEBERG_METASTORE_URIS, metastoreUris);
        properties.put(ICEBERG_CATALOG_TYPE, "hive");
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, properties, hdfsEnvironment);
        List<String> expectResult = Lists.newArrayList("tbl1", "tbl2");
        Assert.assertEquals(expectResult, metadata.listTableNames(db1));
    }

    @Test
    public void testGetTable(@Mocked IcebergHiveCatalog icebergHiveCatalog,
                             @Mocked HiveTableOperations hiveTableOperations) {

        new Expectations() {
            {
                icebergHiveCatalog.loadTable(IcebergUtil.getIcebergTableIdentifier("db", "tbl"));
                result = new BaseTable(hiveTableOperations, "tbl");
                minTimes = 0;
            }
        };

        Map<String, String> properties = new HashMap<>();
        HdfsEnvironment hdfsEnvironment = new HdfsEnvironment();
        String metastoreUris = "thrift://127.0.0.1:9083";
        properties.put(ICEBERG_METASTORE_URIS, metastoreUris);
        properties.put(ICEBERG_CATALOG_TYPE, "hive");
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, properties, hdfsEnvironment);
        Table expectResult = new Table(0, "tbl", ICEBERG, new ArrayList<>());
        Assert.assertEquals(expectResult, metadata.getTable("db", "tbl"));
    }
    
    @Test
    public void testNotExistTable(@Mocked IcebergHiveCatalog icebergHiveCatalog,
                                  @Mocked HiveTableOperations hiveTableOperations) {
        new Expectations() {
            {
                icebergHiveCatalog.loadTable(IcebergUtil.getIcebergTableIdentifier("db", "tbl"));
                result = new BaseTable(hiveTableOperations, "tbl");
                minTimes = 0;
            }
        };

        Map<String, String> properties = new HashMap<>();
        HdfsEnvironment hdfsEnvironment = new HdfsEnvironment();
        String metastoreUris = "thrift://127.0.0.1:9083";
        properties.put(ICEBERG_METASTORE_URIS, metastoreUris);
        properties.put(ICEBERG_CATALOG_TYPE, "hive");
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, properties, hdfsEnvironment);
        Assert.assertNull(metadata.getTable("db", "tbl2").getName());
    }
}
