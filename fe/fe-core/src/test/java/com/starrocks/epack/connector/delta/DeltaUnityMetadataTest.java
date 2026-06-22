// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.connector.delta;

import com.databricks.sdk.core.DatabricksConfig;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DeltaLakeTable;
import com.starrocks.catalog.Table;
import com.starrocks.connector.ConnectorMetadataRequestContext;
import com.starrocks.connector.ConnectorProperties;
import com.starrocks.connector.ConnectorType;
import com.starrocks.connector.DatabaseTableName;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.MetastoreType;
import com.starrocks.connector.delta.CachingDeltaLakeMetastore;
import com.starrocks.connector.delta.DeltaLakeCatalogProperties;
import com.starrocks.connector.delta.DeltaLakeMetadata;
import com.starrocks.connector.delta.DeltaLakeMetastore;
import com.starrocks.connector.delta.DeltaLakeSnapshot;
import com.starrocks.connector.delta.DeltaMetastoreOperations;
import com.starrocks.connector.delta.DeltaUtils;
import com.starrocks.connector.metastore.MetastoreTable;
import com.starrocks.qe.ConnectContext;
import io.delta.kernel.Scan;
import io.delta.kernel.ScanBuilder;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.defaults.internal.data.DefaultColumnarBatch;
import io.delta.kernel.defaults.internal.data.vector.DefaultBinaryVector;
import io.delta.kernel.defaults.internal.data.vector.DefaultMapVector;
import io.delta.kernel.defaults.internal.data.vector.DefaultStructVector;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.types.BasePrimitiveType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Optional;

public class DeltaUnityMetadataTest {
    private static final String DATABRICKS_HOST = "https://xxxx.cloud.databricks.com";
    private static final String DATABRICKS_TOKEN = "test_token";
    private DeltaLakeMetadata deltaLakeUnityMetadata;

    @Before
    public void setUp() throws Exception {
        DatabricksConfig config = new DatabricksConfig().setHost(DATABRICKS_HOST).setToken(DATABRICKS_TOKEN);
        HdfsEnvironment hdfsEnvironment = new HdfsEnvironment(Maps.newHashMap());

        DatabricksUnityMetastore databricksUnityMetastore = new DatabricksUnityMetastore("databricks0",
                "databricks_catalog", new MockDatabricksWorkspaceClient(config), hdfsEnvironment,
                new DeltaLakeCatalogProperties(Maps.newHashMap()));
        UnityBackedDeltaLakeMetastore unityBackedDeltaLakeMetastore = new UnityBackedDeltaLakeMetastore("databricks0",
                databricksUnityMetastore, hdfsEnvironment.getConfiguration(),
                new DeltaLakeCatalogProperties(Maps.newHashMap()));

        DeltaMetastoreOperations metastoreOperations = new DeltaMetastoreOperations(
                CachingDeltaLakeMetastore.createQueryLevelInstance(unityBackedDeltaLakeMetastore, 10000),
                false, MetastoreType.UNITY);
        deltaLakeUnityMetadata = new DeltaLakeMetadata(hdfsEnvironment, "databricks0", metastoreOperations, null,
                new ConnectorProperties(ConnectorType.DELTALAKE));
    }

    @Test
    public void testGetCatalogName() {
        Assert.assertEquals("databricks0", deltaLakeUnityMetadata.getCatalogName());
    }

    @Test
    public void testGetTableType() {
        Assert.assertEquals(Table.TableType.DELTALAKE, deltaLakeUnityMetadata.getTableType());
    }

    @Test
    public void testListDbNames() {
        List<String> databaseNames = deltaLakeUnityMetadata.listDbNames(new ConnectContext());
        Assert.assertEquals(2, databaseNames.size());
        Assert.assertEquals(Lists.newArrayList("db1", "db2"), databaseNames);
    }

    @Test
    public void testListTableNames() {
        List<String> tableNames = deltaLakeUnityMetadata.listTableNames(new ConnectContext(), "db1");
        Assert.assertEquals(2, tableNames.size());
        Assert.assertEquals(Lists.newArrayList("table1", "table2"), tableNames);
    }

    @Test
    public void testGetDb() {
        Database db = deltaLakeUnityMetadata.getDb(new ConnectContext(), "db1");
        Assert.assertEquals("db1", db.getFullName());
        Assert.assertEquals("s3://bucket/path/to/db1", db.getLocation());
    }

    @Test
    public void testGetTable() {
        new MockUp<CachingDeltaLakeMetastore>() {
            @mockit.Mock
            public DeltaLakeSnapshot getCachedSnapshot(DatabaseTableName databaseTableName) {
                return new DeltaLakeSnapshot("db1", "table1", null, null,
                        new MetastoreTable("db1", "table1", "s3://bucket/path/to/table", 123));
            }
        };

        new MockUp<DeltaUtils>() {
            @Mock
            public DeltaLakeTable convertDeltaSnapshotToSRTable(String catalog, DeltaLakeSnapshot snapshot) {
                return new DeltaLakeTable(1, "databricks0", "db1", "table1",
                        Lists.newArrayList(), Lists.newArrayList(), null, null,
                        new MetastoreTable("db1", "table1", "s3://bucket/path/to/table", 123));
            }
        };

        Table table = deltaLakeUnityMetadata.getTable(new ConnectContext(), "db1", "table1");
        Assert.assertEquals("table1", table.getName());
        Assert.assertEquals("databricks0", table.getCatalogName());
        Assert.assertTrue(table.isDeltalakeTable());
    }

    @Test
    public void testTableExist() {
        Assert.assertTrue(deltaLakeUnityMetadata.tableExists(new ConnectContext(), "db1", "table1"));
    }

    @Test
    public void testListPartitionNames(@Mocked SnapshotImpl snapshot, @Mocked ScanBuilder scanBuilder,
                                       @Mocked Scan scan) {
        new MockUp<DeltaLakeMetastore>() {
            @mockit.Mock
            public DeltaLakeSnapshot getLatestSnapshot(String dbName, String tableName) {
                return new DeltaLakeSnapshot("db1", "table1", null, null,
                        new MetastoreTable("db1", "table1", "s3://bucket/path/to/table", 123));
            }
        };

        new MockUp<DeltaUtils>() {
            @Mock
            public DeltaLakeTable convertDeltaSnapshotToSRTable(String catalog, DeltaLakeSnapshot deltaLakeSnapshot) {
                return new DeltaLakeTable(1, "databricks0", "db1", "table1",
                        Lists.newArrayList(), Lists.newArrayList("ts"), snapshot, null,
                        new MetastoreTable("db1", "table1", "s3://bucket/path/to/table", 123));
            }
        };

        // mock schema:
        // struct<add:struct<path:string,partitionValues:map<string,string>>>
        List<FilteredColumnarBatch> filteredColumnarBatches = Lists.newArrayList();

        ColumnVector[] addFileCols = new ColumnVector[2];
        addFileCols[0] = new DefaultBinaryVector(BasePrimitiveType.createPrimitive("string"),
                3, new byte[][] {new byte[] {'0', '0', '0', '0'},
                    new byte[] {'0', '0', '0', '1'}, new byte[] {'0', '0', '0', '2'}});

        int[] offsets = new int[] {0, 1, 2, 3};
        DataType mapType = new MapType(StringType.STRING, StringType.STRING, true);
        addFileCols[1] = new DefaultMapVector(3, mapType, Optional.empty(), offsets,
                new DefaultBinaryVector(BasePrimitiveType.createPrimitive("string"),
                        3, new byte[][] {new byte[] {'t', 's'}, new byte[] {'t', 's'}, new byte[] {'t', 's'}}),
                new DefaultBinaryVector(BasePrimitiveType.createPrimitive("string"),
                        3, new byte[][] {new byte[] {'1', '9', '9', '9'}, new byte[] {'2', '0', '0', '0'},
                            new byte[] {'2', '0', '0', '1'}})
        );
        // addFile schema, here we only care about the partitionValues, so not use all fields
        StructType addFileSchema = new StructType(Lists.newArrayList(
                new StructField("path", BasePrimitiveType.createPrimitive("string"), true),
                new StructField("partitionValues", mapType, true)));
        DefaultStructVector addFile = new DefaultStructVector(3, addFileSchema, Optional.empty(), addFileCols);
        // construct a columnar batch which only contains addFile
        ColumnarBatch columnarBatch = new DefaultColumnarBatch(3,
                new StructType(Lists.newArrayList(new StructField("add", addFileSchema, true))),
                new DefaultStructVector[] {addFile});

        FilteredColumnarBatch filteredColumnarBatch = new FilteredColumnarBatch(columnarBatch, Optional.empty());
        filteredColumnarBatches.add(filteredColumnarBatch);
        CloseableIterator<FilteredColumnarBatch> scanFilesAsBatches = new CloseableIterator<FilteredColumnarBatch>() {
            private int index = 0;

            @Override
            public boolean hasNext() {
                return index < filteredColumnarBatches.size();
            }

            @Override
            public FilteredColumnarBatch next() {
                return filteredColumnarBatches.get(index++);
            }

            @Override
            public void close() {
            }
        };

        new Expectations() {
            {
                snapshot.getScanBuilder();
                result = scanBuilder;
                minTimes = 0;

                scanBuilder.build();
                result = scan;
                minTimes = 0;

                scan.getScanFiles((Engine) any);
                result = scanFilesAsBatches;
                minTimes = 0;
            }
        };
        List<String> partitionNames = deltaLakeUnityMetadata.listPartitionNames("db1", "table1",
                ConnectorMetadataRequestContext.DEFAULT);
        Assert.assertEquals(3, partitionNames.size());
        Assert.assertEquals("ts=1999", partitionNames.get(0));
        Assert.assertEquals("ts=2000", partitionNames.get(1));
        Assert.assertEquals("ts=2001", partitionNames.get(2));
    }
}
