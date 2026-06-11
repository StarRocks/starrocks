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

package com.starrocks.connector.odps;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Partition;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OdpsTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.ConnectorMetadataRequestContext;
import com.starrocks.connector.GetRemoteFilesParams;
import com.starrocks.connector.PartitionInfo;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.credential.CloudType;
import com.starrocks.credential.aliyun.AliyunCloudConfiguration;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.PartitionValue;
import com.starrocks.thrift.TTableDescriptor;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

public class OdpsMetadataTest extends MockedBase {

    @Mock
    protected static OdpsMetadata odpsMetadata;

    @BeforeAll
    public static void setUp() throws IOException, ExecutionException, OdpsException {
        initMock();
        odpsMetadata = new OdpsMetadata(odps, "odps", aliyunCloudCredential, odpsProperties);
    }

    @Test
    public void testInitMeta() {
        Map<String, String> properties = new HashMap<>();
        properties.put(OdpsProperties.ACCESS_ID, "ak");
        properties.put(OdpsProperties.ACCESS_KEY, "sk");
        properties.put(OdpsProperties.ENDPOINT, "http://127.0.0.1");
        properties.put(OdpsProperties.PROJECT, "project");
        properties.put(OdpsProperties.TUNNEL_QUOTA, "pay-as-you-go");
        properties.put(OdpsProperties.ENABLE_PARTITION_CACHE, "false");
        properties.put(OdpsProperties.ENABLE_TABLE_CACHE, "false");
        properties.put(OdpsProperties.ENABLE_TABLE_NAME_CACHE, "true");
        OdpsMetadata metadata = new OdpsMetadata(odps, "odps", aliyunCloudCredential, new OdpsProperties(properties));
        Assertions.assertNotNull(metadata);
    }

    @Test
    public void testGetMetadata() {
        OdpsConnector connector = new OdpsConnector(context);
        Assertions.assertNotNull(connector);
        ConnectorMetadata metadata = connector.getMetadata();
        Assertions.assertNotNull(metadata);
    }

    @Test
    public void testListDbNames() {
        List<String> expectedDbNames = Collections.singletonList("project");
        List<String> dbNames = odpsMetadata.listDbNames(new ConnectContext());
        Assertions.assertEquals(dbNames, expectedDbNames);
    }

    @Test
    public void testGetDb() {
        Database database = odpsMetadata.getDb(new ConnectContext(), "project");
        Assertions.assertNotNull(database);
        Assertions.assertEquals(database.getFullName(), "project");
    }

    @Test
    public void testListTableNames() {
        List<String> project = odpsMetadata.listTableNames(new ConnectContext(), "project");
        Assertions.assertEquals(Collections.singletonList("tableName"), project);
    }

    @Test
    public void testGetTable() throws ExecutionException {
        OdpsTable table = (OdpsTable) odpsMetadata.getTable(new ConnectContext(), "project", "tableName");
        Assertions.assertTrue(table.isOdpsTable());
        Assertions.assertEquals("tableName", table.getName());
        Assertions.assertEquals("project", table.getCatalogDBName());
        Assertions.assertFalse(table.isUnPartitioned());
        Assertions.assertEquals("c1", table.getColumn("c1").getName());
    }

    @Test
    public void testListPartitionNames() {
        List<String> partitionNames =
                odpsMetadata.listPartitionNames("project", "tableName", ConnectorMetadataRequestContext.DEFAULT);
        Assertions.assertEquals(Collections.singletonList("p1=a/p2=b"), partitionNames);
    }

    @Test
    public void testListPartitionNamesByValue() {
        List<String> partitions = odpsMetadata.listPartitionNamesByValue("project", "tableName",
                ImmutableList.of(Optional.of("a"), Optional.empty()));
        Assertions.assertEquals(Collections.singletonList("p1=a/p2=b"), partitions);

        partitions = odpsMetadata.listPartitionNamesByValue("project", "tableName",
                ImmutableList.of(Optional.empty(), Optional.of("b")));
        Assertions.assertEquals(Collections.singletonList("p1=a/p2=b"), partitions);
    }

    @Test
    public void testGetPartitions() {
        Table table = odpsMetadata.getTable(new ConnectContext(), "db", "tbl");
        List<String> partitionNames = odpsMetadata.listPartitionNames("db", "tbl", ConnectorMetadataRequestContext.DEFAULT);
        List<PartitionInfo> partitions = odpsMetadata.getPartitions(table, partitionNames);
        Assertions.assertEquals(1, partitions.size());
        PartitionInfo partitionInfo = partitions.get(0);
        Assertions.assertTrue(partitionInfo.getModifiedTime() > 0);
    }

    @Test
    public void testRefreshTable() {
        Table odpsTable = odpsMetadata.getTable(new ConnectContext(), "project", "tableName");
        // mock schema change
        when(table.getSchema()).thenReturn(new TableSchema());

        Table cacheTable = odpsMetadata.getTable(new ConnectContext(), "project", "tableName");
        Assertions.assertTrue(cacheTable.getColumns().size() > 0);

        odpsMetadata.refreshTable("project", odpsTable, null, false);
        Table refreshTable = odpsMetadata.getTable(new ConnectContext(), "project", "tableName");
        Assertions.assertTrue(refreshTable.getColumns().size() == 0);
    }

    @Test
    public void testGetRemoteFiles() throws AnalysisException, IOException {
        Table odpsTable = odpsMetadata.getTable(new ConnectContext(), "project", "tableName");
        PartitionKey partitionKey =
                PartitionKey.createPartitionKey(ImmutableList.of(new PartitionValue("a"), new PartitionValue("b")),
                        odpsTable.getPartitionColumns());
        GetRemoteFilesParams params = GetRemoteFilesParams.newBuilder().setFieldNames(odpsTable.getPartitionColumnNames())
                .setPartitionKeys(ImmutableList.of(partitionKey)).build();
        List<RemoteFileInfo> remoteFileInfos =
                odpsMetadata.getRemoteFiles(odpsTable, params, mockTableReadSessionBuilder);
        Assertions.assertEquals(1, remoteFileInfos.size());
    }

    @Test
    public void testGetCloudConfiguration() {
        AliyunCloudConfiguration cloudConfiguration = (AliyunCloudConfiguration) odpsMetadata.getCloudConfiguration();
        Assertions.assertEquals(CloudType.ALIYUN, cloudConfiguration.getCloudType());
        Assertions.assertEquals("ak", cloudConfiguration.getAliyunCloudCredential().getAccessKey());
        Assertions.assertEquals("sk", cloudConfiguration.getAliyunCloudCredential().getSecretKey());
        Assertions.assertEquals("http://127.0.0.1", cloudConfiguration.getAliyunCloudCredential().getEndpoint());
    }

    @Test
    public void testOdpsTableToThrift() {
        OdpsTable odpsTable = new OdpsTable("catalog", table);
        TTableDescriptor thrift = odpsTable.toThrift(null);
        Assertions.assertNotNull(thrift);
    }

    @Test
    public void testGetMaxPartitionValueWithCache() {
        // Use a unique table name to avoid affecting other tests
        String testDbName = "test_project_cache";
        String testTableName = "test_table_cache";
        
        // Create multiple partitions with different values
        // We'll create partitions with values: 20230101, 20230102, 20230103, 20230105
        // The max partition should be "20230105"
        Partition partition1 = Mockito.mock(Partition.class);
        Partition partition2 = Mockito.mock(Partition.class);
        Partition partition3 = Mockito.mock(Partition.class);
        Partition partition4 = Mockito.mock(Partition.class);

        PartitionSpec spec1 = new PartitionSpec("p1=20230101");
        PartitionSpec spec2 = new PartitionSpec("p1=20230102");
        PartitionSpec spec3 = new PartitionSpec("p1=20230103");
        PartitionSpec spec4 = new PartitionSpec("p1=20230105");

        when(partition1.getPartitionSpec()).thenReturn(spec1);
        when(partition1.getPhysicalSize()).thenReturn(1000L);
        when(partition2.getPartitionSpec()).thenReturn(spec2);
        when(partition2.getPhysicalSize()).thenReturn(2000L);
        when(partition3.getPartitionSpec()).thenReturn(spec3);
        when(partition3.getPhysicalSize()).thenReturn(3000L);
        when(partition4.getPartitionSpec()).thenReturn(spec4);
        when(partition4.getPhysicalSize()).thenReturn(4000L);

        // Create a new independent mock Table to avoid affecting other tests
        com.aliyun.odps.Table testOdpsTable = Mockito.mock(com.aliyun.odps.Table.class);
        when(testOdpsTable.getPartitions()).thenReturn(ImmutableList.of(partition1, partition2, partition3, partition4));
        when(testOdpsTable.getName()).thenReturn(testTableName);
        when(testOdpsTable.getCreatedTime()).thenReturn(new java.util.Date());
        when(testOdpsTable.getProject()).thenReturn(testDbName);
        try {
            doNothing().when(testOdpsTable).reload();
        } catch (OdpsException e) {
            // Ignore
        }
        
        // Set up table schema similar to MockedBase
        TableSchema tableSchema = new TableSchema();
        tableSchema.addColumn(new Column("c1", TypeInfoFactory.STRING));
        tableSchema.addColumn(new Column("c2", TypeInfoFactory.BIGINT));
        tableSchema.addPartitionColumn(new Column("p1", TypeInfoFactory.STRING));
        when(testOdpsTable.getSchema()).thenReturn(tableSchema);
        
        // Mock odps.tables().get() to return our test table for the specific table name
        // This won't affect other tests since we use a unique table name
        when(tables.get(testDbName, testTableName)).thenReturn(testOdpsTable);
        
        // Create a new OdpsMetadata instance with partition cache enabled
        Map<String, String> properties = new HashMap<>();
        properties.put(OdpsProperties.ACCESS_ID, "ak");
        properties.put(OdpsProperties.ACCESS_KEY, "sk");
        properties.put(OdpsProperties.ENDPOINT, "http://127.0.0.1");
        properties.put(OdpsProperties.PROJECT, testDbName);
        properties.put(OdpsProperties.TUNNEL_QUOTA, "pay-as-you-go");
        properties.put(OdpsProperties.ENABLE_PARTITION_CACHE, "true");
        properties.put(OdpsProperties.PARTITION_CACHE_EXPIRE_TIME, "1");
        properties.put(OdpsProperties.PARTITION_CACHE_SIZE, "1000");
        OdpsMetadata metadata = new OdpsMetadata(odps, "odps", aliyunCloudCredential, new OdpsProperties(properties));

        // Get table to populate the cache
        Table testTable = metadata.getTable(new ConnectContext(), testDbName, testTableName);
        Assertions.assertNotNull(testTable);

        // Test with nonEmptyPartition = false - should return max partition value from cache
        // Expected max: "20230105" (the largest value among all partitions)
        String maxPartition = metadata.getMaxPartitionValue(testTable, false);
        Assertions.assertNotNull(maxPartition);
        Assertions.assertEquals("20230105", maxPartition,
                "Max partition should be 20230105, which is the largest value among all partitions in cache");

        // Test with nonEmptyPartition = true - should also return max partition value from cache
        // Since all partitions have physicalSize > 0, the result should be the same
        String maxPartitionNonEmpty = metadata.getMaxPartitionValue(testTable, true);
        Assertions.assertNotNull(maxPartitionNonEmpty);
        Assertions.assertEquals("20230105", maxPartitionNonEmpty,
                "Max partition should be 20230105, which is the largest value among non-empty partitions in cache");
    }

    @Test
    public void testGetMaxPartitionValueWithNonEmptyPartition() {
        // Use a unique table name to avoid affecting other tests
        String testDbName = "test_project_nonempty";
        String testTableName = "test_table_nonempty";
        
        // Create a metadata instance with partition cache enabled
        Map<String, String> properties = new HashMap<>();
        properties.put(OdpsProperties.ACCESS_ID, "ak");
        properties.put(OdpsProperties.ACCESS_KEY, "sk");
        properties.put(OdpsProperties.ENDPOINT, "http://127.0.0.1");
        properties.put(OdpsProperties.PROJECT, testDbName);
        properties.put(OdpsProperties.TUNNEL_QUOTA, "pay-as-you-go");
        properties.put(OdpsProperties.ENABLE_PARTITION_CACHE, "true");
        properties.put(OdpsProperties.PARTITION_CACHE_EXPIRE_TIME, "1");
        properties.put(OdpsProperties.PARTITION_CACHE_SIZE, "1000");
        OdpsMetadata metadata = new OdpsMetadata(odps, "odps", aliyunCloudCredential, new OdpsProperties(properties));

        // Create partitions with different sizes and values
        // We'll create partitions with values: 20230101 (empty), 20230102 (non-empty), 20230103 (empty), 20230104 (non-empty)
        // The max non-empty partition should be "20230104"
        Partition partition1 = Mockito.mock(Partition.class);
        Partition partition2 = Mockito.mock(Partition.class);
        Partition partition3 = Mockito.mock(Partition.class);
        Partition partition4 = Mockito.mock(Partition.class);

        PartitionSpec spec1 = new PartitionSpec("p1=20230101");
        PartitionSpec spec2 = new PartitionSpec("p1=20230102");
        PartitionSpec spec3 = new PartitionSpec("p1=20230103");
        PartitionSpec spec4 = new PartitionSpec("p1=20230104");

        when(partition1.getPartitionSpec()).thenReturn(spec1);
        when(partition1.getPhysicalSize()).thenReturn(0L); // Empty partition
        when(partition2.getPartitionSpec()).thenReturn(spec2);
        when(partition2.getPhysicalSize()).thenReturn(2000L); // Non-empty partition
        when(partition3.getPartitionSpec()).thenReturn(spec3);
        when(partition3.getPhysicalSize()).thenReturn(0L); // Empty partition
        when(partition4.getPartitionSpec()).thenReturn(spec4);
        when(partition4.getPhysicalSize()).thenReturn(3000L); // Non-empty partition, this should be the max

        // Create a new independent mock Table to avoid affecting other tests
        com.aliyun.odps.Table testOdpsTable = Mockito.mock(com.aliyun.odps.Table.class);
        when(testOdpsTable.getPartitions()).thenReturn(ImmutableList.of(partition1, partition2, partition3, partition4));
        when(testOdpsTable.getName()).thenReturn(testTableName);
        when(testOdpsTable.getCreatedTime()).thenReturn(new java.util.Date());
        when(testOdpsTable.getProject()).thenReturn(testDbName);
        try {
            doNothing().when(testOdpsTable).reload();
        } catch (OdpsException e) {
            // Ignore
        }
        
        // Set up table schema similar to MockedBase
        TableSchema tableSchema = new TableSchema();
        tableSchema.addColumn(new Column("c1", TypeInfoFactory.STRING));
        tableSchema.addColumn(new Column("c2", TypeInfoFactory.BIGINT));
        tableSchema.addPartitionColumn(new Column("p1", TypeInfoFactory.STRING));
        when(testOdpsTable.getSchema()).thenReturn(tableSchema);
        
        // Mock odps.tables().get() to return our test table for the specific table name
        // This won't affect other tests since we use a unique table name
        when(tables.get(testDbName, testTableName)).thenReturn(testOdpsTable);

        // Get table to populate the cache
        Table testTable = metadata.getTable(new ConnectContext(), testDbName, testTableName);
        Assertions.assertNotNull(testTable);

        // Test with nonEmptyPartition = true - should return max partition value from non-empty partitions
        // Expected max: "20230104" (the largest value among non-empty partitions: 20230102 and 20230104)
        String maxPartition = metadata.getMaxPartitionValue(testTable, true);
        Assertions.assertNotNull(maxPartition);
        Assertions.assertEquals("20230104", maxPartition, 
                "Max partition should be 20230104, which is the largest value among non-empty partitions");

        // Test with nonEmptyPartition = false - should return max partition value from all partitions
        // Expected max: "20230104" (the largest value among all partitions)
        String maxPartitionAll = metadata.getMaxPartitionValue(testTable, false);
        Assertions.assertNotNull(maxPartitionAll);
        Assertions.assertEquals("20230104", maxPartitionAll,
                "Max partition should be 20230104, which is the largest value among all partitions");
    }

    @Test
    public void testGetMaxPartitionValueFromIterator() throws OdpsException {
        // Use a unique table name to avoid affecting other tests
        String testDbName = "test_project_iterator";
        String testTableName = "test_table_iterator";
        
        // Create a metadata instance with partition cache disabled
        Map<String, String> properties = new HashMap<>();
        properties.put(OdpsProperties.ACCESS_ID, "ak");
        properties.put(OdpsProperties.ACCESS_KEY, "sk");
        properties.put(OdpsProperties.ENDPOINT, "http://127.0.0.1");
        properties.put(OdpsProperties.PROJECT, testDbName);
        properties.put(OdpsProperties.TUNNEL_QUOTA, "pay-as-you-go");
        properties.put(OdpsProperties.ENABLE_PARTITION_CACHE, "false");
        OdpsMetadata metadata = new OdpsMetadata(odps, "odps", aliyunCloudCredential, new OdpsProperties(properties));

        // Create a unique mock table for this test
        com.aliyun.odps.Table testOdpsTable = Mockito.mock(com.aliyun.odps.Table.class);
        when(testOdpsTable.getName()).thenReturn(testTableName);
        when(testOdpsTable.getProject()).thenReturn(testDbName);
        when(testOdpsTable.getCreatedTime()).thenReturn(new Date()); // Required by OdpsTable constructor

        // Set up table schema
        TableSchema tableSchema = new TableSchema();
        tableSchema.addColumn(new Column("c1", TypeInfoFactory.STRING));
        tableSchema.addPartitionColumn(new Column("p1", TypeInfoFactory.STRING));
        when(testOdpsTable.getSchema()).thenReturn(tableSchema);

        try {
            doNothing().when(testOdpsTable).reload();
        } catch (OdpsException e) {
            // Ignore
        }

        // Return empty list so partition cache stays empty and we always use the iterator branch
        when(testOdpsTable.getPartitions()).thenReturn(Collections.emptyList());

        // Mock odps.tables().get() to return our test table (doReturn so it overrides MockedBase's anyString stub)
        doReturn(testOdpsTable).when(tables).get(testDbName, testTableName);

        // Mock partition iterator with multiple partitions
        Partition partition1 = Mockito.mock(Partition.class);
        PartitionSpec spec1 = new PartitionSpec("p1=20230101");
        when(partition1.getPartitionSpec()).thenReturn(spec1);
        when(partition1.getPhysicalSize()).thenReturn(0L); // Empty partition

        Partition partition2 = Mockito.mock(Partition.class);
        PartitionSpec spec2 = new PartitionSpec("p1=20230102");
        when(partition2.getPartitionSpec()).thenReturn(spec2);
        when(partition2.getPhysicalSize()).thenReturn(1000L); // Non-empty partition

        Partition partition3 = Mockito.mock(Partition.class);
        PartitionSpec spec3 = new PartitionSpec("p1=20230103");
        when(partition3.getPartitionSpec()).thenReturn(spec3);
        when(partition3.getPhysicalSize()).thenReturn(2000L); // Non-empty partition

        // Use a real iterator over partition list so each getPartitionIterator() call gets a fresh iteration
        List<Partition> partitionList = Arrays.asList(partition1, partition2, partition3);
        when(testOdpsTable.getPartitionIterator(any(), anyBoolean(), anyLong(), anyLong()))
                .thenAnswer((Answer<Iterator<Partition>>) invocation -> partitionList.iterator());

        Table testTable = metadata.getTable(new ConnectContext(), testDbName, testTableName);
        Assertions.assertNotNull(testTable);

        // Test getMaxPartitionValue when cache is empty, should use iterator
        // With nonEmptyPartition = false, should return first partition value (20230101)
        String maxPartition = metadata.getMaxPartitionValue(testTable, false);
        Assertions.assertNotNull(maxPartition);
        Assertions.assertEquals("20230101", maxPartition, 
                "When nonEmptyPartition=false, should return first partition value from iterator");

        // With nonEmptyPartition = true, should return first non-empty partition value (20230102)
        String maxPartitionNonEmpty = metadata.getMaxPartitionValue(testTable, true);
        Assertions.assertNotNull(maxPartitionNonEmpty);
        Assertions.assertEquals("20230102", maxPartitionNonEmpty,
                "When nonEmptyPartition=true, should return first non-empty partition value from iterator");
    }
}

