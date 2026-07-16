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

package com.starrocks.connector.fluss;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.FlussTable;
import com.starrocks.connector.ConnectorMetadataRequestContext;
import com.starrocks.connector.GetRemoteFilesParams;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.RemoteFileDesc;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.type.IntegerType;
import com.starrocks.type.TypeFactory;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import mockit.Verifications;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.DatabaseNotExistException;
import org.apache.fluss.exception.LakeTableSnapshotNotExistException;
import org.apache.fluss.exception.TableNotPartitionedException;
import org.apache.fluss.flink.source.split.SourceSplitBase;
import org.apache.fluss.flink.utils.LakeSourceUtils;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class FlussMetadataTest {
    private static final String CATALOG = "fluss0";
    private static final String DB = "db1";
    private static final String TABLE = "orders";
    private static final long TABLE_ID = 42L;

    @Mocked
    private Connection connection;
    @Mocked
    private Admin admin;

    private FlussMetadata metadata;

    @BeforeEach
    public void setUp() {
        Configuration catalogConf = new Configuration();
        catalogConf.setString("bootstrap.servers", "localhost:9123");
        metadata = new FlussMetadata(CATALOG, new HdfsEnvironment(), connection, admin, catalogConf);
    }

    @Test
    public void testGetDb() {
        new Expectations() {
            {
                admin.getDatabaseInfo(DB);
                result = CompletableFuture.completedFuture(null);
                admin.getDatabaseInfo("missing");
                result = CompletableFuture.failedFuture(
                        new DatabaseNotExistException("database does not exist"));
            }
        };

        Assertions.assertEquals(DB, metadata.getDb(new ConnectContext(), DB).getFullName());
        Assertions.assertNull(metadata.getDb(new ConnectContext(), "missing"));
    }

    @Test
    public void testGetTable() {
        TableInfo tableInfo = tableInfo(true);
        org.apache.fluss.client.table.Table nativeTable = nativeTable(tableInfo);
        new Expectations() {
            {
                admin.getTableInfo(TablePath.of(DB, TABLE));
                result = CompletableFuture.completedFuture(tableInfo);
                connection.getTable(TablePath.of(DB, TABLE));
                result = nativeTable;
            }
        };

        FlussTable table = (FlussTable) metadata.getTable(new ConnectContext(), DB, TABLE);
        Assertions.assertEquals(CATALOG, table.getCatalogName());
        Assertions.assertEquals(DB, table.getCatalogDBName());
        Assertions.assertEquals(TABLE, table.getCatalogTableName());
        Assertions.assertEquals(Lists.newArrayList("dt", "region"), table.getPartitionColumnNames());
        Assertions.assertEquals(Lists.newArrayList("id", "name", "dt", "region"), table.getFieldNames());
        Assertions.assertEquals(IntegerType.INT, table.getBaseSchema().get(0).getType());
        Assertions.assertEquals(TypeFactory.createDefaultCatalogString(), table.getBaseSchema().get(1).getType());
        Assertions.assertEquals("test fluss table", table.getComment());
    }

    @Test
    public void testGetPrimaryKeyTableWithReadModeSuffix() {
        TableInfo tableInfo = tableInfo(false, true);
        new Expectations() {
            {
                admin.getTableInfo(TablePath.of(DB, TABLE));
                result = CompletableFuture.completedFuture(tableInfo);
            }
        };

        Assertions.assertThrows(StarRocksConnectorException.class,
                () -> metadata.getTable(new ConnectContext(), DB, TABLE + "$lake"));
    }

    @Test
    public void testGetTableWithLakeAndRt() {
        TableInfo tableInfo = tableInfo(false);
        org.apache.fluss.client.table.Table nativeTable = nativeTable(tableInfo);
        new Expectations() {
            {
                admin.getTableInfo(TablePath.of(DB, TABLE));
                result = CompletableFuture.completedFuture(tableInfo);
                connection.getTable(TablePath.of(DB, TABLE));
                result = nativeTable;
            }
        };

        FlussTable lakeTable = (FlussTable) metadata.getTable(new ConnectContext(), DB, TABLE + "$lake");
        Assertions.assertEquals(TABLE, lakeTable.getCatalogTableName());
        Assertions.assertEquals("$lake", lakeTable.getTableNamePrefix());

        FlussTable rtTable = (FlussTable) metadata.getTable(new ConnectContext(), DB, TABLE + "$rt");
        Assertions.assertEquals(TABLE, rtTable.getCatalogTableName());
        Assertions.assertEquals("$rt", rtTable.getTableNamePrefix());
    }

    @Test
    public void testTableExistsWithLakeAndRt() {
        TablePath basePath = TablePath.of(DB, TABLE);
        new Expectations() {
            {
                admin.tableExists(basePath);
                result = CompletableFuture.completedFuture(true);
                minTimes = 0;
            }
        };

        Assertions.assertTrue(metadata.tableExists(new ConnectContext(), DB, TABLE));
        Assertions.assertTrue(metadata.tableExists(new ConnectContext(), DB, TABLE + "$lake"));
        Assertions.assertTrue(metadata.tableExists(new ConnectContext(), DB, TABLE + "$rt"));
        new Verifications() {
            {
                admin.tableExists(basePath);
                times = 3;
            }
        };
    }

    @Test
    public void testListPartitionNames() {
        PartitionInfo p1 = partitionInfo(1001L, "2026-07-01", "cn");
        PartitionInfo p2 = partitionInfo(1002L, "2026-07-02", "us");
        new Expectations() {
            {
                admin.listPartitionInfos(TablePath.of(DB, TABLE));
                result = CompletableFuture.completedFuture(Lists.newArrayList(p1, p2));
            }
        };

        List<String> partitionNames =
                metadata.listPartitionNames(DB, TABLE, ConnectorMetadataRequestContext.DEFAULT);
        Assertions.assertEquals(Lists.newArrayList("dt=2026-07-01/region=cn", "dt=2026-07-02/region=us"),
                partitionNames);

        List<com.starrocks.connector.PartitionInfo> partitions = metadata.getPartitions(
                flussTable(tableInfo(true), ""), Lists.newArrayList(partitionNames.get(0), "dt=missing/region=missing"));
        Assertions.assertEquals(1, partitions.size());
        Assertions.assertEquals(partitionNames.get(0), ((Partition) partitions.get(0)).getPartitionName());
    }

    @Test
    public void testUnpartitionedTablePartitions() {
        new Expectations() {
            {
                admin.listPartitionInfos(TablePath.of(DB, TABLE));
                result = CompletableFuture.<List<PartitionInfo>>failedFuture(
                        new TableNotPartitionedException("table is not partitioned"));
            }
        };

        Assertions.assertTrue(metadata.listPartitionNames(
                DB, TABLE, ConnectorMetadataRequestContext.DEFAULT).isEmpty());

        List<com.starrocks.connector.PartitionInfo> partitions = metadata.getPartitions(
                flussTable(tableInfo(false), ""), Collections.emptyList());
        Assertions.assertEquals(1, partitions.size());
        Assertions.assertEquals(TABLE, ((Partition) partitions.get(0)).getPartitionName());
    }

    @Test
    public void testGetRemoteFilesWithEmptyPartitionKeys() {
        FlussTable table = flussTable(tableInfo(true), "");
        GetRemoteFilesParams params = GetRemoteFilesParams.newBuilder()
                .setPartitionKeys(Collections.emptyList())
                .build();
        List<SourceSplitBase> splits = getFlussSplits(table, params);

        Assertions.assertTrue(splits.isEmpty());
    }

    @Test
    public void testGetRemoteFilesNoLakeSnapshot() {
        mockLakeSourceAsNull();
        new Expectations() {
            {
                admin.getReadableLakeSnapshot(TablePath.of(DB, TABLE));
                result = CompletableFuture.failedFuture(new LakeTableSnapshotNotExistException("no snapshot"));
            }
        };
        FlussTable table = flussTable(tableInfo(false), "");

        StarRocksConnectorException exception = Assertions.assertThrows(StarRocksConnectorException.class,
                () -> metadata.getRemoteFiles(table, GetRemoteFilesParams.newBuilder().build()));
        Assertions.assertTrue(exception.getMessage().contains("No readable Fluss lake snapshot exists"));
    }

    private void mockLakeSourceAsNull() {
        new MockUp<LakeSourceUtils>() {
            @Mock
            public static LakeSource<LakeSplit> createLakeSource(TablePath tablePath, Map<String, String> options) {
                return null;
            }
        };
    }

    private List<SourceSplitBase> getFlussSplits(FlussTable table, GetRemoteFilesParams params) {
        List<RemoteFileInfo> remoteFileInfos = metadata.getRemoteFiles(table, params);
        Assertions.assertEquals(1, remoteFileInfos.size());
        List<RemoteFileDesc> remoteFileDescs = remoteFileInfos.get(0).getFiles();
        Assertions.assertEquals(1, remoteFileDescs.size());
        FlussRemoteFileDesc remoteFileDesc = (FlussRemoteFileDesc) remoteFileDescs.get(0);
        return remoteFileDesc.getFlussSplitsInfo().getFlussSplits();
    }

    private static FlussTable flussTable(TableInfo tableInfo, String tableNamePrefix) {
        FlussTable table = new FlussTable(CATALOG, DB, TABLE, schema(), nativeTable(tableInfo), new Configuration());
        table.setTableNamePrefix(tableNamePrefix);
        return table;
    }

    private static org.apache.fluss.client.table.Table nativeTable(TableInfo tableInfo) {
        return new org.apache.fluss.client.table.Table() {
            @Override
            public TableInfo getTableInfo() {
                return tableInfo;
            }

            @Override
            public org.apache.fluss.client.table.scanner.Scan newScan() {
                throw new UnsupportedOperationException();
            }

            @Override
            public org.apache.fluss.client.lookup.Lookup newLookup() {
                throw new UnsupportedOperationException();
            }

            @Override
            public org.apache.fluss.client.table.writer.Append newAppend() {
                throw new UnsupportedOperationException();
            }

            @Override
            public org.apache.fluss.client.table.writer.Upsert newUpsert() {
                throw new UnsupportedOperationException();
            }

            @Override
            public void close() {
            }
        };
    }

    private static List<Column> schema() {
        return Arrays.asList(
                new Column("id", IntegerType.INT, true),
                new Column("name", TypeFactory.createDefaultCatalogString(), true),
                new Column("dt", TypeFactory.createDefaultCatalogString(), true),
                new Column("region", TypeFactory.createDefaultCatalogString(), true));
    }

    private static TableInfo tableInfo(boolean partitioned) {
        return tableInfo(partitioned, false);
    }

    private static TableInfo tableInfo(boolean partitioned, boolean primaryKey) {
        Schema.Builder schemaBuilder = Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column("name", DataTypes.STRING())
                .column("dt", DataTypes.STRING())
                .column("region", DataTypes.STRING());
        if (primaryKey) {
            schemaBuilder.primaryKey("id");
        }
        Schema schema = schemaBuilder.build();
        TableDescriptor.Builder descriptorBuilder = TableDescriptor.builder()
                .schema(schema)
                .distributedBy(3, "id")
                .comment("test fluss table")
                .property("table.datalake.paimon.warehouse", "oss://warehouse");
        if (partitioned) {
            descriptorBuilder.partitionedBy("dt", "region");
        }
        return TableInfo.of(TablePath.of(DB, TABLE), TABLE_ID, 7, descriptorBuilder.build(), 1000L, 2000L);
    }

    private static PartitionInfo partitionInfo(long partitionId, String dt, String region) {
        return new PartitionInfo(partitionId,
                new ResolvedPartitionSpec(Lists.newArrayList("dt", "region"), Lists.newArrayList(dt, region)));
    }
}
