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

package com.starrocks.connector.unified;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.DeltaLakeTable;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.HudiTable;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.KuduTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.AlreadyExistsException;
import com.starrocks.common.DdlException;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.connector.ConnectorMetadatRequestContext;
import com.starrocks.connector.GetRemoteFilesParams;
import com.starrocks.connector.MetaPreparationItem;
import com.starrocks.connector.PartitionInfo;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.connector.TableVersionRange;
import com.starrocks.connector.delta.DeltaLakeMetadata;
import com.starrocks.connector.hive.HiveMetadata;
import com.starrocks.connector.hudi.HudiMetadata;
import com.starrocks.connector.iceberg.IcebergMetaSpec;
import com.starrocks.connector.iceberg.IcebergMetadata;
import com.starrocks.connector.kudu.KuduMetadata;
import com.starrocks.connector.paimon.PaimonMetadata;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.CloudType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.CreateTableStmt;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.wildfly.common.Assert;

import java.util.List;

import static com.starrocks.catalog.Table.TableType.DELTALAKE;
import static com.starrocks.catalog.Table.TableType.HIVE;
import static com.starrocks.catalog.Table.TableType.HUDI;
import static com.starrocks.catalog.Table.TableType.ICEBERG;
import static com.starrocks.catalog.Table.TableType.KUDU;
import static com.starrocks.catalog.Table.TableType.PAIMON;
import static com.starrocks.connector.unified.UnifiedMetadata.DELTA_LAKE_PROVIDER;
import static com.starrocks.connector.unified.UnifiedMetadata.ICEBERG_TABLE_TYPE_NAME;
import static com.starrocks.connector.unified.UnifiedMetadata.ICEBERG_TABLE_TYPE_VALUE;
import static com.starrocks.connector.unified.UnifiedMetadata.SPARK_TABLE_PROVIDER_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class UnifiedMetadataTest {
    @Mocked
    private HiveMetadata hiveMetadata;
    @Mocked
    private IcebergMetadata icebergMetadata;
    @Mocked
    private HudiMetadata hudiMetadata;
    @Mocked
    private DeltaLakeMetadata deltaLakeMetadata;
    @Mocked
    private PaimonMetadata paimonMetadata;
    @Mocked
    private KuduMetadata kuduMetadata;
    private final CreateTableStmt createTableStmt = new CreateTableStmt(false, true,
            new TableName("test_db", "test_tbl"), ImmutableList.of(), "hive",
            null, null, null, null, null, null);

    private UnifiedMetadata unifiedMetadata;

    private GetRemoteFilesParams getRemoteFilesParams;

    @BeforeEach
    public void setUp() {
        this.unifiedMetadata = new UnifiedMetadata(ImmutableMap.of(
                HIVE, hiveMetadata,
                ICEBERG, icebergMetadata,
                HUDI, hudiMetadata,
                DELTALAKE, deltaLakeMetadata,
                PAIMON, paimonMetadata,
                KUDU, kuduMetadata
        )
        );
        this.getRemoteFilesParams = GetRemoteFilesParams.newBuilder().setPartitionKeys(ImmutableList.of())
                .setTableVersionRange(TableVersionRange.empty()).build();
    }

    @Test
    public void testAlwaysRouteToHiveConnector() throws DdlException, AlreadyExistsException, MetaNotFoundException {
        new Expectations() {
            {
                hiveMetadata.listDbNames((ConnectContext) any);
                result = ImmutableList.of("test_db1", "test_db2");
                times = 1;
            }

            {
                hiveMetadata.listTableNames((ConnectContext) any, "test_db");
                result = ImmutableList.of("test_tbl1", "test_tbl2");
            }

            {
                hiveMetadata.createDb("test_db");
                times = 1;
            }

            {
                hiveMetadata.createDb("test_db", ImmutableMap.of("key", "value"));
                times = 1;
            }

            {
                hiveMetadata.dbExists((ConnectContext) any, "test_db");
                result = true;
                times = 1;
            }

            {
                hiveMetadata.dropDb((ConnectContext) any, "test_db", false);
                times = 1;
            }

            {
                hiveMetadata.getCloudConfiguration();
                result = new CloudConfiguration();
                times = 1;
            }
        };

        List<String> dbNames = unifiedMetadata.listDbNames(new ConnectContext());
        assertEquals(ImmutableList.of("test_db1", "test_db2"), dbNames);
        List<String> tblNames = unifiedMetadata.listTableNames(new ConnectContext(), "test_db");
        assertEquals(ImmutableList.of("test_tbl1", "test_tbl2"), tblNames);
        unifiedMetadata.createDb("test_db");
        unifiedMetadata.createDb("test_db", ImmutableMap.of("key", "value"));
        assertTrue(unifiedMetadata.dbExists(new ConnectContext(), "test_db"));
        unifiedMetadata.dropDb(new ConnectContext(), "test_db", false);
        CloudConfiguration cloudConfiguration = unifiedMetadata.getCloudConfiguration();
        assertEquals(CloudType.DEFAULT, cloudConfiguration.getCloudType());
    }

    @Test
    public void testRouteToHiveConnector() throws DdlException {
        HiveTable hiveTable = new HiveTable();

        new Expectations() {
            {
                hiveMetadata.getTable((ConnectContext) any, "test_db", "test_tbl");
                result = hiveTable;
                minTimes = 1;
            }

            {
                hiveMetadata.listPartitionNames("test_db", "test_tbl", ConnectorMetadatRequestContext.DEFAULT);
                result = ImmutableList.of("test_part1", "test_part2");
                times = 1;
            }

            {
                hiveMetadata.listPartitionNamesByValue("test_db", "test_tbl", ImmutableList.of());
                result = ImmutableList.of("test_part1", "test_part2");
                times = 1;
            }

            {
                hiveMetadata.getRemoteFiles(hiveTable, getRemoteFilesParams);
                result = ImmutableList.of();
                times = 1;
            }

            {
                hiveMetadata.getPartitions(hiveTable, ImmutableList.of());
                result = ImmutableList.of();
                times = 1;
            }

            {
                hiveMetadata.refreshTable("test_db", hiveTable, ImmutableList.of(), false);
                times = 1;
            }

            {
                hiveMetadata.finishSink("test_db", "test_tbl", ImmutableList.of(), null);
                times = 1;
            }

            {
                hiveMetadata.createTable(createTableStmt);
                result = true;
                times = 1;
            }
        };

        Table table = unifiedMetadata.getTable(new ConnectContext(), "test_db", "test_tbl");
        assertTrue(table instanceof HiveTable);
        List<String> partitionNames =
                unifiedMetadata.listPartitionNames("test_db", "test_tbl", ConnectorMetadatRequestContext.DEFAULT);
        assertEquals(ImmutableList.of("test_part1", "test_part2"), partitionNames);
        partitionNames = unifiedMetadata.listPartitionNamesByValue("test_db", "test_tbl", ImmutableList.of());
        assertEquals(ImmutableList.of("test_part1", "test_part2"), partitionNames);

        List<RemoteFileInfo> remoteFileInfos = unifiedMetadata.getRemoteFiles(hiveTable, getRemoteFilesParams);
        assertEquals(ImmutableList.of(), remoteFileInfos);
        List<PartitionInfo> partitionInfos = unifiedMetadata.getPartitions(hiveTable, ImmutableList.of());
        assertEquals(ImmutableList.of(), partitionInfos);
        unifiedMetadata.refreshTable("test_db", hiveTable, ImmutableList.of(), false);
        unifiedMetadata.finishSink("test_db", "test_tbl", ImmutableList.of(), null);
        createTableStmt.setEngineName("hive");
        assertTrue(unifiedMetadata.createTable(createTableStmt));
    }

    @Test
    public void testRouteToIcebergConnector(@Mocked HiveTable hiveTable) throws DdlException {
        Table icebergTable = new IcebergTable();

        new Expectations() {
            {
                hiveTable.getProperties();
                result = ImmutableMap.of(ICEBERG_TABLE_TYPE_NAME, ICEBERG_TABLE_TYPE_VALUE);
                minTimes = 1;
            }

            {
                hiveMetadata.getTable((ConnectContext) any, "test_db", "test_tbl");
                result = hiveTable;
                minTimes = 1;
            }

            {
                icebergMetadata.getTable((ConnectContext) any, "test_db", "test_tbl");
                result = icebergTable;
                times = 1;
            }

            {
                icebergMetadata.listPartitionNames("test_db", "test_tbl", ConnectorMetadatRequestContext.DEFAULT);
                result = ImmutableList.of("test_part1", "test_part2");
                times = 1;
            }

            {
                icebergMetadata.listPartitionNamesByValue("test_db", "test_tbl", ImmutableList.of());
                result = ImmutableList.of("test_part1", "test_part2");
                times = 1;
            }

            {
                icebergMetadata.getRemoteFiles(icebergTable, getRemoteFilesParams);
                result = ImmutableList.of();
                times = 1;
            }

            {
                icebergMetadata.getSerializedMetaSpec("test_db", "test_tbl", -1, null, null);
                result = new IcebergMetaSpec(null, null, false);
                times = 1;
            }

            {
                icebergMetadata.getPartitions(icebergTable, ImmutableList.of());
                result = ImmutableList.of();
                times = 1;
            }

            {
                icebergMetadata.refreshTable("test_db", icebergTable, ImmutableList.of(), false);
                times = 1;
            }

            {
                icebergMetadata.finishSink("test_db", "test_tbl", ImmutableList.of(), null);
                times = 1;
            }

            {
                icebergMetadata.createTable(createTableStmt);
                result = true;
                times = 1;
            }

            {
                icebergMetadata.prepareMetadata((MetaPreparationItem) any, null, null);
                result = true;
                times = 1;
            }
        };

        Table table = unifiedMetadata.getTable(new ConnectContext(), "test_db", "test_tbl");
        assertTrue(table instanceof IcebergTable);
        List<String> partitionNames =
                unifiedMetadata.listPartitionNames("test_db", "test_tbl", ConnectorMetadatRequestContext.DEFAULT);
        assertEquals(ImmutableList.of("test_part1", "test_part2"), partitionNames);
        partitionNames = unifiedMetadata.listPartitionNamesByValue("test_db", "test_tbl", ImmutableList.of());
        assertEquals(ImmutableList.of("test_part1", "test_part2"), partitionNames);
        List<RemoteFileInfo> remoteFileInfos = unifiedMetadata.getRemoteFiles(icebergTable, getRemoteFilesParams);
        assertEquals(ImmutableList.of(), remoteFileInfos);
        List<PartitionInfo> partitionInfos = unifiedMetadata.getPartitions(icebergTable, ImmutableList.of());
        assertEquals(ImmutableList.of(), partitionInfos);
        unifiedMetadata.refreshTable("test_db", icebergTable, ImmutableList.of(), false);
        unifiedMetadata.finishSink("test_db", "test_tbl", ImmutableList.of(), null);
        createTableStmt.setEngineName("iceberg");
        assertTrue(unifiedMetadata.createTable(createTableStmt));
        Assert.assertTrue(unifiedMetadata.prepareMetadata(new MetaPreparationItem(icebergTable, null,
                -1, TableVersionRange.empty()), null, null));
        Assert.assertNotNull(unifiedMetadata.getSerializedMetaSpec("test_db", "test_tbl", -1, null, null));
    }

    @Test
    public void testRouteToHudiConnector() throws DdlException {
        HudiTable hudiTable = new HudiTable();

        new Expectations() {
            {
                hiveMetadata.getTable((ConnectContext) any, "test_db", "test_tbl");
                result = hudiTable;
                minTimes = 1;
            }

            {
                hudiMetadata.getTable((ConnectContext) any, "test_db", "test_tbl");
                result = hudiTable;
                times = 1;
            }

            {
                hudiMetadata.listPartitionNames("test_db", "test_tbl", ConnectorMetadatRequestContext.DEFAULT);
                result = ImmutableList.of("test_part1", "test_part2");
                times = 1;
            }

            {
                hudiMetadata.listPartitionNamesByValue("test_db", "test_tbl", ImmutableList.of());
                result = ImmutableList.of("test_part1", "test_part2");
                times = 1;
            }

            {
                hudiMetadata.getRemoteFiles(hudiTable, getRemoteFilesParams);
                result = ImmutableList.of();
                times = 1;
            }

            {
                hudiMetadata.getPartitions(hudiTable, ImmutableList.of());
                result = ImmutableList.of();
                times = 1;
            }

            {
                hudiMetadata.refreshTable("test_db", hudiTable, ImmutableList.of(), false);
                times = 1;
            }

            {
                hudiMetadata.finishSink("test_db", "test_tbl", ImmutableList.of(), null);
                times = 1;
            }

            {
                hudiMetadata.createTable(createTableStmt);
                result = true;
                times = 1;
            }
        };

        Table table = unifiedMetadata.getTable(new ConnectContext(), "test_db", "test_tbl");
        assertTrue(table instanceof HudiTable);
        List<String> partitionNames =
                unifiedMetadata.listPartitionNames("test_db", "test_tbl", ConnectorMetadatRequestContext.DEFAULT);
        assertEquals(ImmutableList.of("test_part1", "test_part2"), partitionNames);
        partitionNames = unifiedMetadata.listPartitionNamesByValue("test_db", "test_tbl", ImmutableList.of());
        assertEquals(ImmutableList.of("test_part1", "test_part2"), partitionNames);
        List<RemoteFileInfo> remoteFileInfos = unifiedMetadata.getRemoteFiles(hudiTable, getRemoteFilesParams);
        assertEquals(ImmutableList.of(), remoteFileInfos);
        List<PartitionInfo> partitionInfos = unifiedMetadata.getPartitions(hudiTable, ImmutableList.of());
        assertEquals(ImmutableList.of(), partitionInfos);
        unifiedMetadata.refreshTable("test_db", hudiTable, ImmutableList.of(), false);
        unifiedMetadata.finishSink("test_db", "test_tbl", ImmutableList.of(), null);
        createTableStmt.setEngineName("hudi");
        assertTrue(unifiedMetadata.createTable(createTableStmt));
    }

    @Test
    public void testRouteToDeltaLakeConnector(@Mocked HiveTable hiveTable) throws DdlException {
        Table deltaLakeTable = new DeltaLakeTable();

        new Expectations() {
            {
                hiveTable.getProperties();
                result = ImmutableMap.of(SPARK_TABLE_PROVIDER_KEY, DELTA_LAKE_PROVIDER);
                minTimes = 1;
            }

            {
                hiveMetadata.getTable((ConnectContext) any, "test_db", "test_tbl");
                result = hiveTable;
                minTimes = 1;
            }

            {
                deltaLakeMetadata.getTable((ConnectContext) any, "test_db", "test_tbl");
                result = deltaLakeTable;
                times = 1;
            }

            {
                deltaLakeMetadata.listPartitionNames("test_db", "test_tbl", ConnectorMetadatRequestContext.DEFAULT);
                result = ImmutableList.of("test_part1", "test_part2");
                times = 1;
            }

            {
                deltaLakeMetadata.listPartitionNamesByValue("test_db", "test_tbl", ImmutableList.of());
                result = ImmutableList.of("test_part1", "test_part2");
                times = 1;
            }

            {
                deltaLakeMetadata.getRemoteFiles(deltaLakeTable, getRemoteFilesParams);
                result = ImmutableList.of();
                times = 1;
            }

            {
                deltaLakeMetadata.getPartitions(deltaLakeTable, ImmutableList.of());
                result = ImmutableList.of();
                times = 1;
            }

            {
                deltaLakeMetadata.refreshTable("test_db", deltaLakeTable, ImmutableList.of(), false);
                times = 1;
            }

            {
                deltaLakeMetadata.finishSink("test_db", "test_tbl", ImmutableList.of(), null);
                times = 1;
            }

            {
                deltaLakeMetadata.createTable(createTableStmt);
                result = true;
                times = 1;
            }
        };

        Table table = unifiedMetadata.getTable(new ConnectContext(), "test_db", "test_tbl");
        assertTrue(table instanceof DeltaLakeTable);
        List<String> partitionNames =
                unifiedMetadata.listPartitionNames("test_db", "test_tbl", ConnectorMetadatRequestContext.DEFAULT);
        assertEquals(ImmutableList.of("test_part1", "test_part2"), partitionNames);
        partitionNames = unifiedMetadata.listPartitionNamesByValue("test_db", "test_tbl", ImmutableList.of());
        assertEquals(ImmutableList.of("test_part1", "test_part2"), partitionNames);
        List<RemoteFileInfo> remoteFileInfos = unifiedMetadata.getRemoteFiles(deltaLakeTable, getRemoteFilesParams);
        assertEquals(ImmutableList.of(), remoteFileInfos);
        List<PartitionInfo> partitionInfos = unifiedMetadata.getPartitions(deltaLakeTable, ImmutableList.of());
        assertEquals(ImmutableList.of(), partitionInfos);
        unifiedMetadata.refreshTable("test_db", deltaLakeTable, ImmutableList.of(), false);
        unifiedMetadata.finishSink("test_db", "test_tbl", ImmutableList.of(), null);
        createTableStmt.setEngineName("deltalake");
        assertTrue(unifiedMetadata.createTable(createTableStmt));
    }

    @Test
    public void testRouteToKuduConnector() throws DdlException {
        Table kuduTable = new KuduTable();

        new Expectations() {
            {
                hiveMetadata.getTable((ConnectContext) any, "test_db", "test_tbl");
                result = kuduTable;
                minTimes = 1;
            }

            {
                kuduMetadata.getTable((ConnectContext) any, "test_db", "test_tbl");
                result = kuduTable;
                minTimes = 1;
            }

            {
                kuduMetadata.listPartitionNames("test_db", "test_tbl", ConnectorMetadatRequestContext.DEFAULT);
                result = ImmutableList.of("test_part1", "test_part2");
                times = 1;
            }

            {
                kuduMetadata.listPartitionNamesByValue("test_db", "test_tbl", ImmutableList.of());
                result = ImmutableList.of("test_part1", "test_part2");
                times = 1;
            }

            {
                kuduMetadata.getRemoteFiles(kuduTable, getRemoteFilesParams);
                result = ImmutableList.of();
                times = 1;
            }

            {
                kuduMetadata.getPartitions(kuduTable, ImmutableList.of());
                result = ImmutableList.of();
                times = 1;
            }
        };

        Table table = unifiedMetadata.getTable(new ConnectContext(), "test_db", "test_tbl");
        assertTrue(table instanceof KuduTable);
        List<String> partitionNames =
                unifiedMetadata.listPartitionNames("test_db", "test_tbl", ConnectorMetadatRequestContext.DEFAULT);
        assertEquals(ImmutableList.of("test_part1", "test_part2"), partitionNames);
        partitionNames = unifiedMetadata.listPartitionNamesByValue("test_db", "test_tbl", ImmutableList.of());
        assertEquals(ImmutableList.of("test_part1", "test_part2"), partitionNames);
        List<RemoteFileInfo> remoteFileInfos = unifiedMetadata.getRemoteFiles(kuduTable, getRemoteFilesParams);
        assertEquals(ImmutableList.of(), remoteFileInfos);
        List<PartitionInfo> partitionInfos = unifiedMetadata.getPartitions(kuduTable, ImmutableList.of());
        assertEquals(ImmutableList.of(), partitionInfos);
    }

    @Test
    public void testTableExists(@Mocked HiveTable hiveTable) {
        new Expectations() {
            {
                hiveMetadata.tableExists((ConnectContext) any, "test_db", "test_tbl");
                result = true;
                minTimes = 1;
            }
        };
        boolean exists = unifiedMetadata.tableExists(new ConnectContext(), "test_db", "test_tbl");
        Assert.assertTrue(exists);
    }
}
