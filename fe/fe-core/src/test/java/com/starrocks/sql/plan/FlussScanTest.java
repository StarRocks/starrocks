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

package com.starrocks.sql.plan;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.FlussTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.common.DdlException;
import com.starrocks.common.tvr.TvrVersionRange;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.ConnectorMetadataRequestContext;
import com.starrocks.connector.GetRemoteFilesParams;
import com.starrocks.connector.MockedMetadataMgr;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.connector.fluss.FlussRemoteFileDesc;
import com.starrocks.connector.fluss.FlussSplitsInfo;
import com.starrocks.planner.FlussScanNode;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.THdfsScanRange;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.StringType;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.source.split.SourceSplitBase;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class FlussScanTest extends PlanTestBase {
    private static final String CATALOG = "fluss0";
    private static final String DB = "fluss_db";
    private static final String LOG_EVENTS = "log_events";
    private static final String LOG_PART_STRING = "log_part_string";

    private static MockedFlussMetadata mockedFlussMetadata;

    @BeforeAll
    public static void beforeAll() throws Exception {
        new MockUp<ConnectionFactory>() {
            @Mock
            public static Connection createConnection(Configuration configuration) {
                return new FakeConnection(configuration);
            }
        };

        CatalogMgr catalogMgr = GlobalStateMgr.getCurrentState().getCatalogMgr();
        if (!catalogMgr.catalogExists(CATALOG)) {
            Map<String, String> properties = Maps.newHashMap();
            properties.put("type", "fluss");
            properties.put("bootstrap.servers", "localhost:9123");
            catalogMgr.createCatalog("fluss", CATALOG, "", properties);
        }

        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        mockedFlussMetadata = new MockedFlussMetadata();
        MockedMetadataMgr metadataMgr = new MockedMetadataMgr(
                globalStateMgr.getLocalMetastore(), globalStateMgr.getConnectorMgr());
        globalStateMgr.setMetadataMgr(metadataMgr);
        metadataMgr.registerMockedMetadata(CATALOG, mockedFlussMetadata);
    }

    @BeforeEach
    public void setUp() {
        super.setUp();
        mockedFlussMetadata.reset();
        try {
            connectContext.changeCatalogDb(CATALOG + "." + DB);
        } catch (DdlException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testFlussScan(@Mocked SourceSplitBase split) throws Exception {
        new MockUp<FlussScanNode>() {
            @Mock
            public static String encodeSplitToString(SourceSplitBase ignored) {
                return "encoded-split";
            }
        };
        mockedFlussMetadata.setSplits(ImmutableList.of(split));

        ExecPlan execPlan = getExecPlan(
                "select id from fluss0.fluss_db.log_events where id = 1 limit 10");
        String plan = execPlan.getExplainString(TExplainLevel.NORMAL);

        assertContains(plan, "FlussScanNode", "TABLE: log_events",
                "NON-PARTITION PREDICATES: 1: id = 1");
        GetRemoteFilesParams params = mockedFlussMetadata.getLastParams();
        Assertions.assertEquals(ImmutableList.of("id"), params.getFieldNames());
        Assertions.assertEquals("1: id = 1", params.getPredicate().toString());
        Assertions.assertNull(params.getPartitionKeys());
        Assertions.assertEquals(10, params.getLimit());

        FlussScanNode scanNode = Assertions.assertInstanceOf(
                FlussScanNode.class, execPlan.getScanNodes().get(0));
        Assertions.assertEquals(1, scanNode.getScanRangeLocations(0).size());
        THdfsScanRange hdfsScanRange = scanNode.getScanRangeLocations(0).get(0)
                .getScan_range().getHdfs_scan_range();
        Assertions.assertTrue(hdfsScanRange.isUse_fluss_jni_reader());
        Assertions.assertEquals("encoded-split", hdfsScanRange.getFluss_split_info());
        Assertions.assertNotNull(hdfsScanRange.getJni_predicate_info());

        TPlanNode thriftNode = scanNode.treeToThrift().getNodes().get(0);
        Assertions.assertEquals(TPlanNodeType.HDFS_SCAN_NODE, thriftNode.getNode_type());
        Assertions.assertEquals("fluss0.fluss_db.log_events", thriftNode.getHdfs_scan_node().getTable_name());
        Assertions.assertEquals("1: id = 1", thriftNode.getHdfs_scan_node().getSql_predicates());
        Assertions.assertTrue(thriftNode.getHdfs_scan_node().isSetCloud_configuration());

        assertContains(execPlan.getExplainString(TExplainLevel.VERBOSE), "partitions=1/1");
        assertContains(UtFrameUtils.printPlan(execPlan), "- Fluss-SCAN [log_events]");
    }

    @Test
    public void testPartitionPrune() throws Exception {
        String plan = getVerboseExplain(
                "select * from fluss0.fluss_db.log_part_string where category = 'phone'");

        assertContains(plan, "FlussScanNode", "TABLE: log_part_string",
                "PARTITION PREDICATES: 4: category = 'phone'", "partitions=1/3");
        GetRemoteFilesParams params = mockedFlussMetadata.getLastParams();
        Assertions.assertEquals(1, params.getPartitionKeys().size());
        Assertions.assertEquals("category=phone", toPartitionName(params.getPartitionKeys().get(0)));
        Assertions.assertEquals("4: category = phone", params.getPredicate().toString());
    }

    @Test
    public void testNoEvalPartitionPrune() throws Exception {
        String plan = getVerboseExplain(
                "select * from fluss0.fluss_db.log_part_string where substr(category, 1, 1) = 'p'");

        Assertions.assertFalse(plan.contains("EMPTYSET"), plan);
        assertContains(plan, "FlussScanNode", "TABLE: log_part_string",
                "NO EVAL-PARTITION PREDICATES: substr(4: category, 1, 1) = 'p'", "partitions=3/3");
        GetRemoteFilesParams params = mockedFlussMetadata.getLastParams();
        Assertions.assertEquals("substr(4: category, 1, 1) = p", params.getPredicate().toString());
        Assertions.assertEquals(3, params.getPartitionKeys().size());
    }

    private static String toPartitionName(PartitionKey partitionKey) {
        return PartitionUtil.toHivePartitionName(ImmutableList.of("category"), partitionKey);
    }

    private static class MockedFlussMetadata implements ConnectorMetadata {
        private final AtomicLong idGen = new AtomicLong(0);
        private final Map<String, FlussTable> tables;
        private GetRemoteFilesParams lastParams;
        private List<SourceSplitBase> splits = Collections.emptyList();

        private MockedFlussMetadata() {
            tables = ImmutableMap.of(
                    LOG_EVENTS, flussTable(LOG_EVENTS, false),
                    LOG_PART_STRING, flussTable(LOG_PART_STRING, true));
        }

        private void reset() {
            lastParams = null;
            splits = Collections.emptyList();
        }

        private void setSplits(List<SourceSplitBase> splits) {
            this.splits = splits;
        }

        private GetRemoteFilesParams getLastParams() {
            Assertions.assertNotNull(lastParams, "expected FlussScanNode to request remote files");
            return lastParams;
        }

        @Override
        public Table.TableType getTableType() {
            return Table.TableType.FLUSS;
        }

        @Override
        public List<String> listDbNames(ConnectContext context) {
            return ImmutableList.of(DB);
        }

        @Override
        public List<String> listTableNames(ConnectContext context, String dbName) {
            return ImmutableList.of(LOG_EVENTS, LOG_PART_STRING);
        }

        @Override
        public Database getDb(ConnectContext context, String dbName) {
            return new Database(idGen.incrementAndGet(), dbName);
        }

        @Override
        public Table getTable(ConnectContext context, String dbName, String tblName) {
            return tables.get(tblName);
        }

        @Override
        public List<String> listPartitionNames(String databaseName, String tableName,
                                               ConnectorMetadataRequestContext requestContext) {
            if (LOG_PART_STRING.equals(tableName)) {
                return ImmutableList.of("category=phone", "category=book", "category=toy");
            }
            return Collections.emptyList();
        }

        @Override
        public List<RemoteFileInfo> getRemoteFiles(Table table, GetRemoteFilesParams params) {
            lastParams = params;

            FlussSplitsInfo flussSplitsInfo = new FlussSplitsInfo(Collections.emptyList(), splits);
            RemoteFileInfo remoteFileInfo = new RemoteFileInfo();
            remoteFileInfo.setFiles(ImmutableList.of(
                    FlussRemoteFileDesc.createFlussRemoteFileDesc(flussSplitsInfo)));
            return ImmutableList.of(remoteFileInfo);
        }

        @Override
        public Statistics getTableStatistics(OptimizerContext session, Table table,
                                             Map<ColumnRefOperator, Column> columns,
                                             List<PartitionKey> partitionKeys,
                                             ScalarOperator predicate,
                                             long limit,
                                             TvrVersionRange tableVersionRange) {
            Statistics.Builder builder = Statistics.builder().setOutputRowCount(1);
            for (ColumnRefOperator columnRefOperator : columns.keySet()) {
                builder.addColumnStatistic(columnRefOperator, ColumnStatistic.unknown());
            }
            return builder.build();
        }
    }

    private static FlussTable flussTable(String tableName, boolean partitioned) {
        TableInfo tableInfo = tableInfo(tableName, partitioned);
        List<Column> schema = ImmutableList.of(
                new Column("id", IntegerType.INT, true),
                new Column("region", StringType.STRING, true),
                new Column("amount", IntegerType.INT, true),
                new Column("category", StringType.STRING, true));
        return new FlussTable(CATALOG, DB, tableName, schema, nativeTable(tableInfo), new Configuration());
    }

    private static TableInfo tableInfo(String tableName, boolean partitioned) {
        Schema schema = Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column("region", DataTypes.STRING())
                .column("amount", DataTypes.INT())
                .column("category", DataTypes.STRING())
                .build();
        TableDescriptor.Builder descriptorBuilder = TableDescriptor.builder()
                .schema(schema)
                .distributedBy(3, "id");
        if (partitioned) {
            descriptorBuilder.partitionedBy("category");
        }
        return TableInfo.of(TablePath.of(DB, tableName), 42L, 7, descriptorBuilder.build(), 1000L, 2000L);
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

    private static class FakeConnection implements Connection {
        private final Configuration configuration;

        private FakeConnection(Configuration configuration) {
            this.configuration = configuration;
        }

        @Override
        public Configuration getConfiguration() {
            return configuration;
        }

        @Override
        public Admin getAdmin() {
            return null;
        }

        @Override
        public org.apache.fluss.client.table.Table getTable(TablePath tablePath) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
        }
    }
}
