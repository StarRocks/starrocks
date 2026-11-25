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

package com.starrocks.planner;

import com.starrocks.catalog.PaimonTable;
import com.starrocks.catalog.Table;
import com.starrocks.connector.CatalogConnector;
import com.starrocks.connector.metadata.MetadataTableFactoryProvider;
import com.starrocks.connector.metadata.MetadataTableType;
import com.starrocks.connector.metadata.iceberg.IcebergMetadataTableFactory;
import com.starrocks.connector.paimon.PaimonGlobalIndexMetadataTable;
import com.starrocks.connector.paimon.PaimonGlobalIndexPlanner;
import com.starrocks.connector.paimon.PaimonGlobalIndexRequest;
import com.starrocks.connector.paimon.PaimonMetadataTableFactory;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.thrift.THdfsFileFormat;
import com.starrocks.thrift.TPaimonGlobalIndexFile;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.thrift.TTableDescriptor;
import com.starrocks.type.IntegerType;
import mockit.Expectations;
import mockit.Mocked;
import mockit.Verifications;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PaimonGlobalIndexScanNodeTest {
    private static PaimonTable origin() {
        PaimonTable origin = mock(PaimonTable.class);
        when(origin.getCatalogName()).thenReturn("paimon");
        when(origin.getCatalogDBName()).thenReturn("db");
        when(origin.getCatalogTableName()).thenReturn("orders");
        when(origin.toThrift(anyList())).thenAnswer(call -> new TTableDescriptor());
        return origin;
    }

    private static PaimonGlobalIndexRequest request() {
        return PaimonGlobalIndexRequest.create("file:/orders", "main", 42, 1, 1000,
                new BinaryPredicateOperator(BinaryType.EQ, new ColumnRefOperator(1, IntegerType.INT, "k", true),
                        ConstantOperator.createInt(10)), Map.of(1, 7));
    }

    @Test
    void testMetadataFactoryRoutingAndInternalOnlyAccess(@Mocked GlobalStateMgr state, @Mocked MetadataMgr manager) {
        assertSame(PaimonMetadataTableFactory.INSTANCE, MetadataTableFactoryProvider.getFactory("paimon"));
        assertSame(PaimonMetadataTableFactory.INSTANCE,
                MetadataTableFactoryProvider.getFactory("unified", MetadataTableType.PAIMON_GLOBAL_INDEX));
        assertSame(IcebergMetadataTableFactory.INSTANCE, MetadataTableFactoryProvider.getFactory("unified"));
        ConnectContext context = new ConnectContext();
        var factory = PaimonMetadataTableFactory.INSTANCE;
        assertThrows(IllegalArgumentException.class,
                () -> factory.createTable(context, "paimon", "db", "orders", MetadataTableType.PAIMON_GLOBAL_INDEX));
        assertThrows(IllegalArgumentException.class,
                () -> factory.createTable(null, "paimon", "db", "orders", MetadataTableType.PAIMON_GLOBAL_INDEX));
        context.setMetadataContext(true);
        var origin = origin();
        new Expectations() {{
                GlobalStateMgr.getCurrentState().getMetadataMgr();
                result = manager;
                manager.getTable(context, "paimon", "db", "orders");
                result = origin;
                manager.getTable(context, "paimon", "db", "other");
                result = mock(Table.class);
            }};
        var metadata = (PaimonGlobalIndexMetadataTable) factory.createTable(
                context, "paimon", "db", "orders", MetadataTableType.PAIMON_GLOBAL_INDEX);
        assertSame(origin, metadata.getOrigin());
        assertTrue(metadata.isInternalOnly());
        assertTrue(metadata.supportBuildPlan());
        assertEquals(IntegerType.BIGINT, metadata.getColumn("shard_id").getType());
        assertNotNull(metadata.getColumn("index_result"));
        TTableDescriptor descriptor = metadata.toThrift(List.of());
        assertEquals(metadata.getId(), descriptor.getId());
        assertEquals("orders$paimon_global_index", descriptor.getTableName());
        assertEquals(2, descriptor.getNumCols());
        assertThrows(IllegalArgumentException.class,
                () -> factory.createTable(context, "paimon", "db", "other", MetadataTableType.PAIMON_GLOBAL_INDEX));
    }

    @Test
    void testScanRangesCarryRowIdsAndPhysicalFileBytes(@Mocked GlobalStateMgr state,
            @Mocked CatalogConnector connector, @Mocked MetadataMgr manager) {
        var binding = request();
        TPaimonGlobalIndexFile file = new TPaimonGlobalIndexFile();
        file.setField_id(7);
        file.setField_type("int");
        file.setPath("file:/orders/index/index-7");
        file.setFile_size(2048);
        file.setRow_range_from(100);
        file.setRow_range_to(199);
        file.setMetadata(new byte[9]);
        var shard = new PaimonGlobalIndexPlanner.Shard(0, 100, 199, 2048, binding, List.of(file));
        var metadata = new PaimonGlobalIndexPlanner.Metadata(binding, List.of(shard));
        new Expectations() {{
                GlobalStateMgr.getCurrentState().getConnectorMgr().getConnector("paimon");
                result = connector;
                connector.getMetadata().getCloudConfiguration();
                result = new CloudConfiguration();
                GlobalStateMgr.getCurrentState().getMetadataMgr();
                result = manager;
                manager.getSerializedMetaSpec("paimon", "db", "orders", 42,
                        binding.toTransportString(), MetadataTableType.PAIMON_GLOBAL_INDEX);
                result = metadata;
            }};
        TupleDescriptor descriptor = new TupleDescriptor(new TupleId(3));
        descriptor.setTable(new PaimonGlobalIndexMetadataTable(origin()));
        var scan = new PaimonGlobalIndexScanNode(new PlanNodeId(0), descriptor);
        scan.preProcessMetadataPredicate(binding.toTransportString());
        scan.setupScanRangeLocations();
        var locations = scan.getScanRangeLocations(0);
        assertEquals(1, locations.size());
        var range = locations.get(0).getScan_range().getHdfs_scan_range();
        assertEquals(2048, range.getLength());
        assertEquals(2048, range.getFile_length());
        assertEquals(0, range.getOffset());
        assertEquals(THdfsFileFormat.UNKNOWN, range.getFile_format());
        assertEquals("file:/orders/#global-index/42/0", range.getFull_path());
        var task = range.getPaimon_global_index_scan_range();
        assertEquals(PaimonGlobalIndexRequest.CURRENT_VERSION, task.getProtocol_version());
        assertEquals(0, task.getShard_id());
        assertEquals(100, task.getRange_from());
        assertEquals(199, task.getRange_to());
        assertEquals(42, task.getSnapshot_id());
        assertEquals("file:/orders", task.getTable_path());
        assertEquals(binding.toJson(), task.getQuery_json());
        assertEquals(List.of(file), task.getIndex_files());
        assertNotNull(scan.getScanNodePredicates());
        TPlanNode node = new TPlanNode();
        scan.toThrift(node);
        assertEquals(TPlanNodeType.HDFS_SCAN_NODE, node.getNode_type());
        assertEquals(3, node.getHdfs_scan_node().getTuple_id());
        assertEquals("PAIMON_GLOBAL_INDEX", node.getHdfs_scan_node().getMetadata_table_type());
        assertTrue(node.getHdfs_scan_node().isSetCloud_configuration());
    }

    @Test
    void testInvalidTransportDoesNotDiscoverMetadata(@Mocked GlobalStateMgr state, @Mocked CatalogConnector connector) {
        new Expectations() {{
                GlobalStateMgr.getCurrentState().getConnectorMgr().getConnector("paimon");
                result = connector;
            }};
        TupleDescriptor descriptor = new TupleDescriptor(new TupleId(0));
        descriptor.setTable(new PaimonGlobalIndexMetadataTable(origin()));
        var scan = new PaimonGlobalIndexScanNode(new PlanNodeId(0), descriptor);
        scan.preProcessMetadataPredicate("invalid base64");
        assertThrows(IllegalArgumentException.class, scan::setupScanRangeLocations);
        new Verifications() {{
                state.getMetadataMgr();
                times = 0;
            }};
    }
}
