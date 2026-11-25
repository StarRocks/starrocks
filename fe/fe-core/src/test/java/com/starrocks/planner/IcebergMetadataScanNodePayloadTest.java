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

import com.starrocks.catalog.Table;
import com.starrocks.common.tvr.TvrTableSnapshot;
import com.starrocks.connector.SerializedMetaSpec;
import com.starrocks.connector.iceberg.IcebergMetaSpec;
import com.starrocks.connector.metadata.MetadataTable;
import com.starrocks.connector.metadata.MetadataTableType;
import com.starrocks.server.MetadataMgr;
import com.starrocks.thrift.THdfsScanNode;
import com.starrocks.thrift.TPlanNode;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class IcebergMetadataScanNodePayloadTest {

    private static THdfsScanNode toThrift(IcebergMetaSpec spec) throws Exception {
        new MockUp<MetadataMgr>() {
            @Mock
            public SerializedMetaSpec getSerializedMetaSpec(String catalogName, String dbName, String tableName,
                                                            long snapshotId, String serializedPredicate,
                                                            MetadataTableType type) {
                return spec;
            }
        };
        MetadataTable table = new MetadataTable("catalog", 1L, "t$logical_iceberg_metadata", Table.TableType.METADATA,
                List.of(), "db", "t", MetadataTableType.LOGICAL_ICEBERG_METADATA);
        TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
        desc.setTable(table);
        IcebergMetadataScanNode node = new IcebergMetadataScanNode(new PlanNodeId(0), desc,
                "IcebergMetadataScanNode", TvrTableSnapshot.empty());
        node.preProcessIcebergPredicate("java-predicate");
        node.setupScanRangeLocations();
        TPlanNode planNode = new TPlanNode();
        node.toThrift(planNode);
        return planNode.getHdfs_scan_node();
    }

    @Test
    public void testShipsJsonFormsWhenPresent() throws Exception {
        THdfsScanNode scanNode = toThrift(new IcebergMetaSpec("java-table", "{\"format\":1}", "true", List.of(), false));
        Assertions.assertEquals("java-table", scanNode.getSerialized_table());
        Assertions.assertEquals("java-predicate", scanNode.getSerialized_predicate());
        Assertions.assertEquals("{\"format\":1}", scanNode.getIceberg_metadata_payload());
        Assertions.assertEquals("true", scanNode.getIceberg_predicate_json());
    }

    @Test
    public void testShipsJavaFormsOnlyWithoutJson() throws Exception {
        THdfsScanNode scanNode = toThrift(new IcebergMetaSpec("java-table", List.of(), false));
        Assertions.assertEquals("java-table", scanNode.getSerialized_table());
        Assertions.assertFalse(scanNode.isSetIceberg_metadata_payload());
        Assertions.assertFalse(scanNode.isSetIceberg_predicate_json());
    }
}
