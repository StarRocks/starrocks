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

import com.starrocks.catalog.Column;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.VectorSearchOptions;
import com.starrocks.type.IntegerType;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TStorageType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

public class OlapScanNodeTest {

    private OlapScanNode createOlapScanNode(Table.TableType tableType) {
        OlapTable table = new OlapTable(tableType);
        table.maySetDatabaseId(1L);
        table.setBaseIndexMetaId(1L);
        table.setIndexMeta(1L, "base",
                Collections.singletonList(new Column("c0", IntegerType.INT)),
                0, 0, (short) 1, TStorageType.COLUMN, KeysType.DUP_KEYS);
        table.setDefaultDistributionInfo(
                new HashDistributionInfo(3, Collections.emptyList()));

        TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
        desc.setTable(table);

        return new OlapScanNode(new PlanNodeId(0), desc, "OlapScanNode",
                table.getBaseIndexMetaId());
    }

    @Test
    public void testLakeScanNodeWithVectorSearchOptions() {
        OlapScanNode scanNode = createOlapScanNode(Table.TableType.CLOUD_NATIVE);

        VectorSearchOptions opts = new VectorSearchOptions();
        opts.setEnableUseANN(true);
        opts.setLimitK(10);
        opts.setDistanceColumnName("distance");
        opts.setDistanceSlotId(1);
        opts.setQueryVector(Arrays.asList("1.0", "2.0", "3.0"));
        opts.setResultOrder(true);
        scanNode.setVectorSearchOptions(opts);

        TPlanNode msg = new TPlanNode();
        scanNode.toThrift(msg);

        Assertions.assertNotNull(msg.lake_scan_node);
        Assertions.assertNotNull(msg.lake_scan_node.getVector_search_options());
        Assertions.assertTrue(msg.lake_scan_node.getVector_search_options().isEnable_use_ann());
        Assertions.assertEquals(10, msg.lake_scan_node.getVector_search_options().getVector_limit_k());
        Assertions.assertEquals("distance",
                msg.lake_scan_node.getVector_search_options().getVector_distance_column_name());
        Assertions.assertEquals(Arrays.asList("1.0", "2.0", "3.0"),
                msg.lake_scan_node.getVector_search_options().getQuery_vector());
    }

    @Test
    public void testLakeScanNodeWithVectorSearchOptionsDisabled() {
        // toThrift must not emit vector_search_options on the lake_scan_node when ANN is disabled.
        // Covers the `vectorSearchOptions != null && vectorSearchOptions.isEnableUseANN()` guard:
        // the negative side of the `&&`, which the positive-case test cannot exercise.
        OlapScanNode scanNode = createOlapScanNode(Table.TableType.CLOUD_NATIVE);

        VectorSearchOptions opts = new VectorSearchOptions();
        opts.setEnableUseANN(false);
        scanNode.setVectorSearchOptions(opts);

        TPlanNode msg = new TPlanNode();
        scanNode.toThrift(msg);

        Assertions.assertNotNull(msg.lake_scan_node);
        Assertions.assertFalse(msg.lake_scan_node.isSetVector_search_options());
    }

    @Test
    public void testLakeScanNodeWithNullVectorSearchOptions() {
        // toThrift must not emit vector_search_options when the field has been set to null.
        // Covers the `vectorSearchOptions != null` short-circuit branch in toThrift.
        OlapScanNode scanNode = createOlapScanNode(Table.TableType.CLOUD_NATIVE);
        scanNode.setVectorSearchOptions(null);

        TPlanNode msg = new TPlanNode();
        scanNode.toThrift(msg);

        Assertions.assertNotNull(msg.lake_scan_node);
        Assertions.assertFalse(msg.lake_scan_node.isSetVector_search_options());
    }
}
