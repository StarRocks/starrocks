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

import com.starrocks.catalog.LanceTable;
import com.starrocks.planner.expression.ExprToThrift;
import com.starrocks.sql.ast.expression.BoolLiteral;
import com.starrocks.thrift.THdfsScanRange;
import com.starrocks.thrift.TPlanNode;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class LanceScanNodeTest {
    private LanceScanNode newScan(List<Long> nodes) {
        TupleDescriptor tuple = new DescriptorTable().createTupleDescriptor();
        tuple.setTable(new LanceTable(1, "vectors", List.of(), "file:///tmp/vectors.lance"));
        return new LanceScanNode(new PlanNodeId(0), tuple, "LanceScanNode") {
            @Override
            public List<Long> getAllAvailableBackendOrComputeIds() {
                return nodes;
            }
        };
    }

    @Test
    public void testWholeDatasetRangeIsIdempotent() {
        LanceScanNode scan = newScan(List.of(1L));
        scan.setupScanRangeLocations(null, null);
        scan.setupScanRangeLocations(null, null);
        Assertions.assertEquals(1, scan.getScanRangeLocations(0).size());
        THdfsScanRange range = scan.getScanRangeLocations(0).get(0).scan_range.hdfs_scan_range;
        Assertions.assertTrue(range.isUse_lance_jni_reader());
        Assertions.assertFalse(range.isSetLance_split_info());
    }

    @Test
    public void testNoBackendFailsInsteadOfReturningEmptyResults() {
        LanceScanNode scan = newScan(List.of());
        Assertions.assertThrows(IllegalStateException.class, () -> scan.setupScanRangeLocations(null, null));
        Assertions.assertTrue(scan.getScanRangeLocations(0).isEmpty());
    }

    @Test
    public void testSerializationPreservesScanPredicates() {
        LanceScanNode scan = newScan(List.of(1L));
        BoolLiteral predicate = new BoolLiteral(false);
        scan.getConjuncts().add(predicate);
        scan.setScanOptimizeOption(new com.starrocks.sql.optimizer.ScanOptimizeOption());
        TPlanNode node = scan.treeToThrift().getNodes().get(0);
        Assertions.assertEquals(List.of(ExprToThrift.treeToThrift(predicate)), node.getConjuncts());
    }
}
