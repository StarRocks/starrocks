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
import com.starrocks.catalog.KuduTable;
import com.starrocks.catalog.LanceTable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.expression.BoolLiteral;
import com.starrocks.sql.plan.HDFSScanNodePredicates;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.type.ArrayType;
import com.starrocks.type.IntegerType;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ScanNodeExplainTest {
    private static Stream<Arguments> scanCases() {
        return Stream.of(false, true).flatMap(kudu -> Arrays.stream(TExplainLevel.values())
                .map(level -> Arguments.of(kudu, level)));
    }

    private ScanNode newScan(boolean kudu) {
        DescriptorTable descriptors = new DescriptorTable();
        TupleDescriptor tuple = descriptors.createTupleDescriptor();
        if (kudu) {
            GlobalStateMgr state = mock(GlobalStateMgr.class, RETURNS_DEEP_STUBS);
            new MockUp<GlobalStateMgr>() {
                @Mock
                public GlobalStateMgr getCurrentState() {
                    return state;
                }
            };
            KuduTable table = mock(KuduTable.class);
            when(table.getName()).thenReturn("vectors");
            when(table.getCatalogName()).thenReturn("kudu_catalog");
            tuple.setTable(table);
        } else {
            tuple.setTable(new LanceTable(1, "vectors", List.of(), "file:///tmp/vectors.lance"));
        }
        SlotDescriptor scalar = descriptors.addSlotDescriptor(tuple);
        scalar.setColumn(new Column("id", IntegerType.INT));
        SlotDescriptor array = descriptors.addSlotDescriptor(tuple);
        array.setColumn(new Column("embedding", new ArrayType(IntegerType.INT)));
        ScanNode scan = kudu ? new KuduScanNode(new PlanNodeId(0), tuple, "KuduScanNode")
                : new LanceScanNode(new PlanNodeId(0), tuple, "LanceScanNode");
        scan.cardinality = 12;
        scan.avgRowSize = 8.5F;
        return scan;
    }

    @ParameterizedTest
    @MethodSource("scanCases")
    public void testEmptyPredicatesPreserveExplainLayout(boolean kudu, TExplainLevel level) {
        ScanNode scan = newScan(kudu);
        String expected = "  TABLE: vectors\n"
                + (level == TExplainLevel.VERBOSE ? "" : "  cardinality=12\n")
                + "\n  avgRowSize=8.5\n"
                + (level == TExplainLevel.VERBOSE ? "  Pruned type: 1 <-> [ARRAY<INT>]\n" : "");
        Assertions.assertEquals(expected, scan.getNodeExplainString("  ", level));
    }

    @ParameterizedTest
    @MethodSource("scanCases")
    public void testAllPredicateGroupsPreserveExplainLayout(boolean kudu, TExplainLevel level) {
        ScanNode scan = newScan(kudu);
        HDFSScanNodePredicates predicates = kudu ? ((KuduScanNode) scan).getScanNodePredicates()
                : ((LanceScanNode) scan).getScanNodePredicates();
        scan.sortColumn = "id";
        scan.getConjuncts().add(new BoolLiteral(false));
        predicates.getPartitionConjuncts().addAll(List.of(new BoolLiteral(true), new BoolLiteral(false)));
        predicates.getNonPartitionConjuncts().add(new BoolLiteral(false));
        predicates.getNoEvalPartitionConjuncts().add(new BoolLiteral(true));
        predicates.getMinMaxConjuncts().add(new BoolLiteral(false));
        String expected = "  TABLE: vectors\n  SORT COLUMN: id\n"
                + (kudu ? "" : "  PREDICATES: FALSE\n")
                + "  PARTITION PREDICATES: TRUE, FALSE\n"
                + "  NON-PARTITION PREDICATES: FALSE\n"
                + "  NO EVAL-PARTITION PREDICATES: TRUE\n"
                + "  MIN/MAX PREDICATES: FALSE\n"
                + (level == TExplainLevel.VERBOSE ? "" : "  cardinality=12\n")
                + "\n  avgRowSize=8.5\n"
                + (level == TExplainLevel.VERBOSE ? "  Pruned type: 1 <-> [ARRAY<INT>]\n" : "");
        Assertions.assertEquals(expected, scan.getNodeExplainString("  ", level));
    }
}
