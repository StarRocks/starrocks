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
import com.starrocks.catalog.LanceTable;
import com.starrocks.sql.ast.expression.BoolLiteral;
import com.starrocks.sql.plan.HDFSScanNodePredicates;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.type.ArrayType;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.List;

public class LanceScanNodeExplainTest {
    private LanceScanNode newScan() {
        DescriptorTable descriptors = new DescriptorTable();
        TupleDescriptor tuple = descriptors.createTupleDescriptor();
        tuple.setTable(new LanceTable(1, "vectors", List.of(), "file:///tmp/vectors.lance"));
        SlotDescriptor scalar = descriptors.addSlotDescriptor(tuple);
        scalar.setColumn(new Column("id", IntegerType.INT));
        SlotDescriptor array = descriptors.addSlotDescriptor(tuple);
        array.setColumn(new Column("embedding", new ArrayType(IntegerType.INT)));
        LanceScanNode scan = new LanceScanNode(new PlanNodeId(0), tuple, "LanceScanNode");
        scan.cardinality = 12;
        scan.avgRowSize = 8.5F;
        return scan;
    }

    @ParameterizedTest
    @EnumSource(TExplainLevel.class)
    public void testEmptyPredicatesPreserveExplainLayout(TExplainLevel level) {
        LanceScanNode scan = newScan();
        String expected = "  TABLE: vectors\n"
                + (level == TExplainLevel.VERBOSE ? "" : "  cardinality=12\n")
                + "\n  avgRowSize=8.5\n"
                + (level == TExplainLevel.VERBOSE ? "  Pruned type: 1 <-> [ARRAY<INT>]\n" : "");
        Assertions.assertEquals(expected, scan.getNodeExplainString("  ", level));
    }

    @ParameterizedTest
    @EnumSource(TExplainLevel.class)
    public void testAllPredicateGroupsPreserveExplainLayout(TExplainLevel level) {
        LanceScanNode scan = newScan();
        HDFSScanNodePredicates predicates = scan.getScanNodePredicates();
        scan.sortColumn = "id";
        scan.getConjuncts().add(new BoolLiteral(false));
        predicates.getPartitionConjuncts().addAll(List.of(new BoolLiteral(true), new BoolLiteral(false)));
        predicates.getNonPartitionConjuncts().add(new BoolLiteral(false));
        predicates.getNoEvalPartitionConjuncts().add(new BoolLiteral(true));
        predicates.getMinMaxConjuncts().add(new BoolLiteral(false));
        String expected = "  TABLE: vectors\n  SORT COLUMN: id\n"
                + "  PREDICATES: FALSE\n"
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
