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
import com.starrocks.catalog.IcebergTable;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.connector.iceberg.IcebergMORParams;
import com.starrocks.connector.iceberg.IcebergTableMORParams;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.ast.JoinOperator;
import com.starrocks.sql.ast.expression.BinaryPredicate;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.Set;

/**
 * Which runtime filters may reach the delete side of an Iceberg equality-delete anti join. Dropping deletes there lets
 * deleted data rows through the anti join, so only a join filter of an inner or left semi join above it, with no limit
 * in between, may do it, and only through a `<=>` on a column the delete scan takes from the partition value.
 */
public class IcebergEqDeleteRuntimeFilterPushDownTest {
    private int nextId = 0;

    private SlotDescriptor slot(TupleDescriptor tuple, String name) {
        Column column = new Column(name, IntegerType.INT);
        SlotDescriptor slot = new SlotDescriptor(new SlotId(nextId++), tuple);
        slot.setType(IntegerType.INT);
        slot.setColumn(column);
        tuple.addSlot(slot);
        return slot;
    }

    private IcebergScanNode scan(TupleDescriptor tuple, IcebergMORParams morParams) {
        return new IcebergScanNode(new PlanNodeId(nextId++), tuple, "IcebergScanNode", IcebergTableMORParams.EMPTY,
                morParams, null);
    }

    private TupleDescriptor tuple() {
        TupleDescriptor tuple = new TupleDescriptor(new TupleId(nextId++));
        tuple.setTable(Mockito.mock(IcebergTable.class));
        return tuple;
    }

    private static BinaryPredicate nullSafeEq(SlotDescriptor left, SlotDescriptor right) {
        return new BinaryPredicate(BinaryType.EQ_FOR_NULL, new SlotRef(left), new SlotRef(right));
    }

    private static boolean canPrune(HashJoinNode antiJoin, BinaryPredicate conjunct, RuntimeFilterDescription filter) {
        return Deencapsulation.invoke(antiJoin, "isIcebergEqDeletePruningConjunct", conjunct, filter);
    }

    private static RuntimeFilterDescription filterBuiltBy(PlanNode join) {
        RuntimeFilterDescription filter = new RuntimeFilterDescription(new SessionVariable());
        filter.setBuildPlanNode(join);
        return filter;
    }

    @Test
    public void testFilterReachesThePartitionColumnOfTheDeleteScan() {
        TupleDescriptor dataTuple = tuple();
        SlotDescriptor dataKey = slot(dataTuple, "k");
        SlotDescriptor dataPart = slot(dataTuple, "p");
        TupleDescriptor deleteTuple = tuple();
        SlotDescriptor deleteKey = slot(deleteTuple, "k");
        SlotDescriptor deletePart = slot(deleteTuple, "p");
        TupleDescriptor dimTuple = tuple();
        SlotDescriptor dimPart = slot(dimTuple, "p");

        IcebergScanNode dataScan = scan(dataTuple, IcebergMORParams.DATA_FILE_WITH_EQ_DELETE);
        IcebergScanNode deleteScan = scan(deleteTuple,
                IcebergMORParams.of(IcebergMORParams.ScanTaskType.EQ_DELETE, List.of(1)));
        deleteScan.setRuntimeFilterPruningColumns(Set.of("p"));
        IcebergScanNode dimScan = scan(dimTuple, IcebergMORParams.EMPTY);

        BinaryPredicate keyMatch = nullSafeEq(dataKey, deleteKey);
        BinaryPredicate partMatch = nullSafeEq(dataPart, deletePart);
        HashJoinNode antiJoin = new HashJoinNode(new PlanNodeId(nextId++), dataScan, deleteScan,
                JoinOperator.LEFT_ANTI_JOIN, List.<Expr>of(keyMatch, partMatch), List.of());
        HashJoinNode innerJoin = new HashJoinNode(new PlanNodeId(nextId++), antiJoin, dimScan, JoinOperator.INNER_JOIN,
                List.<Expr>of(new BinaryPredicate(BinaryType.EQ, new SlotRef(dataPart), new SlotRef(dimPart))),
                List.of());

        RuntimeFilterDescription filter = filterBuiltBy(innerJoin);
        Assertions.assertTrue(canPrune(antiJoin, partMatch, filter));
        // the equality key is not a partition value: dropping deletes by it is not covered here
        Assertions.assertFalse(canPrune(antiJoin, keyMatch, filter));

        // a top-n or aggregation filter has no join that drops the rows it rejects
        RuntimeFilterDescription topN = filterBuiltBy(innerJoin);
        topN.setRuntimeFilterType(RuntimeFilterDescription.RuntimeFilterType.TOPN_FILTER);
        Assertions.assertFalse(canPrune(antiJoin, partMatch, topN));

        // a join that keeps the probe rows the filter rejects, or one that is not above the anti join
        HashJoinNode rightJoin = new HashJoinNode(new PlanNodeId(nextId++), antiJoin, dimScan,
                JoinOperator.RIGHT_OUTER_JOIN, List.<Expr>of(), List.of());
        Assertions.assertFalse(canPrune(antiJoin, partMatch, filterBuiltBy(rightJoin)));
        HashJoinNode elsewhere = new HashJoinNode(new PlanNodeId(nextId++), dimScan, dataScan, JoinOperator.INNER_JOIN,
                List.<Expr>of(), List.of());
        Assertions.assertFalse(canPrune(antiJoin, partMatch, filterBuiltBy(elsewhere)));

        // a limit between the filter's join and the anti join, or on the anti join, keeps the deletes
        antiJoin.setLimit(10);
        Assertions.assertFalse(canPrune(antiJoin, partMatch, filter));
        antiJoin.unsetLimit();
        Assertions.assertTrue(canPrune(antiJoin, partMatch, filter));

        // a plain anti join of the same shape, or a delete scan that does not take p from the partition
        deleteScan.setRuntimeFilterPruningColumns(Set.of());
        Assertions.assertFalse(canPrune(antiJoin, partMatch, filter));
        deleteScan.setRuntimeFilterPruningColumns(Set.of("p"));
        BinaryPredicate plainEq = new BinaryPredicate(BinaryType.EQ, new SlotRef(dataPart), new SlotRef(deletePart));
        Assertions.assertFalse(canPrune(antiJoin, plainEq, filter));
        HashJoinNode semiJoin = new HashJoinNode(new PlanNodeId(nextId++), dataScan, deleteScan,
                JoinOperator.LEFT_SEMI_JOIN, List.<Expr>of(partMatch), List.of());
        HashJoinNode aboveSemi = new HashJoinNode(new PlanNodeId(nextId++), semiJoin, dimScan, JoinOperator.INNER_JOIN,
                List.<Expr>of(), List.of());
        Assertions.assertFalse(canPrune(semiJoin, partMatch, filterBuiltBy(aboveSemi)));
    }
}
