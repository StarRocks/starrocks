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

package com.starrocks.sql;

import com.starrocks.planner.DescriptorTable;
import com.starrocks.planner.PlanNode;
import com.starrocks.planner.PlanNodeId;
import com.starrocks.planner.SlotDescriptor;
import com.starrocks.planner.SlotId;
import com.starrocks.planner.TupleDescriptor;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TPlanNode;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MergeIntoPlannerTest {

    private static final class TestPlanNode extends PlanNode {
        private TestPlanNode(TupleDescriptor tupleDescriptor) {
            super(new PlanNodeId(0), tupleDescriptor.getId().asList(), "TEST");
        }

        @Override
        protected void toThrift(TPlanNode msg) {
        }

        @Override
        protected String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
            return "";
        }
    }

    @Test
    public void testCollectSlotsSortedByIdSkipsNonMaterializedSlots() {
        DescriptorTable descTbl = new DescriptorTable();
        TupleDescriptor tupleDesc = descTbl.createTupleDescriptor();

        SlotDescriptor commonSubExprSlot = descTbl.addSlotDescriptor(tupleDesc, new SlotId(1));
        commonSubExprSlot.setIsMaterialized(false);

        SlotDescriptor fileSlot = descTbl.addSlotDescriptor(tupleDesc, new SlotId(2));
        fileSlot.setIsMaterialized(true);

        SlotDescriptor posSlot = descTbl.addSlotDescriptor(tupleDesc, new SlotId(3));
        posSlot.setIsMaterialized(true);

        SlotDescriptor dataSlot = descTbl.addSlotDescriptor(tupleDesc, new SlotId(4));
        dataSlot.setIsMaterialized(true);

        List<SlotId> actual = MergeIntoPlanner.collectSlotsSortedById(new TestPlanNode(tupleDesc), descTbl);
        assertEquals(Arrays.asList(fileSlot.getId(), posSlot.getId(), dataSlot.getId()), actual);
    }
}
