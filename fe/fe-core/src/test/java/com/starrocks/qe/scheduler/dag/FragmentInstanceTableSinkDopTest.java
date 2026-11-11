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

package com.starrocks.qe.scheduler.dag;

import com.starrocks.planner.PlanFragment;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FragmentInstanceTableSinkDopTest {
    // An OLAP table sink fragment whose fragment-wide DOP was left at the scan DOP of the last assigned worker.
    private static PlanFragment olapSinkFragment(int fragmentDop, boolean scanRangesPerDriverSeq) {
        PlanFragment fragment = mock(PlanFragment.class);
        when(fragment.collectScanNodes()).thenReturn(Map.of());
        when(fragment.forceSetTableSinkDop()).thenReturn(true);
        when(fragment.isForceAssignScanRangesPerDriverSeq()).thenReturn(scanRangesPerDriverSeq);
        when(fragment.getPipelineDop()).thenReturn(fragmentDop);
        // A null sink takes the same branch as OlapTableSink.
        when(fragment.getSink()).thenReturn(null);
        return fragment;
    }

    @Test
    public void testTableSinkDopFollowsEachInstanceScanDop() {
        // 15 scan ranges on 10 workers: some instances scan 2 ranges, some 1, and the last one assigned had 2.
        ExecutionFragment execFragment = new ExecutionFragment(null, olapSinkFragment(2, true), 0);
        FragmentInstance twoRanges = new FragmentInstance(null, execFragment);
        twoRanges.setPipelineDop(2);
        FragmentInstance oneRange = new FragmentInstance(null, execFragment);
        oneRange.setPipelineDop(1);

        assertEquals(2, twoRanges.getTableSinkDop());
        assertEquals(1, oneRange.getTableSinkDop());
    }

    @Test
    public void testTableSinkDopUsesFragmentDopWithoutPerDriverSeqAssignment() {
        ExecutionFragment execFragment = new ExecutionFragment(null, olapSinkFragment(4, false), 0);
        FragmentInstance instance = new FragmentInstance(null, execFragment);
        instance.setPipelineDop(1);

        assertEquals(4, instance.getTableSinkDop());
    }

    @Test
    public void testTableSinkDopUsesFragmentDopWhenInstanceDopIsUnset() {
        ExecutionFragment execFragment = new ExecutionFragment(null, olapSinkFragment(4, true), 0);
        FragmentInstance instance = new FragmentInstance(null, execFragment);

        assertEquals(4, instance.getTableSinkDop());
    }
}
