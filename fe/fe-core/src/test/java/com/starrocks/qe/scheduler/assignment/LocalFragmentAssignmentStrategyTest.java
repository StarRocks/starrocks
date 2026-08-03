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

package com.starrocks.qe.scheduler.assignment;

import com.starrocks.qe.scheduler.dag.FragmentInstance;
import com.starrocks.qe.scheduler.dag.FragmentInstance.DeployedScanRangeLayout;
import com.starrocks.thrift.TScanRangeParams;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

public class LocalFragmentAssignmentStrategyTest {
    @Test
    public void testFragmentInstanceRecordsDeployedScanRangeLayoutPerScanNode() {
        FragmentInstance instance = new FragmentInstance(null, null);

        instance.recordDeployedScanRangesWithoutDriverSeq(1);
        instance.recordDeployedScanRangesPerDriverSeq(2, 4);

        DeployedScanRangeLayout normalLayout = instance.getDeployedScanRangeLayout(1).orElseThrow();
        Assertions.assertFalse(normalLayout.isPerDriverSeq());

        DeployedScanRangeLayout perDriverLayout = instance.getDeployedScanRangeLayout(2).orElseThrow();
        Assertions.assertTrue(perDriverLayout.isPerDriverSeq());
        Assertions.assertEquals(4, perDriverLayout.getDriverSeqCount());
    }

    @Test
    public void testPaddingScanRangesUsesPerScanNodeDeployedLayout() {
        FragmentInstance instance = new FragmentInstance(null, null);
        instance.recordDeployedScanRangesPerDriverSeq(1, 2);
        instance.recordDeployedScanRangesPerDriverSeq(2, 4);

        instance.addScanRanges(1, 0, List.of());
        instance.addScanRanges(2, 0, List.of());
        instance.paddingScanRanges();

        Map<Integer, List<TScanRangeParams>> scanNode1DriverSeqToScanRanges =
                instance.getNode2DriverSeqToScanRanges().get(1);
        Assertions.assertEquals(2, scanNode1DriverSeqToScanRanges.size());
        Assertions.assertTrue(scanNode1DriverSeqToScanRanges.containsKey(0));
        Assertions.assertTrue(scanNode1DriverSeqToScanRanges.containsKey(1));

        Map<Integer, List<TScanRangeParams>> scanNode2DriverSeqToScanRanges =
                instance.getNode2DriverSeqToScanRanges().get(2);
        Assertions.assertEquals(4, scanNode2DriverSeqToScanRanges.size());
        Assertions.assertTrue(scanNode2DriverSeqToScanRanges.containsKey(0));
        Assertions.assertTrue(scanNode2DriverSeqToScanRanges.containsKey(1));
        Assertions.assertTrue(scanNode2DriverSeqToScanRanges.containsKey(2));
        Assertions.assertTrue(scanNode2DriverSeqToScanRanges.containsKey(3));
    }

    @Test
    public void testResetScanRangesKeepsDeployedScanRangeLayout() {
        FragmentInstance instance = new FragmentInstance(null, null);
        instance.recordDeployedScanRangesPerDriverSeq(1, 3);
        instance.addScanRanges(1, 0, List.of());

        instance.resetAllScanRanges();

        Assertions.assertTrue(instance.getNode2DriverSeqToScanRanges().isEmpty());
        DeployedScanRangeLayout layout = instance.getDeployedScanRangeLayout(1).orElseThrow();
        Assertions.assertTrue(layout.isPerDriverSeq());
        Assertions.assertEquals(3, layout.getDriverSeqCount());
    }

}
