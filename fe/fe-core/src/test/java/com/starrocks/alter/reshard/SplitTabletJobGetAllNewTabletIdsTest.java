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

package com.starrocks.alter.reshard;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SplitTabletJobGetAllNewTabletIdsTest {

    /**
     * getAllNewTabletIds walks physical-partition -> index -> tablet, collecting the new tablet ids of each
     * real (non-identical) SplittingTablet and skipping tablets whose SplittingTablet is null or identical.
     * A real SplittingTablet with a single new id is identical (isIdenticalTablet() = size == 1), and its
     * getSplittingTablet() returns null — so it is skipped; a >=2-id one is collected.
     */
    @Test
    public void collectsNewIdsSkippingIdenticalAndNull() {
        SplittingTablet collectedA = new SplittingTablet(100L, List.of(201L, 202L)); // size 2 -> collected
        SplittingTablet identicalReal = new SplittingTablet(101L, List.of(203L)); // size 1 -> identical -> skipped
        SplittingTablet collectedB = new SplittingTablet(104L, List.of(205L, 206L)); // size 2 -> collected

        // A non-null SplittingTablet that reports identical exercises the defensive `|| isIdenticalTablet()` branch.
        SplittingTablet identicalMock = mock(SplittingTablet.class);
        when(identicalMock.isIdenticalTablet()).thenReturn(true);
        ReshardingTablet identicalMockTablet = mock(ReshardingTablet.class);
        when(identicalMockTablet.getSplittingTablet()).thenReturn(identicalMock);

        // A tablet whose getSplittingTablet() is null outright.
        ReshardingTablet nullTablet = mock(ReshardingTablet.class);
        when(nullTablet.getSplittingTablet()).thenReturn(null);

        ReshardingMaterializedIndex index = mock(ReshardingMaterializedIndex.class);
        when(index.getReshardingTablets()).thenReturn(
                List.of(collectedA, identicalReal, identicalMockTablet, nullTablet, collectedB));
        ReshardingPhysicalPartition partition = mock(ReshardingPhysicalPartition.class);
        when(partition.getReshardingIndexes()).thenReturn(Map.of(1L, index));

        SplitTabletJob job = new SplitTabletJob(1L, 2L, 3L, Map.of(10L, partition));

        Assertions.assertEquals(List.of(201L, 202L, 205L, 206L), job.getAllNewTabletIds());
    }

    @Test
    public void emptyReshardMapYieldsEmptyList() {
        SplitTabletJob job = new SplitTabletJob(1L, 2L, 3L, Map.of());
        Assertions.assertTrue(job.getAllNewTabletIds().isEmpty());
    }
}
