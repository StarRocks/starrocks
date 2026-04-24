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

public class PublishTabletsInfoTest {
    @Test
    public void testPublishTabletsInfo() {
        PublishTabletsInfo publishTabletsInfo = new PublishTabletsInfo();

        Assertions.assertTrue(publishTabletsInfo.getTabletIds().isEmpty());
        Assertions.assertTrue(publishTabletsInfo.getReshardingTablets().isEmpty());
        Assertions.assertTrue(publishTabletsInfo.getOldTabletIds().isEmpty());

        publishTabletsInfo.addTabletId(1);
        publishTabletsInfo.addReshardingTablet(new SplittingTablet(2, List.of(21L, 22L)));
        publishTabletsInfo.addReshardingTablet(new MergingTablet(List.of(31L, 32L), 3));
        publishTabletsInfo.addReshardingTablet(new IdenticalTablet(4, 4));

        Assertions.assertFalse(publishTabletsInfo.getTabletIds().isEmpty());
        Assertions.assertFalse(publishTabletsInfo.getReshardingTablets().isEmpty());
        Assertions.assertFalse(publishTabletsInfo.getOldTabletIds().isEmpty());
    }

    @Test
    public void testAddReshardingTabletIsIdempotentByIdentityKey() {
        // The same MergingTablet is registered under every one of its old tablet ids
        // (see MergeTabletJob.registerReshardingTablets), so Utils.processTablets calls
        // addReshardingTablet once per input. The dedup guard must keep the reshard op
        // in the wire list exactly once regardless of how many times it is added.
        PublishTabletsInfo info = new PublishTabletsInfo();

        MergingTablet merge = new MergingTablet(List.of(31L, 32L, 33L, 34L), 100L);
        info.addReshardingTablet(merge);
        info.addReshardingTablet(merge);
        info.addReshardingTablet(merge);
        info.addReshardingTablet(merge);

        Assertions.assertEquals(1, info.getReshardingTablets().size());
        Assertions.assertEquals(100L, merge.getFirstNewTabletId());
    }

    @Test
    public void testAddReshardingTabletDedupsLogicallyEqualButDistinctObjects() {
        // Robustness goal: survive future changes that might produce multiple
        // ReshardingTablet instances for the same logical op (e.g. image reload,
        // replay, clone). Dedup is by content-derived identity key (first new tablet id),
        // not by Java reference.
        PublishTabletsInfo info = new PublishTabletsInfo();

        info.addReshardingTablet(new MergingTablet(List.of(31L, 32L), 100L));
        info.addReshardingTablet(new MergingTablet(List.of(31L, 32L), 100L));

        Assertions.assertEquals(1, info.getReshardingTablets().size());
    }

    @Test
    public void testAddReshardingTabletAcceptsDistinctReshardOps() {
        // Different reshard ops have different new tablet ids (globally allocated
        // via GlobalStateMgr.getNextId()), so they must not dedupe.
        PublishTabletsInfo info = new PublishTabletsInfo();

        info.addReshardingTablet(new MergingTablet(List.of(31L, 32L), 100L));
        info.addReshardingTablet(new MergingTablet(List.of(41L, 42L), 200L));
        info.addReshardingTablet(new SplittingTablet(50L, List.of(300L, 301L)));
        info.addReshardingTablet(new IdenticalTablet(60L, 400L));

        Assertions.assertEquals(4, info.getReshardingTablets().size());
    }
}
