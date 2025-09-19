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

package com.starrocks.alter.dynamictablet;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class PublishTabletsInfoTest {
    @Test
    public void testPublishTabletsInfo() {
        PublishTabletsInfo publishTabletsInfo = new PublishTabletsInfo();

        Assertions.assertTrue(publishTabletsInfo.getTabletIds().isEmpty());
        Assertions.assertTrue(publishTabletsInfo.isDynamicTabletsEmpty());
        Assertions.assertTrue(publishTabletsInfo.getDynamicTablets() == null);
        Assertions.assertTrue(publishTabletsInfo.getOldTabletIds().isEmpty());

        publishTabletsInfo.addTabletId(1);
        publishTabletsInfo.addDynamicTablet(new SplittingTablet(2, List.of(21L, 22L)));
        publishTabletsInfo.addDynamicTablet(new MergingTablet(List.of(31L, 32L), 3));
        publishTabletsInfo.addDynamicTablet(new IdenticalTablet(4, 4));

        Assertions.assertFalse(publishTabletsInfo.getTabletIds().isEmpty());
        Assertions.assertFalse(publishTabletsInfo.isDynamicTabletsEmpty());
        Assertions.assertTrue(publishTabletsInfo.getDynamicTablets() != null);
        Assertions.assertFalse(publishTabletsInfo.getOldTabletIds().isEmpty());
    }
}
