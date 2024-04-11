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

package com.starrocks.connector;

import com.starrocks.connector.paimon.Partition;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class ConnectorPartitionTraitsTest {

    @Test
    public void testMaxPartitionRefreshTs() {

        Map<String, PartitionInfo> fakePartitionInfo = new HashMap<>();
        Partition p1 = new Partition("p1", 100);
        Partition p2 = new Partition("p2", 200);
        fakePartitionInfo.put("p1", p1);
        fakePartitionInfo.put("p2", p2);
        new MockUp<ConnectorPartitionTraits.DefaultTraits>() {
            @Mock
            public Map<String, PartitionInfo> getPartitionNameWithPartitionInfo() {
                return fakePartitionInfo;
            }
        };

        Optional<Long> result = new ConnectorPartitionTraits.PaimonPartitionTraits().maxPartitionRefreshTs();
        Assert.assertTrue(result.isPresent());
        Assert.assertEquals(200L, result.get().longValue());
    }
}
