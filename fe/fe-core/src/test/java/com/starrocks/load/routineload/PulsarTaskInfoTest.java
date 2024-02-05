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

package com.starrocks.load.routineload;

import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Map;
import java.util.UUID;

public class PulsarTaskInfoTest {

    @Test
    public void testRenew() {
        Map<String, Long> offsets1 = Maps.newHashMap();
        offsets1.put("0", 101L);
        offsets1.put("1", 102L);
        PulsarTaskInfo task1 = new PulsarTaskInfo(UUID.randomUUID(), 1, 1000,
                2000, Arrays.asList("0", "1"), offsets1, 3000);

        Map<String, Long> offsets2 = Maps.newHashMap();
        offsets2.put("0", 103L);
        offsets2.put("1", 104L);
        PulsarTaskInfo task2 = new PulsarTaskInfo(2001, task1, offsets2);

        Assert.assertEquals(task1.getBeId(), task2.getBeId());
        Assert.assertEquals(task1.getJobId(), task2.getJobId());
        Assert.assertEquals(task1.getPartitions(), task2.getPartitions());
        Assert.assertEquals(task1.getTimeoutMs(), task2.getTimeoutMs());
    }
}
