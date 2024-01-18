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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.starrocks.common.exception.DdlException;
import com.starrocks.common.exception.UserException;
import com.starrocks.common.structure.Pair;
import com.starrocks.common.util.KafkaUtil;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class KafkaProgressTest {

    @Test
    public void testConvertOffset() throws Exception {
        new MockUp<KafkaUtil>() {
            @Mock
            public Map<Integer, Long> getLatestOffsets(String brokerList, String topic,
                                                       ImmutableMap<String, String> properties,
                                                       List<Integer> partitions) throws UserException {
                Map<Integer, Long> result = Maps.newHashMap();
                result.put(0, 100L);
                return result;
            }

            @Mock
            public Map<Integer, Long> getBeginningOffsets(String brokerList, String topic,
                                                          ImmutableMap<String, String> properties,
                                                          List<Integer> partitions) throws UserException {
                Map<Integer, Long> result = Maps.newHashMap();
                result.put(1, 1L);
                return result;
            }
        };

        KafkaProgress progress = new KafkaProgress();
        // modify offset while paused when partition is not ready
        try {
            List<Pair<Integer, Long>> partitionToOffset = new ArrayList<>();
            partitionToOffset.add(new Pair<>(3, 20L));
            progress.modifyOffset(partitionToOffset);
        } catch (DdlException e) {
            Assert.assertEquals("The specified partition 3 is not in the consumed partitions", e.getMessage());
        }

        progress.addPartitionOffset(new Pair<>(0, -1L));
        progress.addPartitionOffset(new Pair<>(1, -2L));
        progress.addPartitionOffset(new Pair<>(2, 10L));
        progress.addPartitionOffset(new Pair<>(3, 10L));
        progress.convertOffset("127.0.0.1:9020", "topic", Maps.newHashMap());

        List<Pair<Integer, Long>> partitionToOffset = new ArrayList<>();
        partitionToOffset.add(new Pair<>(3, 20L));
        progress.modifyOffset(partitionToOffset);
        Assert.assertEquals(4, partitionToOffset.size());

        Assert.assertEquals(100L, (long) progress.getOffsetByPartition(0));
        Assert.assertEquals(1L, (long) progress.getOffsetByPartition(1));
        Assert.assertEquals(10L, (long) progress.getOffsetByPartition(2));
        Assert.assertEquals(20L, (long) progress.getOffsetByPartition(3));
    }
}
