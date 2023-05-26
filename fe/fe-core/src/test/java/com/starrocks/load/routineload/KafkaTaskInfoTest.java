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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.common.Pair;
import com.starrocks.common.UserException;
import com.starrocks.common.util.KafkaUtil;
import mockit.Injectable;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.UUID;

public class KafkaTaskInfoTest {

    @Test
    public void testReadyToExecute(@Injectable KafkaRoutineLoadJob kafkaRoutineLoadJob) throws Exception {
        new MockUp<RoutineLoadMgr>() {
            @Mock
            public RoutineLoadJob getJob(long jobId) {
                return kafkaRoutineLoadJob;
            }
        };

        new MockUp<KafkaUtil>() {
            @Mock
            public Map<Integer, Long> getLatestOffsets(String brokerList, String topic,
                                                       ImmutableMap<String, String> properties,
                                                       List<Integer> partitions) throws UserException {
                Map<Integer, Long> offsets = Maps.newHashMap();
                offsets.put(0, 100L);
                offsets.put(1, 100L);
                return offsets;
            }
        };

        Map<Integer, Long> offset1 = Maps.newHashMap();
        offset1.put(0, 99L);
        KafkaTaskInfo kafkaTaskInfo1 = new KafkaTaskInfo(UUID.randomUUID(),
                1L,
                System.currentTimeMillis(),
                System.currentTimeMillis(),
                offset1);
        Assert.assertTrue(kafkaTaskInfo1.readyToExecute());

        Map<Integer, Long> offset2 = Maps.newHashMap();
        offset1.put(0, 100L);
        KafkaTaskInfo kafkaTaskInfo2 = new KafkaTaskInfo(UUID.randomUUID(),
                1L,
                System.currentTimeMillis(),
                System.currentTimeMillis(),
                offset2);
        Assert.assertFalse(kafkaTaskInfo2.readyToExecute());
    }

    @Test
    public void testProgressKeepUp(@Injectable KafkaRoutineLoadJob kafkaRoutineLoadJob) throws Exception {
        new MockUp<RoutineLoadMgr>() {
            @Mock
            public RoutineLoadJob getJob(long jobId) {
                return kafkaRoutineLoadJob;
            }
        };

        new MockUp<KafkaUtil>() {
            @Mock
            public Map<Integer, Long> getLatestOffsets(String brokerList, String topic,
                                                       ImmutableMap<String, String> properties,
                                                       List<Integer> partitions) throws UserException {
                Map<Integer, Long> offsets = Maps.newHashMap();
                offsets.put(0, 100L);
                offsets.put(1, 100L);
                return offsets;
            }
        };

        Map<Integer, Long> offset = Maps.newHashMap();
        offset.put(0, 99L);
        KafkaTaskInfo kafkaTaskInfo = new KafkaTaskInfo(UUID.randomUUID(),
                1L,
                System.currentTimeMillis(),
                System.currentTimeMillis(),
                offset);
        // call readyExecute to cache latestPartOffset
        kafkaTaskInfo.readyToExecute();

        KafkaProgress kafkaProgress = new KafkaProgress();
        kafkaProgress.addPartitionOffset(new Pair<>(0, 98L));
        kafkaProgress.addPartitionOffset(new Pair<>(1, 98L));
        Assert.assertFalse(kafkaTaskInfo.isProgressKeepUp(kafkaProgress));

        kafkaProgress.modifyOffset(Lists.newArrayList(new Pair<>(0, 99L)));
        Assert.assertFalse(kafkaTaskInfo.isProgressKeepUp(kafkaProgress));

        kafkaProgress.modifyOffset(Lists.newArrayList(new Pair<>(1, 99L)));
        Assert.assertTrue(kafkaTaskInfo.isProgressKeepUp(kafkaProgress));
    }
}
