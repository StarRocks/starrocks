// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.load.routineload;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.starrocks.common.Pair;
import com.starrocks.common.UserException;
import com.starrocks.common.util.KafkaUtil;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
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
        // modify offset while paused
        progress.modifyOffset(Arrays.asList(new Pair<>(3, 20L)));
        progress.addPartitionOffset(new Pair<>(0, -1L));
        progress.addPartitionOffset(new Pair<>(1, -2L));
        progress.addPartitionOffset(new Pair<>(2, 10L));
        progress.convertOffset("127.0.0.1:9020", "topic", Maps.newHashMap());
        Assert.assertEquals(100L, (long) progress.getOffsetByPartition(0));
        Assert.assertEquals(1L, (long) progress.getOffsetByPartition(1));
        Assert.assertEquals(10L, (long) progress.getOffsetByPartition(2));
        Assert.assertEquals(20L, (long) progress.getOffsetByPartition(3));
    }
}
