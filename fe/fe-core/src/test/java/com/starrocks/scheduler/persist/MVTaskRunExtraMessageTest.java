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


package com.starrocks.scheduler.persist;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.common.Config;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

public class MVTaskRunExtraMessageTest {
    @Test
    public void testMessageWithNormalMVPartitionsToRefresh() {
        MVTaskRunExtraMessage extraMessage = new MVTaskRunExtraMessage();
        Set<String> mvPartitionsToRefresh = Sets.newHashSet();
        for (int i = 0; i < Config.max_mv_task_run_meta_message_values_length; i++) {
            mvPartitionsToRefresh.add("partition" + i);
        }
        extraMessage.setMvPartitionsToRefresh(mvPartitionsToRefresh);
        Assertions.assertTrue(extraMessage.getMvPartitionsToRefresh().size() ==
                Config.max_mv_task_run_meta_message_values_length);
    }

    @Test
    public void testMessageWithTooLongMVPartitionsToRefresh() {
        MVTaskRunExtraMessage extraMessage = new MVTaskRunExtraMessage();
        Set<String> mvPartitionsToRefresh = Sets.newHashSet();
        for (int i = 0; i < 100; i++) {
            mvPartitionsToRefresh.add("partition" + i);
        }
        extraMessage.setMvPartitionsToRefresh(mvPartitionsToRefresh);
        Assertions.assertTrue(extraMessage.getMvPartitionsToRefresh().size() ==
                Config.max_mv_task_run_meta_message_values_length);
    }

    @Test
    public void testMessageWithNormalRefBasePartitionsToRefreshMap() {
        Map<String, Set<String>> refBasePartitionsToRefreshMap = Maps.newHashMap();
        for (int i = 0; i < Config.max_mv_task_run_meta_message_values_length; i++) {
            Set<String> partitions = Sets.newHashSet();
            for (int j = 0; j < 10; j++) {
                partitions.add("partition" + j);
            }
            refBasePartitionsToRefreshMap.put("table" + i, partitions);
        }
        MVTaskRunExtraMessage message = new MVTaskRunExtraMessage();
        message.setRefBasePartitionsToRefreshMap(refBasePartitionsToRefreshMap);
        Assertions.assertEquals(message.getRefBasePartitionsToRefreshMap().size(),
                Config.max_mv_task_run_meta_message_values_length);
    }

    @Test
    public void testMessageWithTooLongRefBasePartitionsToRefreshMap() {
        Map<String, Set<String>> refBasePartitionsToRefreshMap = Maps.newHashMap();
        for (int i = 0; i < 100; i++) {
            Set<String> partitions = Sets.newHashSet();
            for (int j = 0; j < 10; j++) {
                partitions.add("partition" + j);
            }
            refBasePartitionsToRefreshMap.put("table" + i, partitions);
        }
        MVTaskRunExtraMessage message = new MVTaskRunExtraMessage();
        message.setRefBasePartitionsToRefreshMap(refBasePartitionsToRefreshMap);
        Assertions.assertTrue(message.getRefBasePartitionsToRefreshMap().size() ==
                Config.max_mv_task_run_meta_message_values_length);
    }

    @Test
    public void testMessageWithNormalBasePartitionsToRefreshMap() {
        Map<String, Set<String>> basePartitionsToRefreshMap = Maps.newHashMap();
        for (int i = 0; i < 15; i++) {
            Set<String> partitions = Sets.newHashSet();
            for (int j = 0; j < 10; j++) {
                partitions.add("partition" + j);
            }
            basePartitionsToRefreshMap.put("table" + i, partitions);
        }
        MVTaskRunExtraMessage message = new MVTaskRunExtraMessage();
        message.setBasePartitionsToRefreshMap(basePartitionsToRefreshMap);
        Assertions.assertEquals(message.getBasePartitionsToRefreshMap().size(),
                Config.max_mv_task_run_meta_message_values_length);
    }

    @Test
    public void testMessageWithTooLongBasePartitionsToRefreshMap() {
        Map<String, Set<String>> basePartitionsToRefreshMap = Maps.newHashMap();
        for (int i = 0; i < 100; i++) {
            Set<String> partitions = Sets.newHashSet();
            for (int j = 0; j < 10; j++) {
                partitions.add("partition" + j);
            }
            basePartitionsToRefreshMap.put("table" + i, partitions);
        }
        MVTaskRunExtraMessage message = new MVTaskRunExtraMessage();
        message.setBasePartitionsToRefreshMap(basePartitionsToRefreshMap);
        Assertions.assertTrue(message.getBasePartitionsToRefreshMap().size() ==
                Config.max_mv_task_run_meta_message_values_length);
    }

    @Test
    public void testImvSourceRangesShrunkToConfiguredSize() {
        Map<String, Map<String, String>> versionRanges = Maps.newHashMap();
        Map<String, Map<String, String>> timestampRanges = Maps.newHashMap();
        for (int i = 0; i < Config.max_mv_task_run_meta_message_values_length + 10; i++) {
            versionRanges.put("catalog.db.table" + i, ImmutableMap.of("start", "1", "end", "2"));
            timestampRanges.put("catalog.db.table" + i, ImmutableMap.of("start", "1000", "end", "2000"));
        }
        MVTaskRunExtraMessage message = new MVTaskRunExtraMessage();
        message.setImvSourceVersionRange(versionRanges);
        message.setImvSourceTimestampRange(timestampRanges);
        Assertions.assertEquals(Config.max_mv_task_run_meta_message_values_length,
                message.getImvSourceVersionRange().size());
        Assertions.assertEquals(Config.max_mv_task_run_meta_message_values_length,
                message.getImvSourceTimestampRange().size());
    }

    @Test
    public void testImvSourceRangesSerializedToJson() {
        MVTaskRunExtraMessage message = new MVTaskRunExtraMessage();
        message.setImvSourceVersionRange(
                ImmutableMap.of("iceberg.db.tbl", ImmutableMap.of("start", "100", "end", "128")));
        message.setImvSourceTimestampRange(
                ImmutableMap.of("iceberg.db.tbl", ImmutableMap.of("start", "1717999999000", "end", "1718000000000")));
        String json = message.toString();
        Assertions.assertTrue(json.contains(
                "\"imvSourceVersionRange\":{\"iceberg.db.tbl\":{\"start\":\"100\",\"end\":\"128\"}}"), json);
        Assertions.assertTrue(json.contains(
                "\"imvSourceTimestampRange\":{\"iceberg.db.tbl\":" +
                        "{\"start\":\"1717999999000\",\"end\":\"1718000000000\"}}"), json);
    }
}
