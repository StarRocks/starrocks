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

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.common.Config;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import java.util.Set;

public class MVTaskRunExtraMessageTest {
    @Test
    public void testMessageWithNormalMVPartitionsToRefresh() {
        MVTaskRunExtraMessage extraMessage = new MVTaskRunExtraMessage();
        Set<String> mvPartitionsToRefresh = Sets.newHashSet();
        for (int i = 0; i < 10; i++) {
            mvPartitionsToRefresh.add("partition" + i);
        }
        extraMessage.setMvPartitionsToRefresh(mvPartitionsToRefresh);
        Assert.assertTrue(extraMessage.getMvPartitionsToRefresh().size() ==
                10);
    }

    @Test
    public void testMessageWithTooLongMVPartitionsToRefresh() {
        MVTaskRunExtraMessage extraMessage = new MVTaskRunExtraMessage();
        Set<String> mvPartitionsToRefresh = Sets.newHashSet();
        for (int i = 0; i < 100; i++) {
            mvPartitionsToRefresh.add("partition" + i);
        }
        extraMessage.setMvPartitionsToRefresh(mvPartitionsToRefresh);
        Assert.assertTrue(extraMessage.getMvPartitionsToRefresh().size() ==
                Config.max_mv_task_run_meta_message_values_length);
    }

    @Test
    public void testMessageWithNormalRefBasePartitionsToRefreshMap() {
        Map<String, Set<String>> refBasePartitionsToRefreshMap = Maps.newHashMap();
        for (int i = 0; i < 15; i++) {
            Set<String> partitions = Sets.newHashSet();
            for (int j = 0; j < 10; j++) {
                partitions.add("partition" + j);
            }
            refBasePartitionsToRefreshMap.put("table" + i, partitions);
        }
        MVTaskRunExtraMessage message = new MVTaskRunExtraMessage();
        message.setRefBasePartitionsToRefreshMap(refBasePartitionsToRefreshMap);
        Assert.assertTrue(message.getRefBasePartitionsToRefreshMap().size() == 15);
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
        Assert.assertTrue(message.getRefBasePartitionsToRefreshMap().size() ==
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
        Assert.assertTrue(message.getBasePartitionsToRefreshMap().size() == 15);
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
        Assert.assertTrue(message.getBasePartitionsToRefreshMap().size() ==
                Config.max_mv_task_run_meta_message_values_length);
    }
}
