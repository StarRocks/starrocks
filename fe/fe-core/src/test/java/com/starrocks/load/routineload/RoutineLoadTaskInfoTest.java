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
import com.starrocks.common.UserException;
import com.starrocks.transaction.TransactionState;
import mockit.Injectable;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import java.util.UUID;

public class RoutineLoadTaskInfoTest {

    @Test
    public void testProcessTimeOutTasks(@Injectable TransactionState transactionState)
            throws UserException, InterruptedException {

        long maxBatchIntervalS = 10;
        Map<Integer, Long> partitionIdsToOffset = Maps.newHashMap();
        partitionIdsToOffset.put(100, 0L);
        KafkaTaskInfo kafkaTaskInfo = new KafkaTaskInfo(new UUID(1, 1), 1L,
                maxBatchIntervalS * 2 * 1000, System.currentTimeMillis(), partitionIdsToOffset,
                10000L, 5);
        kafkaTaskInfo.setExecuteStartTimeMs(System.currentTimeMillis() - maxBatchIntervalS * 2 * 1000 - 1);

        kafkaTaskInfo.afterAborted(transactionState, false, "some reason");

        Thread.sleep(10000);

        Assert.assertThrows(UserException.class, () -> {
            kafkaTaskInfo.afterAborted(transactionState, false, "some reason");
        });
    }
}
