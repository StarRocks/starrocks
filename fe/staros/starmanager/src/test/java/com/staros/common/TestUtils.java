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

package com.staros.common;

import com.staros.proto.ReplicationType;
import com.staros.proto.WarmupLevel;
import com.staros.proto.WorkerGroupSpec;
import com.staros.worker.WorkerManager;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.junit.Assert;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

public class TestUtils {

    public static long createWorkerGroupForTest(WorkerManager manager, String serviceId, int replicaNumber) {
        // create a worker group with no worker added
        return manager.createWorkerGroup(serviceId, "commonUtilsForTest",
                WorkerGroupSpec.newBuilder().setSize("x0").build(),
                null, null, replicaNumber, ReplicationType.NO_REPLICATION, WarmupLevel.WARMUP_NOT_SET);
    }

    /**
     * Use reflection to get executors from WorkerManager, and check if the threadExecutors is idle.
     * @param manager   worker manager
     * @return  true or false indicates the executors is idle or not
     */
    public static boolean workerManagerExecutorsIdle(WorkerManager manager) {
        ThreadPoolExecutor executor = getWorkerManagerExecutor(manager);
        Assert.assertNotNull(executor);
        return executor.getCompletedTaskCount() == executor.getTaskCount();
    }

    public static ThreadPoolExecutor getWorkerManagerExecutor(WorkerManager manager) {
        try {
            ExecutorService executor = (ExecutorService) FieldUtils.readField(manager, "executor", true);
            Assert.assertTrue(executor instanceof ThreadPoolExecutor);
            return (ThreadPoolExecutor) executor;
        } catch (IllegalAccessException exception) {
            Assert.fail(String.format("getWorkerManagerExecutor failed with exception: %s", exception.getMessage()));
            return null;
        }
    }
}
