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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class AsyncTaskQueueTest {

    public void runTest(ExecutorService executorService, int maxRunningTaskCount, int maxOutputQueueSize, int taskSize,
                        int bufferSize,
                        boolean throwException) {
        int repeatNumber = 3;
        int outputNumber = 20;
        class MyTask implements AsyncTaskQueue.Task<String> {
            int idx;
            String name;
            int repeat = repeatNumber;

            public MyTask(int idx, String name) {
                this.idx = idx;
                this.name = name;
            }

            @Override
            public List<String> run() throws InterruptedException {
                repeat -= 1;
                // Thread.sleep((taskSize - idx) * 10);
                ArrayList<String> outputs = new ArrayList<>();
                for (int i = 0; i < outputNumber; i++) {
                    outputs.add(String.format("%s-%02d", name, i));
                }
                if (throwException && (idx == (taskSize - 1)) && repeat == 0) {
                    throw new RuntimeException("");
                }
                return outputs;
            }

            @Override
            public boolean isDone() {
                return repeat == 0;
            }

            @Override
            public String toString() {
                return name;
            }
        }

        class MyAsyncTaskQueue extends AsyncTaskQueue<String> {

            public MyAsyncTaskQueue(Executor executor) {
                super(executor);
            }

            @Override
            public int computeOutputSize(String output) {
                return output.length();
            }
        }
        AsyncTaskQueue<String> asyncTaskQueue = new MyAsyncTaskQueue(executorService);
        asyncTaskQueue.setMaxRunningTaskCount(maxRunningTaskCount);
        asyncTaskQueue.setMaxOutputQueueSize(maxOutputQueueSize);
        List<MyTask> tasks = new ArrayList<>();
        for (int i = 0; i < taskSize; i++) {
            tasks.add(new MyTask(i, "mytask" + i));
        }
        asyncTaskQueue.start(tasks);
        // int threads, int maxRunningTaskCount, int maxOutputQueueSize, int taskSize, int bufferSize
        System.out.printf("a = %d, b = %d, c = %d, d= %d, e = %s\n", maxRunningTaskCount,
                maxOutputQueueSize,
                taskSize, bufferSize, throwException);
        try {
            List<String> ans = new ArrayList<>();
            while (asyncTaskQueue.hasMoreOutput()) {
                List<String> outputs = asyncTaskQueue.getOutputs(bufferSize);
                ans.addAll(outputs);
            }
            Assert.assertEquals(repeatNumber * outputNumber * taskSize, ans.size());
            Assert.assertTrue(!throwException);
        } catch (Exception e) {
            Assert.assertTrue(throwException);
        }
    }

    @Test
    public void testGenerateString() {
        ExecutorService executorService =
                Executors.newFixedThreadPool(4,
                        new ThreadFactoryBuilder().setNameFormat("pull-hive-remote-files-%d").build());
        for (int maxRunningTaskCount = 2; maxRunningTaskCount <= 8; maxRunningTaskCount += 2) {
            for (int maxOutputQueueSize = 10; maxOutputQueueSize <= 40; maxOutputQueueSize += 10) {
                for (int taskSize = 2; taskSize <= 8; taskSize += 2) {
                    for (int bufferSize = 10; bufferSize <= 40; bufferSize += 10) {
                        runTest(executorService, maxRunningTaskCount, maxOutputQueueSize, taskSize, bufferSize, false);
                        runTest(executorService, maxRunningTaskCount, maxOutputQueueSize, taskSize, bufferSize, true);
                    }
                }
            }
        }
        executorService.shutdown();
    }

    // @Test
    public void stressTest() {
        for (int i = 0; i < 1000; i++) {
            testGenerateString();
        }
    }
}