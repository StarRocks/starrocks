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

package com.starrocks.alter;

import com.starrocks.server.GlobalStateMgr;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class AlterJobV2PublishTest {
    private static final class TestJob extends LakeTableAddIndexJob {
        @Override
        protected boolean lakePublishVersion() {
            return true;
        }

        boolean pollPublish() {
            return publishVersion();
        }
    }

    private static final class SwitchableExecutor extends ThreadPoolExecutor {
        private volatile boolean reject = true;

        SwitchableExecutor() {
            super(0, 1, 60L, TimeUnit.SECONDS, new SynchronousQueue<>(), new AbortPolicy());
        }

        void setReject(boolean reject) {
            this.reject = reject;
        }

        @Override
        public void execute(Runnable command) {
            if (reject) {
                throw new RejectedExecutionException("injected rejection");
            }
            super.execute(command);
        }
    }

    @Test
    public void testRejectedPublishIsRetryable() throws Exception {
        SwitchableExecutor executor = new SwitchableExecutor();
        new MockUp<GlobalStateMgr>() {
            @Mock
            public ThreadPoolExecutor getLakeAlterPublishExecutor() {
                return executor;
            }
        };

        try {
            TestJob job = new TestJob();

            Assertions.assertFalse(job.pollPublish());
            Assertions.assertNull(job.publishVersionFuture);

            executor.setReject(false);
            Assertions.assertFalse(job.pollPublish());
            boolean published = false;
            for (int i = 0; i < 100 && !published; i++) {
                Thread.sleep(10L);
                published = job.pollPublish();
            }
            Assertions.assertTrue(published);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testFailedPublishFutureIsCleared() {
        TestJob job = new TestJob();
        job.publishVersionFuture = CompletableFuture.failedFuture(new RuntimeException("publish failed"));

        Assertions.assertFalse(job.pollPublish());
        Assertions.assertNull(job.publishVersionFuture);
    }

    @Test
    public void testLakeAlterPublishExecutorRejectsSaturatedSubmissions() {
        ThreadPoolExecutor executor = GlobalStateMgr.getCurrentState().getLakeAlterPublishExecutor();

        Assertions.assertInstanceOf(ThreadPoolExecutor.AbortPolicy.class, executor.getRejectedExecutionHandler());
    }
}
