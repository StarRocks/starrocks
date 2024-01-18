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
package com.starrocks.common.lock;

import com.starrocks.common.util.concurrent.lock.LockType;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.starrocks.common.lock.LockResult.makeWaitStateLockResult;

public class LockTask implements Future<LockResult> {
    public LockState lockState;
    public Long rid;
    public LockType lockType;
    public long timeout;

    private final LockThread lockThread;
    protected CountDownLatch latch;
    LockResult lockResult;

    public LockTask(LockState lockState, LockThread lockThread) {
        this.lockState = lockState;
        this.lockThread = lockThread;
        this.latch = new CountDownLatch(1);
    }

    public void setResult(LockResult lockResult) {
        this.lockResult = lockResult;
        latch.countDown();
    }

    @Override
    public boolean isDone() {
        return latch.getCount() == 0;
    }

    @Override
    public LockResult get() throws InterruptedException, ExecutionException {
        while (true) {
            boolean result = latch.await(100, TimeUnit.MILLISECONDS);
            if (result) {
                return lockResult;
            } else {
                if (lockThread.getState() == Thread.State.WAITING || lockThread.getState() == Thread.State.TIMED_WAITING) {
                    return makeWaitStateLockResult();
                } else {
                    continue;
                }
            }
        }
    }

    @Override
    public LockResult get(long timeout, @NotNull TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {

        while (true) {
            boolean result = latch.await(timeout, unit);
            if (result) {
                return lockResult;
            } else {
                if (lockThread.getState() == Thread.State.WAITING || lockThread.getState() == Thread.State.TIMED_WAITING) {
                    return makeWaitStateLockResult();
                } else {
                    continue;
                }
            }
        }
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        // cannot cancel for now
        return false;
    }

    @Override
    public boolean isCancelled() {
        // cannot cancel for now
        return false;
    }

    enum LockState {
        LOCK,
        RELEASE
    }
}
