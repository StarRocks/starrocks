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

import com.starrocks.common.util.concurrent.lock.IllegalLockStateException;
import com.starrocks.common.util.concurrent.lock.LockTimeoutException;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;

import java.util.concurrent.BlockingQueue;

import static com.starrocks.common.lock.LockResult.makeIllegalLockStateException;
import static com.starrocks.common.lock.LockResult.makeSuccessLockResult;

public class LockThread extends Thread {
    private Locker locker;
    private final BlockingQueue<LockTask> lockTaskBlockingQueue;

    public LockThread(BlockingQueue<LockTask> queue) {
        this.lockTaskBlockingQueue = queue;
    }

    public void run() {
        locker = new Locker();
        while (true) {
            LockTask lockTask = null;
            try {
                lockTask = lockTaskBlockingQueue.take();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            LockResult lockResult = null;

            switch (lockTask.lockState) {
                case LOCK: {
                    Long rid = lockTask.rid;
                    LockType lockType = lockTask.lockType;
                    long timeout = lockTask.timeout;

                    try {
                        locker.lock(rid, lockType, timeout);
                        lockResult = makeSuccessLockResult();
                    } catch (IllegalLockStateException deadlockException) {
                        lockResult = makeIllegalLockStateException(deadlockException);
                    } catch (LockTimeoutException e) {
                        lockResult = makeIllegalLockStateException(e);
                    }
                    break;
                }
                case RELEASE: {
                    Long rid = lockTask.rid;
                    LockType lockType = lockTask.lockType;
                    try {
                        locker.release(rid, lockType);
                        lockResult = makeSuccessLockResult();
                    } catch (IllegalMonitorStateException illegalLockStateException) {
                        lockResult = makeIllegalLockStateException(illegalLockStateException);
                    }
                    break;
                }
            }

            lockTask.setResult(lockResult);
        }
    }

    public Locker getLocker() {
        return locker;
    }
}
