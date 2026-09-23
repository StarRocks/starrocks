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

import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.common.util.concurrent.lock.DeadlockException;
import com.starrocks.common.util.concurrent.lock.LockException;
import com.starrocks.common.util.concurrent.lock.LockType;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicReference;

public class LockTestUtils {
    private static final String DEFAULT_BLOCKING_CALL_VALIDATION_MODE = Config.lock_blocking_call_validation_mode;

    /**
     * Opt a test out of the blocking-call-under-lock check.
     * <p>
     * For a test that holds a metadata lock across a blocking call <em>on purpose</em> -- one that
     * reproduces such a site, or asserts what the FE does when the connector is slow. A test that
     * merely happens to trip the check is reporting a real defect and must not use this. Pair with
     * {@link #restoreBlockingCallValidation()} in teardown.
     */
    public static void disableBlockingCallValidation() {
        Config.lock_blocking_call_validation_mode = "off";
    }

    public static void restoreBlockingCallValidation() {
        Config.lock_blocking_call_validation_mode = DEFAULT_BLOCKING_CALL_VALIDATION_MODE;
    }

    /**
     * Fake {@code ThreadPoolExecutor.submit} so the task runs on a thread of its own and is joined,
     * instead of running inline on the caller's thread.
     * <p>
     * Tests fake the pool to make an asynchronous step deterministic, and the usual one-liner --
     * {@code CompletableFuture.completedFuture(task.call())} -- runs the task on the submitting
     * thread. When that thread holds a metadata lock, as an alter job does while it submits its
     * publish, the task inherits a critical section it never has in production, so everything it
     * does under it -- a BE RPC, say -- runs under a lock the running system does not hold there.
     * Joining right away keeps the determinism the fake was for.
     * <p>
     * A plain {@link Thread}, not an {@link java.util.concurrent.ExecutorService}: the fake is
     * global, so a nested executor's {@code submit} would land back in it.
     * <p>
     * Whatever the task throws is re-thrown on the caller's thread, {@link Error} included. An
     * assertion inside the task raises an Error, and a thread of its own is where an Error goes
     * unnoticed: it would kill that thread, leave {@code join} to return normally, and hand back a
     * successfully completed Future holding null -- a test that fails when the task runs inline
     * would go green instead.
     */
    public static void fakeSynchronousExecutorOffTheCallersThread() {
        new MockUp<ThreadPoolExecutor>() {
            @Mock
            public <T> Future<T> submit(Callable<T> task) throws Exception {
                AtomicReference<T> result = new AtomicReference<>();
                AtomicReference<Throwable> failure = new AtomicReference<>();
                Thread thread = new Thread(() -> {
                    try {
                        result.set(task.call());
                    } catch (Throwable t) {
                        failure.set(t);
                    }
                });
                thread.start();
                thread.join();
                Throwable failed = failure.get();
                if (failed instanceof Exception) {
                    throw (Exception) failed;
                } else if (failed instanceof Error) {
                    throw (Error) failed;
                } else if (failed != null) {
                    throw new IllegalStateException(failed);
                }
                return CompletableFuture.completedFuture(result.get());
            }
        };
    }

    public static void assertLockSuccess(Future<LockResult> lockTaskResultFuture) {
        try {
            LockResult lockResult = lockTaskResultFuture.get();
            Assertions.assertSame(LockResult.LockTaskResultType.SUCCESS, lockResult.resultType);
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static void assertLockWait(Future<LockResult> lockTaskResultFuture) {
        try {
            LockResult lockResult = lockTaskResultFuture.get();
            Assertions.assertSame(LockResult.LockTaskResultType.WAIT, lockResult.resultType);
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static int assertDeadLock(List<TestLocker> testLockers,
                                     List<Pair<Long, LockType>> rids,
                                     List<Future<LockResult>> waitLockers) {
        boolean hasDeadLock = false;

        int deadLockIdx = -1;
        for (int i = 0; i < waitLockers.size(); ++i) {
            Future<LockResult> waitLocker = waitLockers.get(i);
            LockResult lockResult = null;
            try {
                lockResult = waitLocker.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
            if (lockResult.exception instanceof DeadlockException) {
                hasDeadLock = true;
                deadLockIdx = i;
            }
        }

        Assertions.assertTrue(hasDeadLock);

        assertLockSuccess(testLockers.get(deadLockIdx).release(rids.get(deadLockIdx).first, rids.get(deadLockIdx).second));

        boolean hasSuccess = false;
        int retryTimes = 5;
        while (retryTimes-- > 0) {
            for (int i = 0; i < waitLockers.size(); ++i) {
                if (i == deadLockIdx) {
                    continue;
                }
                Future<LockResult> waitLocker = waitLockers.get(i);

                try {
                    LockResult lockResult = waitLocker.get();
                    if (LockResult.LockTaskResultType.SUCCESS.equals(lockResult.resultType)) {
                        hasSuccess = true;
                        break;
                    } else {
                        System.out.println("LockResult" + retryTimes + " : " + lockResult.resultType);
                    }

                } catch (ExecutionException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            if (hasSuccess) {
                break;
            }
        }

        Assertions.assertTrue(hasSuccess);

        return deadLockIdx;
    }

    public static void assertLockFail(Future<LockResult> lockTaskResultFuture, String msg) {
        try {
            LockResult lockResult = lockTaskResultFuture.get();
            Assertions.assertSame(LockResult.LockTaskResultType.FAIL, lockResult.resultType);
            Assertions.assertTrue(lockResult.exception instanceof LockException);
            Assertions.assertTrue(lockResult.exception.getMessage().contains(msg));
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static void assertLockFail(Future<LockResult> lockTaskResultFuture, Class<? extends Exception> c) {
        try {
            LockResult lockResult = lockTaskResultFuture.get();
            Assertions.assertSame(LockResult.LockTaskResultType.FAIL, lockResult.resultType);
            Assertions.assertTrue(lockResult.exception.getClass().isAssignableFrom(c));
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
