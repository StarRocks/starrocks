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

import com.starrocks.common.Pair;
import com.starrocks.common.util.concurrent.lock.DeadlockException;
import com.starrocks.common.util.concurrent.lock.IllegalLockStateException;
import com.starrocks.common.util.concurrent.lock.LockType;
import org.junit.Assert;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class LockTestUtils {
    public static void assertLockSuccess(Future<LockResult> lockTaskResultFuture) {
        try {
            LockResult lockResult = lockTaskResultFuture.get();
            Assert.assertSame(LockResult.LockTaskResultType.SUCCESS, lockResult.resultType);
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static void assertLockWait(Future<LockResult> lockTaskResultFuture) {
        try {
            LockResult lockResult = lockTaskResultFuture.get();
            Assert.assertSame(LockResult.LockTaskResultType.WAIT, lockResult.resultType);
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

        Assert.assertTrue(hasDeadLock);

        assertLockSuccess(testLockers.get(deadLockIdx).release(rids.get(deadLockIdx).first, rids.get(deadLockIdx).second));

        for (int i = 0; i < waitLockers.size(); ++i) {
            if (i == deadLockIdx) {
                continue;
            }
            Future<LockResult> waitLocker = waitLockers.get(i);
            assertLockSuccess(waitLocker);
        }

        return deadLockIdx;
    }

    public static void assertLockFail(Future<LockResult> lockTaskResultFuture, String msg) {
        try {
            LockResult lockResult = lockTaskResultFuture.get();
            Assert.assertSame(LockResult.LockTaskResultType.FAIL, lockResult.resultType);
            Assert.assertTrue(lockResult.exception instanceof IllegalLockStateException);
            Assert.assertTrue(lockResult.exception.getMessage().contains(msg));
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static void assertLockFail(Future<LockResult> lockTaskResultFuture, Class<? extends Exception> c) {
        try {
            LockResult lockResult = lockTaskResultFuture.get();
            Assert.assertSame(LockResult.LockTaskResultType.FAIL, lockResult.resultType);
            Assert.assertTrue(lockResult.exception.getClass().isAssignableFrom(c));
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
