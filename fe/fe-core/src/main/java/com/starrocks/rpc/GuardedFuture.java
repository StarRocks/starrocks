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

package com.starrocks.rpc;

import com.starrocks.common.util.concurrent.lock.BlockingCallValidator;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A future that carries the blocking-call door on its wait.
 *
 * <h3>Why the door is here and not at the send</h3>
 *
 * An asynchronous send returns immediately; the thread waits in {@code get()}. So for every brpc
 * service the FE calls, the moment a lock would be held across a round trip is the {@code get()},
 * which is scattered across the callers -- a dozen sites for the lake service alone, each with its
 * own loop, timeout and error handling. Guarding each of them means finding each of them, and means
 * a new caller arrives with no door.
 *
 * <p>Wrapping the future instead moves the door to where the waits are, once: the decorator that
 * hands out the future attaches it, and every caller that waits on that future passes through it,
 * including callers written later. The wrapper is what the FE's own code sees as a
 * {@link Future}; nothing casts these futures to a concrete type, which is what makes the
 * substitution safe.
 *
 * <h3>A completed future is not a wait</h3>
 *
 * {@code get()} on a future that is already done returns without blocking, so reporting it would be
 * the same false positive as reporting a cache hit -- the thing the whole guard design exists to
 * avoid. The check is therefore below that branch: only a thread that is about to actually wait is
 * reported. This is deliberately a decision made at the instant of the call; a future that
 * completes a microsecond after the check still counts as a wait, because the caller committed to
 * one.
 *
 * @see BlockingCallValidator for where the other doors are mounted, and the rules they follow
 */
public class GuardedFuture<T> implements Future<T> {
    private final Future<T> delegate;
    private final String transport;

    private GuardedFuture(Future<T> delegate, String transport) {
        this.delegate = delegate;
        this.transport = transport;
    }

    /**
     * @param transport short tag for the system this future's response comes from, e.g.
     *                  {@code "lake-vacuum"}. It is what makes "lock-held time grouped by
     *                  transport" aggregatable, so it names the request rather than the class.
     */
    public static <T> Future<T> guard(Future<T> delegate, String transport) {
        return new GuardedFuture<>(delegate, transport);
    }

    @Override
    public T get() throws InterruptedException, ExecutionException {
        validateNotUnderLock();
        return delegate.get();
    }

    @Override
    public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        validateNotUnderLock();
        return delegate.get(timeout, unit);
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return delegate.cancel(mayInterruptIfRunning);
    }

    @Override
    public boolean isCancelled() {
        return delegate.isCancelled();
    }

    @Override
    public boolean isDone() {
        return delegate.isDone();
    }

    private void validateNotUnderLock() {
        if (delegate.isDone()) {
            return;
        }
        BlockingCallValidator.validateNotUnderLock(transport);
    }
}
