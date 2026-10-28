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

package com.starrocks.common.util.concurrent.lock;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;

/**
 * The refusal {@link LockInvariantViolations.Mode#ERROR} raises: this operation broke a lock
 * invariant and was not allowed to proceed.
 *
 * <p>It exists as a type, rather than the bare {@code IllegalStateException} it used to be, because
 * a refusal has to survive the code it is raised inside. The FE is full of critical sections whose
 * {@code catch} rewrites or swallows whatever comes out -- a cache loader whose caller turns any
 * exception into "table not found", a read-through cache that wraps a {@code RuntimeException} into
 * an {@code IOException}, a cleanup path that only logs -- and a refusal turned into one of those is
 * worse than no check at all: the build goes green, or the query answers wrongly, and the lock
 * violation is gone. Those places rethrow this type before their own wrapping, which they can only
 * do if it is nameable.
 *
 * <p>It stays an {@link IllegalStateException} so that callers and tests written against the old
 * type keep working.
 *
 * @see BlockingCallValidator the guard that raises it for a remote call under a lock
 */
public class LockInvariantViolationException extends IllegalStateException {
    public LockInvariantViolationException(String message) {
        super(message);
    }

    /**
     * Whether this throwable, or anything it wraps, is a refusal. Cache and executor layers hand back
     * their own wrapper, so the cause chain is what has to be asked.
     */
    public static boolean isRefusal(Throwable throwable) {
        return refusalIn(throwable) != null;
    }

    /**
     * Rethrow the refusal inside {@code throwable}, if there is one, so a generic {@code catch} can let
     * it through before applying its own wrapping. Does nothing otherwise.
     */
    public static void rethrowIfRefusal(Throwable throwable) {
        LockInvariantViolationException refusal = refusalIn(throwable);
        if (refusal != null) {
            throw refusal;
        }
    }

    /**
     * The walk is bounded rather than trusting the chain to end. A cause chain can be a cycle -- Java
     * forbids {@code t.initCause(t)} but not {@code a -> b -> a}, and wrapper layers do build those -- and
     * a diagnostic helper that hangs on one would be worse than the hole it was added to close.
     */
    private static LockInvariantViolationException refusalIn(Throwable throwable) {
        Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        for (Throwable t = throwable; t != null && seen.add(t); t = t.getCause()) {
            if (t instanceof LockInvariantViolationException refusal) {
                return refusal;
            }
        }
        return null;
    }
}
