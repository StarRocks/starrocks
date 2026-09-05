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

package com.starrocks.alter.reshard.presplit;

import com.starrocks.common.TimeoutException;

/**
 * Surfaced by {@link TabletPreSplitCoordinator#runPreSplit} when an admitted
 * reshard job does not reach the {@code FINISHED} state within the
 * post-submit wait window. Propagates through the load executor's DML
 * try/catch so the load transaction aborts cleanly — committing against
 * stale tablet metadata is not safe.
 */
public final class PreSplitPostSubmitTimeoutException extends TimeoutException {
    private static final long serialVersionUID = 1L;

    public PreSplitPostSubmitTimeoutException(String reason) {
        super(reason);
    }

    /**
     * Convert any {@link TimeoutException} surfacing from the post-submit phase into
     * the dedicated type so the load executor's catch (and the test fixtures) can
     * match on a single class. Already-typed timeouts pass through; bare timeouts are
     * wrapped with the original as {@link #getCause}.
     */
    public static PreSplitPostSubmitTimeoutException from(TimeoutException timeout) {
        if (timeout instanceof PreSplitPostSubmitTimeoutException typed) {
            return typed;
        }
        PreSplitPostSubmitTimeoutException wrapped = new PreSplitPostSubmitTimeoutException(timeout.getMessage());
        wrapped.initCause(timeout);
        return wrapped;
    }
}
