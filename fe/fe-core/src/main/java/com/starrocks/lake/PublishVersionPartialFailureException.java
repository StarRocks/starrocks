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

package com.starrocks.lake;

import com.starrocks.rpc.RpcException;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Thrown by {@link Utils#publishVersionBatch} when the compute nodes answered for some tablets of a
 * partition but not for all of them. Everything the published tablets produced (compaction scores,
 * tablet stats, txn-log owners) has already been written into the maps the caller passed in, so the
 * caller only has to resend {@link #getFailedTabletIds()}.
 * <p>
 * It is still an {@link RpcException}: callers that do not track partial progress keep treating
 * the attempt as a failure, exactly as before.
 */
public class PublishVersionPartialFailureException extends RpcException {
    private final Set<Long> failedTabletIds;
    private final boolean inProgress;

    public PublishVersionPartialFailureException(String host, Set<Long> failedTabletIds, boolean inProgress,
                                                 String message) {
        super(host, message);
        this.failedTabletIds = Collections.unmodifiableSet(new HashSet<>(failedTabletIds));
        this.inProgress = inProgress;
    }

    /** Tablets the compute nodes did not publish in this attempt. Never empty. */
    public Set<Long> getFailedTabletIds() {
        return failedTabletIds;
    }

    /**
     * True when every failed tablet was turned down with RESOURCE_BUSY or a deadline timeout. The node
     * is still applying an earlier request for that tablet (or the task did not fit in the deadline),
     * so the right reaction is to wait and ask again, not to report an error.
     */
    public boolean isInProgress() {
        return inProgress;
    }
}
