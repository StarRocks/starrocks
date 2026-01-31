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


package com.staros.schedule;

import com.staros.exception.StarException;
import com.staros.util.LogRateLimiter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.concurrent.CountDownLatch;

class ScheduleRequestContext implements Comparable<ScheduleRequestContext> {
    private static final Logger LOG = LogManager.getLogger(ScheduleRequestContext.class);
    private static final LogRateLimiter LOG_LIMITER = new LogRateLimiter(LOG, 1);

    // requests fields
    private final String serviceId;
    private final long shardId;
    private final long workerGroupId;
    private final CountDownLatch latch;
    private String desc;

    // Context part, including intermittent info
    private Collection<Long> workerIds = null;
    private Collection<Long> exclusiveGroupIds = null;
    private StarException exception = null;
    private Runnable runnable = null;
    private boolean generateTempReplica = false;

    @Override
    public int compareTo(ScheduleRequestContext rhs) {
        if (rhs == this) {
            return 0;
        }
        if (rhs.shardId != shardId) {
            return Long.compare(shardId, rhs.shardId);
        }
        if (rhs.workerGroupId != workerGroupId) {
            return Long.compare(workerGroupId, rhs.workerGroupId);
        }
        return serviceId.compareTo(rhs.serviceId);
    }

    public ScheduleRequestContext(String serviceId, long shardId, long wgId, CountDownLatch latch) {
        this.serviceId = serviceId;
        this.shardId = shardId;
        this.workerGroupId = wgId;
        this.latch = latch;
    }

    public String getServiceId() {
        return serviceId;
    }

    public long getShardId() {
        return shardId;
    }

    public long getWorkerGroupId() {
        return workerGroupId;
    }

    public void setException(StarException e) {
        if (this.exception == null && e != null) {
            exception = e;
        }
    }

    public StarException getException() {
        return exception;
    }

    public void setWorkerIds(Collection<Long> workerIds) {
        this.workerIds = workerIds;
    }

    public Collection<Long> getWorkerIds() {
        return workerIds;
    }

    public void setExclusiveGroupIds(Collection<Long> groupIds) {
        this.exclusiveGroupIds = groupIds;
    }

    public Collection<Long> getExclusiveGroupIds() {
        return exclusiveGroupIds;
    }

    public void setGenerateTempReplica(boolean isTemp) {
        generateTempReplica = isTemp;
    }

    public boolean isGenerateTempReplica() {
        return generateTempReplica;
    }

    public void reset() {
        this.exception = null;
        this.workerIds = null;
        this.exclusiveGroupIds = null;
        this.runnable = null;
        this.generateTempReplica = false;
    }

    public void setRunnable(Runnable runnable) {
        this.runnable = runnable;
    }

    public Runnable getRunnable() {
        return this.runnable;
    }

    public boolean isWaited() {
        return latch != null;
    }

    public void done() {
        this.done(null);
    }

    public void done(StarException exception) {
        setException(exception);
        if (latch != null) {
            latch.countDown();
        }
        if (this.exception != null && !isWaited()) {
            // done with error but no one waited, log a message
            LOG_LIMITER.info("{} done with exception: {}", this, exception.getMessage());
        }
    }

    // Wait for the latch notification, which means the schedule request is done
    public void await() throws InterruptedException {
        if (latch != null) {
            latch.await();
        }
    }

    public String toString() {
        if (desc == null) {
            desc = String.format(
                    "ScheduleRequestContext [ ServiceId: %s, ShardId: %d, WorkerGroupId: %d, WaitForResult: %b ]",
                    serviceId, shardId, workerGroupId, latch != null);
        }
        return desc;
    }

    @Override
    public int hashCode() {
        return (int) (shardId ^ workerGroupId);
    }

    // Only shardId and workerGroupId matters for the equality comparison,
    // which means the same request will be unique in a sorted set
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (this.getClass() != obj.getClass()) {
            return false;
        }

        ScheduleRequestContext rhs = (ScheduleRequestContext) obj;
        return this.shardId == rhs.shardId
                && this.workerGroupId == rhs.workerGroupId
                && this.serviceId.equals(rhs.serviceId);
    }
}
