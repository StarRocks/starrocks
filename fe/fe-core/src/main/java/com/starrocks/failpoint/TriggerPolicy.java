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
package com.starrocks.failpoint;

import com.google.common.annotations.VisibleForTesting;
import com.starrocks.thrift.TUpdateFailPointRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class TriggerPolicy {
    private static final Logger LOG = LogManager.getLogger(TriggerPolicy.class);

    private final TriggerMode mode;
    private double probability;
    private int times;

    // PAUSE only. A one-shot gate: countDown() is idempotent, releases every waiter, and a thread
    // arriving after the release returns immediately -- exactly the semantics a pause needs.
    private final CountDownLatch releaseLatch = new CountDownLatch(1);
    private final AtomicInteger pausedThreadCount = new AtomicInteger();
    // Snapshotted when the policy is armed, never re-read from Config at park time: an
    // ADMIN SET FRONTEND CONFIG between arming and parking must not desynchronize this frontend from
    // its peers or from the backends, which snapshot the same value into the trigger mode they store.
    private int pauseTimeoutSecond = 1;

    public static TriggerPolicy enablePolicy() {
        return new TriggerPolicy(TriggerMode.ENABLE);
    }

    /**
     * @param timeoutSecond the already-normalized pause timeout carried by the arming request; values
     *                      below 1 are clamped so a misconfigured value can never mean "wait forever".
     */
    public static TriggerPolicy pausePolicy(int timeoutSecond) {
        TriggerPolicy policy = new TriggerPolicy(TriggerMode.PAUSE);
        policy.pauseTimeoutSecond = normalizePauseTimeoutSecond(timeoutSecond);
        return policy;
    }

    /**
     * The single definition of the pause-timeout clamp. Applied once at the arming site so the value
     * that reaches a frontend policy, a follower frontend, and every backend is byte-identical.
     */
    public static int normalizePauseTimeoutSecond(int timeoutSecond) {
        return Math.max(1, timeoutSecond);
    }

    public static TriggerPolicy probabilityPolicy(double probability) {
        return new TriggerPolicy(TriggerMode.PROBABILITY_ENABLE, probability);
    }

    public static TriggerPolicy timesPolicy(int times) {
        return new TriggerPolicy(TriggerMode.ENABLE_N_TIMES, times);
    }

    public TriggerPolicy(TriggerMode mode) {
        this.mode = mode;
    }

    public TriggerPolicy(TriggerMode mode, double probability) {
        this.mode = mode;
        this.probability = probability;
    }

    public TriggerPolicy(TriggerMode mode, int times) {
        this.mode = mode;
        this.times = times;
    }

    @VisibleForTesting
    public TriggerMode getMode() {
        return mode;
    }

    /**
     * Threads parked in this policy right now. Test-only: FE deliberately exposes no pause counters
     * on any user-visible surface (no SQL, thrift, or log surface) -- see the design doc.
     */
    @VisibleForTesting
    public int getPausedThreadCount() {
        return pausedThreadCount.get();
    }

    public boolean shouldTrigger(String name) {
        if (mode == TriggerMode.ENABLE) {
            return true;
        }
        if (mode == TriggerMode.PROBABILITY_ENABLE) {
            return Math.random() < probability;
        }
        if (mode == TriggerMode.ENABLE_N_TIMES) {
            if (times > 0) {
                times--;
                return true;
            } else {
                return false;
            }
        }
        if (mode == TriggerMode.PAUSE) {
            return pauseUntilReleased(name);
        }
        return false;
    }

    /**
     * Block until {@link #release()} or the timeout, then ALWAYS return false: a released pause
     * continues normally and never injects the rule's action.
     */
    private boolean pauseUntilReleased(String name) {
        LOG.info("failpoint {} paused, waiting for ADMIN DISABLE FAILPOINT", name);
        pausedThreadCount.incrementAndGet();
        boolean timedOut = false;
        try {
            // await() is nanoTime-based, so a wall-clock adjustment cannot extend or truncate the
            // wait, and its boolean return already distinguishes released from timed out.
            timedOut = !releaseLatch.await(pauseTimeoutSecond, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn("failpoint {} pause interrupted, resuming", name);
            return false;
        } finally {
            pausedThreadCount.decrementAndGet();
        }
        if (timedOut) {
            // Disarm, do not merely resume: leaving the policy armed would park every NEWLY arriving
            // thread for another full timeout, so a failpoint on a hot path would keep the frontend
            // wedged indefinitely -- the opposite of the self-healing this timeout exists to give.
            // Conditional on this policy still being the installed one: a plain remove would delete a
            // replacement armed concurrently and discard the operator's new mode.
            if (FailPoint.removeTriggerPolicyIf(name, this)) {
                LOG.warn("failpoint {} pause timed out after {}s, disarming it and resuming",
                        name, pauseTimeoutSecond);
            } else {
                LOG.warn("failpoint {} pause timed out after {}s, resuming; its policy changed "
                        + "concurrently so the current state is left unchanged", name, pauseTimeoutSecond);
            }
        } else {
            LOG.info("failpoint {} pause released", name);
        }
        return false;
    }

    /**
     * Wake every thread parked in a PAUSE policy. Called when the policy is removed
     * (ADMIN DISABLE FAILPOINT) or replaced by a re-arm. Idempotent, and safe on a non-PAUSE policy.
     */
    public void release() {
        releaseLatch.countDown();
    }

    /**
     * Whether this request arms a policy rather than removing one. A pause deliberately carries
     * is_enable = false -- that is what makes an FE predating the pause field remove the policy
     * instead of arming an ENABLE it cannot honour -- so a receiver must not read is_enable alone.
     */
    public static boolean isArming(TUpdateFailPointRequest request) {
        return (request.isSetPause() && request.isPause()) || request.isIs_enable();
    }

    public static TriggerPolicy fromThrift(TUpdateFailPointRequest request) {
        if (request.isSetPause() && request.isPause()) {
            // The timeout is snapshotted by the arming frontend and carried on the request; falling
            // back to this node's own Config would reintroduce the desync the snapshot prevents.
            return TriggerPolicy.pausePolicy(request.getPause_timeout_second());
        } else if (request.isSetTimes()) {
            return TriggerPolicy.timesPolicy(request.getTimes());
        } else if (request.isSetProbability()) {
            return TriggerPolicy.probabilityPolicy(request.getProbability());
        } else {
            return TriggerPolicy.enablePolicy();
        }
    }
}
