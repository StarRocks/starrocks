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
import com.starrocks.common.Config;
import com.starrocks.thrift.TUpdateFailPointRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class TriggerPolicy {
    private static final Logger LOG = LogManager.getLogger(TriggerPolicy.class);

    private final TriggerMode mode;
    private double probability;
    private int times;

    // PAUSE only. Threads park on this monitor; release() wakes them.
    private final Object pauseLock = new Object();
    private boolean released = false;
    private int pausedThreadCount = 0;

    public static TriggerPolicy enablePolicy() {
        return new TriggerPolicy(TriggerMode.ENABLE);
    }

    public static TriggerPolicy pausePolicy() {
        return new TriggerPolicy(TriggerMode.PAUSE);
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

    public TriggerMode getMode() {
        return mode;
    }

    /**
     * Threads parked in this policy right now. Test-only: FE deliberately exposes no pause counters
     * on any user-visible surface (no SQL, thrift, or log surface) -- see the design doc.
     */
    @VisibleForTesting
    public int getPausedThreadCount() {
        synchronized (pauseLock) {
            return pausedThreadCount;
        }
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
        // Normalize once: a misconfigured 0 or negative must mean "clamp to 1s", never "do not wait"
        // and never "wait forever". The same normalization is applied when the value is sent to BEs.
        int timeoutSecond = Math.max(1, Config.failpoint_pause_timeout_second);
        // nanoTime, not currentTimeMillis: a wall-clock adjustment must not extend or truncate the
        // wait.
        long deadlineNanos = System.nanoTime() + timeoutSecond * 1_000_000_000L;
        LOG.info("failpoint {} paused, waiting for ADMIN DISABLE FAILPOINT", name);
        boolean timedOut = false;
        try {
            synchronized (pauseLock) {
                pausedThreadCount++;
                try {
                    while (!released) {
                        long remainNanos = deadlineNanos - System.nanoTime();
                        if (remainNanos <= 0) {
                            timedOut = true;
                            break;
                        }
                        pauseLock.wait(remainNanos / 1_000_000L, (int) (remainNanos % 1_000_000L));
                    }
                } finally {
                    pausedThreadCount--;
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn("failpoint {} pause interrupted, resuming", name);
            return false;
        }
        if (timedOut) {
            LOG.warn("failpoint {} pause timed out after {}s, resuming", name, timeoutSecond);
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
        synchronized (pauseLock) {
            released = true;
            pauseLock.notifyAll();
        }
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
            return TriggerPolicy.pausePolicy();
        } else if (request.isSetTimes()) {
            return TriggerPolicy.timesPolicy(request.getTimes());
        } else if (request.isSetProbability()) {
            return TriggerPolicy.probabilityPolicy(request.getProbability());
        } else {
            return TriggerPolicy.enablePolicy();
        }
    }
}
