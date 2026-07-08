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

package com.starrocks.context;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Leader-local long-poll coordination for channel pulls. Senders bump a generation counter per
 * channel key and wake waiters; pullers can block until the generation advances or the timeout
 * expires.
 */
class ChannelWaitRegistry {

    private final ConcurrentHashMap<String, ChannelWaitState> states = new ConcurrentHashMap<>();

    long currentGeneration(String channelKey) {
        return state(channelKey).generation.get();
    }

    void signal(String channelKey) {
        ChannelWaitState state = state(channelKey);
        synchronized (state.monitor) {
            state.generation.incrementAndGet();
            state.monitor.notifyAll();
        }
    }

    boolean awaitChange(String channelKey, long observedGeneration, long timeoutMs) throws InterruptedException {
        if (timeoutMs <= 0L) {
            return currentGeneration(channelKey) > observedGeneration;
        }
        ChannelWaitState state = state(channelKey);
        long remaining = timeoutMs;
        long deadline = System.currentTimeMillis() + timeoutMs;
        synchronized (state.monitor) {
            while (state.generation.get() <= observedGeneration && remaining > 0L) {
                state.monitor.wait(remaining);
                remaining = deadline - System.currentTimeMillis();
            }
            return state.generation.get() > observedGeneration;
        }
    }

    private ChannelWaitState state(String channelKey) {
        return states.computeIfAbsent(channelKey == null ? "" : channelKey, ignored -> new ChannelWaitState());
    }

    private static final class ChannelWaitState {
        private final AtomicLong generation = new AtomicLong();
        private final Object monitor = new Object();
    }
}
