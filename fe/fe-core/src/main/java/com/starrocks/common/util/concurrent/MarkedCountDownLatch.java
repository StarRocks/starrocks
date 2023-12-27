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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/common/MarkedCountDownLatch.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.common.util.concurrent;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.starrocks.common.Status;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public class MarkedCountDownLatch<K, V> extends CountDownLatch {
    private static final Logger LOG = LogManager.getLogger(MarkedCountDownLatch.class);

    private final Multimap<K, V> marks;
    private final List<Runnable> listeners = Lists.newArrayList();
    private Status st = Status.OK;

    public MarkedCountDownLatch(int count) {
        super(count);
        marks = HashMultimap.create();
    }

    public synchronized void addMark(K key, V value) {
        marks.put(key, value);
    }

    public synchronized void addListener(Runnable listener) {
        listeners.add(new OneShotListener(listener));
        triggerListeners();
    }

    public synchronized boolean markedCountDown(K key, V value) {
        if (marks.remove(key, value)) {
            super.countDown();
            triggerListeners();
            return true;
        }
        return false;
    }

    public synchronized boolean markedCountDown(K key, V value, Status status) {
        if (st.ok()) {
            st = status;
        }
        return markedCountDown(key, value);
    }

    public synchronized List<Entry<K, V>> getLeftMarks() {
        return Lists.newArrayList(marks.entries());
    }

    public Status getStatus() {
        return st;
    }

    public synchronized void countDownToZero(Status status) {
        // update status first before countDown.
        // so that the waiting thread will get the correct status.
        if (st.ok()) {
            st = status;
        }
        while (getCount() > 0) {
            super.countDown();
        }
        triggerListeners();
    }

    private synchronized void triggerListeners() {
        if (getCount() > 0) {
            return;
        }
        for (Runnable listener : listeners) {
            try {
                listener.run();
            } catch (Throwable e) {
                LOG.warn("Listener invoke failed", e);
            }
        }
    }

    private static final class OneShotListener implements Runnable {
        private final AtomicBoolean hasRun = new AtomicBoolean(false);
        private final Runnable runnable;

        public OneShotListener(Runnable runnable) {
            this.runnable = runnable;
        }

        @Override
        public void run() {
            if (hasRun.compareAndSet(false, true)) {
                runnable.run();
            }
        }
    }
}
