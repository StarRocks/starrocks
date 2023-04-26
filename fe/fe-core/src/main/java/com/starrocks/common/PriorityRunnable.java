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


package com.starrocks.common;

import java.util.concurrent.atomic.AtomicLong;

public abstract class PriorityRunnable
        implements Runnable, Comparable<PriorityRunnable> {

    private final long seqNum;
    private int priority = 0;

    static final AtomicLong SEQ = new AtomicLong(0);

    public PriorityRunnable(int priority) {
        seqNum = SEQ.getAndIncrement();
        this.priority = priority;
    }

    public int getPriority() {
        return priority;
    }

    public void setPriority(int priority) {
        this.priority = priority;
    }

    public Runnable getRun() {
        return this;
    }

    @Override
    public void run() {
    }

    @Override
    public int compareTo(PriorityRunnable other) {
        int res = 0;
        if (this.priority == other.priority) {
            if (other != this) {
                res = (seqNum < other.seqNum ? -1 : 1);
            }
        } else {
            res = this.priority > other.priority ? -1 : 1;
        }
        return res;
    }

    @Override
    public String toString() {
        return "PriorityRunnable{" + "priority=" + getPriority() + ", runnable=" + getRun() + '}';
    }
}