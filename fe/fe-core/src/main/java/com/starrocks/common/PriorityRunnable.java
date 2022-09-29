// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

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