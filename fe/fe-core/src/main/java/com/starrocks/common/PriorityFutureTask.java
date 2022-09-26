// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.common;

import java.util.concurrent.FutureTask;

public class PriorityFutureTask<V> extends FutureTask<V> implements
        Comparable<PriorityFutureTask<V>> {

    private PriorityRunnable run;

    public PriorityFutureTask(PriorityRunnable runnable, V result) {
        super(runnable, result);
        this.run = runnable;
    }

    public int getPriority() {
        return run.getPriority();
    }

    public void setPriority(int priority) {
        run.setPriority(priority);
    }

    public PriorityRunnable getRun() {
        return run;
    }

    @Override
    public int compareTo(PriorityFutureTask<V> other) {
        if (other == null || !(other instanceof PriorityFutureTask)) {
            return -1;
        }
        return this.run.compareTo(other.run);
    }

    @Override
    public String toString() {
        return "PriorityFutureTask{" + "priority=" + getPriority() + ", runnable=" + getRun() + '}';
    }
}