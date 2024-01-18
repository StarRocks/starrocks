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


package com.starrocks.common.concurrent;

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