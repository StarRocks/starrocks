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

package com.staros.common;

import com.staros.exception.StarException;
import com.staros.journal.DummyJournalSystem;
import com.staros.journal.Journal;

// add a write delay to simulate the real world IO latency
public class DelayedDummyJournalSystem extends DummyJournalSystem {

    protected int delayMilliseconds = 0;
    protected int delayNanoseconds = 0;

    public void setWriteDelayMicroSeconds(int delayMicroseconds) {
        this.delayMilliseconds = delayMicroseconds / 1000;
        this.delayNanoseconds = delayMicroseconds % 1000 * 1000;
    }

    @Override
    protected void writeInternal(Journal journal) throws StarException {
        if (delayMilliseconds > 0) {
            try {
                Thread.sleep(delayMilliseconds);
            } catch (InterruptedException e) {
                // do nothing
            }
        }
        if (delayNanoseconds > 0) {
            busyWaitNanoSeconds(delayNanoseconds);
        }
    }

    public static void busyWaitNanoSeconds(long nanoSeconds) {
        long waitUntil = System.nanoTime() + nanoSeconds;
        while (waitUntil > System.nanoTime()) {
            ;
        }
    }
}
