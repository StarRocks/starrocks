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
