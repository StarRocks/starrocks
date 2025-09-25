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

package com.staros.journal;

import com.staros.exception.StarException;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

public class DummyJournalSystem implements JournalSystem {
    private final ConcurrentLinkedQueue<Journal> journalQueue = new ConcurrentLinkedQueue<>();
    private final ExecutorService executorService = Executors.newSingleThreadExecutor();

    public void write(Journal journal) throws StarException {
        journalQueue.offer(journal);
        processWriteInOrder();
    }

    private synchronized void processWriteInOrder() throws StarException {
        if (journalQueue.isEmpty()) {
            return;
        }
        while (!journalQueue.isEmpty()) {
            Journal journal = journalQueue.poll();
            if (journal == null) {
                break;
            }
            writeInternal(journal);
        }
    }

    protected void writeInternal(Journal journal) throws StarException {
        // do nothing
    }

    @Override
    public Future<Boolean> writeAsync(Journal journal) throws StarException {
        journalQueue.offer(journal);
        FutureTask<Boolean> task = new FutureTask<>(this::processWriteInOrder, true);
        executorService.submit(task);
        return task;
    }

    public void replayTo(long journalId) throws StarException {
        // do nothing
    }

    public void setReplayId(long replayId) {
        // do nothing
    }

    public long getReplayId() {
        return 0;
    }

    public void onBecomeLeader() {
        // do nothing
    }

    public void onBecomeFollower() {
        // do nothing
    }
}
