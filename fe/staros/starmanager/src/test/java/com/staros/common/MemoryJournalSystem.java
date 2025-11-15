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
import com.staros.journal.Journal;
import com.staros.journal.JournalSystem;
import com.staros.proto.OperationType;
import com.staros.util.LockCloseable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MemoryJournalSystem implements JournalSystem {

    private long replayId = -1;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final List<Journal> journals = new ArrayList<>();
    private final Map<OperationType, Integer> journalTypes = new HashMap<>();

    @Override
    public void write(Journal journal) throws StarException {
        try (LockCloseable ignored = new LockCloseable(lock.writeLock())) {
            journals.add(journal);
            journalTypes.merge(journal.getEntry().getHeader().getOperationType(), 1, Integer::sum);
        }
    }

    @Override
    public Future<Boolean> writeAsync(Journal journal) throws StarException {
        write(journal);
        return CompletableFuture.completedFuture(true);
    }

    public void clearJournals() {
        try (LockCloseable ignored = new LockCloseable(lock.writeLock())) {
            journals.clear();
        }
    }

    // NOTE: not thread safe
    public List<Journal> getJournals() {
        try (LockCloseable ignored = new LockCloseable(lock.readLock())) {
            return journals;
        }
    }

    public int getJournalTypeCount(OperationType type) {
        try (LockCloseable ignored = new LockCloseable(lock.readLock())) {
            return journalTypes.getOrDefault(type, 0);
        }
    }

    @Override
    public void replayTo(long journalId) throws StarException {
        // Nothing to do
    }

    @Override
    public void setReplayId(long replayId) {
        try (LockCloseable ignored = new LockCloseable(lock.writeLock())) {
            this.replayId = replayId;
        }
    }

    @Override
    public long getReplayId() {
        try (LockCloseable ignored = new LockCloseable(lock.readLock())) {
            return this.replayId;
        }
    }

    @Override
    public void onBecomeLeader() {
        // Nothing to do
    }

    @Override
    public void onBecomeFollower() {
        // Nothing to
    }
}
