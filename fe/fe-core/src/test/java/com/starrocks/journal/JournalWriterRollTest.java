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

package com.starrocks.journal;

import com.starrocks.common.Config;
import com.starrocks.common.io.DataOutputBuffer;
import com.starrocks.common.io.Text;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class JournalWriterRollTest {
    @AfterEach
    public void cleanup() {
        Config.edit_log_roll_num = 50000;
        Config.edit_log_roll_bytes = 0;
    }

    @Test
    public void testRollByBytesWhenCountNotReached() throws Exception {
        Config.edit_log_roll_num = 50000;
        Config.edit_log_roll_bytes = 100;
        TestJournal journal = new TestJournal();
        BlockingQueue<JournalTask> queue = new ArrayBlockingQueue<>(16);
        JournalWriter writer = new JournalWriter(journal, queue);
        writer.init(3L);
        journal.clearRollJournalIds();

        // one task of estimated size 8 + 100 >= 100 triggers the size-based roll
        queue.put(new JournalTask(System.nanoTime(), makeBuffer(100), -1));
        writer.writeOneBatch();
        Assertions.assertEquals(List.of(5L), journal.getRollJournalIds());
        Assertions.assertEquals(0, writer.rollJournalCounter);
        Assertions.assertEquals(0, writer.rollJournalBytes);

        // counters were reset: a small follow-up batch must not roll again
        queue.put(new JournalTask(System.nanoTime(), makeBuffer(10), -1));
        writer.writeOneBatch();
        Assertions.assertEquals(List.of(5L), journal.getRollJournalIds());
    }

    @Test
    public void testRollBytesAccumulateAcrossBatches() throws Exception {
        Config.edit_log_roll_num = 50000;
        Config.edit_log_roll_bytes = 200;
        TestJournal journal = new TestJournal();
        BlockingQueue<JournalTask> queue = new ArrayBlockingQueue<>(16);
        JournalWriter writer = new JournalWriter(journal, queue);
        writer.init(3L);
        journal.clearRollJournalIds();

        // first batch: 8 + 100 = 108 < 200, no roll
        queue.put(new JournalTask(System.nanoTime(), makeBuffer(100), -1));
        writer.writeOneBatch();
        Assertions.assertEquals(List.of(), journal.getRollJournalIds());
        Assertions.assertEquals(108, writer.rollJournalBytes);

        // second batch: 216 >= 200, roll
        queue.put(new JournalTask(System.nanoTime(), makeBuffer(100), -1));
        writer.writeOneBatch();
        Assertions.assertEquals(List.of(6L), journal.getRollJournalIds());
        Assertions.assertEquals(0, writer.rollJournalBytes);
    }

    @Test
    public void testNoRollWhenSizeTriggerDisabled() throws Exception {
        Config.edit_log_roll_num = 50000;
        Config.edit_log_roll_bytes = 0;
        TestJournal journal = new TestJournal();
        BlockingQueue<JournalTask> queue = new ArrayBlockingQueue<>(16);
        JournalWriter writer = new JournalWriter(journal, queue);
        writer.init(3L);
        journal.clearRollJournalIds();

        queue.put(new JournalTask(System.nanoTime(), makeBuffer(1000), -1));
        writer.writeOneBatch();
        Assertions.assertEquals(List.of(), journal.getRollJournalIds());
        Assertions.assertEquals(1008, writer.rollJournalBytes);
    }

    @Test
    public void testRollByCountStillWorks() throws Exception {
        Config.edit_log_roll_num = 2;
        Config.edit_log_roll_bytes = 0;
        TestJournal journal = new TestJournal();
        BlockingQueue<JournalTask> queue = new ArrayBlockingQueue<>(16);
        JournalWriter writer = new JournalWriter(journal, queue);
        writer.init(3L);
        journal.clearRollJournalIds();

        queue.put(new JournalTask(System.nanoTime(), makeBuffer(10), -1));
        queue.put(new JournalTask(System.nanoTime(), makeBuffer(10), -1));
        writer.writeOneBatch();
        Assertions.assertEquals(List.of(6L), journal.getRollJournalIds());
        Assertions.assertEquals(0, writer.rollJournalCounter);
        Assertions.assertEquals(0, writer.rollJournalBytes);
    }

    private DataOutputBuffer makeBuffer(int size) throws IOException {
        DataOutputBuffer buffer = new DataOutputBuffer();
        Text.writeString(buffer, "x".repeat(size - 4));
        Assertions.assertEquals(size, buffer.getLength());
        return buffer;
    }

    private static final class TestJournal implements Journal {
        private long maxJournalId = 0L;
        private final List<Long> stagingJournalIds = new ArrayList<>();
        private final List<Long> rollJournalIds = new ArrayList<>();

        @Override
        public void open() {
        }

        @Override
        public void rollJournal(long journalId) {
            rollJournalIds.add(journalId);
        }

        @Override
        public long getMaxJournalId() {
            return maxJournalId;
        }

        @Override
        public void close() {
        }

        @Override
        public JournalCursor read(long fromKey, long toKey) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void deleteJournals(long deleteJournalToId) {
        }

        @Override
        public long getFinalizedJournalId() {
            return 0;
        }

        @Override
        public List<Long> getDatabaseNames() {
            return List.of();
        }

        @Override
        public void batchWriteBegin() {
            stagingJournalIds.clear();
        }

        @Override
        public void batchWriteAppend(long journalId, DataOutputBuffer buffer) {
            stagingJournalIds.add(journalId);
        }

        @Override
        public void batchWriteCommit() {
            maxJournalId += stagingJournalIds.size();
            stagingJournalIds.clear();
        }

        @Override
        public void batchWriteAbort() {
            stagingJournalIds.clear();
        }

        @Override
        public String getPrefix() {
            return "";
        }

        public void clearRollJournalIds() {
            rollJournalIds.clear();
        }

        public List<Long> getRollJournalIds() {
            return rollJournalIds;
        }
    }
}
