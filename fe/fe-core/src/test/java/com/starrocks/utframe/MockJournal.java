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


package com.starrocks.utframe;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.common.io.DataOutputBuffer;
import com.starrocks.ha.HAProtocol;
import com.starrocks.journal.Journal;
import com.starrocks.journal.JournalCursor;
import com.starrocks.journal.JournalEntity;
import com.starrocks.journal.JournalException;
import com.starrocks.persist.OperationType;
import org.apache.commons.collections.map.HashedMap;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class MockJournal implements Journal {
    private final AtomicLong nextJournalId = new AtomicLong(0);
    private final Map<Long, JournalEntity> values = Maps.newConcurrentMap();
    private Map<Long, JournalEntity> staggingEntityMap = new HashedMap();

    public MockJournal() {
    }

    @Override
    public void open() throws InterruptedException, JournalException {
    }

    @Override
    public void rollJournal(long journalId) {

    }

    @Override
    public long getMaxJournalId() {
        return nextJournalId.get();
    }

    @Override
    public void close() {
    }

    // only for MockedJournal
    protected JournalEntity read(long journalId) {
        return values.get(journalId);
    }

    @Override
    public JournalCursor read(long fromKey, long toKey) throws JournalException {
        return new MockJournalCursor(this, fromKey, toKey);
    }

    @Override
    public void deleteJournals(long deleteJournalToId) {
        values.remove(deleteJournalToId);
    }

    @Override
    public long getFinalizedJournalId() {
        return 0;
    }

    @Override
    public List<Long> getDatabaseNames() {
        return Lists.newArrayList(0L);
    }

    @Override
    public void batchWriteBegin() throws InterruptedException, JournalException {
        staggingEntityMap.clear();
    }

    @Override
    public void batchWriteAppend(long journalId, DataOutputBuffer buffer)
            throws InterruptedException, JournalException {
        JournalEntity je = new JournalEntity(OperationType.OP_INVALID, null);
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(buffer.getData()));
            je = new JournalEntity(in.readShort(), null);
        } catch (IOException e) {
            e.printStackTrace();
        }
        staggingEntityMap.put(journalId, je);
    }

    @Override
    public void batchWriteCommit() throws InterruptedException, JournalException {
        values.putAll(staggingEntityMap);
        nextJournalId.set(staggingEntityMap.size() + nextJournalId.get());
        staggingEntityMap.clear();
    }

    @Override
    public void batchWriteAbort() throws InterruptedException, JournalException {
        staggingEntityMap.clear();
    }

    @Override
    public String getPrefix() {
        return "";
    }

    private static class MockJournalCursor implements JournalCursor {
        private final MockJournal instance;
        private long start;
        private long end;

        public MockJournalCursor(MockJournal instance, long start, long end) {
            this.instance = instance;
            this.start = start;
            this.end = end;
        }

        @Override
        public void refresh() {}

        @Override
        public JournalEntity next() {
            if (end > 0 && start > end) {
                return null;
            }
            JournalEntity je = instance.read(start);
            start++;
            return je;
        }

        @Override
        public void close() {
        }

        @Override
        public void skipNext() {}
    }

    public static class MockProtocol implements HAProtocol {

        @Override
        public boolean fencing() {
            return true;
        }

        @Override
        public List<InetSocketAddress> getObserverNodes() {
            return Lists.newArrayList();
        }

        @Override
        public List<InetSocketAddress> getElectableNodes(boolean leaderIncluded) {
            return Lists.newArrayList();
        }

        @Override
        public InetSocketAddress getLeader() {
            return null;
        }

        @Override
        public String getLeaderNodeName() {
            return "";
        }

        @Override
        public boolean removeElectableNode(String nodeName) {
            return true;
        }

        @Override
        public long getLatestEpoch() {
            return 0;
        }

        @Override
        public void removeUnstableNode(String nodeName, int currentFollowerCnt) {

        }
    }
}
