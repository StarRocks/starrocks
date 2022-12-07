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


package com.starrocks.staros;


import com.starrocks.journal.JournalCursor;
import com.starrocks.journal.JournalEntity;
import com.starrocks.journal.JournalException;
import com.starrocks.journal.JournalInconsistentException;
import com.starrocks.journal.JournalWriter;
import com.starrocks.journal.bdbje.BDBEnvironment;
import com.starrocks.journal.bdbje.BDBJEJournal;
import com.starrocks.journal.bdbje.BDBJournalCursor;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

public class BDBJEJournalSystemTest {
    @Mocked
    private BDBEnvironment environment;
    @Mocked
    private BDBJEJournal journal;
    @Mocked
    private BDBJournalCursor cursor;

    @Test
    public void testBDBJEJournalSystem() throws Exception {
        // test construction
        BDBJEJournalSystem journalSystem = new BDBJEJournalSystem(environment);

        BDBJEJournalSystem journalSystem2 = new BDBJEJournalSystem(journal);

        // test get and set
        long id = journalSystem.getReplayId();
        journalSystem.setReplayId(id);
        Assert.assertEquals(journalSystem.getReplayId(), id);

        journalSystem.getJournal();

        // test role switch
        new MockUp<BDBJEJournal>() {
            @Mock
            public JournalCursor read(long from, long to)
                    throws JournalException, JournalInconsistentException, InterruptedException {
                return cursor;
            }
            @Mock
            public void open() throws InterruptedException, JournalException {
            }
            @Mock
            public long getMaxJournalId() {
                return -1;
            }
        };
        new MockUp<BDBJournalCursor>() {
            @Mock
            public void refresh() throws InterruptedException, JournalException, JournalInconsistentException {
            }
            @Mock
            public JournalEntity next() throws InterruptedException, JournalException, JournalInconsistentException {
                return null;
            }
            @Mock
            public void close() {
            }
        };
        new MockUp<JournalWriter>() {
            @Mock
            public void init(long id) throws JournalException {
            }
            @Mock
            public void startDaemon() {
            }
        };
        journalSystem.onBecomeFollower();
        try { // wait for replay
            Thread.sleep(1000);
        } catch (InterruptedException e) {
        }
        journalSystem.onBecomeLeader();
    }
}
