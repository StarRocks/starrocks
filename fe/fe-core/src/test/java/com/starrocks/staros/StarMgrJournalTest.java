// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.staros;

import com.staros.journal.Journal;
import com.starrocks.persist.EditLog;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataOutput;
import java.io.IOException;

public class StarMgrJournalTest {
    @Mocked
    private Journal journal;
    @Mocked
    private EditLog editLog;

    @Test
    public void testStarMgrJournal() {
        new MockUp<Journal>() {
            @Mock
            public void write(DataOutput out) throws IOException {
            }
        };
        new MockUp<EditLog>() {
            @Mock
            public void logStarMgrOperation(StarMgrJournal journal) {
            }
        };

        StarMgrJournal starMgrJournal = new StarMgrJournal(journal);

        try {
            starMgrJournal.write(null);
        } catch (IOException e) {
        }

        Assert.assertEquals(journal, starMgrJournal.getJournal());

        editLog.logStarMgrOperation(starMgrJournal);
    }
}
