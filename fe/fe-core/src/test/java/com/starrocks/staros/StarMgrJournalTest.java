// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.staros;

import com.staros.journal.Journal;
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

    @Test
    public void testStarMgrJournal() {
        new MockUp<Journal>() {
            @Mock
            public void write(DataOutput out) throws IOException {
            }
        };

        StarMgrJournal starMgrJournal = new StarMgrJournal(journal);

        try {
            starMgrJournal.write(null);
        } catch (IOException e) {
        }

        Assert.assertEquals(journal, starMgrJournal.getJournal());
    }
}
