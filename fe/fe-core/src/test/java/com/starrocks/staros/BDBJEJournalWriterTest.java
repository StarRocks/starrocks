// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.staros;

import com.staros.exception.ExceptionCode;
import com.staros.exception.StarException;
import com.starrocks.persist.EditLog;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

public class BDBJEJournalWriterTest {
    @Mocked
    private EditLog editLog;

    @Test
    public void testBDBJEJournalWriterBasic() {
        new MockUp<EditLog>() {
            @Mock
            public void logStarMgrOperation(StarMgrJournal journal) {
            }
        };

        BDBJEJournalWriter journalWriter = new BDBJEJournalWriter(editLog);

        journalWriter.write(null);
    }

    @Test
    public void testBDBJEJournalWriterException() {
        new MockUp<EditLog>() {
            @Mock
            public void logStarMgrOperation(StarMgrJournal journal) {
                throw new StarException(ExceptionCode.JOURNAL, "hello, world.");
            }
        };

        BDBJEJournalWriter journalWriter = new BDBJEJournalWriter(editLog);

        StarException thrown = Assert.assertThrows(StarException.class, () -> {
            journalWriter.write(null);
        });
        Assert.assertEquals(thrown.getMessage(), "hello, world.");
    }
}
