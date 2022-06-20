// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.journal;

import com.starrocks.journal.bdbje.BDBEnvironment;
import com.starrocks.journal.bdbje.BDBJEJournal;

public class JournalFactory {
    public enum JournalType {
        BDB_JOURNAL
    };

    // TODO: Make it configurable if we support multiple types of journal in the future
    protected static JournalType defaultJournalType = JournalType.BDB_JOURNAL;

    public static Journal create(String nodeName) throws JournalException, InterruptedException {
        switch (defaultJournalType) {
            case BDB_JOURNAL: {
                BDBEnvironment environment = BDBEnvironment.initBDBEnvironment(nodeName);
                Journal journal = new BDBJEJournal(environment);
                return journal;
            }

            default:
                throw new JournalException(String.format("unknown journal type %s", defaultJournalType));
        }
    }
}
