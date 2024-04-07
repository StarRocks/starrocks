// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.journal;

import com.starrocks.common.Config;
import com.starrocks.journal.bdbje.BDBEnvironment;
import com.starrocks.journal.bdbje.BDBJEJournal;

public class JournalFactory {
    public enum JournalType {
        BDB
    };

    public static Journal create(String nodeName) throws JournalException, InterruptedException {
        JournalType type = JournalType.valueOf(Config.edit_log_type.toUpperCase());
        switch (type) {
            case BDB: {
                BDBEnvironment environment = BDBEnvironment.initBDBEnvironment(nodeName);
                return new BDBJEJournal(environment);
            }

            default:
                throw new JournalException(String.format("unknown journal type %s", Config.edit_log_type));
        }
    }
}
