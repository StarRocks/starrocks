// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.journal;

import com.starrocks.journal.bdbje.BDBJEJournal;

public class JournalFactory {

    public static Journal create(String name) {
        return new BDBJEJournal(name);
    }
}
