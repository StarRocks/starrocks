// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.staros;

import com.staros.exception.ExceptionCode;
import com.staros.exception.StarException;
import com.staros.journal.Journal;
import com.staros.journal.JournalWriter;
import com.starrocks.persist.EditLog;

// wrapper for star manager to use bdbje
public class BDBJEJournalWriter implements JournalWriter {
    private EditLog editLog;

    public BDBJEJournalWriter(EditLog editLog) {
        this.editLog = editLog;
    }

    public void write(Journal journal) throws StarException {
        try {
            editLog.logStarMgrOperation(new StarMgrJournal(journal));
        } catch (Exception e) {
            throw new StarException(ExceptionCode.JOURNAL, e.getMessage());
        }
    }
}

