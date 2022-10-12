// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.staros;

import com.staros.journal.Journal;
import com.starrocks.common.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// wrapper for star manager journal to help write to bdbje
public class StarMgrJournal implements Writable {
    private Journal journal;

    public StarMgrJournal(Journal journal) {
        this.journal = journal;
    }

    public Journal getJournal() {
        return journal;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        journal.write(out);
    }

    public static StarMgrJournal read(DataInput in) throws IOException {
        Journal journal = Journal.read(in);
        return new StarMgrJournal(journal);
    }
}
