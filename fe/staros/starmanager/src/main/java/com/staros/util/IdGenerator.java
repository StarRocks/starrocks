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


package com.staros.util;

import com.staros.exception.StarException;
import com.staros.journal.Journal;
import com.staros.journal.JournalSystem;
import com.staros.journal.StarMgrJournal;

public class IdGenerator {
    public static final int BATCH_ID_INTERVAL = 1000;

    private long nextId;
    private long nextPersistentId;
    private JournalSystem journalSystem;

    public IdGenerator(JournalSystem journalSystem) {
        this.nextId = Constant.DEFAULT_ID + 1;
        this.nextPersistentId = this.nextId;
        this.journalSystem = journalSystem;
    }

    public synchronized long getNextId() throws StarException {
        if (nextId >= nextPersistentId) {
            nextPersistentId = nextPersistentId + BATCH_ID_INTERVAL;
            if (journalSystem != null) {
                Journal journal = StarMgrJournal.logSetId(nextPersistentId);
                journalSystem.write(journal);
            }
        }
        return nextId++;
    }

    public synchronized void setNextId(long id) {
        if (id > nextPersistentId) {
            nextId = id;
            nextPersistentId = nextId;
        }
    }

    public synchronized long getNextPersistentId() {
        return nextPersistentId;
    }
}
