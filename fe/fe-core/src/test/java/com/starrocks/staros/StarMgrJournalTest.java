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

import com.staros.journal.Journal;
import com.starrocks.cloudnative.staros.StarMgrJournal;
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
