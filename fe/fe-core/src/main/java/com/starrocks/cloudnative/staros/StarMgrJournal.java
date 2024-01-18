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


package com.starrocks.cloudnative.staros;

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
