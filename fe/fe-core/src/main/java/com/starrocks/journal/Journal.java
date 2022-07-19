// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.journal;

import com.starrocks.common.io.DataOutputBuffer;

import java.util.List;

public interface Journal {

    // Open the journal environment
    public void open() throws InterruptedException, JournalException;

    // Roll Edit file or database
    public void rollJournal(long journalId) throws JournalException;

    // Get the newest journal id 
    public long getMaxJournalId();

    // Close the environment
    public void close();

    // Get all the journals whose id: fromKey <= id <= toKey
    // toKey = -1 means toKey = Long.Max_Value
    public JournalCursor read(long fromKey, long toKey)
            throws JournalException, JournalInconsistentException, InterruptedException;

    // Delete journals whose max id is less than deleteToJournalId
    public void deleteJournals(long deleteJournalToId);

    // Current db's min journal id - 1
    public long getFinalizedJournalId();

    // Get all the dbs' name
    public List<Long> getDatabaseNames();

    // only support batch write
    // start batch write
    public void batchWriteBegin() throws InterruptedException, JournalException;

    // append buffer to current batch
    public void batchWriteAppend(long journalId, DataOutputBuffer buffer) throws InterruptedException, JournalException;

    // persist current batch
    public void batchWriteCommit() throws InterruptedException, JournalException;

    // abort current batch
    public void batchWriteAbort() throws InterruptedException, JournalException;

    public String getPrefix();
}
