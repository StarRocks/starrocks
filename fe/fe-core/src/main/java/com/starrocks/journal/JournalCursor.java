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

// This class is like JDBC ResultSet.
public interface JournalCursor {

    // meaning read till the end
    public static long CUROSR_END_KEY = -1;

    // Return the next journal.
    // Return null when there is no more journals, or if need to retry from outside.
    // Raise a JournalException if there is an error in reading from the underlying storage.
    // Raise a JournalInconsistentException if read dirty data and need to exit
    public JournalEntity next() throws InterruptedException, JournalException, JournalInconsistentException;

    // refresh offer a way to update environment, such as update databases in current environment
    public void refresh() throws InterruptedException, JournalException, JournalInconsistentException;

    public void close();

    // skip current log
    public void skipNext();
}
