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


package com.starrocks.journal.bdbje;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * this class guarantee that when bdb database is closing, there will be neither read or write operations on that db
 */
public class CloseSafeDatabase {
    private final Database db;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    CloseSafeDatabase(Database db) {
        this.db = db;
    }

    public OperationStatus put(final Transaction txn, final DatabaseEntry key, final DatabaseEntry data) {
        lock.readLock().lock();
        try {
            return this.db.put(txn, key, data);
        } finally {
            lock.readLock().unlock();
        }
    }

    public OperationStatus putNoOverwrite(final Transaction txn, final DatabaseEntry key, final DatabaseEntry data) {
        lock.readLock().lock();
        try {
            return this.db.putNoOverwrite(txn, key, data);
        } finally {
            lock.readLock().unlock();
        }
    }

    public OperationStatus get(final Transaction txn, final DatabaseEntry key, final DatabaseEntry data,
                               LockMode lockMode) {
        lock.readLock().lock();
        try {
            return this.db.get(txn, key, data, lockMode);
        } finally {
            lock.readLock().unlock();
        }
    }

    public void close() {
        lock.writeLock().lock();
        try {
            this.db.close();
        } finally {
            lock.writeLock().unlock();
        }
    }

    public Database getDb() {
        return db;
    }

    public String getDatabaseName() {
        try {
            return db.getDatabaseName();
        } catch (Throwable t) {
            return "";
        }
    }

    @Override
    public String toString() {
        return "CloseSafeDatabase{" +
                "db=" + this.getDatabaseName() +
                '}';
    }
}
