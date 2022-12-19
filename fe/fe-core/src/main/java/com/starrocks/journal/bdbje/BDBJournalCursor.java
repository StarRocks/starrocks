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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/journal/bdbje/BDBJournalCursor.java

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

package com.starrocks.journal.bdbje;

import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.rep.InsufficientLogException;
import com.sleepycat.je.rep.RestartRequiredException;
import com.starrocks.journal.JournalCursor;
import com.starrocks.journal.JournalEntity;
import com.starrocks.journal.JournalException;
import com.starrocks.journal.JournalInconsistentException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.List;

public class BDBJournalCursor implements JournalCursor {
    private static final Logger LOG = LogManager.getLogger(BDBJournalCursor.class);
    private static final int RETRY_TIME = 3;
    private static final long SLEEP_INTERVAL_SEC = 3;

    private long toKey;
    private long nextKey;
    private BDBEnvironment environment;
    // names of all local databases, will set on initialization, and will update every time `prelong()` is called
    protected List<Long> localDBNames = null;
    // index of next db to be opened
    protected int nextDbPositionIndex = -1;
    // the database of current log
    protected CloseSafeDatabase database = null;
    private String prefix;

    /**
     * handle DatabaseException carefully
     *
     * 1. wrap as JournalException and return if it's a normal DatabaseException.
     * 2. RestartRequiredException is a fatal error since we're replaying as a follower, we must exit by raising an
     * JournalInconsistentException. Same with EnvironmentFailureException when replicated environment is invalid.
     * 3. If it is an InsufficientLogException, we should call refreshLog() to restore the data as much as possible.
     */
    protected JournalException wrapDatabaseException(DatabaseException originException, String errMsg)
            throws JournalInconsistentException {
        if (originException instanceof RestartRequiredException) {
            errMsg += "Got RestartRequiredException, will exit.";
            LOG.warn(errMsg, originException);
            if (originException instanceof InsufficientLogException) {
                // for InsufficientLogException we should refresh the log and
                // then exit the process because we may have read dirty data.
                environment.refreshLog((InsufficientLogException) originException);
            }
            JournalInconsistentException journalInconsistentException = new JournalInconsistentException(errMsg);
            journalInconsistentException.initCause(originException);
            throw journalInconsistentException;
        } else if (originException instanceof EnvironmentFailureException
                && ! environment.getReplicatedEnvironment().isValid()) {
            errMsg += "Got EnvironmentFailureException and the current ReplicatedEnvironment is invalid, will exit.";
            LOG.warn(errMsg, originException);
            JournalInconsistentException journalInconsistentException = new JournalInconsistentException(errMsg);
            journalInconsistentException.initCause(originException);
            throw journalInconsistentException;
        }
        LOG.warn(errMsg);
        JournalException exception = new JournalException(errMsg);
        exception.initCause(originException);
        return exception;
    }

    public static BDBJournalCursor getJournalCursor(BDBEnvironment env, long fromKey, long toKey)
            throws JournalException, JournalInconsistentException, InterruptedException {
        return getJournalCursor(env, "", fromKey, toKey);
    }

    /**
     * init journal cursor
     * if toKey = -1(CUROSR_END_KEY), it will automatically search the end.
     */
    public static BDBJournalCursor getJournalCursor(BDBEnvironment env, String prefix, long fromKey, long toKey)
            throws JournalException, JournalInconsistentException, InterruptedException {
        if (fromKey < 0  // fromKey must be a positive number
                || (toKey > 0 && toKey < fromKey)  // if toKey is a positive number, it must be smaller than fromKey
                || (toKey <= 0 && toKey != JournalCursor.CUROSR_END_KEY)  // if toKey is a negative number, it must be END
            ) {
            throw new JournalException(String.format("Invalid key range! fromKey %s toKey %s", fromKey, toKey));
        }
        BDBJournalCursor cursor = new BDBJournalCursor(env, prefix, fromKey, toKey);
        cursor.refresh();
        return cursor;
    }

    /**
     * calculate the index of next db to be opened
     *
     * there are two cases:
     * 1. if this is the first time, we're actually looking for the db of nextKey
     * 2. otherwise, we've already opened a db for previous key. Now we're looking for the next db of the previous key
     **/
    protected void calculateNextDbIndex() throws JournalException {
        if (!localDBNames.isEmpty() && nextKey < localDBNames.get(0)) {
            throw new JournalException(String.format(
                    "Can not find the key[%d] in %s: key too small", nextKey, localDBNames));
        }
        long objectKey = (database == null) ? nextKey : nextKey - 1;
        // find the db index which may contain objectKey
        int dbIndex = -1;
        for (long db : localDBNames) {
            if (objectKey >= db) {
                dbIndex++;
            } else {
                break;
            }
        }
        if (database != null) {
            dbIndex += 1;
        }
        nextDbPositionIndex = dbIndex;
        LOG.info("nextKey {}, currentDatabase {}, index of next opened db is {}",
                nextKey, database, nextDbPositionIndex);
    }

    protected BDBJournalCursor(BDBEnvironment env, String prefix, long fromKey, long toKey) {
        this.environment = env;
        this.prefix = prefix;
        this.nextKey = fromKey;
        this.toKey = toKey;
    }

    @Override
    public void refresh() throws JournalException, JournalInconsistentException, InterruptedException {
        // 1. refresh current db names
        List<Long> dbNames = null;
        JournalException exception = null;
        for (int i = 0; i < RETRY_TIME; ++ i) {
            if (i != 0) {
                Thread.sleep(SLEEP_INTERVAL_SEC * 1000L);
            }
            try {
                dbNames = environment.getDatabaseNamesWithPrefix(prefix);
                break;
            } catch (DatabaseException e) {
                String errMsg = String.format("failed to get DB names for %s times!", i + 1);
                exception = wrapDatabaseException(e, errMsg);
            }
        }
        if (dbNames == null) {
            if (exception != null) {
                throw exception;
            } else {
                throw new JournalException("failed to get db names!");
            }
        }

        // 2. no db changed ( roll new db / delete db after checkpoint )
        if (dbNames.equals(localDBNames)) {
            return;
        }

        // 3. update db index
        LOG.info("update dbnames {} -> {}", localDBNames, dbNames);
        localDBNames = dbNames;
        calculateNextDbIndex();
    }

    private boolean shouldOpenDatabase() {
        // the very first time
        if (database == null) {
            return true;
        }
        // if current db does not contain any more data, then we go to search the next db
        return nextDbPositionIndex < localDBNames.size() && nextKey == localDBNames.get(nextDbPositionIndex);
    }

    protected void openDatabaseIfNecessary()
            throws InterruptedException, JournalException, JournalInconsistentException {
        if (!shouldOpenDatabase()) {
            return;
        }
        // close previous db
        if (database != null) {
            database.close();
            database = null;
        }

        String dbName = prefix + Long.toString(localDBNames.get(nextDbPositionIndex));
        JournalException exception = null;
        for (int i = 0; i < RETRY_TIME; ++ i) {
            try {
                if (i != 0) {
                    Thread.sleep(SLEEP_INTERVAL_SEC * 1000L);
                }

                database = environment.openDatabase(dbName);
                LOG.info("open next database {}", database);
                nextDbPositionIndex++;
                return;
            } catch (DatabaseException e) {
                String errMsg = String.format("failed to open %s for %s times!", dbName, i + 1);
                exception = wrapDatabaseException(e, errMsg);
            }
        }

        // failed after retry
        if (exception != null) {
            throw exception;
        }
    }

    protected JournalEntity deserializeData(DatabaseEntry data) throws JournalException {
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(data.getData()));
        JournalEntity ret = new JournalEntity();
        try {
            ret.readFields(in);
        } catch (IOException e) {
            // bad data, will not retry
            String errMsg = String.format("fail to read journal entity key=%s, data=%s",
                    nextKey, data);
            LOG.error(errMsg, e);
            JournalException exception = new JournalException(errMsg);
            exception.initCause(e);
            throw exception;
        }
        return ret;
    }

    @Override
    public JournalEntity next() throws InterruptedException, JournalException, JournalInconsistentException {
        // EOF
        if (toKey > 0 && nextKey > toKey) {
            LOG.info("cursor reaches the end: next key {} > to key {}", nextKey, toKey);
            return null;
        }

        // if current db does not contain any more data, then we go to search the next db
        openDatabaseIfNecessary();

        // make the key
        Long key = nextKey;
        DatabaseEntry theKey = new DatabaseEntry();
        TupleBinding<Long> myBinding = TupleBinding.getPrimitiveBinding(Long.class);
        myBinding.objectToEntry(key, theKey);

        DatabaseEntry theData = new DatabaseEntry();
        JournalException exception = null;
        for (int i = 0; i < RETRY_TIME; i++) {
            // 1. sleep after retry
            if (i != 0) {
                Thread.sleep(SLEEP_INTERVAL_SEC * 1000L);
            }

            // 2. read from bdb & error handling
            try {
                OperationStatus operationStatus = database.get(null, theKey, theData, LockMode.READ_COMMITTED);

                if (operationStatus == OperationStatus.SUCCESS) {
                    // 3. serialized
                    JournalEntity entity = deserializeData(theData);
                    nextKey++;
                    return entity;
                } else if (operationStatus == OperationStatus.NOTFOUND) {
                    // read until there is no more log exists, return
                    if (toKey == JournalCursor.CUROSR_END_KEY) {
                        return null;
                    }
                    // In the case:
                    // On non-master FE, the replayer will first get the max journal id,
                    // then try to replay logs from current replayed id to the max journal id. But when
                    // master FE try to write a log to bdbje, but crashed before this log is committed,
                    // the non-master FE may still get this incomplete log's id as max journal id,
                    // and try to replay it. We will first get LockTimeoutException (because the transaction
                    // is hanging and waiting to be aborted after timeout). and after this log abort,
                    // we will get NOTFOUND.
                    // So we simply throw a exception and let the replayer get the max id again.
                    LOG.warn("canot find journal {} in db {}, maybe because master switched, will try again.",
                            key, database);
                    return null;
                } else {
                    // other error status, will record error message and retry
                    String errMsg = String.format("failed to read after retried %d times! key = %d, db = %s, status = %s",
                            i + 1, key, database, operationStatus);
                    LOG.warn(errMsg);
                    exception = new JournalException(errMsg);
                }
            } catch (DatabaseException e) {
                String errMsg = String.format("failed to read after retried %d times! key = %d, db = %s",
                        i + 1, key, database);
                exception = wrapDatabaseException(e, errMsg);
            }
        } // for i in retry

        // failed after retry
        throw exception;
    }

    @Override
    public void close() {
        if (database != null) {
            database.close();
        }
    }

    @Override
    public void skipNext() {
        LOG.error("!!! DANGER: CURSOR SKIP {} !!!", nextKey);
        nextKey++;
    }
}
