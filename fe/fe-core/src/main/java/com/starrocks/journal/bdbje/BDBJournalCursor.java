// This file is made available under Elastic License 2.0.
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
    private static final Logger LOG = LogManager.getLogger(JournalCursor.class);
    private static final int RETRY_TIME = 3;
    private static final long SLEEP_INTERVAL_SEC = 3;

    private long toKey;
    private long currentKey;
    private BDBEnvironment environment;
    private List<Long> dbNames;
    private CloseSafeDatabase database;
    private int nextDbPositionIndex;
    private String prefix;

    public static BDBJournalCursor getJournalCursor(BDBEnvironment env, long fromKey, long toKey) throws
            JournalException {
        return new BDBJournalCursor(env, "", fromKey, toKey);
    }

    public static BDBJournalCursor getJournalCursor(BDBEnvironment env, String prefix, long fromKey, long toKey) throws
            JournalException {
        if (toKey < fromKey || fromKey < 0) {
            throw new JournalException(String.format("Invalid key range! fromKey %s toKey %s", fromKey, toKey));
        }
        return new BDBJournalCursor(env, prefix, fromKey, toKey);
    }

    protected BDBJournalCursor(BDBEnvironment env, long fromKey, long toKey) throws JournalException {
        this(env, "", fromKey, toKey);
    }

    protected BDBJournalCursor(BDBEnvironment env, String prefix, long fromKey, long toKey) throws JournalException {
        this.environment = env;
        this.toKey = toKey;
        this.currentKey = fromKey;
        this.prefix = prefix;
        this.dbNames = env.getDatabaseNames(prefix);
        if (dbNames == null) {
            throw new JournalException("failed to get db names!");
        }
        this.nextDbPositionIndex = 0;

        // find the db which may contain the fromKey
        String dbName = null;
        for (long db : dbNames) {
            if (fromKey >= db) {
                dbName = Long.toString(db);
                nextDbPositionIndex++;
            } else {
                break;
            }
        }
        nextDbPositionIndex -= 1;

        if (dbName == null) {
            throw new JournalException(String.format("Can not find the key:%d, fail to get journal cursor!", fromKey));
        }
    }

    private boolean shouldOpenDatabase() {
        // the very first time
        if (database == null) {
            return true;
        }
        // if current db does not contain any more data, then we go to search the next db
        return nextDbPositionIndex < dbNames.size() && currentKey == dbNames.get(nextDbPositionIndex);
    }

    protected void openDatabaseIfNecessary() throws InterruptedException, JournalException, JournalInconsistentException {
        if (!shouldOpenDatabase()) {
            return;
        }

        String dbName = prefix + Long.toString(dbNames.get(nextDbPositionIndex));
        JournalException exception = null;
        for (int i = 0; i < RETRY_TIME; ++ i) {
            try {
                if (i != 0) {
                    Thread.sleep(SLEEP_INTERVAL_SEC * 1000);
                }

                database = environment.openDatabase(dbName);
                nextDbPositionIndex++;
                return;
            } catch (RestartRequiredException e) {
                String errMsg = String.format(
                        "failed to open database because of RestartRequiredException, will exit. db[%s]", database);
                LOG.warn(errMsg, e);
                if (e instanceof InsufficientLogException) {
                    // for InsufficientLogException we should refresh the log and
                    // then exit the process because we may have read dirty data.
                    environment.refreshLog((InsufficientLogException) e);
                }
                JournalInconsistentException journalInconsistentException = new JournalInconsistentException(errMsg);
                journalInconsistentException.initCause(e);
                throw journalInconsistentException;
            } catch (DatabaseException e) {
                String errMsg = String.format("failed to open %s for %s times!", dbName, i + 1);
                LOG.warn(errMsg);
                exception = new JournalException(errMsg);
                exception.initCause(e);
            }
        }

        // failed after retry
        throw exception;
    }

    protected JournalEntity deserializeData(DatabaseEntry data) throws JournalException {
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(data.getData()));
        JournalEntity ret = new JournalEntity();
        try {
            ret.readFields(in);
        } catch (IOException e) {
            // bad data, will not retry
            String errMsg = String.format("fail to read journal entity key=%s, data=%s",
                    currentKey, data);
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
        if (currentKey > toKey) {
            return null;
        }

        // if current db does not contain any more data, then we go to search the next db
        openDatabaseIfNecessary();

        // make the key
        Long key = currentKey;
        DatabaseEntry theKey = new DatabaseEntry();
        TupleBinding<Long> myBinding = TupleBinding.getPrimitiveBinding(Long.class);
        myBinding.objectToEntry(key, theKey);

        DatabaseEntry theData = new DatabaseEntry();
        JournalException exception = null;
        for (int i = 0; i < RETRY_TIME; i++) {
            // 1. sleep after retry
            if (i != 0) {
                Thread.sleep(SLEEP_INTERVAL_SEC * 1000);
            }

            // 2. read from bdb & error handling
            try {
                OperationStatus operationStatus = database.get(null, theKey, theData, LockMode.READ_COMMITTED);

                if (operationStatus == OperationStatus.SUCCESS) {
                    // 3. serialized
                    JournalEntity entity = deserializeData(theData);
                    currentKey++;
                    return entity;
                } else if (operationStatus == OperationStatus.NOTFOUND) {
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
            } catch (RestartRequiredException e) {
                String errMsg = String.format(
                        "failed to read next because of RestartRequiredException, will exit. db[%s], current key[%s]",
                        database, theKey);
                LOG.warn(errMsg, e);
                if (e instanceof InsufficientLogException) {
                    // for InsufficientLogException we should refresh the log and
                    // then exit the process because we may have read dirty data.
                    environment.refreshLog((InsufficientLogException) e);
                }
                JournalInconsistentException journalInconsistentException = new JournalInconsistentException(errMsg);
                journalInconsistentException.initCause(e);
                throw journalInconsistentException;
            } catch (DatabaseException e) {
                String errMsg = String.format("failed to read after retried %d times! key = %d, db = %s",
                        i + 1, key, database);
                LOG.error(errMsg, e);
                exception = new JournalException(errMsg);
                exception.initCause(e);
            }
        } // for i in retry

        // failed after retry
        throw exception;
    }

    @Override
    public void close() {
    }
}
