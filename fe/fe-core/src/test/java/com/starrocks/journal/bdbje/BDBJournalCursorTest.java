// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.journal.bdbje;

import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseNotFoundException;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.rep.InsufficientLogException;
import com.starrocks.common.io.DataOutputBuffer;
import com.starrocks.common.io.Text;
import com.starrocks.journal.JournalEntity;
import com.starrocks.journal.JournalException;
import com.starrocks.journal.JournalInconsistentException;
import com.starrocks.persist.OperationType;
import mockit.Delegate;
import mockit.Expectations;
import mockit.Mocked;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

public class BDBJournalCursorTest {
    private static final Logger LOG = LogManager.getLogger(BDBJournalCursorTest.class);

    private JournalEntity fakeJournalEntity;
    private byte[] fakeJournalEntityBytes;
    @Mocked
    private BDBEnvironment environment;
    @Mocked
    private CloseSafeDatabase database;

    @Before
    public void setup() throws Exception {
        // init fake journal entity
        fakeJournalEntity = new JournalEntity();
        short op = OperationType.OP_SAVE_NEXTID;
        Text text = new Text(Long.toString(123));
        fakeJournalEntity.setData(text);
        DataOutputBuffer buffer = new DataOutputBuffer();
        buffer.writeShort(op);
        text.write(buffer);
        fakeJournalEntityBytes = buffer.getData();
    }

    private DatabaseEntry makeKey(long key) {
        DatabaseEntry theKey = new DatabaseEntry();
        TupleBinding<Long> myBinding = TupleBinding.getPrimitiveBinding(Long.class);
        myBinding.objectToEntry((Long) key, theKey);
        return theKey;
    }

    @Test
    public void testNormal() throws Exception {
        // db = [10, 12]
        // from 11 ->14
        new Expectations(environment) {
            {
                environment.openDatabase("10");
                times = 1;
                result = database;

                environment.getDatabaseNamesWithPrefix("");
                minTimes = 0;
                result = Arrays.asList(Long.valueOf(10), Long.valueOf(12));
            }
        };
        new Expectations(database) {
            {
                database.get(null, makeKey(11), (DatabaseEntry) any, (LockMode) any);
                times = 1;
                result = new Delegate() {
                    public OperationStatus fakeGet(
                            final Transaction txn, final DatabaseEntry key, final DatabaseEntry data,
                            LockMode lockMode) {
                        data.setData(fakeJournalEntityBytes);
                        return OperationStatus.SUCCESS;
                    }
                };
            }
        };
        // 1. get 11
        BDBJournalCursor bdbJournalCursor = new BDBJournalCursor(environment, 11, 13);
        JournalEntity entity = bdbJournalCursor.next();
        Assert.assertEquals(entity.getOpCode(), fakeJournalEntity.getOpCode());
        Assert.assertEquals(entity.getData().toString(), fakeJournalEntity.getData().toString());

        // 2. get 12, will open new db
        new Expectations(environment) {
            {
                environment.openDatabase("12");
                times = 1;
                result = database;
            }
        };
        new Expectations(database) {
            {
                database.get(null, makeKey(12), (DatabaseEntry) any, (LockMode) any);
                times = 1;
                result = new Delegate() {
                    public OperationStatus fakeGet(
                            final Transaction txn, final DatabaseEntry key, final DatabaseEntry data,
                            LockMode lockMode) {
                        data.setData(fakeJournalEntityBytes);
                        return OperationStatus.SUCCESS;
                    }
                };
            }
        };
        entity = bdbJournalCursor.next();
        Assert.assertEquals(entity.getOpCode(), fakeJournalEntity.getOpCode());
        Assert.assertEquals(entity.getData().toString(), fakeJournalEntity.getData().toString());

        // 3. get 13
        new Expectations(database) {
            {
                database.get(null, makeKey(13), (DatabaseEntry) any, (LockMode) any);
                times = 1;
                result = new Delegate() {
                    public OperationStatus fakeGet(
                            final Transaction txn, final DatabaseEntry key, final DatabaseEntry data,
                            LockMode lockMode) {
                        data.setData(fakeJournalEntityBytes);
                        return OperationStatus.SUCCESS;
                    }
                };
            }
        };
        entity = bdbJournalCursor.next();
        Assert.assertEquals(entity.getOpCode(), fakeJournalEntity.getOpCode());
        Assert.assertEquals(entity.getData().toString(), fakeJournalEntity.getData().toString());

        // 4. get eof
        Assert.assertNull(bdbJournalCursor.next());
    }

    @Test
    public void testReadFailedRetriedSucceed() throws Exception {
        // db = [10, 12]
        // from 10->14
        new Expectations(environment) {
            {
                environment.openDatabase("10");
                times = 1;
                result = database;

                environment.getDatabaseNamesWithPrefix("");
                minTimes = 0;
                result = Arrays.asList(Long.valueOf(10), Long.valueOf(12));
            }
        };
        BDBJournalCursor bdbJournalCursor = new BDBJournalCursor(environment, 10, 14);

        // get 10
        new Expectations(database) {
            {
                database.get(null, makeKey(10), (DatabaseEntry) any, (LockMode) any);
                times = 2;
                result = new DatabaseNotFoundException("mock mock");
                result = new Delegate() {
                    public OperationStatus fakeGet(
                            final Transaction txn, final DatabaseEntry key, final DatabaseEntry data,
                            LockMode lockMode) {
                        data.setData(fakeJournalEntityBytes);
                        return OperationStatus.SUCCESS;
                    }
                };
            }
        };
        new Expectations() {
            {
                Thread.sleep(anyLong);
                minTimes = 1;
            }
        };
        JournalEntity entity = bdbJournalCursor.next();
        Assert.assertEquals(entity.getOpCode(), fakeJournalEntity.getOpCode());
        Assert.assertEquals(entity.getData().toString(), fakeJournalEntity.getData().toString());
    }

    @Test
    public void testNotFoundAfterFailover() throws Exception {
        // db = [10, 12]
        // from 12->13
        new Expectations(environment) {
            {
                environment.openDatabase("12");
                times = 1;
                result = database;

                environment.getDatabaseNamesWithPrefix("");
                minTimes = 0;
                result = Arrays.asList(Long.valueOf(10), Long.valueOf(12));
            }
        };
        // get 12
        new Expectations(database) {
            {
                database.get(null, makeKey(12), (DatabaseEntry) any, (LockMode) any);
                times = 1;
                result = OperationStatus.NOTFOUND;
            }
        };
        BDBJournalCursor bdbJournalCursor = new BDBJournalCursor(environment, 12, 13);
        Assert.assertNull(bdbJournalCursor.next());
    }

    @Test
    public void testOpenDBFailedAndRetry() throws Exception {
        // db = [10, 12]
        // from 12->13
        new Expectations(environment) {
            {
                environment.getDatabaseNamesWithPrefix("");
                minTimes = 0;
                result = Arrays.asList(Long.valueOf(10), Long.valueOf(12));

                environment.openDatabase("12");
                times = 2;
                result = new DatabaseNotFoundException("mock mock");
                result = database;

            }
        };
        BDBJournalCursor bdbJournalCursor = new BDBJournalCursor(environment, 12, 13);
        bdbJournalCursor.openDatabaseIfNecessary();
    }

    @Test(expected = JournalException.class)
    public void testOpenDBFailedEvenAfterRetry() throws Exception {
        // db = [10, 12]
        // from 12->13
        new Expectations(environment) {
            {
                environment.getDatabaseNamesWithPrefix("");
                minTimes = 0;
                result = Arrays.asList(Long.valueOf(10), Long.valueOf(12));

                environment.openDatabase("12");
                minTimes = 0;
                result = new DatabaseNotFoundException("mock mock");

            }
        };
        new Expectations() {
            {
                Thread.sleep(anyLong);
                minTimes = 0;
            }
        };
        BDBJournalCursor bdbJournalCursor = new BDBJournalCursor(environment, 12, 13);
        bdbJournalCursor.openDatabaseIfNecessary();
        Assert.fail();
    }

    @Test(expected = JournalInconsistentException.class)
    public void testOpenDBFailedOnInsufficientLogException() throws Exception {
        // db = [10, 12]
        // from 12->13
        new Expectations(environment) {
            {
                environment.getDatabaseNamesWithPrefix("");
                minTimes = 0;
                result = Arrays.asList(Long.valueOf(10), Long.valueOf(12));

                environment.openDatabase("12");
                times = 1;
                result = new InsufficientLogException("mock mock");

                environment.refreshLog((InsufficientLogException)any);
                times = 1;
            }
        };

        BDBJournalCursor bdbJournalCursor = new BDBJournalCursor(environment, 12, 13);
        bdbJournalCursor.openDatabaseIfNecessary();
        Assert.fail();
    }

    @Test(expected = JournalException.class)
    public void testBadData() throws Exception {
        // db = [10, 12]
        // from 11 ->13
        new Expectations(environment) {
            {
                environment.openDatabase("10");
                times = 1;
                result = database;

                environment.getDatabaseNamesWithPrefix("");
                minTimes = 0;
                result = Arrays.asList(Long.valueOf(10), Long.valueOf(12));
            }
        };
        new Expectations(database) {
            {
                database.get(null, makeKey(11), (DatabaseEntry) any, (LockMode) any);
                times = 1;
                result = new Delegate() {
                    public OperationStatus fakeGet(
                            final Transaction txn, final DatabaseEntry key, final DatabaseEntry data,
                            LockMode lockMode) {
                        data.setData(new String("lalala").getBytes());
                    return OperationStatus.SUCCESS;
                    }
                };
            }
        };
        BDBJournalCursor bdbJournalCursor = new BDBJournalCursor(environment, 11, 13);
        bdbJournalCursor.next();
        Assert.fail();
    }

    @Test(expected = JournalInconsistentException.class)
    public void testNextFailedOnInsufficientLogException() throws Exception {
        // db = [10, 12]
        // from 12->13
        new Expectations(environment) {
            {
                environment.getDatabaseNamesWithPrefix("");
                minTimes = 0;
                result = Arrays.asList(Long.valueOf(10), Long.valueOf(12));

                environment.openDatabase("10");
                times = 1;
                result = database;

                environment.refreshLog((InsufficientLogException)any);
                times = 1;
            }
        };

        new Expectations(database) {
            {
                database.get(null, makeKey(11), (DatabaseEntry) any, (LockMode) any);
                times = 1;
                result = new InsufficientLogException("mock mock");
            }
        };
        BDBJournalCursor bdbJournalCursor = new BDBJournalCursor(environment, 11, 13);
        bdbJournalCursor.next();
        Assert.fail();
    }


    @Test(expected = JournalException.class)
    public void testDatabaseNamesFails() throws Exception {
        new Expectations(environment) {
            {
                environment.getDatabaseNamesWithPrefix("");
                minTimes = 0;
                result = null;
            }
        };
        BDBJournalCursor.getJournalCursor(environment, 10, 10);
        Assert.fail();
    }

    @Test(expected = JournalException.class)
    public void testNegativeToKey() throws Exception {
        BDBJournalCursor.getJournalCursor(environment, 10, -1);
        Assert.fail();
    }

    @Test(expected = JournalException.class)
    public void testInvalidKeyRange() throws Exception {
        // db = [10, 12]
        // from 9,9
        new Expectations(environment) {
            {
                environment.getDatabaseNamesWithPrefix("");
                minTimes = 0;
                result = Arrays.asList(Long.valueOf(10), Long.valueOf(12));
            }
        };
        BDBJournalCursor.getJournalCursor(environment, 9, 9);
        Assert.fail();
    }
}
