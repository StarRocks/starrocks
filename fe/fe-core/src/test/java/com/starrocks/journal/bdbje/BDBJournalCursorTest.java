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

import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseNotFoundException;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.rep.InsufficientLogException;
import com.starrocks.common.io.DataOutputBuffer;
import com.starrocks.common.io.Text;
import com.starrocks.journal.JournalCursor;
import com.starrocks.journal.JournalEntity;
import com.starrocks.journal.JournalException;
import com.starrocks.journal.JournalInconsistentException;
import com.starrocks.persist.OperationType;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Delegate;
import mockit.Expectations;
import mockit.Mocked;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;

public class BDBJournalCursorTest {
    private static final Logger LOG = LogManager.getLogger(BDBJournalCursorTest.class);

    private JournalEntity fakeJournalEntity;
    private byte[] fakeJournalEntityBytes;
    private File tempDir = null;

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

    @After
    public void cleanup() throws Exception {
        if (tempDir != null) {
            FileUtils.deleteDirectory(tempDir);
        }
    }

    private BDBEnvironment initBDBEnv() throws Exception {
        tempDir = Files.createTempDirectory(Paths.get("."), "BDBJEJournalCursorTest").toFile();
        String selfNodeHostPort = "127.0.0.1:" + UtFrameUtils.findValidPort();
        BDBEnvironment environment = new BDBEnvironment(
                tempDir,
                "test",
                selfNodeHostPort,
                selfNodeHostPort,
                true);
        environment.setup();
        return environment;
    }

    private DatabaseEntry makeKey(long key) {
        DatabaseEntry theKey = new DatabaseEntry();
        TupleBinding<Long> myBinding = TupleBinding.getPrimitiveBinding(Long.class);
        myBinding.objectToEntry((Long) key, theKey);
        return theKey;
    }

    private DataOutputBuffer makeBuffer(long l) throws IOException {
        DataOutputBuffer buffer = new DataOutputBuffer(128);
        JournalEntity je = new JournalEntity();
        je.setData(new Text(Long.toString(l)));
        je.setOpCode(OperationType.OP_SAVE_NEXTID);
        je.write(buffer);
        return buffer;
    }

    @Test
    public void testNormalNoMock() throws Exception {
        BDBEnvironment environment = initBDBEnv();
        BDBJEJournal journal = new BDBJEJournal(environment);
        journal.open();

        //
        // >>> write 1->6
        //
        // db1: 1
        journal.batchWriteBegin();
        journal.batchWriteAppend(1, makeBuffer(1));
        journal.batchWriteCommit();

        // db2: 2
        journal.rollJournal(2);
        journal.batchWriteBegin();
        journal.batchWriteAppend(2, makeBuffer(2));
        journal.batchWriteCommit();

        // db3: 3-4
        journal.rollJournal(3);
        journal.batchWriteBegin();
        journal.batchWriteAppend(3, makeBuffer(3));
        journal.batchWriteAppend(4, makeBuffer(4));
        journal.batchWriteCommit();

        // db5: 5-6
        journal.rollJournal(5);
        journal.batchWriteBegin();
        journal.batchWriteAppend(5, makeBuffer(5));
        journal.batchWriteAppend(6, makeBuffer(6));
        journal.batchWriteCommit();

        for (int i = 1; i <= 5; ++ i) {
            // <<< read from i to 6
            LOG.info("pretend I'm reading from {} to 6", i);
            BDBJournalCursor bdbJournalCursor = BDBJournalCursor.getJournalCursor(environment, i, -1);
            for (int j = i; j <= 6; ++ j) {
                JournalEntity entity = bdbJournalCursor.next();
                Assert.assertEquals(OperationType.OP_SAVE_NEXTID, entity.getOpCode());
                Assert.assertEquals(String.valueOf(j), entity.getData().toString());
            }
            bdbJournalCursor.close();
        }

        //
        // <<< read 6->6
        //
        BDBJournalCursor bdbJournalCursor = BDBJournalCursor.getJournalCursor(environment, 6, -1);
        JournalEntity entity = bdbJournalCursor.next();
        Assert.assertEquals(OperationType.OP_SAVE_NEXTID, entity.getOpCode());
        Assert.assertEquals("6", entity.getData().toString());

        Assert.assertNull(bdbJournalCursor.next());

        //
        // >>> write 7
        //
        journal.batchWriteBegin();
        journal.batchWriteAppend(7, makeBuffer(7));
        journal.batchWriteCommit();

        //
        // <<< read 7
        //
        bdbJournalCursor.refresh();
        entity = bdbJournalCursor.next();
        Assert.assertEquals(OperationType.OP_SAVE_NEXTID, entity.getOpCode());
        Assert.assertEquals("7", entity.getData().toString());

        Assert.assertNull(bdbJournalCursor.next());

        //
        // >>> write 8-9
        //
        // db8: 8-9
        journal.rollJournal(8);
        journal.batchWriteBegin();
        journal.batchWriteAppend(8, makeBuffer(8));
        journal.batchWriteAppend(9, makeBuffer(9));
        journal.batchWriteCommit();

        // drop db1, db2, db3 now we only have 5, 8
        // [1, 2, 3, 5] -> [5, 8]
        journal.deleteJournals(5);

        // refresh will fail if try to read old value
        boolean expected = false;
        try {
            BDBJournalCursor.getJournalCursor(environment, 4, -1);
        } catch (JournalException e) {
            LOG.info("got an expected exception, ", e);
            expected = true;
        }
        Assert.assertTrue(expected);

        //
        // <<< read 8
        //
        bdbJournalCursor.refresh();

        entity = bdbJournalCursor.next();
        Assert.assertEquals(OperationType.OP_SAVE_NEXTID, entity.getOpCode());
        Assert.assertEquals("8", entity.getData().toString());

        //
        // >>> write 10
        //
        // db10: 10
        journal.rollJournal(10);
        journal.batchWriteBegin();
        journal.batchWriteAppend(10, makeBuffer(10));
        journal.batchWriteCommit();

        // drop db 5, now we only have db8, db10
        // [5, 8] -> [8, 10]
        journal.deleteJournals(8);

        //
        // <<< read 9-10
        //
        bdbJournalCursor.refresh();

        entity = bdbJournalCursor.next();
        Assert.assertEquals(OperationType.OP_SAVE_NEXTID, entity.getOpCode());
        Assert.assertEquals("9", entity.getData().toString());

        entity = bdbJournalCursor.next();
        Assert.assertEquals(OperationType.OP_SAVE_NEXTID, entity.getOpCode());
        Assert.assertEquals("10", entity.getData().toString());
        Assert.assertNull(bdbJournalCursor.next());

        journal.close();
        bdbJournalCursor.close();
    }

    @Test
    public void testReadFailedRetriedSucceed(@Mocked BDBEnvironment environment, @Mocked CloseSafeDatabase database)
            throws Exception {
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
        BDBJournalCursor bdbJournalCursor = BDBJournalCursor.getJournalCursor(environment, 10, 14);

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
    public void testNotFoundAfterFailover(@Mocked BDBEnvironment environment, @Mocked CloseSafeDatabase database)
            throws Exception {
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
        BDBJournalCursor bdbJournalCursor = BDBJournalCursor.getJournalCursor(environment, 12, 13);
        Assert.assertNull(bdbJournalCursor.next());
    }

    @Test
    public void testOpenDBFailedAndRetry(@Mocked BDBEnvironment environment, @Mocked CloseSafeDatabase database)
            throws Exception {
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
        BDBJournalCursor bdbJournalCursor = BDBJournalCursor.getJournalCursor(environment, 12, 13);
        bdbJournalCursor.openDatabaseIfNecessary();
        Assert.assertEquals(database, bdbJournalCursor.database);
    }

    @Test(expected = JournalException.class)
    public void testOpenDBFailedEvenAfterRetry(@Mocked BDBEnvironment environment) throws Exception {
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
        BDBJournalCursor bdbJournalCursor = BDBJournalCursor.getJournalCursor(environment, 12, 13);
        bdbJournalCursor.openDatabaseIfNecessary();
        Assert.fail();
    }

    @Test(expected = JournalInconsistentException.class)
    public void testOpenDBFailedOnInsufficientLogException(@Mocked BDBEnvironment environment) throws Exception {
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

                environment.refreshLog((InsufficientLogException) any);
                times = 1;
            }
        };

        BDBJournalCursor bdbJournalCursor = BDBJournalCursor.getJournalCursor(environment, 12, 13);
        bdbJournalCursor.openDatabaseIfNecessary();
        Assert.fail();
    }

    @Test(expected = JournalException.class)
    public void testBadData(@Mocked BDBEnvironment environment, @Mocked CloseSafeDatabase database) throws Exception {
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
        BDBJournalCursor bdbJournalCursor = BDBJournalCursor.getJournalCursor(environment, 11, 13);
        bdbJournalCursor.next();
        Assert.fail();
    }

    @Test(expected = JournalException.class)
    public void testDatabaseNamesFails(@Mocked BDBEnvironment environment) throws Exception {
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
    public void testInvalidKeyRange(@Mocked BDBEnvironment environment) throws Exception {
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

    @Ignore
    @Test
    public void testRefreshTime() throws Exception {
        BDBEnvironment environment = initBDBEnv();
        BDBJEJournal journal = new BDBJEJournal(environment);
        journal.open();
        for (int i = 1; i < 10; ++ i) {
            if (i % 2 == 1) {
                journal.batchWriteBegin();
            }
            journal.batchWriteAppend(i, makeBuffer(i));
            if (i % 2 == 0) {
                journal.batchWriteCommit();
                journal.rollJournal(i + 1);
            }
        }
        journal.batchWriteCommit();
        journal.close();
        Assert.assertEquals(Arrays.asList(1L, 3L, 5L, 7L, 9L), environment.getDatabaseNames());

        long cnt = 1000000;
        long start = System.currentTimeMillis();
        for (int i = 0; i < cnt; ++ i) {
            environment.getDatabaseNames();
        }
        long interval = System.currentTimeMillis() - start;

        // 2022-07-12 17:20:49 .. - call environment.getDatabaseNames() 1000000 times cost 6797 ms
        // 2022-07-12 18:56:41 .. - call environment.getDatabaseNames() 1000000 times cost 6892 ms
        // seems like an in-memory operation, only cost 6µs
        LOG.info("call environment.getDatabaseNames() {} times cost {} ms", cnt, interval);
    }

    @Test
    public void refreshFailedRetriedSucceed(@Mocked BDBEnvironment environment) throws Exception {
        new Expectations(environment) {
            {
                environment.getDatabaseNamesWithPrefix("");
                times = 2;
                result = new DatabaseNotFoundException("mock mock");
                result = Arrays.asList(3L, 23L, 45L);
            }
        };
        BDBJEJournal journal = new BDBJEJournal(environment);
        journal.read(10, 10);
    }

    @Test(expected = JournalException.class)
    public void refreshFailed(@Mocked BDBEnvironment environment) throws Exception {
        new Expectations(environment) {
            {
                environment.getDatabaseNamesWithPrefix("");
                result = new DatabaseNotFoundException("mock mock");
            }
        };
        BDBJEJournal journal = new BDBJEJournal(environment);
        JournalCursor cursor = journal.read(10, 10);
        cursor.refresh();
    }
}
