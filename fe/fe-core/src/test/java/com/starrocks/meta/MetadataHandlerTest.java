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
package com.starrocks.meta;

import com.google.common.base.Joiner;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.dbi.DupKeyData;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.starrocks.meta.kv.ByteCoder;
import com.starrocks.meta.store.bdb.BDBDatabase;
import com.starrocks.meta.store.bdb.BDBTransaction;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Random;

public class MetadataHandlerTest {
    private ReplicatedEnvironment environment;
    private Database database;
    private Gson gson;
    private MetadataHandler metadataHandler;

    @Before
    public void setUp() {
        File file = new File("./bdb");
        file.deleteOnExit();
        file.mkdir();

        environment = BDBEnvironment.setupEnvironment();
        database = BDBDatabase.openDatabase(environment, "db");
        gson = new GsonBuilder().create();
        metadataHandler = new MetadataHandler(database);

        long times = 10;
        Transaction txn = BDBTransaction.startTransaction(environment);
        for (int i = 0; i < times; ++i) {
            for (int j = 0; j < times; ++j) {
                byte[] key = ByteCoder.encode(i, j);
                String value = "v" + i + j + " : abcd123456789sdfs3456789abcdabcerw3456789abcdabcd123456789abcdabcd123456789abcd";

                TestJson testJson = new TestJson(i + "/" + j, value);
                metadataHandler.put(txn, key, testJson, TestJson.class);
            }
        }

        txn.commit();
    }

    public static class TestJson {
        private String v;
        private String k;

        public TestJson(String v, String k) {
            this.v = v;
            this.k = k;
        }
    }

    @Test
    public void test() {
        Random r = new Random(System.currentTimeMillis());
        for (int i = 0; i < 10; ++i) {
            int l1 = r.nextInt(10);
            for (int j = 0; j < 10; ++j) {
                int l2 = r.nextInt(10);

                String value = metadataHandler.get(null,
                        ByteCoder.encode(String.valueOf(l1), String.valueOf(l2)), String.class);
                System.out.println(value);
            }
        }
    }

    @Test
    public void testPrefix() {
        List<List<Object>> values = metadataHandler.getPrefixNoReturnValue(null, ByteCoder.encode(5, -1));
        for (List<Object> value : values) {
            System.out.println(Joiner.on("/").join(value));
        }
    }

    @Test
    public void testPrefix2() {
        List<List<Object>> values = metadataHandler.getPrefixNoReturnValue(null, ByteCoder.encode("a", "b"));
        for (List<Object> value : values) {
            System.out.println(Joiner.on("/").join(value));
        }
    }


    @Test
    public void test2() {
        Transaction transaction = null;
        byte[] prefix = ByteCoder.encode(String.valueOf(11));

        CursorConfig cursorConfig = CursorConfig.READ_COMMITTED;
        Cursor cursor = database.openCursor(transaction, cursorConfig);

        TupleBinding<String> binding = TupleBinding.getPrimitiveBinding(String.class);
        DatabaseEntry prefixStart = new DatabaseEntry();
        binding.objectToEntry(String.valueOf(5), prefixStart);

        DatabaseEntry prefixEnd = new DatabaseEntry(DupKeyData.makePrefixKey(
                prefixStart.getData(), prefixStart.getOffset(), prefixStart.getSize()));

        DatabaseEntry noReturnData = new DatabaseEntry();
        noReturnData.setPartial(0, 0, true);
        DatabaseEntry key = new DatabaseEntry(prefix);
        OperationStatus status = cursor.getSearchKeyRange(key, noReturnData, LockMode.DEFAULT);

        System.out.println(status);
        System.out.println(new String(key.getData(), StandardCharsets.UTF_8));

        System.out.println(new String(DupKeyData.makePrefixKey(key.getData(),
                key.getOffset(), key.getSize()), StandardCharsets.UTF_8));

        DatabaseEntry k = new DatabaseEntry();
        DatabaseEntry v = new DatabaseEntry();
        status = cursor.getNext(k, v, LockMode.DEFAULT);
        System.out.println(status);
        System.out.println(new String(k.getData(), StandardCharsets.UTF_8));
        System.out.println(new String(v.getData(), StandardCharsets.UTF_8));
    }

    @Test
    public void test3() {
        byte[] bytes = ByteCoder.encode(1, "a", 2);
        List<Object> objects = ByteCoder.decode(bytes);
        Assert.assertEquals(1L, ((Long) objects.get(0)).longValue());
        Assert.assertEquals("a", (String) objects.get(1));
        Assert.assertEquals(2L, ((Long) objects.get(2)).longValue());

        bytes = ByteCoder.encode(1);
        objects = ByteCoder.decode(bytes);
        Assert.assertEquals(1L, ((Long) objects.get(0)).longValue());

        bytes = ByteCoder.encode("a");
        objects = ByteCoder.decode(bytes);
        Assert.assertEquals("a", (String) objects.get(0));
    }

    @Test
    public void test4() {
        List<List<Object>> values = metadataHandler.getAllKeys(null);
        for (List<Object> value : values) {
            System.out.println(Joiner.on("/").join(value));
        }
    }

    @Test
    public void testNegative() {
        byte b = (byte) -2;

        byte[] bytes = ByteCoder.encode(-1, "a", -2);
        List<Object> objects = ByteCoder.decode(bytes);
        Assert.assertEquals(-1L, ((Long) objects.get(0)).longValue());
        Assert.assertEquals("a", (String) objects.get(1));
        Assert.assertEquals(-2L, ((Long) objects.get(2)).longValue());
    }
}
