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

import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Durability;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import com.starrocks.common.Pair;
import com.starrocks.journal.bdbje.BDBEnvironment;
import com.starrocks.meta.kv.ByteCoder;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.GlobalStateMgr;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class MetadataHandler {
    public static MetadataHandler getInstance() {
        return GlobalStateMgr.getCurrentState().getMetadataHandler();
    }

    private BDBEnvironment bdbEnvironment;
    private final Database database;

    public MetadataHandler(BDBEnvironment bdbEnvironment) {
        this.bdbEnvironment = bdbEnvironment;

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(true);
        if (GlobalStateMgr.getCurrentState().isElectable()) {
            dbConfig.setAllowCreate(true);
            dbConfig.setReadOnly(false);
        } else {
            dbConfig.setAllowCreate(false);
            dbConfig.setReadOnly(true);
        }
        this.database = bdbEnvironment.getReplicatedEnvironment().openDatabase(null, "meta", dbConfig);
    }


    public MetadataHandler(Database database) {
        this.database = database;
    }

    public Transaction starTransaction() {
        TransactionConfig transactionConfig = new TransactionConfig();
        transactionConfig.setTxnTimeout(500, TimeUnit.SECONDS);
        transactionConfig.setDurability(new Durability(Durability.SyncPolicy.SYNC,
                Durability.SyncPolicy.SYNC, Durability.ReplicaAckPolicy.ALL));

        return bdbEnvironment.getReplicatedEnvironment().beginTransaction(null, transactionConfig);
    }

    public <T> OperationStatus put(Transaction transaction, byte[] key, Object value, Class<T> c) {
        TupleBinding<String> binding = TupleBinding.getPrimitiveBinding(String.class);
        DatabaseEntry keyEntry = new DatabaseEntry(key);
        DatabaseEntry valueEntry = new DatabaseEntry();
        binding.objectToEntry(GsonUtils.GSON_V2.toJson(value, c), valueEntry);
        return database.put(transaction, keyEntry, valueEntry);
    }

    public OperationStatus delete(Transaction transaction, byte[] key) {
        DatabaseEntry keyEntry = new DatabaseEntry(key);
        return database.delete(transaction, keyEntry);
    }

    public <T> T get(Transaction transaction, byte[] key, Class<T> c) {
        TupleBinding<String> binding = TupleBinding.getPrimitiveBinding(String.class);
        DatabaseEntry databaseEntry = new DatabaseEntry(key);
        DatabaseEntry result = new DatabaseEntry();
        OperationStatus status = database.get(transaction, databaseEntry, result, LockMode.READ_COMMITTED);
        if (status.equals(OperationStatus.NOTFOUND)) {
            return null;
        } else {
            return GsonUtils.GSON_V2.fromJson(binding.entryToObject(result), c);
        }
    }

    public String get(Transaction transaction, byte[] key) {
        TupleBinding<String> binding = TupleBinding.getPrimitiveBinding(String.class);
        DatabaseEntry databaseEntry = new DatabaseEntry(key);
        DatabaseEntry result = new DatabaseEntry();
        OperationStatus status = database.get(transaction, databaseEntry, result, LockMode.READ_COMMITTED);
        if (status.equals(OperationStatus.NOTFOUND)) {
            return null;
        } else {
            return binding.entryToObject(result);
        }
    }

    public List<List<Object>> getPrefixNoReturnValue(Transaction transaction, byte[] prefix) {
        List<List<Object>> keyList = new ArrayList<>();
        CursorConfig cursorConfig = CursorConfig.READ_COMMITTED;
        try (Cursor cursor = database.openCursor(transaction, cursorConfig)) {
            DatabaseEntry key = new DatabaseEntry(prefix);
            DatabaseEntry noReturnData = new DatabaseEntry();
            noReturnData.setPartial(0, 0, true);
            OperationStatus status = cursor.getSearchKeyRange(key, noReturnData, LockMode.DEFAULT);

            if (status == OperationStatus.NOTFOUND) {
                return keyList;
            }

            do {
                if (key.getData().length < prefix.length) {
                    break;
                }

                if (!Arrays.equals(prefix, 0, prefix.length, key.getData(), 0, prefix.length)) {
                    break;
                }
                keyList.add(ByteCoder.decode(key.getData()));
            } while (cursor.getNext(key, noReturnData, LockMode.DEFAULT) == OperationStatus.SUCCESS);

            return keyList;
        }
    }

    public List<List<Object>> getAllKeys(Transaction transaction) {
        CursorConfig cursorConfig = CursorConfig.READ_COMMITTED;
        try (Cursor cursor = database.openCursor(transaction, cursorConfig)) {

            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry noReturnData = new DatabaseEntry();
            noReturnData.setPartial(0, 0, true);
            cursor.getFirst(key, noReturnData, LockMode.DEFAULT);

            List<List<Object>> keyList = new ArrayList<>();
            do {
                keyList.add(ByteCoder.decode(key.getData()));
            } while (cursor.getNext(key, noReturnData, LockMode.DEFAULT) == OperationStatus.SUCCESS);

            return keyList;
        }
    }

    public List<Pair<List<Object>, String>> getPrefix(Transaction transaction, byte[] prefix) {
        CursorConfig cursorConfig = CursorConfig.READ_COMMITTED;
        TupleBinding<String> binding = TupleBinding.getPrimitiveBinding(String.class);

        try (Cursor cursor = database.openCursor(transaction, cursorConfig)) {
            DatabaseEntry keyEntry = new DatabaseEntry(prefix);
            DatabaseEntry valueEntry = new DatabaseEntry();
            cursor.getSearchKeyRange(keyEntry, valueEntry, LockMode.DEFAULT);

            List<Pair<List<Object>, String>> result = new ArrayList<>();
            do {
                if (!Arrays.equals(prefix, 0, prefix.length, keyEntry.getData(), 0, prefix.length)) {
                    break;
                }

                Pair<List<Object>, String> r = new Pair<>(
                        ByteCoder.decode(keyEntry.getData()),
                        binding.entryToObject(valueEntry));
                result.add(r);
            } while (cursor.getNext(keyEntry, valueEntry, LockMode.DEFAULT) == OperationStatus.SUCCESS);

            return result;
        }
    }
}
