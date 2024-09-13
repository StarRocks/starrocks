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
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Durability;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.dbi.DupKeyData;
import com.starrocks.journal.bdbje.BDBEnvironment;
import com.starrocks.server.GlobalStateMgr;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class MetadataHandler {
    public static MetadataHandler getInstance() {
        return GlobalStateMgr.getCurrentState().getMetadataHandler();
    }

    private final BDBEnvironment bdbEnvironment;
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

    public Transaction starTransaction() {
        TransactionConfig transactionConfig = new TransactionConfig();
        transactionConfig.setTxnTimeout(500, TimeUnit.SECONDS);
        transactionConfig.setDurability(new Durability(Durability.SyncPolicy.SYNC,
                Durability.SyncPolicy.SYNC, Durability.ReplicaAckPolicy.ALL));

        return bdbEnvironment.getReplicatedEnvironment().beginTransaction(null, transactionConfig);
    }

    public OperationStatus put(Transaction transaction, String keyS, String valueS) {
        TupleBinding<String> binding = TupleBinding.getPrimitiveBinding(String.class);
        DatabaseEntry key = new DatabaseEntry();
        binding.objectToEntry(keyS, key);

        DatabaseEntry value = new DatabaseEntry();
        binding.objectToEntry(valueS, value);
        return database.put(transaction, key, value);
    }

    public OperationStatus put(Transaction transaction, byte[] keyS, String valueS) {
        TupleBinding<String> binding = TupleBinding.getPrimitiveBinding(String.class);
        DatabaseEntry key = new DatabaseEntry(keyS);
        DatabaseEntry value = new DatabaseEntry();
        binding.objectToEntry(valueS, value);
        return database.put(transaction, key, value);
    }

    public byte[] get(Transaction transaction, byte[] keyS) {
        TupleBinding<String> binding = TupleBinding.getPrimitiveBinding(String.class);
        DatabaseEntry key = new DatabaseEntry(keyS);
        DatabaseEntry result = new DatabaseEntry();
        database.get(transaction, key, result, LockMode.READ_COMMITTED);

        binding.entryToObject(result);
        return result.getData();
    }

    public <T> T get(Transaction transaction, byte[] key, Class<T> c) {
        TupleBinding<T> binding = TupleBinding.getPrimitiveBinding(c);
        DatabaseEntry databaseEntry = new DatabaseEntry(key);
        DatabaseEntry result = new DatabaseEntry();
        OperationStatus status = database.get(transaction, databaseEntry, result, LockMode.READ_COMMITTED);
        if (status.equals(OperationStatus.NOTFOUND)) {
            return null;
        } else {
            return binding.entryToObject(result);
        }
    }

    public List<byte[]> getPrefix(Transaction transaction, byte[] prefix) {
        Cursor cursor = database.openCursor(transaction, null);

        DatabaseEntry key = new DatabaseEntry(prefix);

        DatabaseEntry prefixStart = new DatabaseEntry(prefix);
        DatabaseEntry prefixEnd = new DatabaseEntry(DupKeyData.makePrefixKey(key.getData(), key.getOffset(), key.getSize()));

        DatabaseEntry noReturnData = new DatabaseEntry();
        noReturnData.setPartial(0, 0, true);
        cursor.getSearchKeyRange(key, noReturnData, LockMode.READ_COMMITTED);

        List<byte[]> keyList = new ArrayList<>();
        do {
            if (DupKeyData.compareMainKey(
                    key.getData(),
                    prefixEnd.getData(),
                    prefixEnd.getOffset(),
                    prefixEnd.getSize(),
                    database.getConfig().getBtreeComparator()) == 0) {
                break;
            }

            keyList.add(key.getData());

        } while (cursor.getNext(key, noReturnData, LockMode.READ_COMMITTED) == OperationStatus.SUCCESS);

        return keyList;
    }

    public OperationStatus delete(Transaction transaction, byte[] keyS) {
        return null;
    }
}
