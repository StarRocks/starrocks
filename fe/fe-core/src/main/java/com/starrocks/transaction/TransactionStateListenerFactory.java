// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.transaction;

import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.lake.LakeTable;

public class TransactionStateListenerFactory {
    public TransactionStateListener create(DatabaseTransactionMgr dbTxnMgr, Table table) {
        if (table.isLakeTable()) {
            return new LakeTableTxnStateListener(dbTxnMgr, (LakeTable) table);
        }
        if (table.isLocalTable()) {
            return new OlapTableTxnStateListener(dbTxnMgr, (OlapTable) table);
        }
        return null;
    }
}
