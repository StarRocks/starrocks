// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.transaction;

import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.lake.LakeTable;

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
