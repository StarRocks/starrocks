// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.transaction;

import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.lake.LakeTable;

public class TableCommitterFactory {
    public TableCommitter create(DatabaseTransactionMgr dbTxnMgr, Table table) {
        if (table instanceof LakeTable) {
            // todo
            return null;
        }
        if (table instanceof OlapTable) {
            return new OlapTableCommitter(dbTxnMgr, (OlapTable) table);
        }
        return null;
    }
}
