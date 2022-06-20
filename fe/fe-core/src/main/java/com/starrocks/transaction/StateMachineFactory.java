// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.transaction;

import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.lake.LakeTable;

public class StateMachineFactory {
    public StateMachine create(DatabaseTransactionMgr dbTxnMgr, Table table) {
        if (table instanceof LakeTable) {
            return null; // todo
        }
        if (table instanceof OlapTable) {
            return new OlapTableStateMachine(dbTxnMgr, (OlapTable) table);
        }
        return null;
    }
}
