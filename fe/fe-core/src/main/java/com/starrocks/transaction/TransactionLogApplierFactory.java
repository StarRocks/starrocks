// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.transaction;

import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.lake.LakeTable;

public class TransactionLogApplierFactory {
    public TransactionLogApplier create(Table table) {
        if (table.isLakeTable()) {
            return new LakeTableTxnLogApplier((LakeTable) table);
        }
        if (table.isNativeTable()) {
            return new OlapTableTxnLogApplier((OlapTable) table);
        }
        return null;
    }
}
