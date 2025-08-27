package com.starrocks.load.loadv2;

import com.starrocks.catalog.Table;
import com.starrocks.qe.ConnectContext;

public class InsertLoadTxnCallbackFactory {
    public static IVMInsertLoadTxnCallback of(ConnectContext context, long dbId, Table targetTable) {
        if (context.getSessionVariable().isEnableIVMRefresh() && targetTable.isMaterializedView()) {
            return new IVMInsertLoadTxnCallback(dbId, targetTable.getId());
        } else {
            return null;
        }
    }
}
