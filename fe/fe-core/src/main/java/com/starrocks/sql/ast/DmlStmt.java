// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.ast;

import com.starrocks.analysis.RedirectStatus;
import com.starrocks.analysis.TableName;

public abstract class DmlStmt extends StatementBase {
<<<<<<< HEAD
=======
    private long txnId;
    private Map<String, String> optHints;

    protected DmlStmt(NodePosition pos) {
        super(pos);
    }

>>>>>>> a495825fd5 ([BugFix] Fix insert and schema change concurrency issue (#36225))
    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.FORWARD_WITH_SYNC;
    }

    public abstract TableName getTableName();

<<<<<<< HEAD
=======
    public long getTxnId() {
        return txnId;
    }

    public void setTxnId(long txnId) {
        this.txnId = txnId;
    }

    public Map<String, String> getOptHints() {
        return optHints;
    }

    public void setOptHints(Map<String, String> optHints) {
        this.optHints = optHints;
    }
>>>>>>> a495825fd5 ([BugFix] Fix insert and schema change concurrency issue (#36225))
}
