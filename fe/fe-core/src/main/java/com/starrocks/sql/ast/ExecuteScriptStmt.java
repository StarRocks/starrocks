// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.ast;

import com.starrocks.analysis.RedirectStatus;
import com.starrocks.analysis.StatementBase;

// EXECUTE ON <BE_ID> <SCRIPT>
public class ExecuteScriptStmt extends StatementBase {
    static long TIMEOUT_SEC_DEFAULT = 60;

    long beId;
    String script;

    long timeoutSec = TIMEOUT_SEC_DEFAULT;

    public ExecuteScriptStmt(long beId, String script) {
        this.beId = beId;
        this.script = script;
    }

    public long getBeId() {
        return beId;
    }

    public String getScript() {
        return script;
    }

    public void setTimeoutSec(long timeoutSec) {
        this.timeoutSec = timeoutSec;
    }

    public long getTimeoutSec() {
        return timeoutSec;
    }

    @Override
    public String toString() {
        String s = String.format("EXECUTE ON %d %s", beId, script);
        return s;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitExecuteScriptStatement(this, context);
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.NO_FORWARD;
    }
}
