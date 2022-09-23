// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.analysis.RedirectStatus;

/**
 * Representation of a Kill statement.
 * Acceptable syntax:
 * KILL [QUERY | CONNECTION] connection_id
 */
public class KillStmt extends StatementBase {
    private final boolean isConnectionKill;
    private final long connectionId;

    public KillStmt(boolean isConnectionKill, long connectionId) {
        this.isConnectionKill = isConnectionKill;
        this.connectionId = connectionId;
    }

    public boolean isConnectionKill() {
        return isConnectionKill;
    }

    public long getConnectionId() {
        return connectionId;
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.NO_FORWARD;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitKillStatement(this, context);
    }
}

