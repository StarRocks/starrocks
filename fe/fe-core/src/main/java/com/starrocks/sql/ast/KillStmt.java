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


package com.starrocks.sql.ast;

import com.starrocks.analysis.RedirectStatus;
import com.starrocks.sql.parser.NodePosition;

/**
 * Representation of a Kill statement.
 * Acceptable syntax:
 * KILL [QUERY | CONNECTION] connection_id | query_id
 */
public class KillStmt extends StatementBase {
    private boolean isConnectionKill;
    private long connectionId;
    private String queryId;

    public KillStmt(long connectionId, NodePosition pos) {
        this(false, connectionId, pos);
    }

    public KillStmt(boolean isConnectionKill, long connectionId, NodePosition pos) {
        super(pos);
        this.isConnectionKill = isConnectionKill;
        this.connectionId = connectionId;
    }

    public KillStmt(String queryId, NodePosition pos) {
        super(pos);
        this.queryId = queryId;
    }

    public boolean isConnectionKill() {
        return isConnectionKill;
    }

    public long getConnectionId() {
        return connectionId;
    }

    public String getQueryId() {
        return queryId;
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

