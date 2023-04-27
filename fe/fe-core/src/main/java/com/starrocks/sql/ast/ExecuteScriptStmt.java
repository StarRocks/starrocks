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

// EXECUTE ON <BE_ID> <SCRIPT>
public class ExecuteScriptStmt extends StatementBase {
    static long TIMEOUT_SEC_DEFAULT = 60;

    long beId;
    String script;

    long timeoutSec = TIMEOUT_SEC_DEFAULT;

    public ExecuteScriptStmt(long beId, String script) {
        this(beId, script, NodePosition.ZERO);
    }

    public ExecuteScriptStmt(long beId, String script, NodePosition pos) {
        super(pos);
        this.beId = beId;
        this.script = script;
    }

    public long getBeId() {
        return beId;
    }

    public String getScript() {
        return script;
    }

    public boolean isFrontendScript() {
        return beId == -1;
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
