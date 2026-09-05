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

import com.starrocks.sql.parser.NodePosition;

import java.util.Collections;
import java.util.List;

// ADMIN EXECUTE ON <target> <SCRIPT>
//   <target> := FRONTEND
//             | <node_id> (, <node_id>)*
//             | ALL BACKENDS
//             | ALL COMPUTE NODES
public class ExecuteScriptStmt extends StatementBase {
    static long TIMEOUT_SEC_DEFAULT = 600;

    public enum TargetType {
        FRONTEND,
        NODES,
        ALL_BACKENDS,
        ALL_COMPUTE_NODES,
    }

    private final TargetType targetType;
    // Explicit node ids when targetType == NODES; empty otherwise (resolved at execution time).
    private final List<Long> nodeIds;
    private final String script;

    private long timeoutSec = TIMEOUT_SEC_DEFAULT;

    public ExecuteScriptStmt(long beId, String script) {
        this(beId, script, NodePosition.ZERO);
    }

    public ExecuteScriptStmt(long beId, String script, NodePosition pos) {
        super(pos);
        if (beId == -1) {
            this.targetType = TargetType.FRONTEND;
            this.nodeIds = Collections.emptyList();
        } else {
            this.targetType = TargetType.NODES;
            this.nodeIds = Collections.singletonList(beId);
        }
        this.script = script;
    }

    public ExecuteScriptStmt(TargetType targetType, List<Long> nodeIds, String script, NodePosition pos) {
        super(pos);
        this.targetType = targetType;
        this.nodeIds = (nodeIds == null) ? Collections.emptyList() : List.copyOf(nodeIds);
        this.script = script;
    }

    public TargetType getTargetType() {
        return targetType;
    }

    public List<Long> getNodeIds() {
        return nodeIds;
    }

    // Legacy accessor: returns -1 for frontend / unresolved fan-out targets,
    // otherwise the first node id. Prefer getNodeIds() for new code.
    public long getBeId() {
        if (nodeIds.isEmpty()) {
            return -1;
        }
        return nodeIds.get(0);
    }

    public String getScript() {
        return script;
    }

    public boolean isFrontendScript() {
        return targetType == TargetType.FRONTEND;
    }

    public void setTimeoutSec(long timeoutSec) {
        this.timeoutSec = timeoutSec;
    }

    public long getTimeoutSec() {
        return timeoutSec;
    }

    @Override
    public String toString() {
        String targetDesc;
        switch (targetType) {
            case FRONTEND:
                targetDesc = "FRONTEND";
                break;
            case ALL_BACKENDS:
                targetDesc = "ALL BACKENDS";
                break;
            case ALL_COMPUTE_NODES:
                targetDesc = "ALL COMPUTE NODES";
                break;
            case NODES:
            default:
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < nodeIds.size(); i++) {
                    if (i > 0) {
                        sb.append(",");
                    }
                    sb.append(nodeIds.get(i));
                }
                targetDesc = sb.toString();
                break;
        }
        return String.format("EXECUTE ON %s %s", targetDesc, script);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitExecuteScriptStatement(this, context);
    }
}
