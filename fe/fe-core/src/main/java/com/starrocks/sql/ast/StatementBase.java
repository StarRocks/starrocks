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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/StatementBase.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.sql.ast;

import com.google.common.base.Preconditions;
import com.starrocks.analysis.ParseNode;
import com.starrocks.analysis.RedirectStatus;
import com.starrocks.qe.OriginStatement;

public abstract class StatementBase implements ParseNode {

    public enum ExplainLevel {
        NORMAL,
        LOGICAL,
        // True if the describe_stmt print verbose information, if `isVerbose` is true, `isExplain` must be set to true.
        VERBOSE,
        // True if the describe_stmt print costs information, if `isCosts` is true, `isExplain` must be set to true.
        COST,
        OPTIMIZER,
        REWRITE
    }

    private ExplainLevel explainLevel;

    // True if this QueryStmt is the top level query from an EXPLAIN <query>
    protected boolean isExplain = false;

    // Original statement to further usage, eg: enable_sql_blacklist.
    protected OriginStatement origStmt;

    public void setIsExplain(boolean isExplain, ExplainLevel explainLevel) {
        this.isExplain = isExplain;
        this.explainLevel = explainLevel;
    }

    public boolean isExplain() {
        return isExplain;
    }

    public boolean isTrace() {
        return isExplain && (explainLevel == ExplainLevel.OPTIMIZER ||
                explainLevel == ExplainLevel.REWRITE);
    }

    public ExplainLevel getExplainLevel() {
        if (explainLevel == null) {
            return ExplainLevel.NORMAL;
        } else {
            return explainLevel;
        }
    }

    public abstract RedirectStatus getRedirectStatus();

    public void setOrigStmt(OriginStatement origStmt) {
        Preconditions.checkState(origStmt != null);
        this.origStmt = origStmt;
    }

    public OriginStatement getOrigStmt() {
        return origStmt;
    }

    // Override this method and return true
    // if the stmt contains some information which need to be encrypted in audit log
    public boolean needAuditEncryption() {
        return false;
    }
}
