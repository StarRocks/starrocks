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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.analysis.FunctionName;
import com.starrocks.analysis.ParseNode;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Function;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.sql.parser.NodePosition;

import java.util.List;

/**
 * FunctionRef is used to represent all Functions (UDF) with same FunctionName.
 * This description for Function is maked when creating AST with unanalyzed state and maked with
 * analyzed state if given a Function Object.
 */
public class FunctionRef implements ParseNode {
    private final NodePosition pos;
    private FunctionName fnName;
    private String alias;
    // set after analyzed
    private List<Function> fn;
    private boolean analyzed;

    public FunctionRef(List<Function> fn) {
        this.fn = fn;
        this.analyzed = true;
        this.pos = NodePosition.ZERO;
    }

    public FunctionRef(FunctionName fnName, String alias, NodePosition pos) {
        this.fnName = fnName;
        this.pos = pos;
        this.fn = Lists.newArrayList();
        this.alias = alias;
        this.analyzed = false;
    }

    public FunctionName getFnName() {
        return fnName;
    }

    public void analyzeForBackup(Database db) {
        fnName.analyze(db.getFullName());

        this.fn = db.getFunctionsByName(fnName.getFunction());
        
        if (this.fn.isEmpty()) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                    "Invalid backup function(s), function name: " + fnName.toString());
        }
        this.analyzed = true;
    }

    public boolean checkSameFunctionNameForRestore(Function func) {
        return this.fnName.getFunction().equalsIgnoreCase(func.functionName());
    }

    public List<Function> getFunctions() {
        Preconditions.checkState(this.analyzed);
        return this.fn;
    }

    public String getAlias() {
        return this.alias;
    }

    @Override
    public NodePosition getPos() {
        return pos;
    }
}
