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

import com.starrocks.analysis.FunctionName;
import com.starrocks.analysis.RedirectStatus;
import com.starrocks.catalog.FunctionSearchDesc;
import com.starrocks.sql.parser.NodePosition;

public class DropFunctionStmt extends DdlStmt {
    private final FunctionName functionName;

    public FunctionArgsDef getArgsDef() {
        return argsDef;
    }

    private final FunctionArgsDef argsDef;

    // set after analyzed
    private FunctionSearchDesc function;

    public DropFunctionStmt(FunctionName functionName, FunctionArgsDef argsDef) {
        this(functionName, argsDef, NodePosition.ZERO);
    }

    public DropFunctionStmt(FunctionName functionName, FunctionArgsDef argsDef, NodePosition pos) {
        super(pos);
        this.functionName = functionName;
        this.argsDef = argsDef;
    }

    public FunctionName getFunctionName() {
        return functionName;
    }

    public FunctionSearchDesc getFunction() {
        return function;
    }

    public void setFunction(FunctionSearchDesc function) {
        this.function = function;
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.FORWARD_WITH_SYNC;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitDropFunctionStatement(this, context);
    }
}
