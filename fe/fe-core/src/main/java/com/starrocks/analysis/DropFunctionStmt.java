// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/DropFunctionStmt.java

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

package com.starrocks.analysis;

import com.starrocks.catalog.FunctionSearchDesc;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.UserException;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AstVisitor;

public class DropFunctionStmt extends DdlStmt {
    private final FunctionName functionName;

    public FunctionArgsDef getArgsDef() {
        return argsDef;
    }

    private final FunctionArgsDef argsDef;

    // set after analyzed
    private FunctionSearchDesc function;

    public DropFunctionStmt(FunctionName functionName, FunctionArgsDef argsDef) {
        this.functionName = functionName;
        this.argsDef = argsDef;
    }

    public FunctionName getFunctionName() {
        return functionName;
    }

    public FunctionSearchDesc getFunction() {
        return function;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);

        // analyze function name
        functionName.analyze(analyzer.getDefaultDb(), analyzer.getClusterName());

        // check operation privilege
        if (!GlobalStateMgr.getCurrentState().getAuth().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }

        // analyze arguments
        argsDef.analyze();
        function = new FunctionSearchDesc(functionName, argsDef.getArgTypes(), argsDef.isVariadic());
    }

    @Override
    public String toSql() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("DROP FUNCTION ").append(functionName);
        return stringBuilder.toString();
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.FORWARD_WITH_SYNC;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitDropFunction(this, context);
    }

    @Override
    public boolean isSupportNewPlanner() {
        return true;
    }

    public void setFunction(FunctionSearchDesc function) {
        this.function = function;
    }
}
