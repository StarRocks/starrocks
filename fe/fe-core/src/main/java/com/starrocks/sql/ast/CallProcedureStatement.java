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

import com.starrocks.connector.Procedure;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.parser.NodePosition;

import java.util.List;
import java.util.Map;

public class CallProcedureStatement extends DdlStmt {
    private QualifiedName qualifiedName;
    private final List<ProcedureArgument> arguments;
    private Procedure procedure;
    private Map<String, ConstantOperator> analyzedArguments;

    public CallProcedureStatement(QualifiedName qualifiedName, List<ProcedureArgument> arguments, NodePosition pos) {
        super(pos);
        this.qualifiedName = qualifiedName;
        this.arguments = arguments;
    }

    public QualifiedName getQualifiedName() {
        return qualifiedName;
    }

    public void setQualifiedName(QualifiedName qualifiedName) {
        this.qualifiedName = qualifiedName;
    }

    public Procedure getProcedure() {
        return procedure;
    }

    public void setProcedure(Procedure procedure) {
        this.procedure = procedure;
    }

    public List<ProcedureArgument> getArguments() {
        return arguments;
    }

    public Map<String, ConstantOperator> getAnalyzedArguments() {
        return analyzedArguments;
    }

    public void setAnalyzedArguments(Map<String, ConstantOperator> arguments) {
        if (arguments == null) {
            throw new IllegalArgumentException("Arguments cannot be null");
        }
        this.analyzedArguments = arguments;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return ((AstVisitorExtendInterface<R, C>) visitor).visitCallProcedureStatement(this, context);
    }

}
