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
import com.google.common.collect.ImmutableList;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.Parameter;
import com.starrocks.analysis.RedirectStatus;
import com.starrocks.sql.parser.NodePosition;

import java.util.ArrayList;
import java.util.List;

public class PrepareStmt extends StatementBase {
    private String name;

    private final StatementBase innerStmt;

    protected List<Parameter> parameters;

    public PrepareStmt(String name, StatementBase stmt, List<Parameter> parameters) {
        super(NodePosition.ZERO);
        this.name = name;
        this.innerStmt = stmt;
        this.parameters = parameters == null ? ImmutableList.of() : parameters;
    }

    public StatementBase getInnerStmt() {
        return innerStmt;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<Parameter> getParameters() {
        return parameters;
    }

    public void setParameters(List<Parameter> parameters) {
        this.parameters = parameters;
    }

    public StatementBase assignValues(List<Expr> values) {
        Preconditions.checkArgument(values.size() == parameters.size(), "Invalid arguments size");
        for (int i = 0; i < parameters.size(); i++) {
            parameters.get(i).setExpr(values.get(i));
        }
        return innerStmt;
    }

    public List<String> getParameterLabels() {
        List<String> labels = new ArrayList<>();
        for (Parameter parameter : parameters) {
            labels.add("$" + parameter.getSlotId());
        }
        return labels;
    }

    @Override
    public String toSql() {
        return "PREPARE " + name + " FROM " + innerStmt.toSql();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitPrepareStatement(this, context);
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.NO_FORWARD;
    }
}
