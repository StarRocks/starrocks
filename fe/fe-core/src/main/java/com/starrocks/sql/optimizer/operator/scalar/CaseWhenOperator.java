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


package com.starrocks.sql.optimizer.operator.scalar;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Type;

import java.util.List;
import java.util.Objects;
import java.util.Set;

public class CaseWhenOperator extends CallOperator {
    private boolean hasCase;
    private boolean hasElse;

    private int whenStart;
    private int whenEnd;

    public CaseWhenOperator(CaseWhenOperator other, List<ScalarOperator> children) {
        super("CaseWhen", other.type, children);
        this.hasCase = other.hasCase;
        this.hasElse = other.hasElse;
        this.whenStart = other.whenStart;
        this.whenEnd = other.whenEnd;
    }

    public CaseWhenOperator(Type returnType, CaseWhenOperator other) {
        super("CaseWhen", returnType, other.arguments);
        this.hasCase = other.hasCase;
        this.hasElse = other.hasElse;
        this.whenStart = other.whenStart;
        this.whenEnd = other.whenEnd;
    }

    public CaseWhenOperator(Type returnType, ScalarOperator caseClause, ScalarOperator elseClause,
                            List<ScalarOperator> whenThenClauses) {
        super("CaseWhen", returnType, Lists.newArrayList());
        Preconditions.checkState(whenThenClauses.size() % 2 == 0);

        this.hasCase = false;
        this.hasElse = false;
        this.whenStart = 0;
        if (null != caseClause) {
            this.hasCase = true;
            this.arguments.add(caseClause);
            this.whenStart = 1;
        }

        this.arguments.addAll(whenThenClauses);
        this.whenEnd = this.arguments.size();

        if (null != elseClause) {
            this.hasElse = true;
            this.arguments.add(elseClause);
        }
    }

    public boolean hasCase() {
        return hasCase;
    }

    public boolean hasElse() {
        return hasElse;
    }

    // must after call hasElse
    public void setElseClause(ScalarOperator elseClause) {
        Preconditions.checkState(hasElse);
        arguments.set(arguments.size() - 1, elseClause);
    }

    // must after call hasCase
    public ScalarOperator getCaseClause() {
        Preconditions.checkState(hasCase);
        return arguments.get(0);
    }

    // must after call hasCase
    public void setCaseClause(ScalarOperator caseClause) {
        Preconditions.checkState(hasCase);
        arguments.set(0, caseClause);
    }

    // must after call hasElse
    public ScalarOperator getElseClause() {
        Preconditions.checkState(hasElse);
        return arguments.get(arguments.size() - 1);
    }

    public int getWhenClauseSize() {
        return (this.whenEnd - this.whenStart) / 2;
    }

    public ScalarOperator getWhenClause(int i) {
        return arguments.get(2 * i + whenStart);
    }

    public ScalarOperator getThenClause(int i) {
        return arguments.get(2 * i + whenStart + 1);
    }

    // return all then + else
    public List<ScalarOperator> getAllValuesClause() {
        List<ScalarOperator> re = Lists.newArrayList();
        for (int i = 0; i < getWhenClauseSize(); i++) {
            re.add(getThenClause(i));
        }
        if (hasElse()) {
            re.add(getElseClause());
        }
        return re;
    }

    // return all case + when
    public List<ScalarOperator> getAllConditionClause() {
        List<ScalarOperator> re = Lists.newArrayList();
        if (hasCase()) {
            re.add(getCaseClause());
        }
        for (int i = 0; i < getWhenClauseSize(); i++) {
            re.add(getWhenClause(i));
        }

        return re;
    }

    public void setWhenClause(int i, ScalarOperator op) {
        arguments.set(2 * i + whenStart, op);
    }

    public void setThenClause(int i, ScalarOperator op) {
        arguments.set(2 * i + whenStart + 1, op);
    }

    public int getWhenStart() {
        return this.whenStart;
    }

    // This method used for remove useless WhenThenClause,
    // removeSet contains arguments index that should be removed.
    public void removeArguments(Set<Integer> removeSet) {
        List<ScalarOperator> newArguments = Lists.newArrayList();
        for (int i = 0; i < this.arguments.size(); ++i) {
            if (!removeSet.contains(i)) {
                newArguments.add(this.arguments.get(i));
            }
        }
        this.arguments = newArguments;
        this.whenEnd = this.arguments.size();
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("CASE ");
        if (hasCase()) {
            stringBuilder.append(getCaseClause().toString()).append(" ");
        }

        for (int i = 0; i < getWhenClauseSize(); i++) {
            stringBuilder.append("WHEN ").append(getWhenClause(i).toString()).append(" ");
            stringBuilder.append("THEN ").append(getThenClause(i).toString()).append(" ");
        }

        if (hasElse()) {
            stringBuilder.append("ELSE ").append(getElseClause().toString()).append(" ");
        }

        stringBuilder.append("END");
        return stringBuilder.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        CaseWhenOperator that = (CaseWhenOperator) o;
        return hasCase == that.hasCase &&
                hasElse == that.hasElse &&
                whenStart == that.whenStart &&
                whenEnd == that.whenEnd;
    }


    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), hasCase, hasElse, whenStart, whenEnd);
    }

    @Override
    public <R, C> R accept(ScalarOperatorVisitor<R, C> visitor, C context) {
        return visitor.visitCaseWhenOperator(this, context);
    }
}
