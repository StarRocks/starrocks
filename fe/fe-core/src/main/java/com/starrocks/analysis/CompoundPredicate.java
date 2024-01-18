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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/CompoundPredicate.java

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

import com.google.common.base.Preconditions;
import com.starrocks.common.exception.AnalysisException;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.thrift.TExprNode;
import com.starrocks.thrift.TExprNodeType;
import com.starrocks.thrift.TExprOpcode;

import java.util.Objects;

public class CompoundPredicate extends Predicate {
    private final Operator op;

    public CompoundPredicate(Operator op, Expr e1, Expr e2) {
        this(op, e1, e2, NodePosition.ZERO);
    }

    public CompoundPredicate(Operator op, Expr e1, Expr e2, NodePosition pos) {
        super(pos);
        this.op = op;
        Preconditions.checkNotNull(e1);
        children.add(e1);
        Preconditions.checkArgument(
                op == Operator.NOT && e2 == null || op != Operator.NOT && e2 != null);
        if (e2 != null) {
            children.add(e2);
        }
    }

    protected CompoundPredicate(CompoundPredicate other) {
        super(other);
        op = other.op;
    }

    @Override
    public Expr clone() {
        return new CompoundPredicate(this);
    }

    public Operator getOp() {
        return op;
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj) && ((CompoundPredicate) obj).op == op;
    }

    @Override
    public String toSqlImpl() {
        if (children.size() == 1) {
            Preconditions.checkState(op == Operator.NOT);
            return "NOT (" + getChild(0).toSql() + ")";
        } else {
            return "(" + getChild(0).toSql() + ")" + " " + op.toString() + " " + "(" + getChild(
                    1).toSql() + ")";
        }
    }

    @Override
    protected void toThrift(TExprNode msg) {
        msg.node_type = TExprNodeType.COMPOUND_PRED;
        msg.setOpcode(op.toThrift());
    }

    @Override
    public void analyzeImpl(Analyzer analyzer) throws AnalysisException {
    }

    public enum Operator {
        AND("AND", TExprOpcode.COMPOUND_AND),
        OR("OR", TExprOpcode.COMPOUND_OR),
        NOT("NOT", TExprOpcode.COMPOUND_NOT);

        private final String description;
        private final TExprOpcode thriftOp;

        Operator(String description, TExprOpcode thriftOp) {
            this.description = description;
            this.thriftOp = thriftOp;
        }

        @Override
        public String toString() {
            return description;
        }

        public TExprOpcode toThrift() {
            return thriftOp;
        }
    }

    /**
     * Negates a CompoundPredicate.
     */
    @Override
    public Expr negate() {
        if (op == Operator.NOT) {
            return getChild(0);
        }
        Expr negatedLeft = getChild(0).negate();
        Expr negatedRight = getChild(1).negate();
        Operator newOp = (op == Operator.OR) ? Operator.AND : Operator.OR;
        return new CompoundPredicate(newOp, negatedLeft, negatedRight);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), op);
    }

    /**
     * Below function ia added by new analyzer
     */
    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) throws SemanticException {
        return visitor.visitCompoundPredicate(this, context);
    }

    @Override
    public String toString() {
        return toSqlImpl();
    }
}
