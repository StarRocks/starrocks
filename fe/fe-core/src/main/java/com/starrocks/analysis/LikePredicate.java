// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/LikePredicate.java

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
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.thrift.TExprNode;
import com.starrocks.thrift.TExprNodeType;

import java.util.Objects;

public class LikePredicate extends Predicate {

    public enum Operator {
        LIKE("LIKE"),
        REGEXP("REGEXP");

        private final String description;

        Operator(String description) {
            this.description = description;
        }

        @Override
        public String toString() {
            return description;
        }
    }

    private final Operator op;

    private final boolean isAgainst;

    public LikePredicate(Operator op, Expr e1, Expr e2, boolean isAgainst) {
        super();
        this.op = op;
        Preconditions.checkNotNull(e1);
        children.add(e1);
        Preconditions.checkNotNull(e2);
        children.add(e2);
        // TODO: improve with histograms?
        selectivity = 0.1;
        this.isAgainst = isAgainst;
    }

    protected LikePredicate(LikePredicate other) {
        super(other);
        op = other.op;
        isAgainst = other.isAgainst;
    }

    public boolean isAgainst() {
        return isAgainst;
    }

    @Override
    public Expr clone() {
        return new LikePredicate(this);
    }

    public Operator getOp() {
        return this.op;
    }

    @Override
    public boolean equals(Object obj) {
        if (!super.equals(obj)) {
            return false;
        }
        return ((LikePredicate) obj).op == op;
    }

    @Override
    public String toSqlImpl() {
        return getChild(0).toSql() + " " + op.toString() + " " + getChild(1).toSql();
    }

    @Override
    protected void toThrift(TExprNode msg) {
        msg.node_type = TExprNodeType.FUNCTION_CALL;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), op);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) throws SemanticException {
        return visitor.visitLikePredicate(this, context);
    }
}
