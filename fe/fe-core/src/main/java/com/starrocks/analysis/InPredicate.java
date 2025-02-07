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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/InPredicate.java

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

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.thrift.TExprNode;
import com.starrocks.thrift.TExprNodeType;
import com.starrocks.thrift.TExprOpcode;
import com.starrocks.thrift.TInPredicate;

import java.util.List;

/**
 * Class representing a [NOT] IN predicate. It determines if a specified value
 * (first child) matches any value in a subquery (second child) or a list
 * of values (remaining children).
 */

public class InPredicate extends Predicate {
    private final boolean isNotIn;

    // First child is the comparison expr for which we
    // should check membership in the inList (the remaining children).
    public InPredicate(Expr compareExpr, List<Expr> inList, boolean isNotIn) {
        this(compareExpr, inList, isNotIn, NodePosition.ZERO);
    }

    public InPredicate(Expr compareExpr, List<Expr> inList, boolean isNotIn, NodePosition pos) {
        super(pos);
        children.add(compareExpr);
        children.addAll(inList);
        this.isNotIn = isNotIn;
        this.opcode = isNotIn ? TExprOpcode.FILTER_NOT_IN : TExprOpcode.FILTER_IN;
    }

    protected InPredicate(InPredicate other) {
        super(other);
        isNotIn = other.isNotIn();
        this.opcode = isNotIn ? TExprOpcode.FILTER_NOT_IN : TExprOpcode.FILTER_IN;
    }

    public int getInElementNum() {
        // the first child is compare expr
        return getChildren().size() - 1;
    }

    @Override
    public Expr clone() {
        return new InPredicate(this);
    }

    // C'tor for initializing an [NOT] IN predicate with a subquery child.
    public InPredicate(Expr compareExpr, Expr subquery, boolean isNotIn) {
        this(compareExpr, subquery, isNotIn, NodePosition.ZERO);
    }

    public InPredicate(Expr compareExpr, Expr subquery, boolean isNotIn, NodePosition pos) {
        super(pos);
        Preconditions.checkNotNull(compareExpr);
        Preconditions.checkNotNull(subquery);
        children.add(compareExpr);
        children.add(subquery);
        this.isNotIn = isNotIn;
        this.opcode = isNotIn ? TExprOpcode.FILTER_NOT_IN : TExprOpcode.FILTER_IN;
    }

    /**
     * Negates an InPredicate.
     */
    @Override
    public Expr negate() {
        return new InPredicate(getChild(0), children.subList(1, children.size()),
                !isNotIn);
    }

    public List<Expr> getListChildren() {
        return children.subList(1, children.size());
    }

    public boolean isNotIn() {
        return isNotIn;
    }

    public boolean isLiteralChildren() {
        for (int i = 1; i < children.size(); ++i) {
            if (!(children.get(i) instanceof LiteralExpr)) {
                return false;
            }
        }
        return true;
    }

    @Override
    protected void toThrift(TExprNode msg) {
        // Can't serialize a predicate with a subquery
        Preconditions.checkState(!contains(Subquery.class));
        msg.in_predicate = new TInPredicate(isNotIn);
        msg.node_type = TExprNodeType.IN_PRED;
        msg.setOpcode(opcode);
        msg.setVector_opcode(vectorOpcode);
        if (getChild(0).getType().isComplexType()) {
            msg.setChild_type_desc(getChild(0).getType().toThrift());
        } else {
            msg.setChild_type(getChild(0).getType().getPrimitiveType().toThrift());
        }
    }

    @Override
    public String toSqlImpl() {
        StringBuilder strBuilder = new StringBuilder();
        String notStr = (isNotIn) ? "NOT " : "";
        strBuilder.append(getChild(0).toSql()).append(" ").append(notStr).append("IN (");
        for (int i = 1; i < children.size(); ++i) {
            strBuilder.append(getChild(i).toSql());
            strBuilder.append((i + 1 != children.size()) ? ", " : "");
        }
        strBuilder.append(")");
        return strBuilder.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(super.hashCode(), isNotIn);
    }

    @Override
    public boolean equalsWithoutChild(Object obj) {
        if (super.equalsWithoutChild(obj)) {
            InPredicate expr = (InPredicate) obj;
            return isNotIn == expr.isNotIn;
        }
        return false;
    }

    public void setOpcode(TExprOpcode opcode) {
        this.opcode = opcode;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) throws SemanticException {
        return visitor.visitInPredicate(this, context);
    }
}
