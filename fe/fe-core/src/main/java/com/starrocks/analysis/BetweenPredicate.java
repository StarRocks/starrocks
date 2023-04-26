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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/BetweenPredicate.java

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

import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.thrift.TExprNode;

/**
 * Class describing between predicates. After successful analysis, we equal
 * the between predicate to a conjunctive/disjunctive compound predicate
 * to be handed to the backend.
 */
public class BetweenPredicate extends Predicate {
    private final boolean isNotBetween;

    // First child is the comparison expr which should be in [lowerBound, upperBound].
    public BetweenPredicate(Expr compareExpr, Expr lowerBound, Expr upperBound, boolean isNotBetween) {
        this(compareExpr, lowerBound, upperBound, isNotBetween, NodePosition.ZERO);
    }

    public BetweenPredicate(Expr compareExpr, Expr lowerBound, Expr upperBound, boolean isNotBetween, NodePosition pos) {
        super(pos);
        children.add(compareExpr);
        children.add(lowerBound);
        children.add(upperBound);
        this.isNotBetween = isNotBetween;
    }

    protected BetweenPredicate(BetweenPredicate other) {
        super(other);
        isNotBetween = other.isNotBetween;
    }

    @Override
    public Expr clone() {
        return new BetweenPredicate(this);
    }

    public boolean isNotBetween() {
        return isNotBetween;
    }

    @Override
    protected void toThrift(TExprNode msg) {
        throw new IllegalStateException(
                "BetweenPredicate needs to be rewritten into a CompoundPredicate.");
    }

    @Override
    public String toSqlImpl() {
        String notStr = (isNotBetween) ? "NOT " : "";
        return children.get(0).toSql() + " " + notStr + "BETWEEN " +
                children.get(1).toSql() + " AND " + children.get(2).toSql();
    }

    @Override
    public int hashCode() {
        return 31 * super.hashCode() + Boolean.hashCode(isNotBetween);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (!(obj instanceof BetweenPredicate)) {
            return false;
        }
        BetweenPredicate that = (BetweenPredicate) obj;

        return super.equals(that) && isNotBetween == that.isNotBetween;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) throws SemanticException {
        return visitor.visitBetweenPredicate(this, context);
    }
}
