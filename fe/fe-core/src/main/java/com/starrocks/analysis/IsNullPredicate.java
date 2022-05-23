// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/IsNullPredicate.java

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
import com.starrocks.catalog.Function;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.thrift.TExprNode;
import com.starrocks.thrift.TExprNodeType;
import com.starrocks.thrift.TFunctionBinaryType;

public class IsNullPredicate extends Predicate {

    static Function isNullFN = new Function(new FunctionName("is_null_pred"),
            new Type[] {Type.INVALID}, Type.BOOLEAN, false);
    static Function isNotNullFN = new Function(new FunctionName("is_not_null_pred"),
            new Type[] {Type.INVALID}, Type.BOOLEAN, false);
    {
        isNullFN.setBinaryType(TFunctionBinaryType.BUILTIN);
        isNotNullFN.setBinaryType(TFunctionBinaryType.BUILTIN);
    }

    private final boolean isNotNull;

    public IsNullPredicate(Expr e, boolean isNotNull) {
        super();
        this.isNotNull = isNotNull;
        Preconditions.checkNotNull(e);
        children.add(e);
    }

    protected IsNullPredicate(IsNullPredicate other) {
        super(other);
        this.isNotNull = other.isNotNull;
    }

    public boolean isNotNull() {
        return isNotNull;
    }

    @Override
    public Expr clone() {
        return new IsNullPredicate(this);
    }

    @Override
    public boolean equals(Object obj) {
        if (!super.equals(obj)) {
            return false;
        }
        return ((IsNullPredicate) obj).isNotNull == isNotNull;
    }

    @Override
    public String toSqlImpl() {
        return getChild(0).toSql() + (isNotNull ? " IS NOT NULL" : " IS NULL");
    }

    @Override
    public String toDigestImpl() {
        return getChild(0).toDigest() + (isNotNull ? " is not null" : " is null");
    }

    public boolean isSlotRefChildren() {
        return (children.get(0) instanceof SlotRef);
    }

    @Override
    public void analyzeImpl(Analyzer analyzer) throws AnalysisException {
        super.analyzeImpl(analyzer);

        if (isNotNull) {
            fn = isNullFN;
        } else {
            fn = isNotNullFN;
        }

        // determine selectivity
        selectivity = 0.1;
    }

    @Override
    protected void toThrift(TExprNode msg) {
        msg.node_type = TExprNodeType.FUNCTION_CALL;
    }

    /**
     * Negates an IsNullPredicate.
     */
    @Override
    public Expr negate() {
        return new IsNullPredicate(getChild(0), !isNotNull);
    }

    public boolean isNullable() {
        return false;
    }

    @Override
    public Expr getResultValue() throws AnalysisException {
        recursiveResetChildrenResult();
        final Expr ChildValue = getChild(0);

        if (!(ChildValue instanceof LiteralExpr)) {
            return this;
        }

        return ChildValue instanceof NullLiteral ?
                new BoolLiteral(!isNotNull) : new BoolLiteral(isNotNull);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) throws SemanticException {
        return visitor.visitIsNullPredicate(this, context);
    }
}
