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
import com.google.common.collect.Lists;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.ScalarFunction;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.sql.analyzer.ExprVisitor;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.thrift.TExprNode;
import com.starrocks.thrift.TExprNodeType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

// Our new cost based query optimizer is more powerful and stable than old query optimizer,
// The old query optimizer related codes could be deleted safely.
// TODO: Remove old query optimizer related codes before 2021-09-30
public class IsNullPredicate extends Predicate {
    private static final Logger LOG = LogManager.getLogger(IsNullPredicate.class);
    private static final String IS_NULL = "is_null_pred";
    private static final String IS_NOT_NULL = "is_not_null_pred";

    public static void initBuiltins(FunctionSet functionSet) {
        for (Type t : Type.getSupportedTypes()) {
            if (t.isNull()) {
                continue;
            }
            String isNullSymbol;
            if (t == Type.BOOLEAN) {
                isNullSymbol = "_ZN9starrocks15IsNullPredicate7is_nullIN13starrocks_udf10BooleanValE" +
                        "EES3_PNS2_15FunctionContextERKT_";
            } else if (!t.isPseudoType()) {
                String udfType = Function.getUdfType(t.getPrimitiveType());
                isNullSymbol = "_ZN9starrocks15IsNullPredicate7is_nullIN13starrocks_udf" +
                        udfType.length() + udfType +
                        "EEENS2_10BooleanValEPNS2_15FunctionContextERKT_";
            } else {
                // Pseudo types support.
                // NOTE: only the vectorized engine support pseudo types and the vectorized engine will not
                // use the following registered functions, but we have to register them in order to pass
                // the analysis.
                isNullSymbol = "non_exists_symbol_for_pseudo_types";
            }

            functionSet.addBuiltin(ScalarFunction.createBuiltinOperator(
                    IS_NULL, isNullSymbol, Lists.newArrayList(t), Type.BOOLEAN));

            String isNotNullSymbol = isNullSymbol.replace("7is_null", "11is_not_null");
            functionSet.addBuiltin(ScalarFunction.createBuiltinOperator(
                    IS_NOT_NULL, isNotNullSymbol, Lists.newArrayList(t), Type.BOOLEAN));
        }
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

    public boolean isSlotRefChildren() {
        return (children.get(0) instanceof SlotRef);
    }

    @Override
    public void analyzeImpl(Analyzer analyzer) throws AnalysisException {
        super.analyzeImpl(analyzer);

        if (isNotNull) {
            fn = getBuiltinFunction(
                    analyzer, IS_NOT_NULL, collectChildReturnTypes(), Function.CompareMode.IS_INDISTINGUISHABLE);
        } else {
            fn = getBuiltinFunction(
                    analyzer, IS_NULL, collectChildReturnTypes(), Function.CompareMode.IS_INDISTINGUISHABLE);
        }
        Preconditions.checkState(fn != null, "tupleisNull fn == NULL");

        // determine selectivity
        selectivity = 0.1;
        // LOG.debug(toSql() + " selectivity: " + Double.toString(selectivity));
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

    @Override
    public boolean isVectorized() {
        for (Expr expr : children) {
            if (!expr.isVectorized()) {
                return false;
            }
        }

        return true;
    }

    @Override
    public boolean isStrictPredicate() {
        Expr child = getChild(0);
        if (child.unwrapSlotRef() != null) {
            return isNotNull;
        } else {
            return false;
        }
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
    public <R, C> R accept(ExprVisitor<R, C> visitor, C context) throws SemanticException {
        return visitor.visitIsNullPredicate(this, context);
    }
}
