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
import com.starrocks.catalog.ArrayType;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.ScalarFunction;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.thrift.TExprNode;
import com.starrocks.thrift.TExprNodeType;
import com.starrocks.thrift.TExprOpcode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Objects;

/**
 * Fulltext match predicate
 */
public class MatchPredicate extends Predicate {
    private static final Logger LOG = LogManager.getLogger(MatchPredicate.class);

    public enum Operator {
        MATCH_TERMS("MATCH_TERM", "match_term", TExprOpcode.MATCH_TERM),
        MATCH_ALL("MATCH_ALL", "match_all", TExprOpcode.MATCH_ALL),
        MATCH_PHRASE("MATCH_PHRASE", "match_phrase", TExprOpcode.MATCH_PHRASE),
        MATCH_FUZZY("MATCH_FUZZY", "match_fuzzy", TExprOpcode.MATCH_FUZZY),
        MATCH_WILDCARD("MATCH_WILDCARD", "match_wildcard", TExprOpcode.MATCH_WILDCARD),
        MATCH_RANGE("MATCH_RANGE", "match_range", TExprOpcode.MATCH_RANGE);


        private final String description;
        private final String name;
        private final TExprOpcode opcode;

        Operator(String description,
                 String name,
                 TExprOpcode opcode) {
            this.description = description;
            this.name = name;
            this.opcode = opcode;
        }

        @Override
        public String toString() {
            return description;
        }

        public String getName() {
            return name;
        }

        public TExprOpcode getOpcode() {
            return opcode;
        }
    }

    public static void initBuiltins(FunctionSet functionSet) {
        String symbolNotUsed = "symbol_not_used";

        for (Type t : Type.getNumericTypes()) {
            // match_range(column, start, end, include_start, include_end)
            functionSet.addBuiltin(ScalarFunction.createBuiltinOperator(
                    Operator.MATCH_RANGE.getName(),
                    symbolNotUsed,
                    Lists.<Type>newArrayList(t, t, t, Type.BOOLEAN, Type.BOOLEAN),
                    Type.BOOLEAN));
        }
        // match_term(column, term)
        functionSet.addBuiltin(ScalarFunction.createBuiltinOperator(
                Operator.MATCH_TERMS.getName(),
                symbolNotUsed,
                Lists.<Type>newArrayList(Type.VARCHAR, Type.VARCHAR),
                Type.BOOLEAN));
        // match_all(column, content)
        functionSet.addBuiltin(ScalarFunction.createBuiltinOperator(
                Operator.MATCH_ALL.getName(),
                symbolNotUsed,
                Lists.<Type>newArrayList(Type.VARCHAR, Type.VARCHAR),
                Type.BOOLEAN));
        // match_phrase(column, phrase, max_slot)
        functionSet.addBuiltin(ScalarFunction.createBuiltinOperator(
                Operator.MATCH_PHRASE.getName(),
                symbolNotUsed,
                Lists.<Type>newArrayList(Type.VARCHAR, Type.VARCHAR, Type.INT),
                Type.BOOLEAN));
        // match_fuzzy(column, fuzzy_string, tolerant_words_count)
        functionSet.addBuiltin(ScalarFunction.createBuiltinOperator(
                Operator.MATCH_FUZZY.getName(),
                symbolNotUsed,
                Lists.<Type>newArrayList(Type.VARCHAR, Type.VARCHAR, Type.INT),
                Type.BOOLEAN));
        // match_wildcard(column, wildcard_expression)
        functionSet.addBuiltin(ScalarFunction.createBuiltinOperator(
                Operator.MATCH_WILDCARD.getName(),
                symbolNotUsed,
                Lists.<Type>newArrayList(Type.VARCHAR, Type.VARCHAR),
                Type.BOOLEAN));
    }

    private final Operator op;

    public MatchPredicate(Operator op, Expr e1, Expr e2) {
        super();
        this.op = op;
        Preconditions.checkNotNull(e1);
        children.add(e1);
        Preconditions.checkNotNull(e2);
        children.add(e2);
        // TODO: Calculate selectivity
        selectivity = Expr.DEFAULT_SELECTIVITY;
    }

    public Boolean isMatchElement(Operator op) {
        return Objects.equals(op.getName(), Operator.MATCH_RANGE.getName());
    }

    protected MatchPredicate(MatchPredicate other) {
        super(other);
        op = other.op;
    }

    @Override
    public Expr clone() {
        return new MatchPredicate(this);
    }

    public Operator getOp() {
        return this.op;
    }

    @Override
    public boolean equals(Object obj) {
        if (!super.equals(obj)) {
            return false;
        }
        return ((MatchPredicate) obj).op == op;
    }

    @Override
    public String toSqlImpl() {
        return getChild(0).toSql() + " " + op.toString() + " " + getChild(1).toSql();
    }

    @Override
    protected void toThrift(TExprNode msg) {
        // TODO: Modify this to MATCH_PRED
        msg.node_type = TExprNodeType.LITERAL_PRED;
        msg.setOpcode(op.getOpcode());
    }

    @Override
    public void analyzeImpl(Analyzer analyzer) throws AnalysisException {
        super.analyzeImpl(analyzer);
        if (isMatchElement(op) && !getChild(0).getType().isArrayType()) {
            throw new AnalysisException(
                    "left operand of " + op + " must be Array: " + toSql());
        }
        if (getChild(0).getType().isHllType()) {
            throw new AnalysisException(
                    "left operand of " + op + " must not be Bitmap or HLL: " + toSql());
        }
        if (!isMatchElement(op) && !getChild(1).getType().isStringType() && !getChild(1).getType().isNull()) {
            throw new AnalysisException("right operand of " + op + " must be of type STRING: " + toSql());
        }

        if (!getChild(0).getType().isStringType() && !getChild(0).getType().isArrayType()) {
            throw new AnalysisException(
                    "left operand of " + op + " must be of type STRING or ARRAY: " + toSql());
        }

        fn = getBuiltinFunction(op.toString(),
                collectChildReturnTypes(), Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        if (fn == null) {
            throw new AnalysisException(
                    "no function found for " + op + " " + toSql());
        }
        Expr e1 = getChild(0);
        Expr e2 = getChild(1);
        // Here we cast match_element_xxx value type from string to array item type.
        // Because be need to know the actual TExprNodeType when doing Expr Literal transform
        if (isMatchElement(op) && e1.type.isArrayType() && (e2 instanceof StringLiteral)) {
            Type itemType = ((ArrayType) e1.type).getItemType();
            try {
                setChild(1, e2.castTo(itemType));
            } catch (NumberFormatException nfe) {
                throw new AnalysisException("Invalid number format literal: " + ((StringLiteral) e2).getStringValue());
            }
        }
    }

    @Override
    public int hashCode() {
        return 31 * super.hashCode() + Objects.hashCode(op);
    }

}
