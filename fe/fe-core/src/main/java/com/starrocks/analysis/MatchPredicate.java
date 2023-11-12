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

package com.starrocks.analysis;


import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.ScalarFunction;
import com.starrocks.catalog.Type;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.thrift.TExprNode;
import com.starrocks.thrift.TExprNodeType;
import com.starrocks.thrift.TExprOpcode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Objects;

/**
 * Fulltext match predicate
 */
public abstract class MatchPredicate extends Predicate {

    private static final Logger LOG = LogManager.getLogger(MatchPredicate.class);

    // MATCH_TERM/MATCH_ALL/MATCH_WILDCARD
    public static class MatchBinaryPredicate extends MatchPredicate {

        protected MatchBinaryPredicate(MatchBinaryPredicate other) {
            super(other);
        }

        public MatchBinaryPredicate(Expr e1, Expr e2, Operator op) {
            super(op);
            Preconditions.checkNotNull(e1);
            children.add(e1);
            Preconditions.checkNotNull(e2);
            children.add(e2);
        }

        @Override
        public Expr clone() {
            return new MatchBinaryPredicate(this);
        }
    }

    // MATCH_PHRASE/MATCH_FUZZY
    public static class MatchTriplePredicate extends MatchPredicate {

        protected MatchTriplePredicate(MatchTriplePredicate other) {
            super(other);
        }

        public MatchTriplePredicate(Expr column, Expr e2, Expr e3, Operator op) {
            super(op);
            Preconditions.checkNotNull(column);
            children.add(column);
            Preconditions.checkNotNull(e2);
            children.add(e2);
            Preconditions.checkNotNull(e3);
            children.add(e3);

        }

        @Override
        public Expr clone() {
            return new MatchTriplePredicate(this);
        }
    }

    // MATCH_RANGE
    public static class MatchRangePredicate extends MatchPredicate {

        protected MatchRangePredicate(MatchRangePredicate other) {
            super(other);
        }

        public MatchRangePredicate(Expr column, Expr start, Expr end, Expr startInclude, Expr endInclude) {
            super(Operator.MATCH_RANGE);
            Preconditions.checkNotNull(column);
            children.add(column);
            Preconditions.checkNotNull(start);
            children.add(start);
            Preconditions.checkNotNull(end);
            children.add(end);
            Preconditions.checkNotNull(startInclude);
            children.add(startInclude);
            Preconditions.checkNotNull(endInclude);
            children.add(endInclude);
        }

        @Override
        public Expr clone() {
            return new MatchRangePredicate(this);
        }
    }

    public enum Operator {
        MATCH_TERM("MATCH_TERM", "match_term", TExprOpcode.MATCH_TERM),
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
        // match_range(column, start, end, include_start, include_end)
        functionSet.addBuiltin(ScalarFunction.createBuiltinOperator(
                Operator.MATCH_RANGE.getName(),
                symbolNotUsed,
                Lists.<Type>newArrayList(Type.VARCHAR, Type.VARCHAR, Type.VARCHAR, Type.BOOLEAN, Type.BOOLEAN),
                Type.BOOLEAN));

        // match_term(column, term)
        functionSet.addBuiltin(ScalarFunction.createBuiltinOperator(
                Operator.MATCH_TERM.getName(),
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

    public Boolean isMatchElement(Operator op) {
        return Objects.equals(op.getName(), Operator.MATCH_TERM.getName())
                || Objects.equals(op.getName(), Operator.MATCH_ALL.getName())
                || Objects.equals(op.getName(), Operator.MATCH_PHRASE.getName())
                || Objects.equals(op.getName(), Operator.MATCH_FUZZY.getName())
                || Objects.equals(op.getName(), Operator.MATCH_WILDCARD.getName())
                || Objects.equals(op.getName(), Operator.MATCH_RANGE.getName());
    }

    protected MatchPredicate(Operator op) {
        super();
        this.op = op;
    }

    protected MatchPredicate(MatchPredicate other) {
        super(other);
        this.op = other.op;
    }

    public Operator getOp() {
        return this.op;
    }

    @Override
    public String toSqlImpl() {
        return String.format("%s(%s)", op.toString(), String.join(",", childrenToSql()));
    }

    @Override
    protected void toThrift(TExprNode msg) {
        // TODO: Modify this to MATCH_PRED
        msg.node_type = TExprNodeType.LITERAL_PRED;
        msg.setOpcode(op.getOpcode());
    }

    @Override
    public int hashCode() {
        return 31 * super.hashCode() + Objects.hashCode(op);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitMatchPredicate(this, context);
    }
}
