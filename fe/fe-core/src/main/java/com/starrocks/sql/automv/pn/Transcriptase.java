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

package com.starrocks.sql.automv.pn;

import com.google.common.base.Preconditions;
import com.starrocks.sql.ast.expression.ArithmeticExpr;
import com.starrocks.sql.automv.boolalgebra.Modifier;
import com.starrocks.sql.automv.boolalgebra.TriBool;
import com.starrocks.sql.automv.column.ColumnAlias;
import com.starrocks.sql.automv.util.PrettyPrinter;
import com.starrocks.sql.automv.util.TieredMap;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class Transcriptase {
    public static String transcript(Op op, TieredMap<Integer, ColumnAlias> columnAliases) {
        return new Visitor().transcript(op, columnAliases);
    }

    public static class Context {
        private final TieredMap<Integer, ColumnAlias> columnAlias;
        private final List<Result> argResults;

        private Context(TieredMap<Integer, ColumnAlias> columnAlias, List<Result> argResults) {
            this.columnAlias = Objects.requireNonNull(columnAlias);
            this.argResults = Objects.requireNonNull(argResults);
        }

        public static Context of(TieredMap<Integer, ColumnAlias> columnAlias) {
            return new Context(columnAlias, Collections.emptyList());
        }

        public List<Result> getArgResults() {
            return Objects.requireNonNull(argResults);
        }

        public String arg(int i) {
            int n = argResults.size();
            Preconditions.checkArgument(-n <= i && i < n);
            int idx = (i < 0) ? i + n : i;
            return argResults.get(idx).getSql();
        }

        public List<String> args() {
            return argResults.stream().map(Result::getSql).collect(Collectors.toList());
        }

        public TieredMap<Integer, ColumnAlias> getColumnAlias() {
            return columnAlias;
        }

        public Context captureArgResults(List<Result> argResults) {
            return new Context(this.columnAlias, argResults);
        }
    }

    public static class Result {
        private final String sql;

        private Result(String sql) {
            this.sql = Objects.requireNonNull(sql);
        }

        public static Result of(String sql) {
            return new Result(sql);
        }

        public String getSql() {
            return sql;
        }
    }

    public static class Visitor extends OpVisitor<Result, Context> {
        @Override
        public Result visitVal(Val val, Context context) {
            if (val.getType().isStringType() || val.getType().isDateType()) {
                return Result.of(PrettyPrinter.escapedDoubleQuoted(val.getValue().toString()).getResult());
            } else if (val.getValue().isConstantNull()) {
                return Result.of("NULL");
            } else {
                return Result.of(val.getValue().toString());
            }
        }

        @Override
        public Result visitVar(Var var, Context context) {
            if (context.getColumnAlias().get(var.getId()) == null) {
                System.out.print("SATANSON");
            }
            return Result.of(Objects.requireNonNull(context.getColumnAlias().get(var.getId())).getQualifiedName());
        }

        @Override
        public Result visitApply(Apply apply, Context context) {
            if (apply.isCompareOp()) {
                String a = context.arg(0);
                String b = context.arg(1);
                String infixOp = Objects.requireNonNull(Op.COMPARE_OP_TABLE.get(apply.getKind()));
                return Result.of(String.format("(%s %s %s)", a, infixOp, b));
            } else if (ArithmeticExpr.SUPPORT_FUNCTIONS.containsKey(apply.getKind().toString())) {
                ArithmeticExpr.Operator operator = ArithmeticExpr.SUPPORT_FUNCTIONS.get(apply.getKind().toString());
                if (operator.isBinary()) {
                    String a = context.arg(0);
                    String b = context.arg(1);
                    String infixOp = operator.getDescription();
                    return Result.of(String.format("(%s %s %s)", a, infixOp, b));
                } else {
                    String a = context.arg(0);
                    String unaryOp = operator.getDescription();
                    return Result.of(String.format("(%s %s)", unaryOp, a));
                }
            } else if (apply.isSetOf()) {
                return Result.of("(" + String.join(", ", context.args()) + ")");

            } else if (apply.isModify()) {
                Val modify = apply.arg(0).cast();
                Modifier modifier = Modifier.valueOf(modify.getValue().toString());
                String modified = context.arg(1);
                switch (modifier) {
                    case M_ALWAYS_FALSE:
                        return Result.of("FALSE");
                    case M_POSITIVE:
                        return Result.of(modified);
                    case M_IS_NULL:
                        return Result.of(String.format("(%s IS NULL)", modified));
                    case M_TRUE_OR_NULL:
                        return Result.of(String.format("(%s OR (%s is NULL))", modified, modified));
                    case M_ALWAYS_NULL:
                        return Result.of("NULL");
                    case M_IS_NOT_NULL:
                        return Result.of(String.format("(%s IS NOT NULL)", modified));
                    case M_NEGATIVE:
                        return Result.of(String.format("(NOT %s)", modified));
                    case M_FALSE_OR_NULL:
                        return Result.of(String.format("IF(%s, FALSE, TRUE)", modified));
                    case M_ALWAYS_TRUE:
                        return Result.of("TRUE");
                    default: {
                        TriBool falseImg = modifier.transfer(0);
                        TriBool nullImg = modifier.transfer(0);
                        TriBool trueImg = modifier.transfer(0);
                        String caseWhen = String.format(
                                "(CASE WHEN %s THEN %s WHEN (NOT %s) THEN %s ELSE %s END)",
                                modified, trueImg.getSql(),
                                modified, falseImg.getSql(),
                                nullImg.getSql());
                        return Result.of(caseWhen);
                    }
                }
            } else if (apply.isIn()) {
                String item = context.arg(0);
                String valueSet = context.arg(1);
                return Result.of(String.format("%s in %s", item, valueSet));
            } else if (apply.isInRange() || apply.isRangeOf()) {
                throw new IllegalArgumentException("Apply should not be inRange or rangeOf types");
            } else if (apply.isCase()) {
                int nArgs = apply.getArgs().size();
                Optional<String> elseBranch = Optional.empty();
                if (nArgs % 2 == 1) {
                    elseBranch = Optional.of(this.transcript(apply.arg(-1), context.getColumnAlias()));
                }
                int numWhens = nArgs / 2;
                List<Op> whens = IntStream.range(0, numWhens)
                        .mapToObj(i -> apply.arg(i * 2))
                        .collect(Collectors.toList());
                List<Op> thens = IntStream.range(0, numWhens)
                        .mapToObj(i -> apply.arg(i * 2 + 1))
                        .collect(Collectors.toList());
                if (whens.stream().allMatch(Op::isEq) && whens.stream()
                        .map(ve -> ve.arg(0).strict())
                        .collect(Collectors.toSet()).size() == 1) {
                    Op caseOp = whens.get(0).arg(0);

                    String caseClause = this.transcript(caseOp, context.getColumnAlias());
                    List<String> whenClauses = whens.stream()
                            .map(w -> w.arg(1))
                            .map(w -> this.transcript(w, context.getColumnAlias()))
                            .collect(Collectors.toList());
                    List<String> thenClauses = thens.stream()
                            .map(t -> this.transcript(t, context.getColumnAlias()))
                            .collect(Collectors.toList());
                    StringBuilder sb = new StringBuilder();
                    sb.append("(CASE ").append(caseClause).append(" ");
                    for (int i = 0; i < numWhens; ++i) {
                        sb.append("WHEN ").append(whenClauses.get(i)).append(" ");
                        sb.append("THEN ").append(thenClauses.get(i)).append(" ");
                    }
                    elseBranch.ifPresent(alt -> sb.append("ELSE ").append(alt).append(" "));
                    sb.append("END)");
                    return Result.of(sb.toString());
                } else {
                    List<String> whenClauses = IntStream.range(0, numWhens)
                            .mapToObj(i -> context.arg(i * 2))
                            .collect(Collectors.toList());
                    List<String> thenClauses = IntStream.range(0, numWhens)
                            .mapToObj(i -> context.arg(i * 2 + 1))
                            .collect(Collectors.toList());
                    StringBuilder sb = new StringBuilder();
                    sb.append("(CASE ");
                    for (int i = 0; i < numWhens; ++i) {
                        sb.append("WHEN ").append(whenClauses.get(i)).append(" ");
                        sb.append("THEN ").append(thenClauses.get(i)).append(" ");
                    }
                    elseBranch.ifPresent(alt -> sb.append("ELSE ").append(alt).append(" "));
                    sb.append("END)");
                    return Result.of(sb.toString());
                }
            } else if (apply.isCast()) {
                String a = context.arg(0);
                String t = apply.getType().toTypeString();
                return Result.of(String.format("CAST(%s AS %s)", a, t));
            } else if (apply.isAnd()) {
                return Result.of("(" + String.join(" AND ", context.args()) + ")");
            } else if (apply.isOr()) {
                return Result.of("(" + String.join(" OR ", context.args()) + ")");
            } else if (apply.isArray()) {
                return Result.of("[" + String.join(", ", context.args()) + "]");
            } else if (apply.isMap()) {
                Preconditions.checkArgument(context.args().size() % 2 == 0);
                String kvs = IntStream.range(0, context.args().size() / 2)
                        .mapToObj(i -> String.format("%s: %s", context.arg(i * 2), context.arg(i * 2 + 1)))
                        .collect(Collectors.joining(", "));
                return Result.of(String.format("map{%s}", kvs));
            } else if (apply.isArraySlice()) {
                String xs = context.arg(0);
                String l = context.arg(1);
                String u = context.arg(2);
                return Result.of(String.format("%s[%s:%s]", xs, l, u));
            } else if (apply.isCollectionElement()) {
                String xs = context.arg(0);
                String i = context.arg(1);
                return Result.of(String.format("%s[%s]", xs, i));
            } else if (apply.isSubfield()) {
                String s = Stream.concat(
                                Stream.of(context.arg(0)),
                                apply.getArgs().stream().skip(1)
                                        .map(arg -> arg.mustCast(Val.class).getValue().getVarchar()))
                        .collect(Collectors.joining("."));
                return Result.of(s);
            } else if (apply.isLike()) {
                String s = context.arg(0);
                String p = context.arg(1);
                return Result.of(String.format("(%s like %s)", s, p));
            } else if (apply.isRegexp()) {
                String s = context.arg(0);
                String p = context.arg(1);
                return Result.of(String.format("(%s regexp %s)", s, p));
            } else if (apply.isLambda()) {
                String body = context.arg(0);
                String locals = context.args().stream().skip(1).collect(Collectors.joining(", "));
                return Result.of(String.format("(%s)->%s", locals, body));
            } else if (apply.isLocal()) {
                String local = "x_" + context.arg(0);
                Preconditions.checkArgument(context.getColumnAlias().values()
                        .stream().noneMatch(alias -> alias.getQualifiedName().equals(local)));
                return Result.of(local);
            } else if (apply.isDistinct()) {
                return Result.of("DISTINCT " + String.join(", ", context.args()));
            } else {
                return Result.of(String.format("%s(%s)", apply.getKind(), String.join(", ", context.args())));
            }
        }

        private Result transcriptImpl(Op op, Context context) {
            List<Result> results = op.getArgs().stream()
                    .map(arg -> transcriptImpl(arg, context))
                    .collect(Collectors.toList());
            return op.accept(this, context.captureArgResults(results));
        }

        private String transcript(Op op, TieredMap<Integer, ColumnAlias> columnAliases) {
            Context context = Context.of(columnAliases);
            Result result = transcriptImpl(op.eliminateInRange(), context);
            return result.getSql();
        }
    }
}
