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

package com.starrocks.connector.parser.trino;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.expression.ArithmeticExpr;
import com.starrocks.sql.ast.expression.BinaryPredicate;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.ast.expression.BoolLiteral;
import com.starrocks.sql.ast.expression.CastExpr;
import com.starrocks.sql.ast.expression.CollectionElementExpr;
import com.starrocks.sql.ast.expression.CompoundPredicate;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.ast.expression.IntervalLiteral;
import com.starrocks.sql.ast.expression.IsNullPredicate;
import com.starrocks.sql.ast.expression.MapExpr;
import com.starrocks.sql.ast.expression.NullLiteral;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.ast.expression.TimestampArithmeticExpr;
import com.starrocks.type.AnyMapType;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;

import java.util.Collections;
import java.util.Map;

public class ComplexFunctionCallTransformer {
    public static Expr transform(String functionName, Expr... args) {
        if (functionName.equalsIgnoreCase("date_add")) {
            if (args.length == 3 && args[0] instanceof StringLiteral) {
                StringLiteral unit = (StringLiteral) args[0];
                Expr interval = args[1];
                Expr date = args[2];
                return TrinoParserUtils.alignWithInputDatetimeType(new TimestampArithmeticExpr(functionName, date, interval,
                        unit.getStringValue()));
            }
        } else if (functionName.equalsIgnoreCase("json_format")) {
            return new CastExpr(VarcharType.VARCHAR, args[0]);
        } else if (functionName.equalsIgnoreCase("json_extract_scalar")) {
            return new CastExpr(VarcharType.VARCHAR, new FunctionCallExpr("json_query",
                    ImmutableList.of(args[0], args[1])));
        } else if (functionName.equalsIgnoreCase("map") && args.length == 0) {
            return new MapExpr(AnyMapType.ANY_MAP, Collections.emptyList());
        } else if (functionName.equalsIgnoreCase("json_array_get")) {
            if (args.length != 2) {
                throw new RuntimeException("json_array_get function must have 2 arguments");
            }
            Expr leftChild = args[0];

            if (args[0] instanceof StringLiteral) {
                leftChild = new FunctionCallExpr("parse_json", ImmutableList.of(args[0]));
            }
            Expr rightChild = new StringLiteral("$.[" + ((IntLiteral) args[1]).getValue() + "]");
            return new FunctionCallExpr("json_query", ImmutableList.of(leftChild, rightChild));
        } else if (functionName.equalsIgnoreCase("md5")) {
            Expr child = args[0];
            return new FunctionCallExpr("md5", ImmutableList.of(new FunctionCallExpr("from_binary",
                    ImmutableList.of(child, new StringLiteral("utf8")))));
        } else if (functionName.equalsIgnoreCase("sha256")) {
            Expr child = args[0];
            return new FunctionCallExpr("sha2", ImmutableList.of(new FunctionCallExpr("from_binary",
                    ImmutableList.of(child, new StringLiteral("utf8"))), new IntLiteral(256)));
        } else if (functionName.equalsIgnoreCase("last_day_of_month")) {
            Expr child = args[0];
            return new FunctionCallExpr("last_day", ImmutableList.of(child, new StringLiteral("month")));
        } else if (functionName.equalsIgnoreCase("date_diff")) {
            if (args.length != 3) {
                throw new SemanticException("date_diff function must have 3 arguments");
            }
            return new FunctionCallExpr("date_diff", ImmutableList.of(args[0], args[2], args[1]));
        } else if (functionName.equalsIgnoreCase("element_at")) {
            if (args.length != 2) {
                throw new SemanticException("element_at function must have 2 arguments");
            }
            return new CollectionElementExpr(args[0], args[1], false);
        } else if (functionName.equalsIgnoreCase("regexp_extract")) {
            // regexp_extract(string, pattern) -> regexp_extract(str, pattern, 0)
            FunctionCallExpr regexpExtractFunc = new FunctionCallExpr("regexp_extract",
                    ImmutableList.of(args[0], args[1], args.length == 3 ? args[2] : new IntLiteral(0L)));
            BinaryPredicate predicate = new BinaryPredicate(BinaryType.EQ, regexpExtractFunc, new StringLiteral(""));
            // regexp_extract -> if(regexp_extract(xxx)='', null, regexp_extract(xxx))
            return new FunctionCallExpr("if", ImmutableList.of(predicate, new NullLiteral(), regexpExtractFunc));
        } else if ((functionName.equalsIgnoreCase("rand")
                || functionName.equalsIgnoreCase("random")) && args.length > 0) {
            // random(n) -> floor(random()*n)
            // random(m, n) -> floor(random()*(n-m)+m)
            FunctionCallExpr random = new FunctionCallExpr("random", Lists.newArrayList());
            if (args.length == 1) {
                return new FunctionCallExpr("floor",
                        ImmutableList.of(new ArithmeticExpr(ArithmeticExpr.Operator.MULTIPLY, random, args[0])));
            } else {
                ArithmeticExpr subExpr = new ArithmeticExpr(ArithmeticExpr.Operator.SUBTRACT, args[1], args[0]);
                ArithmeticExpr mulExpr = new ArithmeticExpr(ArithmeticExpr.Operator.MULTIPLY, random, subExpr);
                ArithmeticExpr addExpr = new ArithmeticExpr(ArithmeticExpr.Operator.ADD, mulExpr, args[0]);
                return new FunctionCallExpr("floor", ImmutableList.of(addExpr));
            }
        } else if (functionName.equalsIgnoreCase(FunctionSet.ISNULL)) {
            if (args.length != 1) {
                throw new SemanticException("isnull function must have 1 argument");
            }
            return new IsNullPredicate(args[0], false);
        } else if (functionName.equalsIgnoreCase(FunctionSet.ISNOTNULL)) {
            if (args.length != 1) {
                throw new SemanticException("isnotnull function must have 1 argument");
            }
            return new IsNullPredicate(args[0], true);
        } else if (functionName.equalsIgnoreCase("parse_datetime") && args.length == 2
                   && args[1] instanceof StringLiteral) {
            // parse_datetime -> str_to_jodatime
            String formatString = ((StringLiteral) args[1]).getStringValue();
            // "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'" -> "yyyy-MM-ddTHH:mm:ss.SSS"
            formatString = formatString.replace("'", "").replace("Z", "");
            return new FunctionCallExpr("str_to_jodatime", java.util.List.of(args[0], new StringLiteral(formatString)));
        } else if (functionName.equalsIgnoreCase("map_agg")) {
            // map_agg(key, value) -> map_from_arrays(array_agg(key), array_agg(value))
            if (args.length != 2) {
                throw new SemanticException("map_agg function must have 2 argument");
            }
            FunctionCallExpr key = new FunctionCallExpr("array_agg", ImmutableList.of(args[0]));
            FunctionCallExpr value = new FunctionCallExpr("array_agg", ImmutableList.of(args[1]));
            return new FunctionCallExpr("map_from_arrays", ImmutableList.of(key, value));
        } else if (functionName.equalsIgnoreCase(FunctionSet.DATE)) {
            if (args.length != 1) {
                throw new SemanticException("date function must have 1 argument");
            }
            return new CastExpr(DateType.DATE, args[0]);
        } else if (functionName.equalsIgnoreCase("sequence") && (args.length == 2 || args.length == 3)) {
            return transformSequence(args);
        }
        return null;
    }

    // HOUR, MINUTE and SECOND counts per day, for whole-day interval steps
    private static final Map<String, Long> UNITS_PER_DAY = ImmutableMap.of("hour", 24L, "minute", 1440L, "second", 86400L);

    // sequence(start, stop[, step]) -> array_generate(start, stop[, step]). For dates and timestamps Trino's step is
    // an interval, while array_generate takes a constant non-negative INT count and a unit name, and takes the
    // direction from start and stop; calendar units are added to start, so month-end starts don't drift. Trino rejects
    // a zero step and a step whose sign disagrees with start and stop, so check both: a zero step here, and the
    // direction when the query runs.
    private static Expr transformSequence(Expr[] args) {
        if (args.length == 2) {
            return new FunctionCallExpr(FunctionSet.ARRAY_GENERATE, ImmutableList.copyOf(args));
        }
        Expr start = args[0];
        Expr stop = args[1];
        // the step, or null for a step expression such as -1 (an ArithmeticExpr in this dialect), checked when it runs
        Long step;
        Expr generate;
        if (args[2] instanceof IntervalLiteral) {
            IntervalLiteral interval = (IntervalLiteral) args[2];
            if (!(interval.getValue() instanceof StringLiteral)) {
                return null;
            }
            try {
                step = Long.parseLong(((StringLiteral) interval.getValue()).getStringValue().trim());
            } catch (NumberFormatException e) {
                // e.g. INTERVAL '1 12' DAY TO HOUR, which array_generate can't express
                return null;
            }
            String unit = interval.getUnitIdentifier().getDescription().toLowerCase();
            // Trino accepts whole-day steps such as INTERVAL '24' HOUR for dates. A day is the same span for a
            // StarRocks DATETIME, which has no time zone, so use days for both.
            Long unitsPerDay = UNITS_PER_DAY.get(unit);
            if (unitsPerDay != null && step % unitsPerDay == 0) {
                step /= unitsPerDay;
                unit = "day";
            }
            if (Math.abs(step) > Integer.MAX_VALUE) {
                return null;
            }
            generate = new FunctionCallExpr(FunctionSet.ARRAY_GENERATE, ImmutableList.of(start, stop,
                    new IntLiteral(Math.abs(step), IntegerType.INT), new StringLiteral(unit)));
        } else {
            step = args[2] instanceof IntLiteral ? ((IntLiteral) args[2]).getValue() : null;
            generate = new FunctionCallExpr(FunctionSet.ARRAY_GENERATE, ImmutableList.copyOf(args));
        }
        // The checks repeat start, stop and the step, which would evaluate a non-deterministic one twice
        for (Expr arg : args) {
            if (AnalyzerUtils.containsNonDeterministicFunction(arg).first) {
                return generate;
            }
        }
        // Trino returns NULL for a NULL argument before checking the step, so NULL passes both checks, and
        // array_generate returns NULL for it
        Expr result = generate;
        if (step == null || step != 0) {
            Expr ascending = new BinaryPredicate(BinaryType.LE, start.clone(), stop.clone());
            Expr descending = new BinaryPredicate(BinaryType.GE, start.clone(), stop.clone());
            Expr inDirection;
            if (step != null) {
                inDirection = step > 0 ? ascending : descending;
            } else {
                // if(NULL > 0, ...) would pick descending, so pass a NULL step explicitly
                inDirection = new FunctionCallExpr(FunctionSet.IF, ImmutableList.of(
                        new IsNullPredicate(args[2].clone(), false), new BoolLiteral(true),
                        new FunctionCallExpr(FunctionSet.IF, ImmutableList.of(
                                new BinaryPredicate(BinaryType.GT, args[2].clone(), new IntLiteral(0)), ascending,
                                descending))));
            }
            result = new FunctionCallExpr(FunctionSet.IF, ImmutableList.of(assertTrue(inDirection,
                    "sequence stop value should be greater than or equal to start value if step is greater than " +
                            "zero otherwise stop should be less than or equal to start"), result, new NullLiteral()));
        }
        if (step == null || step == 0) {
            // a zero step fails unless start or stop is NULL
            Expr nonZero = step == null ?
                    new BinaryPredicate(BinaryType.NE, args[2].clone(), new IntLiteral(0)) : new BoolLiteral(false);
            Expr nullBound = new CompoundPredicate(CompoundPredicate.Operator.OR,
                    new IsNullPredicate(start.clone(), false), new IsNullPredicate(stop.clone(), false));
            result = new FunctionCallExpr(FunctionSet.IF, ImmutableList.of(assertTrue(
                    new CompoundPredicate(CompoundPredicate.Operator.OR, nullBound, nonZero),
                    "step must not be zero"), result, new NullLiteral()));
        }
        return result;
    }

    // assert_true(coalesce(condition, true), message): fails the query when the condition is false, passes NULL
    private static Expr assertTrue(Expr condition, String message) {
        return new FunctionCallExpr("assert_true", ImmutableList.of(
                new FunctionCallExpr("coalesce", ImmutableList.of(condition, new BoolLiteral(true))),
                new StringLiteral(message)));
    }
}
