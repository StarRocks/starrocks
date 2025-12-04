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

import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.FunctionName;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.TableName;
import com.starrocks.common.Pair;
import com.starrocks.common.util.UnionFind;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.expression.ArithmeticExpr;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.automv.column.ColumnAlias;
import com.starrocks.sql.automv.column.ColumnRefToIdConverter;
import com.starrocks.sql.automv.column.GenericColumn;
import com.starrocks.sql.automv.column.OriginalColumn;
import com.starrocks.sql.automv.util.EitherOr;
import com.starrocks.sql.automv.util.TieredList;
import com.starrocks.sql.automv.util.TieredMap;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.scalar.ArrayOperator;
import com.starrocks.sql.optimizer.operator.scalar.ArraySliceOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CaseWhenOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.CollectionElementOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.LambdaFunctionOperator;
import com.starrocks.sql.optimizer.operator.scalar.LikePredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.MapOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.SubfieldOperator;
import com.starrocks.type.ArrayType;
import com.starrocks.type.BitmapType;
import com.starrocks.type.DateType;
import com.starrocks.type.FloatType;
import com.starrocks.type.HLLType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.MapType;
import com.starrocks.type.PercentileType;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.Type;
import com.starrocks.type.TypeFactory;
import com.starrocks.type.VarcharType;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.starrocks.catalog.Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF;

public class OpTest {
    private final TableName tableName = new TableName("default_catalog", "test_db", "t");
    private final List<ColumnRefOperator> columnRefs =
            Stream.<Stream<Pair<Integer, Type>>>of(
                            IntStream.range(0, 3).boxed().map(i -> Pair.create(i, VarcharType.VARCHAR)),
                            IntStream.range(3, 6).boxed().map(i -> Pair.create(i, IntegerType.INT)),
                            Stream.of(Pair.create(6, TypeFactory.createDecimalV3NarrowestType(38, 9))),
                            Stream.of(Pair.create(7, TypeFactory.createDecimalV3NarrowestType(15, 3))),
                            Stream.of(Pair.create(8, TypeFactory.createDecimalV3NarrowestType(7, 2))),
                            Stream.of(Pair.create(9, FloatType.DOUBLE)))
                    .flatMap(Function.identity())
                    .map(p -> new ColumnRefOperator(p.first, p.second, "c" + p.first, false))
                    .collect(Collectors.toList());

    private final List<Var> vars = columnRefs.stream()
            .map(colRef -> Op.var(colRef.getType(), colRef.getId()))
            .collect(Collectors.toList());
    private final TieredMap<Integer, GenericColumn> inputColumns = IntStream.range(0, 10)
            .mapToObj(i -> Pair.create(i + 1, GenericColumn.original(tableName, new Column("c" + i, VarcharType.VARCHAR))))
            .collect(TieredMap.toMap(p -> p.first, p -> p.second));

    private final TieredMap<Integer, ColumnAlias> columnAliases = inputColumns.entrySet().stream()
            .map(e -> Pair.create(e.getKey(), e.getValue().mustCast(OriginalColumn.class)))
            .collect(TieredMap.toMap(
                    p -> p.first,
                    p -> ColumnAlias.of(p.second.getFqTableName().toSql(), p.second.getColumnName())));
    private final Function<Op, String> opToSql = OpUtil.toOpToSqlConverter(columnAliases);
    private final Function<ScalarOperator, Op> opConverter;
    private final ColumnRefToIdConverter idConverter = new ColumnRefToIdConverter();

    {
        columnRefs.forEach(idConverter::getId);
        opConverter = OpUtil.toOpConverter(idConverter, inputColumns);
    }

    private void testHelper(ScalarOperator operator,
                            String s) {
        Op op = opConverter.apply(operator);
        String actual = opToSql.apply(op);
        Assert.assertEquals(String.format("op=%s\nexpect=%s\nactual=%s", op, s, actual), s, actual);
    }

    @Test
    public void testCaseWhenNoCaseNoElse() {
        List<ScalarOperator> whenThens = Arrays.asList(
                new InPredicateOperator(columnRefs.get(0), ConstantOperator.createVarchar("A")),
                ConstantOperator.createVarchar("a"),
                new InPredicateOperator(columnRefs.get(0), ConstantOperator.createVarchar("B"),
                        ConstantOperator.createVarchar("C")),
                ConstantOperator.createVarchar("b"),
                new LikePredicateOperator(columnRefs.get(0), ConstantOperator.createVarchar("%E%")),
                columnRefs.get(1)
        );
        CaseWhenOperator caseWhen =
                new CaseWhenOperator(VarcharType.VARCHAR, null, null, whenThens);
        testHelper(caseWhen, "(CASE WHEN `test_db`.`t`.c0 in (\"A\") THEN \"a\" " +
                "WHEN `test_db`.`t`.c0 in (\"B\", \"C\") THEN \"b\" " +
                "WHEN (`test_db`.`t`.c0 like \"%E%\") THEN `test_db`.`t`.c1 " +
                "ELSE NULL END)");
    }

    @Test
    public void testCaseWhenNoCaseHasElse() {
        List<ScalarOperator> whenThens = Arrays.asList(
                new InPredicateOperator(columnRefs.get(0), ConstantOperator.createVarchar("A")),
                ConstantOperator.createVarchar("a"),
                new InPredicateOperator(columnRefs.get(0), ConstantOperator.createVarchar("B"),
                        ConstantOperator.createVarchar("C")),
                ConstantOperator.createVarchar("b"),
                new LikePredicateOperator(columnRefs.get(0), ConstantOperator.createVarchar("%E%")),
                columnRefs.get(1)
        );
        ScalarOperator elseClause = new CallOperator("IF", VarcharType.VARCHAR,
                Arrays.asList(
                        CompoundPredicateOperator.or(
                                new IsNullPredicateOperator(false, columnRefs.get(2)),
                                new IsNullPredicateOperator(true, columnRefs.get(3))
                        ),
                        new CastOperator(VarcharType.VARCHAR,
                                new CallOperator("add", IntegerType.INT,
                                        Arrays.asList(
                                                new CastOperator(IntegerType.INT, columnRefs.get(4)),
                                                new CastOperator(IntegerType.INT, columnRefs.get(5))), null)),
                        new CastOperator(VarcharType.VARCHAR,
                                new CallOperator("mod", IntegerType.INT,
                                        Arrays.asList(
                                                new CastOperator(IntegerType.INT, columnRefs.get(6)),
                                                new CastOperator(IntegerType.INT, columnRefs.get(7))), null))

                ));
        CaseWhenOperator caseWhen =
                new CaseWhenOperator(VarcharType.VARCHAR, null, elseClause, whenThens);
        testHelper(caseWhen, "(CASE WHEN `test_db`.`t`.c0 in (\"A\") THEN \"a\" " +
                "WHEN `test_db`.`t`.c0 in (\"B\", \"C\") THEN \"b\" " +
                "WHEN (`test_db`.`t`.c0 like \"%E%\") THEN `test_db`.`t`.c1 " +
                "ELSE IF(((`test_db`.`t`.c3 IS NOT NULL) OR (`test_db`.`t`.c2 IS NULL)), " +
                "CAST((CAST(`test_db`.`t`.c4 AS int) + CAST(`test_db`.`t`.c5 AS int)) AS varchar), " +
                "CAST((CAST(`test_db`.`t`.c6 AS int) % CAST(`test_db`.`t`.c7 AS int)) AS varchar)) " +
                "END)");

    }

    @Test
    public void testCaseWhenNoCaseHasElse2() {
        List<ScalarOperator> whenThens = Arrays.asList(
                new BinaryPredicateOperator(BinaryType.EQ, columnRefs.get(0), ConstantOperator.createVarchar("A")),
                ConstantOperator.createNull(VarcharType.VARCHAR)
        );
        ScalarOperator elseClause = columnRefs.get(1);
        CaseWhenOperator caseWhen =
                new CaseWhenOperator(VarcharType.VARCHAR, null, elseClause, whenThens);

        testHelper(caseWhen, "(CASE `test_db`.`t`.c0 WHEN \"A\" THEN NULL ELSE `test_db`.`t`.c1 END)");
    }

    @Test
    public void testCaseWhenHasCaseNoElse() {
        List<ScalarOperator> whenThens = Arrays.asList(
                ConstantOperator.createVarchar("A"),
                ConstantOperator.createVarchar("a"),
                new CallOperator(FunctionSet.CONCAT, VarcharType.VARCHAR,
                        Arrays.asList(
                                ConstantOperator.createVarchar("B"),
                                ConstantOperator.createVarchar("C"))),
                ConstantOperator.createVarchar("b"),
                new CallOperator(FunctionSet.COALESCE, VarcharType.VARCHAR,
                        Arrays.asList(
                                columnRefs.get(1),
                                columnRefs.get(2),
                                columnRefs.get(3),
                                columnRefs.get(4),
                                columnRefs.get(5),
                                ConstantOperator.createVarchar("GOOD"))),
                columnRefs.get(1)
        );
        ScalarOperator caseClause = new CallOperator("IF", VarcharType.VARCHAR,
                Arrays.asList(
                        CompoundPredicateOperator.or(
                                new IsNullPredicateOperator(false, columnRefs.get(2)),
                                new IsNullPredicateOperator(true, columnRefs.get(3))
                        ),
                        new CastOperator(VarcharType.VARCHAR,
                                new CallOperator("add", IntegerType.INT,
                                        Arrays.asList(
                                                new CastOperator(IntegerType.INT, columnRefs.get(4)),
                                                new CastOperator(IntegerType.INT, columnRefs.get(5))), null)),
                        new CastOperator(VarcharType.VARCHAR,
                                new CallOperator("mod", IntegerType.INT,
                                        Arrays.asList(
                                                new CastOperator(IntegerType.INT, columnRefs.get(6)),
                                                new CastOperator(IntegerType.INT, columnRefs.get(7))), null))

                ));
        CaseWhenOperator caseWhen =
                new CaseWhenOperator(VarcharType.VARCHAR, caseClause, null, whenThens);
        testHelper(caseWhen, "(CASE IF(((`test_db`.`t`.c3 IS NOT NULL) OR (`test_db`.`t`.c2 IS NULL)), " +
                "CAST((CAST(`test_db`.`t`.c4 AS int) + CAST(`test_db`.`t`.c5 AS int)) AS varchar), " +
                "CAST((CAST(`test_db`.`t`.c6 AS int) % CAST(`test_db`.`t`.c7 AS int)) AS varchar)) " +
                "WHEN \"A\" THEN \"a\" " +
                "WHEN concat(\"B\", \"C\") THEN \"b\" " +
                "WHEN coalesce(`test_db`.`t`.c1, `test_db`.`t`.c2, " +
                "`test_db`.`t`.c3, `test_db`.`t`.c4, `test_db`.`t`.c5, \"GOOD\") THEN `test_db`.`t`.c1 " +
                "ELSE NULL " +
                "END)");
    }

    @Test
    public void testCaseWhenHasCaseHasElse() {
        List<ScalarOperator> whenThens = Arrays.asList(
                ConstantOperator.createVarchar("A"),
                ConstantOperator.createVarchar("a"),
                new CallOperator(FunctionSet.CONCAT, VarcharType.VARCHAR,
                        Arrays.asList(
                                ConstantOperator.createVarchar("B"),
                                ConstantOperator.createVarchar("C"))),
                ConstantOperator.createVarchar("b"),
                new CallOperator(FunctionSet.SUBSTR, VarcharType.VARCHAR,
                        Arrays.asList(
                                columnRefs.get(8),
                                ConstantOperator.createInt(2),
                                ConstantOperator.createInt(4))),
                columnRefs.get(1)
        );
        ScalarOperator caseClause = columnRefs.get(9);
        ScalarOperator elseClause = new CallOperator("IF", VarcharType.VARCHAR,
                Arrays.asList(
                        CompoundPredicateOperator.or(
                                new IsNullPredicateOperator(false, columnRefs.get(2)),
                                new IsNullPredicateOperator(true, columnRefs.get(3))
                        ),
                        new CastOperator(VarcharType.VARCHAR,
                                new CallOperator("add", IntegerType.INT,
                                        Arrays.asList(
                                                new CastOperator(IntegerType.INT, columnRefs.get(4)),
                                                new CastOperator(IntegerType.INT, columnRefs.get(5))), null)),
                        new CastOperator(VarcharType.VARCHAR,
                                new CallOperator("mod", IntegerType.INT,
                                        Arrays.asList(
                                                new CastOperator(IntegerType.INT, columnRefs.get(6)),
                                                new CastOperator(IntegerType.INT, columnRefs.get(7))), null))

                ));
        CaseWhenOperator caseWhen =
                new CaseWhenOperator(VarcharType.VARCHAR, caseClause, elseClause, whenThens);
        testHelper(caseWhen, "(CASE `test_db`.`t`.c9 WHEN \"A\" THEN \"a\" " +
                "WHEN concat(\"B\", \"C\") THEN \"b\" " +
                "WHEN substr(`test_db`.`t`.c8, 2, 4) THEN `test_db`.`t`.c1 " +
                "ELSE IF(((`test_db`.`t`.c3 IS NOT NULL) OR (`test_db`.`t`.c2 IS NULL)), " +
                "CAST((CAST(`test_db`.`t`.c4 AS int) + CAST(`test_db`.`t`.c5 AS int)) AS varchar), " +
                "CAST((CAST(`test_db`.`t`.c6 AS int) % CAST(`test_db`.`t`.c7 AS int)) AS varchar)) " +
                "END)");

    }

    @Test
    public void testOp() {
        List<ScalarOperator> lst = Arrays.asList(
                ConstantOperator.createVarchar("abcd"),
                columnRefs.get(0),
                ConstantOperator.createVarchar("defg"),
                columnRefs.get(1));
        ArrayOperator arrayOp = new ArrayOperator(ArrayType.ARRAY_VARCHAR, true, lst);

        ArraySliceOperator arraySliceOp =
                new ArraySliceOperator(VarcharType.VARCHAR,
                        Arrays.asList(arrayOp, ConstantOperator.createInt(1), ConstantOperator.createInt(3)));

        MapOperator mapOp = new MapOperator(MapType.MAP_VARCHAR_VARCHAR, lst);

        CollectionElementOperator arrayElem =
                new CollectionElementOperator(VarcharType.VARCHAR, arrayOp, ConstantOperator.createInt(3), true);
        SubfieldOperator subFieldOp =
                new SubfieldOperator(columnRefs.get(0), VarcharType.VARCHAR, Arrays.asList("a", "b", "c"));

        ColumnRefOperator argx = new ColumnRefOperator(101, VarcharType.VARCHAR, "x", true);
        ColumnRefOperator argy = new ColumnRefOperator(102, VarcharType.VARCHAR, "y", true);
        List<ColumnRefOperator> lambdaArgs = Arrays.asList(argx, argy);
        ScalarOperator lambdaBody = new CallOperator(ArithmeticExpr.Operator.ADD.getName(), IntegerType.INT,
                Arrays.asList(
                        new CastOperator(IntegerType.INT, argx),
                        new CastOperator(IntegerType.INT, argy))
        );
        LambdaFunctionOperator simpleLambda = new LambdaFunctionOperator(lambdaArgs, lambdaBody, IntegerType.INT);
        CallOperator arrayMapSimpleLambda = new CallOperator(FunctionSet.ARRAY_MAP, ArrayType.ARRAY_INT,
                Arrays.asList(
                        simpleLambda,
                        new ArrayOperator(ArrayType.ARRAY_VARCHAR, true, ImmutableList.of(columnRefs.get(0))),
                        new ArrayOperator(ArrayType.ARRAY_VARCHAR, true, ImmutableList.of(columnRefs.get(1)))
                ));
        ColumnRefOperator argz = new ColumnRefOperator(103, VarcharType.VARCHAR, "x", true);
        ScalarOperator outerLambdaBody = new CallOperator(FunctionSet.LEFT, VarcharType.VARCHAR,
                Arrays.asList(
                        new CallOperator(FunctionSet.CONCAT, VarcharType.VARCHAR,
                                Arrays.asList(argz, ConstantOperator.createVarchar("0000000"))),
                        ConstantOperator.createInt(10)));

        LambdaFunctionOperator outerLambda =
                new LambdaFunctionOperator(Arrays.asList(argz), outerLambdaBody, VarcharType.VARCHAR);
        CallOperator arrayMapOuterLambda = new CallOperator(FunctionSet.ARRAY_MAP, ArrayType.ARRAY_VARCHAR,
                Arrays.asList(outerLambda, arrayMapSimpleLambda));
        CallOperator ndvOp = new CallOperator(FunctionSet.NDV, IntegerType.BIGINT, Arrays.asList(
                new CollectionElementOperator(VarcharType.VARCHAR, arrayMapOuterLambda, ConstantOperator.createInt(1), true)));
        IsNullPredicateOperator isNullOp = new IsNullPredicateOperator(false, columnRefs.get(0));
        IsNullPredicateOperator isNotNullOp = new IsNullPredicateOperator(true, columnRefs.get(0));
        LikePredicateOperator likeOp = new LikePredicateOperator(LikePredicateOperator.LikeType.LIKE,
                Arrays.asList(columnRefs.get(0), ConstantOperator.createVarchar("%abc%")));
        LikePredicateOperator regexOp = new LikePredicateOperator(LikePredicateOperator.LikeType.REGEXP,
                Arrays.asList(columnRefs.get(0), ConstantOperator.createVarchar("\\d+(\\.\\d+)?")));
        com.starrocks.catalog.Function searchDesc =
                new com.starrocks.catalog.Function(new FunctionName(FunctionSet.COUNT),
                        new Type[] {VarcharType.VARCHAR}, IntegerType.BIGINT, false);
        com.starrocks.catalog.Function fn =
                GlobalStateMgr.getCurrentState().getFunction(searchDesc, IS_NONSTRICT_SUPERTYPE_OF);

        CallOperator countOp = new CallOperator(FunctionSet.COUNT, IntegerType.BIGINT, Arrays.asList(columnRefs.get(1)), fn);
        CallOperator countDistinctOp =
                new CallOperator(FunctionSet.COUNT, IntegerType.BIGINT, Arrays.asList(columnRefs.get(1)), fn, true);
        CallOperator countMultiDistinctOp =
                new CallOperator(FunctionSet.COUNT, IntegerType.BIGINT, Arrays.asList(columnRefs.get(1), columnRefs.get(2)),
                        fn, true);
        Object[][] cases = new Object[][] {
                {ConstantOperator.createNull(IntegerType.INT), "NULL"},
                {ConstantOperator.createBoolean(true), "true"},
                {ConstantOperator.createBoolean(false), "false"},
                {ConstantOperator.createDouble(2.125), "2.125"},
                {ConstantOperator.createDate(LocalDate.parse("2024-01-01").atTime(0, 0, 0)), "\"2024-01-01\""},
                {ConstantOperator.createDatetime(LocalDateTime.of(2024, 1, 1, 23, 59, 59)), "\"2024-01-01 23:59:59\""},
                {ConstantOperator.createInt(10), "10"},
                {ConstantOperator.createVarchar("abcd"), "\"abcd\""},
                {ConstantOperator.createDecimal(new BigDecimal("3.14"),
                        TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL128, 38, 2)), "3.14"},
                {columnRefs.get(0), "`test_db`.`t`.c0"},
                {arrayOp, "[\"abcd\", `test_db`.`t`.c0, \"defg\", `test_db`.`t`.c1]"},
                {arraySliceOp, "[\"abcd\", `test_db`.`t`.c0, \"defg\", `test_db`.`t`.c1][1:3]"},
                {mapOp, "map{\"abcd\": `test_db`.`t`.c0, \"defg\": `test_db`.`t`.c1}"},
                {arrayElem, "[\"abcd\", `test_db`.`t`.c0, \"defg\", `test_db`.`t`.c1][3]"},
                {subFieldOp, "`test_db`.`t`.c0.a.b.c"},
                {simpleLambda, "(x_0, x_1)->(CAST(x_0 AS int) + CAST(x_1 AS int))"},
                {arrayMapSimpleLambda,
                        "array_map(" +
                                "(x_0, x_1)->(CAST(x_0 AS int) + CAST(x_1 AS int)), " +
                                "[`test_db`.`t`.c0], [`test_db`.`t`.c1])"},
                {ndvOp, "ndv(array_map((x_0)->left(concat(x_0, \"0000000\"), 10), " +
                        "array_map(" +
                        "(x_0, x_1)->(CAST(x_0 AS int) + CAST(x_1 AS int)), " +
                        "[`test_db`.`t`.c0], [`test_db`.`t`.c1]))[1])"},
                {isNotNullOp, "(`test_db`.`t`.c0 IS NOT NULL)"},
                {isNullOp, "(`test_db`.`t`.c0 IS NULL)"},
                {CompoundPredicateOperator.not(isNotNullOp), "(`test_db`.`t`.c0 IS NULL)"},
                {CompoundPredicateOperator.not(isNullOp), "(`test_db`.`t`.c0 IS NOT NULL)"},
                {CompoundPredicateOperator.and(isNotNullOp, isNullOp), "false"},
                {CompoundPredicateOperator.or(isNotNullOp, isNullOp), "true"},
                {likeOp, "(`test_db`.`t`.c0 like \"%abc%\")"},
                {regexOp, "(`test_db`.`t`.c0 regexp \"\\\\d+(\\\\.\\\\d+)?\")"},
                {countOp, "count(`test_db`.`t`.c1)"},
                {countDistinctOp, "count(DISTINCT `test_db`.`t`.c1)"},
                {countMultiDistinctOp, "count(DISTINCT if((`test_db`.`t`.c1 IS NULL), NULL, `test_db`.`t`.c2))"}
        };
        for (Object[] tc : cases) {
            ScalarOperator op = (ScalarOperator) tc[0];
            String actual = opToSql.apply(opConverter.apply(op));
            String expect = (String) tc[1];
            Assert.assertEquals(expect, actual);
        }
    }

    @Test
    public void testOpEquality() {
        {
            Op op1 = Apply.val(ConstantOperator.createBigint(1));
            Op op2 = Apply.val(ConstantOperator.createBigint(1));
            Op op3 = Apply.val(ConstantOperator.createBigint(2));
            Assert.assertEquals(op1, op2);
            Assert.assertNotEquals(op1, op3);
        }

        {
            Op op1 = vars.get(1);
            Op op2 = vars.get(2);
            Op op3 = Apply.eq(op1, Apply.val(ConstantOperator.createVarchar("abc")));
            Op op4 = Apply.eq(op2, Apply.val(ConstantOperator.createVarchar("abc")));

            String addFunc = ArithmeticExpr.Operator.ADD.getName();

            Op add13 = Apply.apply(IntegerType.INT, addFunc, false, vars.get(1), vars.get(3));
            Op add24 = Apply.apply(IntegerType.INT, addFunc, false, vars.get(2), vars.get(4));
            Op op5 = Apply.apply(VarcharType.VARCHAR, FunctionSet.CONCAT, true, vars.get(1), vars.get(6), add13, vars.get(5),
                    vars.get(3));
            Op op6 = Apply.apply(VarcharType.VARCHAR, FunctionSet.CONCAT, true, vars.get(2), vars.get(6), add24, vars.get(5),
                    vars.get(4));

            Op[][] testCases = {
                    {op1, op2},
                    {op3, op4},
                    {op5, op6},
            };
            UnionFind<Integer> eqColumns1 = new UnionFind<>();
            eqColumns1.union(1, 2);
            eqColumns1.union(3, 4);
            eqColumns1 = eqColumns1.sealed();

            UnionFind<Integer> eqColumns2 = new UnionFind<>();
            eqColumns2.union(1, 3);
            eqColumns2 = eqColumns2.sealed();

            for (Op[] tc : testCases) {
                Op lhs = tc[0];
                Op rhs = tc[1];
                Assert.assertFalse(lhs.strict().equals(rhs.strict()));

                Assert.assertFalse(lhs.equals(rhs));
                Assert.assertTrue(lhs.isomorphic(rhs));
                Assert.assertTrue(lhs.strict(eqColumns1).equals(rhs.strict(eqColumns1)));
                Assert.assertFalse(lhs.strict(eqColumns2).equals(rhs.strict(eqColumns2)));
                Assert.assertFalse(lhs.strict(eqColumns1).equals(rhs.strict(eqColumns2)));
                Assert.assertFalse(lhs.strict(eqColumns1).equals(lhs.strict(eqColumns2)));
            }
        }

    }

    private Val dt(String s) {
        return Op.val(ConstantOperator.createDate(LocalDate.parse(s, DateTimeFormatter.ISO_LOCAL_DATE).atStartOfDay()));
    }

    @Test
    public void testGetRangePredicate() {
        {
            Var a = Op.var(DateType.DATE, 1);
            TieredList<Op> conjuncts = Stream.of(
                    Op.le(a, dt("2022-12-02")),
                    Op.le(a, dt("2022-12-03")),
                    Op.le(a, dt("2022-12-04")),
                    Op.le(dt("2022-12-12"), a),
                    Op.le(dt("2022-12-24"), a),
                    Op.in(a, dt("2022-11-28"), dt("2022-12-11"), dt("2022-12-13"))
            ).collect(TieredList.toList());
            Optional<List<TieredList<Op>>> rangeOp =
                    OpUtil.getRangeConjuncts(Arrays.asList(a), Arrays.asList(conjuncts));
            Assert.assertFalse(rangeOp.isPresent());
        }
        {
            Var a = Op.var(DateType.DATE, 1);
            List<Op> conjuncts = Arrays.asList(
                    //Op.le(a, dt("2022-12-01")),
                    Op.le(dt("2022-12-02"), a),
                    Op.le(dt("2022-12-03"), a),
                    Op.le(dt("2022-12-04"), a),
                    Op.le(a, dt("2022-12-12")),
                    Op.le(a, dt("2022-12-24")),
                    Op.in(a, dt("2022-11-28"), dt("2022-12-11"), dt("2022-12-13"))
            );
            Optional<List<TieredList<Op>>> rangeOp = OpUtil.getRangeConjuncts(Arrays.asList(a),
                    Arrays.asList(TieredList.<Op>genesis().concat(conjuncts)));
            Assert.assertFalse(rangeOp.isPresent());
        }

        TieredMap<Integer, ColumnAlias> aliases = TieredMap.<Integer, ColumnAlias>newGenesisTier()
                .put(1, ColumnAlias.of("t1", "a"))
                .put(2, ColumnAlias.of("t1", "a2"))
                .put(3, ColumnAlias.of("t1", "a3")).build();

        Function<Op, String> opToSql = OpUtil.toOpToSqlConverter(aliases);
        {
            Var a = Op.var(DateType.DATE, 1);
            List<Op> conjuncts = Arrays.asList(
                    Op.le(dt("2022-12-02"), a),
                    Op.le(dt("2022-12-03"), a),
                    Op.le(dt("2022-12-04"), a),
                    Op.in(a, dt("2022-12-28"), dt("2022-12-11"), dt("2022-12-13"))
            );
            Var a2 = Op.var(DateType.DATE, 2);
            List<Op> conjuncts2 = Arrays.asList(
                    Op.le(dt("2022-11-02"), a),
                    Op.le(dt("2022-11-03"), a),
                    Op.le(dt("2022-11-04"), a),
                    Op.in(a, dt("2022-11-28"), dt("2022-11-11"), dt("2022-11-13"))
            );
            Optional<List<TieredList<Op>>> rangeOp =
                    OpUtil.getRangeConjuncts(Arrays.asList(a, a2), Arrays.asList(
                            TieredList.<Op>genesis().concat(conjuncts),
                            TieredList.<Op>genesis().concat(conjuncts2)));
            Assert.assertTrue(rangeOp.isPresent());
            Assert.assertEquals(2, rangeOp.get().size());

            String conjunct0 = opToSql.apply(rangeOp.get().get(0).get(0));
            String conjunct1 = opToSql.apply(rangeOp.get().get(1).get(0));
            Assert.assertEquals(conjunct0, conjunct0, "(\"2022-11-02\" <= t1.a)");
            Assert.assertEquals(conjunct1, conjunct1, "(\"2022-11-02\" <= t1.a2)");
        }

        {
            Var a = Op.var(DateType.DATE, 1);
            List<Op> conjuncts = Arrays.asList(
                    Op.le(dt("2022-06-04"), a)
            );
            Var a2 = Op.var(DateType.DATE, 2);
            List<Op> conjuncts2 = Arrays.asList(
                    Op.le(dt("2022-06-06"), a2)
            );

            Var a3 = Op.var(DateType.DATE, 3);
            List<Op> conjuncts3 = Arrays.asList(
                    Op.in(a3, dt("2022-06-02"), dt("2022-06-09"))
            );
            Optional<List<TieredList<Op>>> rangeOp =
                    OpUtil.getRangeConjuncts(Arrays.asList(
                                    a,
                                    a2,
                                    a3),
                            Arrays.asList(
                                    TieredList.<Op>genesis().concat(conjuncts),
                                    TieredList.<Op>genesis().concat(conjuncts2),
                                    TieredList.<Op>genesis().concat(conjuncts3)
                            ));

            Assert.assertTrue(rangeOp.isPresent());
            Assert.assertEquals(3, rangeOp.get().size());
            String conjunct0 = opToSql.apply(rangeOp.get().get(0).get(0));
            String conjunct1 = opToSql.apply(rangeOp.get().get(1).get(0));
            String conjunct2 = opToSql.apply(rangeOp.get().get(2).get(0));

            Assert.assertEquals(conjunct0, conjunct0, "(\"2022-06-02\" <= t1.a)");
            Assert.assertEquals(conjunct1, conjunct1, "(\"2022-06-02\" <= t1.a2)");
            Assert.assertEquals(conjunct2, conjunct2, "(\"2022-06-02\" <= t1.a3)");
        }
    }

    private void aggRewriteHelper(CallOperator aggCall,
                                  TieredMap<Integer, GenericColumn> otherColumns,
                                  ColumnRefToIdConverter newIdConverter,
                                  AggRewriter aggRewriter,
                                  Consumer<Optional<OpPlus2>> checker) {
        Function<ScalarOperator, Op> newOpConverter = OpUtil.toOpConverter(newIdConverter, inputColumns);
        Op aggOp = newOpConverter.apply(aggCall);
        GenericColumn avgColumn = GenericColumn.derived(aggOp);
        int avgId = newIdConverter.nextId();
        OpPlus avgPlus = OpPlus.of(avgColumn.getOp(), avgId);
        TieredMap<StrictOp, Integer> alreadyExists = otherColumns.entrySet()
                .stream()
                .collect(TieredMap.toMap(e -> e.getValue().getOp().strict(), Map.Entry::getKey));
        Optional<OpPlus2> result = aggRewriter.rewrite(avgPlus, newIdConverter::nextId, alreadyExists);
        checker.accept(result);
    }

    private void aggRewriteSuccessHelper(CallOperator aggCall, List<ScalarOperator> otherColumns,
                                         AggRewriter aggRewriter, List<String> expect) {
        ColumnRefToIdConverter newIdConverter = idConverter.duplicate();
        TieredMap<Integer, GenericColumn> columns =
                otherColumns.stream().collect(TieredMap.toMap(
                        op -> newIdConverter.nextId(),
                        op -> GenericColumn.derived(opConverter.apply(op))));
        aggRewriteSuccessHelper(aggCall, columns, newIdConverter, aggRewriter, expect);
    }

    private void aggRewriteSuccessHelper(CallOperator aggCall,
                                         TieredMap<Integer, GenericColumn> otherColumns,
                                         ColumnRefToIdConverter newIdConverter,
                                         AggRewriter aggRewriter, List<String> expect) {

        Consumer<Optional<OpPlus2>> checker2 = result -> {
            Assert.assertTrue(result.isPresent());
            TieredMap.Builder<Integer, ColumnAlias> newColumnAliasesBuilder = columnAliases.newTier();
            result.get().getNewArgs().forEach(newArg -> {
                if (!columnAliases.containsKey(newArg.getId())) {
                    newColumnAliasesBuilder.put(newArg.getId(), ColumnAlias.of("tmp", "col_" + newArg.getId()));
                }
            });
            TieredMap<Integer, ColumnAlias> newColumnAliases = newColumnAliasesBuilder.build();
            TieredMap.Builder<Integer, ColumnAlias> newColumnAliasesBuilder2 = newColumnAliases.newTier();
            otherColumns.forEach((key, value) -> {
                if (!newColumnAliases.containsKey(key)) {
                    newColumnAliasesBuilder2.put(key, ColumnAlias.of("tmp", "col_" + key));
                }
            });
            Function<Op, String> newOpToSql = OpUtil.toOpToSqlConverter(newColumnAliasesBuilder2.build());

            Op newAggOp = result.get().getOp().getOp();
            List<OpPlus> newArgs = result.get().getNewArgs().collect(Collectors.toList());
            List<OpPlus> args = result.get().getArgs().stream().map(EitherOr::get).collect(Collectors.toList());
            ColumnRefSet usedColumnIds = newAggOp.getIds();
            ColumnRefSet columnIds =
                    ColumnRefSet.createByIds(args.stream().map(OpPlus::getId).collect(Collectors.toList()));

            ColumnRefSet newColumnIds =
                    ColumnRefSet.createByIds(newArgs.stream().map(OpPlus::getId).collect(Collectors.toList()));
            Assert.assertTrue(usedColumnIds.containsAll(newColumnIds));
            Assert.assertEquals(usedColumnIds, columnIds);

            List<String> actual = Stream.concat(Stream.of(newAggOp), args.stream().map(OpPlus::getOp))
                    .map(newOpToSql).collect(Collectors.toList());
            Assert.assertEquals(actual.size(), expect.size());
            for (int i = 0; i < actual.size(); ++i) {
                Assert.assertEquals(actual.get(i), actual.get(i), expect.get(i));
            }
        };
        aggRewriteHelper(aggCall, otherColumns, newIdConverter, aggRewriter, checker2);
    }

    @Test
    public void testRewriteAvg() {
        CallOperator avgCallOp = new CallOperator(FunctionSet.AVG, FloatType.DOUBLE, ImmutableList.of(columnRefs.get(9)));
        CallOperator sumCallOp = new CallOperator(FunctionSet.SUM, FloatType.DOUBLE, ImmutableList.of(columnRefs.get(9)));
        CallOperator countCallOp =
                new CallOperator(FunctionSet.COUNT, IntegerType.BIGINT, ImmutableList.of(columnRefs.get(9)));

        Object[][] testCases = new Object[][] {
                {Arrays.asList(),
                        Arrays.asList("(tmp.col_12 / tmp.col_13)", "sum(`test_db`.`t`.c9)", "count(`test_db`.`t`.c9)")},
                {Arrays.asList(sumCallOp),
                        Arrays.asList("(tmp.col_11 / tmp.col_13)", "sum(`test_db`.`t`.c9)", "count(`test_db`.`t`.c9)")},
                {Arrays.asList(countCallOp),
                        Arrays.asList("(tmp.col_13 / tmp.col_11)", "sum(`test_db`.`t`.c9)", "count(`test_db`.`t`.c9)")},
                {Arrays.asList(sumCallOp, countCallOp),
                        Arrays.asList("(tmp.col_11 / tmp.col_12)", "sum(`test_db`.`t`.c9)", "count(`test_db`.`t`.c9)")},
                {Arrays.asList(countCallOp, sumCallOp),
                        Arrays.asList("(tmp.col_12 / tmp.col_11)", "sum(`test_db`.`t`.c9)", "count(`test_db`.`t`.c9)")},
        };

        for (Object[] tc : testCases) {
            List<ScalarOperator> otherAggCall = (List<ScalarOperator>) tc[0];
            List<String> expect = (List<String>) tc[1];
            aggRewriteSuccessHelper(avgCallOp, otherAggCall, OpUtil::rewriteAvg, expect);
        }
    }

    @Test
    public void testRewriteSumExprAndConstant() {
        ColumnRefOperator colRef = columnRefs.get(8);
        Type type = colRef.getType();
        CallOperator exprAddConstantCallOp = new CallOperator(
                ArithmeticExpr.Operator.ADD.getName(), type, Arrays.asList(colRef, ConstantOperator.createInt(10)));

        CallOperator sumExprAddConstantCallOp =
                new CallOperator(FunctionSet.SUM, type, ImmutableList.of(exprAddConstantCallOp));
        CallOperator sumCallOp = new CallOperator(FunctionSet.SUM, type, ImmutableList.of(colRef));
        CallOperator countCallOp =
                new CallOperator(FunctionSet.COUNT, IntegerType.BIGINT, ImmutableList.of(colRef));

        Object[][] testCases = new Object[][] {
                {Arrays.asList(),
                        Arrays.asList("(tmp.col_12 + (10 * tmp.col_13))", "sum(`test_db`.`t`.c8)",
                                "count(`test_db`.`t`.c8)")},
                {Arrays.asList(sumCallOp),
                        Arrays.asList("(tmp.col_11 + (10 * tmp.col_13))", "sum(`test_db`.`t`.c8)",
                                "count(`test_db`.`t`.c8)")},
                {Arrays.asList(countCallOp),
                        Arrays.asList("(tmp.col_13 + (10 * tmp.col_11))", "sum(`test_db`.`t`.c8)",
                                "count(`test_db`.`t`.c8)")},
                {Arrays.asList(sumCallOp, countCallOp),
                        Arrays.asList("(tmp.col_11 + (10 * tmp.col_12))", "sum(`test_db`.`t`.c8)",
                                "count(`test_db`.`t`.c8)")},
                {Arrays.asList(countCallOp, sumCallOp),
                        Arrays.asList("(tmp.col_12 + (10 * tmp.col_11))", "sum(`test_db`.`t`.c8)",
                                "count(`test_db`.`t`.c8)")},
        };

        for (Object[] tc : testCases) {
            List<ScalarOperator> otherAggCall = (List<ScalarOperator>) tc[0];
            List<String> expect = (List<String>) tc[1];
            aggRewriteSuccessHelper(sumExprAddConstantCallOp, otherAggCall, OpUtil::rewriteSumExprAddConstant, expect);
        }
    }

    @Test
    public void testRewriteCountDistinctByBitmap() {
        ColumnRefOperator colRef = columnRefs.get(3);
        Type type = colRef.getType();
        com.starrocks.catalog.Function searchDesc =
                new com.starrocks.catalog.Function(new FunctionName(FunctionSet.COUNT),
                        new Type[] {type}, IntegerType.BIGINT, false);
        com.starrocks.catalog.Function
                fn = GlobalStateMgr.getCurrentState().getFunction(searchDesc, IS_NONSTRICT_SUPERTYPE_OF);
        CallOperator countDistinctCallOp = new CallOperator(
                FunctionSet.COUNT, IntegerType.BIGINT, Arrays.asList(colRef), fn, true);

        CallOperator bitmapAggCallOp = new CallOperator(FunctionSet.BITMAP_AGG, BitmapType.BITMAP, ImmutableList.of(colRef));

        Object[][] testCases = new Object[][] {
                {Arrays.asList(),
                        Arrays.asList("bitmap_count(tmp.col_12)", "bitmap_agg(`test_db`.`t`.c3)")},
                {Arrays.asList(bitmapAggCallOp),
                        Arrays.asList("bitmap_count(tmp.col_11)", "bitmap_agg(`test_db`.`t`.c3)")},
        };

        for (Object[] tc : testCases) {
            List<ScalarOperator> otherAggCall = (List<ScalarOperator>) tc[0];
            List<String> expect = (List<String>) tc[1];
            aggRewriteSuccessHelper(countDistinctCallOp, otherAggCall, OpUtil::rewriteDistinctByBitmapAgg, expect);
        }
    }

    @Test
    public void testRewriteCountDistinctByHll() {
        ColumnRefOperator colRef = columnRefs.get(3);
        Type type = colRef.getType();
        com.starrocks.catalog.Function searchDesc =
                new com.starrocks.catalog.Function(new FunctionName(FunctionSet.COUNT),
                        new Type[] {type}, IntegerType.BIGINT, false);
        com.starrocks.catalog.Function
                fn = GlobalStateMgr.getCurrentState().getFunction(searchDesc, IS_NONSTRICT_SUPERTYPE_OF);
        CallOperator countDistinctCallOp = new CallOperator(
                FunctionSet.COUNT, IntegerType.BIGINT, Arrays.asList(colRef), fn, true);

        CallOperator hllHashCallOp = new CallOperator(FunctionSet.HLL_HASH, HLLType.HLL, ImmutableList.of(colRef));
        CallOperator hllAggCallOp = new CallOperator(FunctionSet.HLL_UNION, HLLType.HLL, ImmutableList.of(hllHashCallOp));

        Object[][] testCases = new Object[][] {
                {Arrays.asList(),
                        Arrays.asList("hll_cardinality(tmp.col_12)", "hll_union(hll_hash(`test_db`.`t`.c3))")},
                {Arrays.asList(hllAggCallOp),
                        Arrays.asList("hll_cardinality(tmp.col_11)", "hll_union(hll_hash(`test_db`.`t`.c3))")},
        };

        for (Object[] tc : testCases) {
            List<ScalarOperator> otherAggCall = (List<ScalarOperator>) tc[0];
            List<String> expect = (List<String>) tc[1];
            aggRewriteSuccessHelper(countDistinctCallOp, otherAggCall, OpUtil::rewriteDistinctByHllAgg, expect);
        }
    }

    @Test
    public void testRewriteCountDistinctByArrayAgg() {
        ColumnRefOperator colRef = columnRefs.get(3);
        Type type = colRef.getType();
        com.starrocks.catalog.Function searchDesc =
                new com.starrocks.catalog.Function(new FunctionName(FunctionSet.COUNT),
                        new Type[] {type}, IntegerType.BIGINT, false);
        com.starrocks.catalog.Function
                fn = GlobalStateMgr.getCurrentState().getFunction(searchDesc, IS_NONSTRICT_SUPERTYPE_OF);
        CallOperator countDistinctCallOp = new CallOperator(
                FunctionSet.COUNT, IntegerType.BIGINT, Arrays.asList(colRef), fn, true);
        com.starrocks.catalog.Function searchDesc2 =
                new com.starrocks.catalog.Function(new FunctionName(FunctionSet.ARRAY_AGG),
                        new Type[] {type}, ArrayType.ARRAY_INT, false);
        com.starrocks.catalog.Function
                arrayAggDistinctFn =
                GlobalStateMgr.getCurrentState().getFunction(searchDesc, IS_NONSTRICT_SUPERTYPE_OF);

        CallOperator arrayAggDistinctCallOp =
                new CallOperator(FunctionSet.ARRAY_AGG, ArrayType.ARRAY_INT, ImmutableList.of(colRef),
                        arrayAggDistinctFn, true);

        Object[][] testCases = new Object[][] {
                {Arrays.asList(),
                        Arrays.asList("array_length(array_filter((x_0)->(x_0 IS NOT NULL), tmp.col_12))",
                                "array_agg(DISTINCT `test_db`.`t`.c3)")},
                {Arrays.asList(arrayAggDistinctCallOp),
                        Arrays.asList("array_length(array_filter((x_0)->(x_0 IS NOT NULL), tmp.col_11))",
                                "array_agg(DISTINCT `test_db`.`t`.c3)")},
        };

        for (Object[] tc : testCases) {
            List<ScalarOperator> otherAggCall = (List<ScalarOperator>) tc[0];
            List<String> expect = (List<String>) tc[1];
            aggRewriteSuccessHelper(countDistinctCallOp, otherAggCall, OpUtil::rewriteDistinctByArrayAggDistinct,
                    expect);
        }
    }

    @Test
    public void testRewriteBitmap() {
        ColumnRefOperator colRef = columnRefs.get(3);
        Type type = colRef.getType();
        CallOperator bitmapUnionIntAggCall = new CallOperator(
                FunctionSet.BITMAP_UNION_INT, IntegerType.BIGINT, Arrays.asList(colRef));
        CallOperator bitmapUnionCountAggCall = new CallOperator(
                FunctionSet.BITMAP_UNION_COUNT, IntegerType.BIGINT, Arrays.asList(colRef));

        CallOperator bitmapAggCall = new CallOperator(
                FunctionSet.BITMAP_AGG, BitmapType.BITMAP, Arrays.asList(colRef));
        CallOperator bitmapUnionCall = new CallOperator(
                FunctionSet.BITMAP_UNION, BitmapType.BITMAP, Arrays.asList(colRef));

        Object[][] testCases = new Object[][] {
                {bitmapUnionIntAggCall, Arrays.asList(),
                        Arrays.asList("bitmap_count(tmp.col_12)", "bitmap_agg(`test_db`.`t`.c3)")},
                {bitmapUnionIntAggCall, Arrays.asList(bitmapAggCall),
                        Arrays.asList("bitmap_count(tmp.col_11)", "bitmap_agg(`test_db`.`t`.c3)")},
                {bitmapUnionCountAggCall, Arrays.asList(),
                        Arrays.asList("bitmap_count(tmp.col_12)", "bitmap_union(`test_db`.`t`.c3)")},
                {bitmapUnionCountAggCall, Arrays.asList(bitmapUnionCall),
                        Arrays.asList("bitmap_count(tmp.col_11)", "bitmap_union(`test_db`.`t`.c3)")},
        };

        for (Object[] tc : testCases) {
            CallOperator bitmapAgg = (CallOperator) tc[0];
            List<ScalarOperator> otherAggCall = (List<ScalarOperator>) tc[1];
            List<String> expect = (List<String>) tc[2];
            aggRewriteSuccessHelper(bitmapAgg, otherAggCall, OpUtil::rewriteBitmap, expect);
        }
    }

    @Test
    public void testRewriteHll() {
        ColumnRefOperator colRef = columnRefs.get(3);
        CallOperator ndvAggCall = new CallOperator(
                FunctionSet.NDV, IntegerType.BIGINT, Arrays.asList(colRef));
        CallOperator approxCountDistinctAggCall = new CallOperator(
                FunctionSet.APPROX_COUNT_DISTINCT, IntegerType.BIGINT, Arrays.asList(colRef));
        CallOperator hllUnionAggCall = new CallOperator(
                FunctionSet.HLL_UNION_AGG, IntegerType.BIGINT, Arrays.asList(colRef));

        CallOperator hllRawCall = new CallOperator(
                FunctionSet.HLL_RAW, HLLType.HLL, Arrays.asList(colRef));
        CallOperator hllUnionCall = new CallOperator(
                FunctionSet.HLL_UNION, HLLType.HLL, Arrays.asList(colRef));

        Object[][] testCases = new Object[][] {
                {ndvAggCall, Arrays.asList(),
                        Arrays.asList("hll_cardinality(tmp.col_12)", "hll_raw(`test_db`.`t`.c3)")},
                {ndvAggCall, Arrays.asList(hllRawCall),
                        Arrays.asList("hll_cardinality(tmp.col_11)", "hll_raw(`test_db`.`t`.c3)")},
                {approxCountDistinctAggCall, Arrays.asList(),
                        Arrays.asList("hll_cardinality(tmp.col_12)", "hll_raw(`test_db`.`t`.c3)")},
                {approxCountDistinctAggCall, Arrays.asList(hllRawCall),
                        Arrays.asList("hll_cardinality(tmp.col_11)", "hll_raw(`test_db`.`t`.c3)")},
                {hllUnionAggCall, Arrays.asList(),
                        Arrays.asList("hll_cardinality(tmp.col_12)", "hll_union(`test_db`.`t`.c3)")},
                {hllUnionAggCall, Arrays.asList(hllUnionCall),
                        Arrays.asList("hll_cardinality(tmp.col_11)", "hll_union(`test_db`.`t`.c3)")},
        };

        for (Object[] tc : testCases) {
            CallOperator bitmapAgg = (CallOperator) tc[0];
            List<ScalarOperator> otherAggCall = (List<ScalarOperator>) tc[1];
            List<String> expect = (List<String>) tc[2];
            aggRewriteSuccessHelper(bitmapAgg, otherAggCall, OpUtil::rewriteHll, expect);
        }
    }

    @Test
    public void testRewritePercentile() {
        ColumnRefOperator colRef = columnRefs.get(9);
        Type type = colRef.getType();
        CallOperator percentileApproxAggCall = new CallOperator(
                FunctionSet.PERCENTILE_APPROX, type, Arrays.asList(colRef, ConstantOperator.createDouble(0.5)));
        CallOperator percentileHashCall = new CallOperator(
                FunctionSet.PERCENTILE_HASH, PercentileType.PERCENTILE, Arrays.asList(colRef));
        CallOperator percentileUnionCall = new CallOperator(
                FunctionSet.PERCENTILE_UNION, PercentileType.PERCENTILE, Arrays.asList(percentileHashCall));

        Object[][] testCases = new Object[][] {
                {percentileApproxAggCall, Arrays.asList(),
                        Arrays.asList("percentile_approx_raw(tmp.col_12, 0.5)",
                                "percentile_union(percentile_hash(`test_db`.`t`.c9))")},
                {percentileApproxAggCall, Arrays.asList(percentileUnionCall),
                        Arrays.asList("percentile_approx_raw(tmp.col_11, 0.5)",
                                "percentile_union(percentile_hash(`test_db`.`t`.c9))")},
        };

        for (Object[] tc : testCases) {
            CallOperator bitmapAgg = (CallOperator) tc[0];
            List<ScalarOperator> otherAggCall = (List<ScalarOperator>) tc[1];
            List<String> expect = (List<String>) tc[2];
            aggRewriteSuccessHelper(bitmapAgg, otherAggCall, OpUtil::rewritePercentile, expect);
        }
    }

    interface AggRewriter {
        Optional<OpPlus2> rewrite(OpPlus agg, Supplier<Integer> nextId, TieredMap<StrictOp, Integer> alreadyExisting);
    }

}
