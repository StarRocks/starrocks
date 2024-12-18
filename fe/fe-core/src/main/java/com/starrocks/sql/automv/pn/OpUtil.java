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

import com.google.api.client.util.Lists;
import com.google.common.base.Preconditions;
import com.google.common.collect.BoundType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.starrocks.analysis.ArithmeticExpr;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.ArrayType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.common.Pair;
import com.starrocks.sql.automv.column.ColumnAlias;
import com.starrocks.sql.automv.column.ColumnRefToIdConverter;
import com.starrocks.sql.automv.column.DerivedColumn;
import com.starrocks.sql.automv.column.GenericColumn;
import com.starrocks.sql.automv.util.EitherOr;
import com.starrocks.sql.automv.util.TieredList;
import com.starrocks.sql.automv.util.TieredMap;
import com.starrocks.sql.automv.util.Util;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.transformer.SqlToScalarOperatorTranslator;

import java.math.BigInteger;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class OpUtil {

    private static final Set<String> ROLLUP_ABLE_TIME_GRANULE_SET = ImmutableSet.<String>builder()
            .add(FunctionSet.DATE_FORMAT)
            .add(FunctionSet.DATE)
            .add(FunctionSet.DATE_SLICE)
            .add(FunctionSet.DATE_TRUNC)
            .add(FunctionSet.DAY)
            .add(FunctionSet.DAYNAME)
            .add(FunctionSet.DAYOFMONTH)
            //.add(FunctionSet.DAY_OF_WEEK_ISO)
            .add(FunctionSet.DAYOFWEEK)
            .add(FunctionSet.DAYOFYEAR)
            .add(FunctionSet.HOUR)
            //.add(FunctionSet.JODATIME_FORMAT)
            .add(FunctionSet.LAST_DAY)
            .add(FunctionSet.MINUTE)
            .add(FunctionSet.SECOND)
            .add(FunctionSet.MONTH)
            .add(FunctionSet.MONTHNAME)
            .add(FunctionSet.NEXT_DAY)
            .add(FunctionSet.PREVIOUS_DAY)
            .add(FunctionSet.QUARTER)
            .add(FunctionSet.STR2DATE)
            .add(FunctionSet.STR_TO_DATE)
            //.add(FunctionSet.STR_TO_JODATIME)
            .add(FunctionSet.TIME_SLICE)
            .add(FunctionSet.TO_DATE)
            .add(FunctionSet.TO_DAYS)
            //.add(FunctionSet.TO_ISO8601)
            .add(FunctionSet.TO_TERA_DATE)
            //.add(FunctionSet.WEEK_ISO)
            .add(FunctionSet.YEAR)
            //.add(FunctionSet.YEARWEEK)
            .add(FunctionSet.WEEK)
            .add(FunctionSet.WEEKOFYEAR)
            .build();

    public static Optional<StrictOp> getExpr(GenericColumn column) {
        return column.cast(DerivedColumn.class).map(DerivedColumn::getExpr);
    }

    public static Function<Op, String> toOpToSqlConverter(TieredMap<Integer, ColumnAlias> columnAliases) {
        return op -> Transcriptase.transcript(op, columnAliases);
    }

    public static Function<ScalarOperator, Op> toOpConverter(final ColumnRefToIdConverter idConverter,
                                                             final Map<Integer, GenericColumn> inputColumns) {
        return scalarOperator -> ReverseTranscriptase.reverseTranscript(scalarOperator, idConverter, inputColumns);
    }

    private static GenericColumn convScalarOperator(final ScalarOperator scalarOperator,
                                                    final ColumnRefToIdConverter idConverter,
                                                    final Map<Integer, GenericColumn> inputColumns) {
        Op op = toOpConverter(idConverter, inputColumns).apply(scalarOperator);
        if (op.isVar() && op.getColumn().isOriginal()) {
            return op.getColumn();
        } else {
            return GenericColumn.derived(op);
        }
    }

    public static Function<ScalarOperator, GenericColumn> toColumnConverter(
            final ColumnRefToIdConverter idConverter,
            final Map<Integer, GenericColumn> inputColumns) {
        return scalarOperator -> convScalarOperator(scalarOperator, idConverter, inputColumns);
    }

    public static Function<Column, GenericColumn> toOriginalColumnConverter(TableName fqTableName) {
        return column -> GenericColumn.original(fqTableName, column);
    }

    public static Function<Pair<Integer, GenericColumn>, String> toColumnToSqlConverter(
            TieredMap<Integer, ColumnAlias> columAliases) {
        Function<Op, String> opToSql = toOpToSqlConverter(columAliases);
        return p -> {
            if (p.second.isOriginal()) {
                return columAliases.get(p.first).getQualifiedName();
            } else {
                return opToSql.apply(p.second.getOp());
            }
        };
    }

    public static Optional<List<String>> conjunctsToSql(List<Op> conjuncts,
                                                        Function<Op, String> opToSql) {
        if (conjuncts.isEmpty()) {
            return Optional.empty();
        } else {
            List<String> sqlList = conjuncts.stream().map(opToSql).collect(Collectors.toList());
            return Optional.of(sqlList);
        }
    }

    public static StrictOp mustGetExpr(GenericColumn column) {
        return Objects.requireNonNull(getExpr(column).orElse(null));
    }

    public static Optional<Op> getOp(GenericColumn column) {
        return column.cast(DerivedColumn.class).map(c -> c.getExpr().getOp());
    }

    public static Op mustGetOp(GenericColumn column) {
        return Objects.requireNonNull(getOp(column).orElse(null));
    }

    public static boolean isFun(GenericColumn column, String name) {
        return getExpr(column).map(StrictOp::getOp).map(c -> c.isFun(name)).orElse(false);
    }

    public static boolean isAvg(GenericColumn column) {
        return isFun(column, FunctionSet.AVG);
    }

    public static boolean isAvgDistinct(GenericColumn column) {
        return isDistinct(column) && isAvg(column);
    }

    public static boolean isSumDistinct(GenericColumn column) {
        return isDistinct(column) && isSum(column);
    }

    public static boolean isCountDistinct(GenericColumn column) {
        return isDistinct(column) && isCount(column);
    }

    public static boolean isCountOrSumDistinct(GenericColumn column) {
        return isCountDistinct(column) || isSumDistinct(column);
    }

    public static boolean isArrayAggDistinct(GenericColumn column) {
        return isFun(column, FunctionSet.ARRAY_AGG_DISTINCT);
    }

    public static Optional<OpPlus2> rewriteDistinctByArrayAggDistinct(
            OpPlus distinctOpPlus, Supplier<Integer> idGen, Map<StrictOp, Integer> alreadyExists) {
        int distinctId = distinctOpPlus.getId();
        Op distinctOp = distinctOpPlus.getOp();
        Type type = distinctOp.getType();
        Preconditions.checkArgument(distinctOp.isSumDistinct() || distinctOp.isCountDistinct());
        List<Op> args = distinctOp.arg(0).getArgs();
        Preconditions.checkArgument(!args.isEmpty());
        Op arg = args.get(0);

        Type arrayType = new ArrayType(arg.getType());
        Op arrayAggDistinctOp =
                Apply.apply(arrayType, FunctionSet.ARRAY_AGG, true,
                        Apply.apply(arrayType, BuiltinKind.DISTINCT, true, args));
        EitherOr<OpPlus> arrayAggDistinctOpPlus = Optional.ofNullable(alreadyExists.get(arrayAggDistinctOp.strict()))
                .map(id -> EitherOr.either(OpPlus.of(arrayAggDistinctOp, id)))
                .orElseGet(() -> EitherOr.or(OpPlus.of(arrayAggDistinctOp, idGen.get())));

        List<EitherOr<OpPlus>> argPlusList = ImmutableList.of(arrayAggDistinctOpPlus);
        // array references to array_agg_distinct(col)
        Op var = arrayAggDistinctOpPlus.get().toVar();
        if (distinctOp.isSumDistinct()) {
            Op arraySumOp = Apply.apply(type, FunctionSet.ARRAY_SUM, true, var);
            return Optional.of(OpPlus2.of(OpPlus.of(arraySumOp, distinctId), argPlusList));
        } else {
            List<Op> locals = Op.getLocals(0, 1);
            Op local0 = locals.get(0);
            // x IS NOT NULL
            Op isNotNullOp = local0.toIsNotNull();
            // x->x IS NOT NULL
            Op lambdaIsNotNullOp = Op.simpleLambda(isNotNullOp, locals);
            // array_filter(x->x IS NOT NULL, array)
            Op arrayFilterOp = Op.apply(arrayType, FunctionSet.ARRAY_FILTER, true, lambdaIsNotNullOp, var);
            // array_length(array_filter(x->x IS NOT NULL, array))
            Op arrayLengthOp = Op.apply(type, FunctionSet.ARRAY_LENGTH, true, arrayFilterOp);
            return Optional.of(OpPlus2.of(OpPlus.of(arrayLengthOp, distinctId), argPlusList));
        }
    }

    public static Optional<OpPlus2> rewriteDistinctByBitmapAgg(
            OpPlus distinctOpPlus, Supplier<Integer> idGen, Map<StrictOp, Integer> alreadyExists) {
        int distinctId = distinctOpPlus.getId();
        Op distinctOp = distinctOpPlus.getOp();
        Type type = distinctOp.getType();
        Preconditions.checkArgument(distinctOp.isSumDistinct() || distinctOp.isCountDistinct());
        List<Op> args = distinctOp.arg(0).getArgs();
        Preconditions.checkArgument(!args.isEmpty());
        Type argType = args.get(0).getType();

        if (!argType.isBoolean() && (!argType.isIntegerType() || argType.isBigint())) {
            return Optional.empty();
        }

        Op bitmapAggOp = Apply.apply(Type.BITMAP, FunctionSet.BITMAP_AGG, true, args);
        EitherOr<OpPlus> bitmapAggOpPlus = Optional.ofNullable(alreadyExists.get(bitmapAggOp.strict()))
                .map(id -> EitherOr.either(OpPlus.of(bitmapAggOp, id)))
                .orElseGet(() -> EitherOr.or(OpPlus.of(bitmapAggOp, idGen.get())));
        Op var = bitmapAggOpPlus.get().toVar();
        if (distinctOp.isCountDistinct()) {
            Op bitmapCountOp = Apply.apply(type, FunctionSet.BITMAP_COUNT, true, var);
            return Optional.of(OpPlus2.of(OpPlus.of(bitmapCountOp, distinctId), bitmapAggOpPlus));
        } else {
            Op bitmapToArrayOp = Apply.apply(type, FunctionSet.BITMAP_TO_ARRAY, true, var);
            Op arraySumOp = Apply.apply(type, FunctionSet.ARRAY_SUM, true, bitmapToArrayOp);
            return Optional.of(OpPlus2.of(OpPlus.of(arraySumOp, distinctId), bitmapAggOpPlus));
        }
    }

    public static Optional<OpPlus2> rewriteDistinctByHllAgg(
            OpPlus distinctOpPlus, Supplier<Integer> idGen, Map<StrictOp, Integer> alreadyExists) {
        int distinctId = distinctOpPlus.getId();
        Op distinctOp = distinctOpPlus.getOp();
        Type type = distinctOp.getType();
        if (!distinctOp.isCountDistinct()) {
            return Optional.empty();
        }
        List<Op> args = distinctOp.arg(0).getArgs();
        Preconditions.checkArgument(!args.isEmpty());
        Type argType = args.get(0).getType();

        Op hllHashOp = Apply.apply(Type.HLL, FunctionSet.HLL_HASH, true, args);
        Op hllAggOp = Apply.apply(Type.HLL, FunctionSet.HLL_UNION, true, hllHashOp);
        EitherOr<OpPlus> hllAggOpPlus = Optional.ofNullable(alreadyExists.get(hllAggOp.strict()))
                .map(id -> EitherOr.either(OpPlus.of(hllAggOp, id)))
                .orElseGet(() -> EitherOr.or(OpPlus.of(hllAggOp, idGen.get())));
        Op var = hllAggOpPlus.get().toVar();
        Op hllCardinalityOp = Apply.apply(type, FunctionSet.HLL_CARDINALITY, true, var);
        return Optional.of(OpPlus2.of(OpPlus.of(hllCardinalityOp, distinctId), hllAggOpPlus));
    }

    public static boolean isFunctionNameInSet(Op op, Set<String> functionNames) {
        return op.cast(Apply.class)
                .map(Apply::getKind)
                .map(kind -> Util.downcast(kind, FunctionKind.class)
                        .map(functionKind -> functionNames.contains(functionKind.toString()))
                        .orElse(false))
                .orElse(false);
    }

    public static boolean isRollupAbleTimeGranule(Op op) {
        return isFunctionNameInSet(op, ROLLUP_ABLE_TIME_GRANULE_SET);
    }

    public static Optional<TimeGranule> getPartitionByTimeGranule(Op op, ColumnRefSet partitionColumnIds) {
        Optional<TimeGranule> optGranule = Optional.ofNullable(TimeGranule.of(op));
        if (optGranule.isEmpty() || !partitionColumnIds.contains(optGranule.get().getVar().getId())) {
            return Optional.empty();
        }
        return optGranule;
    }

    public static Optional<OpPlus2> rewriteRollupAbleTimeGranule(
            OpPlus opPlus, Supplier<Integer> idGen, Map<StrictOp, Integer> alreadyExists) {
        Op op = opPlus.getOp();
        List<Op> timeGranuleOps = op.collect(OpUtil::isRollupAbleTimeGranule);
        if (timeGranuleOps.isEmpty()) {
            return Optional.empty();
        }
        Map<StrictOp, Var> uniqueTimeGranuleMap = Maps.newHashMap();
        ImmutableList.Builder<EitherOr<OpPlus>> argsBuilder = ImmutableList.builder();
        for (Op timeGranule : timeGranuleOps) {
            StrictOp strictTimeGranule = timeGranule.strict();
            if (uniqueTimeGranuleMap.containsKey(strictTimeGranule)) {
                continue;
            }
            EitherOr<OpPlus> timeGranulePlus = Optional.ofNullable(alreadyExists.get(strictTimeGranule))
                    .map(id -> EitherOr.either(OpPlus.of(timeGranule, id)))
                    .orElse(EitherOr.or(OpPlus.of(timeGranule, idGen.get())));
            uniqueTimeGranuleMap.put(strictTimeGranule, timeGranulePlus.get().toVar());
            argsBuilder.add(timeGranulePlus);
        }
        Optional<Op> optNewOp = substSubOp(op, uniqueTimeGranuleMap);
        Preconditions.checkArgument(optNewOp.isPresent());
        return Optional.of(OpPlus2.of(OpPlus.of(optNewOp.get(), opPlus.getId()), argsBuilder.build()));
    }

    public static Optional<Op> substSubOp(Op op, Map<StrictOp, Var> subOpMap) {

        Optional<Op> result = Optional.ofNullable(subOpMap.get(op.strict()));
        if (result.isPresent()) {
            return result;
        }
        List<Pair<Op, Optional<Op>>> optArgs = op.getArgs().stream()
                .map(arg -> Pair.create(arg, substSubOp(arg, subOpMap)))
                .collect(Collectors.toList());

        if (optArgs.stream().allMatch(p -> p.second.isEmpty())) {
            return Optional.empty();
        }

        List<Op> newArgs = optArgs.stream()
                .map(p -> p.second.orElse(p.first))
                .collect(ImmutableList.toImmutableList());
        Apply apply = op.cast();
        return Optional.of(Op.apply(apply.getType(), apply.getKind(), apply.isOrdered(), newArgs));
    }

    public static TieredMap<StrictOp, Integer> columnsToStrictOpMap(TieredMap<Integer, GenericColumn> columns) {
        return columns.entrySet()
                .stream()
                .collect(TieredMap.toMap(e -> OpUtil.columnToOp(e.getKey(), e.getValue()).strict(), Map.Entry::getKey));
    }

    public static TieredMap<Integer, Op> columnsToOpMap(TieredMap<Integer, GenericColumn> columns) {
        return columns.entrySet()
                .stream()
                .collect(TieredMap.toMap(Map.Entry::getKey, e -> OpUtil.columnToOp(e.getKey(), e.getValue())));
    }

    public static boolean isNdv(GenericColumn column) {
        return isFun(column, FunctionSet.NDV);
    }

    // APPROX_COUNT_DISTINCT is alias of NDV
    public static boolean isApproxCountDistinct(GenericColumn column) {
        return isFun(column, FunctionSet.APPROX_COUNT_DISTINCT);
    }

    public static boolean isHllRaw(GenericColumn column) {
        return isFun(column, FunctionSet.HLL_RAW);
    }

    public static boolean isHllUnion(GenericColumn column) {
        return isFun(column, FunctionSet.HLL_UNION);
    }

    // HLL_RAW_AGG is alias of HLL_UNION
    public static boolean isHllRawAgg(GenericColumn column) {
        return isFun(column, FunctionSet.HLL_RAW_AGG);
    }

    public static boolean isHllUnionAgg(GenericColumn column) {
        return isFun(column, FunctionSet.HLL_UNION_AGG);
    }

    public static boolean isHllMerge(GenericColumn column) {
        return isHllRaw(column) || isHllRawAgg(column) || isHllUnion(column);
    }

    public static boolean isHllFinal(GenericColumn column) {
        return isNdv(column) || isApproxCountDistinct(column) || isHllUnionAgg(column);
    }

    public static boolean isBitmapUnion(GenericColumn column) {
        return isFun(column, FunctionSet.BITMAP_UNION);
    }

    public static boolean isBitmapAgg(GenericColumn column) {
        return isFun(column, FunctionSet.BITMAP_AGG);
    }

    public static boolean isBitmapUnionInt(GenericColumn column) {
        return isFun(column, FunctionSet.BITMAP_UNION_INT);
    }

    public static boolean isBitmapUnionCount(GenericColumn column) {
        return isFun(column, FunctionSet.BITMAP_UNION_COUNT);
    }

    public static boolean isBitmapMerge(GenericColumn column) {
        return isBitmapAgg(column) || isBitmapUnion(column);
    }

    public static boolean isBitmapFinal(GenericColumn column) {
        return isBitmapUnionInt(column) || isBitmapUnionCount(column);
    }

    // BITMAP_UNION_INT = BITMAP_COUNT . BITMAP_AGG
    // BITMAP_UNION_COUNT = BITMAP_COUNT . BITMAP_UNION
    public static Optional<OpPlus2> rewriteBitmap(OpPlus bitmapPlus,
                                                  Supplier<Integer> idGen,
                                                  Map<StrictOp, Integer> alreadyExists) {
        int bitmapId = bitmapPlus.getId();
        Op bitmap = bitmapPlus.getOp();
        Op arg;
        if (bitmap.isFun(FunctionSet.BITMAP_UNION_INT)) {
            arg = Apply.apply(Type.BITMAP, FunctionSet.BITMAP_AGG, true, bitmap.getArgs());
        } else if (bitmap.isFun(FunctionSet.BITMAP_UNION_COUNT)) {
            arg = Apply.apply(Type.BITMAP, FunctionSet.BITMAP_UNION, true, bitmap.getArgs());
        } else {
            return Optional.empty();
        }

        EitherOr<OpPlus> argPlus = Optional.ofNullable(alreadyExists.get(arg.strict()))
                .map(id -> EitherOr.either(OpPlus.of(arg, id)))
                .orElseGet(() -> EitherOr.or(OpPlus.of(arg, idGen.get())));

        Op var = argPlus.get().toVar();
        Op bitmapCard = Apply.apply(bitmap.getType(), FunctionSet.BITMAP_COUNT, true, var);
        return Optional.of(OpPlus2.of(OpPlus.of(bitmapCard, bitmapId), argPlus));
    }

    public static boolean isSumExprAddConstant(GenericColumn column) {
        Optional<Op> optOp = OpUtil.getOp(column);
        if (!optOp.isPresent()) {
            return false;
        }
        Op op = optOp.get();
        if (!op.isSum()) {
            return false;
        }
        Op sumArg = op.arg(0);
        final String addFunc = ArithmeticExpr.Operator.ADD.getName();
        final String subFunc = ArithmeticExpr.Operator.SUBTRACT.getName();
        if (!sumArg.isFun(addFunc) && !sumArg.isFun(subFunc)) {
            return false;
        }
        return sumArg.arg(0).isVal() || sumArg.arg(1).isVal();
    }

    public static Optional<OpPlus2> rewriteSumExprAddConstant(OpPlus sumPlus,
                                                              Supplier<Integer> idGen,
                                                              Map<StrictOp, Integer> alreadyExists) {
        int sumId = sumPlus.getId();
        Op sum = sumPlus.getOp();

        Apply addOp = sum.arg(0).cast();
        Op a = addOp.arg(0);
        Op b = addOp.arg(1);
        Function<Op, List<EitherOr<OpPlus>>> newArgsMaker = op -> {
            Op sumAgg = Apply.apply(sum.getType(), FunctionSet.SUM, true, op);
            Op countAgg = Apply.apply(Type.BIGINT, FunctionSet.COUNT, true, op);
            return Stream.of(sumAgg, countAgg).map(o ->
                    Optional.ofNullable(alreadyExists.get(o.strict()))
                            .map(id -> EitherOr.either(OpPlus.of(o, id)))
                            .orElseGet(() -> EitherOr.or(OpPlus.of(o, idGen.get())))
            ).collect(Collectors.toList());
        };
        final String mulFunc = ArithmeticExpr.Operator.MULTIPLY.getName();
        if (a.isVal()) {
            List<EitherOr<OpPlus>> newArgPluses = newArgsMaker.apply(b);
            Op sumVar = newArgPluses.get(0).get().toVar();
            Op countVar = newArgPluses.get(1).get().toVar();
            Op aMulCount = Apply.apply(sum.getType(), mulFunc, true, a, countVar);
            Op newSum = Apply.apply(sum.getType(), addOp.getKind(), true, ImmutableList.of(aMulCount, sumVar));
            return Optional.of(OpPlus2.of(OpPlus.of(newSum, sumId), newArgPluses));
        } else if (b.isVal()) {
            List<EitherOr<OpPlus>> newArgPluses = newArgsMaker.apply(a);
            Op sumVar = newArgPluses.get(0).get().toVar();
            Op countVar = newArgPluses.get(1).get().toVar();
            Op bMulCount = Apply.apply(sum.getType(), mulFunc, true, b, countVar);
            Op newSum = Apply.apply(sum.getType(), addOp.getKind(), true, ImmutableList.of(sumVar, bMulCount));
            return Optional.of(OpPlus2.of(OpPlus.of(newSum, sumId), newArgPluses));
        } else {
            return Optional.empty();
        }
    }

    public static boolean isPercentileUnion(GenericColumn column) {
        return isFun(column, FunctionSet.PERCENTILE_UNION);
    }

    public static boolean isPercentileApprox(GenericColumn column) {
        return isFun(column, FunctionSet.PERCENTILE_APPROX);
    }

    // PERCENTILE_APPROX a r = PERCENTILE_APPROX_RAW . (PERCENTILE_APPROX . PERCENTILE_HASH a) r
    public static Optional<OpPlus2> rewritePercentile(OpPlus percentilePlus, Supplier<Integer> idGen,
                                                      Map<StrictOp, Integer> alreadyExists) {
        int percentileId = percentilePlus.getId();
        Op percentile = percentilePlus.getOp();
        Preconditions.checkArgument(percentile.isFun(FunctionSet.PERCENTILE_APPROX));
        Op arg = percentile.arg(0);
        Op rate = percentile.arg(1);
        Op percentileHash = Op.apply(Type.PERCENTILE, FunctionSet.PERCENTILE_HASH, true, arg);
        Op percentileUnion = Op.apply(Type.PERCENTILE, FunctionSet.PERCENTILE_UNION, true, percentileHash);
        EitherOr<OpPlus> argPlus = Optional.ofNullable(alreadyExists.get(percentileUnion.strict()))
                .map(id -> EitherOr.either(OpPlus.of(percentileUnion, id)))
                .orElseGet(() -> EitherOr.or(OpPlus.of(percentileUnion, idGen.get())));
        Var var = argPlus.get().toVar();
        Op percentileApproxRaw = Op.apply(percentile.getType(), FunctionSet.PERCENTILE_APPROX_RAW, true, var, rate);
        return Optional.of(OpPlus2.of(OpPlus.of(percentileApproxRaw, percentileId), argPlus));
    }

    public static boolean isHll(GenericColumn column) {
        return isNdv(column) ||
                isApproxCountDistinct(column) ||
                isHllRaw(column) ||
                isHllUnion(column) ||
                isHllRawAgg(column) ||
                isHllUnionAgg(column);
    }

    public static boolean isDistinct(GenericColumn column) {
        return getOp(column).map(Op::isDistinctAgg).orElse(false);
    }

    public static boolean isSum(GenericColumn column) {
        return isFun(column, FunctionSet.SUM);
    }

    public static boolean isCount(GenericColumn column) {
        return isFun(column, FunctionSet.COUNT);
    }

    public static boolean isSumOrCount(GenericColumn column) {
        return isCount(column) || isSum(column);
    }

    // AVG = SUM/COUNT
    public static Optional<OpPlus2> rewriteAvg(OpPlus avgPlus, Supplier<Integer> idGen,
                                               Map<StrictOp, Integer> alreadyExists) {
        Op avg = avgPlus.getOp();
        Integer avgId = avgPlus.getId();
        Preconditions.checkArgument(avg != null && avg.isFun(FunctionSet.AVG));

        Apply apply = avg.cast();
        Op sumOp = Op.apply(avg.arg(0).getType(), FunctionSet.SUM, true, apply.getArgs());
        Op countOp = Op.apply(Type.BIGINT, FunctionSet.COUNT, true, apply.getArgs());
        List<EitherOr<OpPlus>> argPlusList =
                Stream.of(sumOp, countOp)
                        .map(arg -> Optional.ofNullable(alreadyExists.get(arg.strict()))
                                .map(id -> EitherOr.either(OpPlus.of(arg, id)))
                                .orElseGet(() -> EitherOr.or(OpPlus.of(arg, idGen.get()))))
                        .collect(ImmutableList.toImmutableList());
        List<Op> args = argPlusList.stream()
                .map(EitherOr::get)
                .map(OpPlus::toVar)
                .collect(ImmutableList.toImmutableList());

        Op newAvg = Op.apply(avg.getType(), FunctionSet.DIVIDE, true, args);
        return Optional.of(OpPlus2.of(OpPlus.of(newAvg, avgId), argPlusList));
    }

    // HLL_RAW: T->HLL
    // NDV/APPROX_COUNT_DISTINCT: T->INT = HLL_CARDINALITY . HLL_RAW
    // HLL_UNION/HLL_RAW_AGG: HLL->HLL
    // HLL_UNION_AGG: HLL->INT = HLL_CARDINALITY . HLL_UNION
    // It seems that NDV = HLL_CARDINALITY . HLL_UNION . HLL_HASH, however,
    //
    // Hash computation in update method of NDV is slightly different with HLL_HASH,
    // HLL_HASH only has string types, while NDV's hash function handle string types
    // and non-string types in different ways. so:
    // NDV(a) != HLL_UNION_AGG(HLL_HASH(a)), instead
    // NDV(CAST(a AS STRING))  = HLL_UNION_AGG(HLL_HASH(a)).
    // so HLL_RAW/NDV/APPROX_COUNT_DISTINCT is incompatible with HLL_UNION/HLL_RAW_AGG/HLL_UNION_AGG
    public static Optional<OpPlus2> rewriteHll(OpPlus hllOp, Supplier<Integer> idGen,
                                               Map<StrictOp, Integer> existingOps) {
        Op op = hllOp.getOp();
        Op arg;
        if (op.isFun(FunctionSet.NDV) || op.isFun(FunctionSet.APPROX_COUNT_DISTINCT)) {
            arg = Op.apply(Type.HLL, FunctionSet.HLL_RAW, true, op.getArgs());
        } else if (op.isFun(FunctionSet.HLL_UNION_AGG)) {
            arg = Op.apply(Type.HLL, FunctionSet.HLL_UNION, true, op.getArgs());
        } else {
            return Optional.empty();
        }

        EitherOr<OpPlus> opPlus = Optional.ofNullable(existingOps.get(arg.strict()))
                .map(id -> EitherOr.either(OpPlus.of(arg, id)))
                .orElseGet(() -> EitherOr.or(OpPlus.of(arg, idGen.get())));

        Op var = opPlus.get().toVar();
        Op hllCardOp = Op.apply(hllOp.getOp().getType(), FunctionSet.HLL_CARDINALITY, true, var);

        OpPlus hllCardOpPlus = OpPlus.of(hllCardOp, hllOp.getId());
        return Optional.of(OpPlus2.of(hllCardOpPlus, opPlus));
    }

    public static ColumnRefSet eliminateTrivialIsNotNull(List<Op> conjuncts) {
        if (conjuncts.stream().noneMatch(Op::isVarIsNotNull)) {
            return new ColumnRefSet();
        }
        Map<Boolean, List<Op>> conjGroups = conjuncts.stream().collect(Collectors.partitioningBy(Op::isVarIsNotNull));
        List<Op> isNotNullConjuncts = conjGroups.get(true);
        Set<Integer> ids = isNotNullConjuncts.stream().map(op -> op.unmodified().getId()).collect(Collectors.toSet());
        List<Op> otherConjuncts = conjGroups.get(false).stream().map(Op::eliminateInRange).collect(Collectors.toList());
        Op conjunct = Op.and(otherConjuncts);
        ColumnRefSet eraseIds = new ColumnRefSet();
        for (Integer id : ids) {
            ColumnRefSet nullIds = new ColumnRefSet(id);
            if (PartiallyApplyNullsEval.isFalseOrNull(conjunct, nullIds)) {
                eraseIds.union(id);
            }
        }
        return eraseIds;
    }

    public static Optional<String> getFnName(GenericColumn column) {
        return getOp(column).map(op -> op.cast(Apply.class))
                .flatMap(Function.identity())
                .map(apply -> apply.getKind().toString());
    }

    public static String mustGetFnName(GenericColumn column) {
        return Objects.requireNonNull(getFnName(column).orElse(null));
    }

    private static Optional<Op> substImpl(Op op, Map<Integer, Op> opMap) {
        if (op.isVal()) {
            return Optional.empty();
        }
        if (op.isVar()) {
            return Optional.ofNullable(opMap.get(op.getId()));
        }
        List<Pair<Op, Optional<Op>>> argPairs = op.getArgs().stream()
                .map(arg -> Pair.create(arg, substImpl(arg, opMap)))
                .collect(Collectors.toList());
        if (argPairs.stream().noneMatch(p -> p.second.isPresent())) {
            return Optional.empty();
        } else {
            List<Op> newArgs = argPairs.stream()
                    .map(p -> p.second.orElse(p.first))
                    .collect(ImmutableList.toImmutableList());
            Apply oldApply = op.cast();
            Op newOp = Op.apply(oldApply.getType(), oldApply.getKind(), oldApply.isOrdered(), newArgs);
            return Optional.of(newOp);
        }
    }

    public static Op substId(Op op, Map<Integer, Integer> idToId) {
        if (!op.getIds().isIntersect(ColumnRefSet.createByIds(idToId.keySet()))) {
            return op;
        } else {
            return substIdImpl(op, idToId).orElse(op);
        }
    }

    public static TieredList<Op> substId(Collection<Op> ops, Map<Integer, Integer> idToId) {
        return ops.stream().map(op -> substId(op, idToId)).collect(TieredList.<Op>toList());
    }

    public static Optional<Op> substIdImpl(Op op, Map<Integer, Integer> idToId) {
        if (op.isVal()) {
            return Optional.empty();
        }

        if (op.isVar()) {
            return Optional.ofNullable(idToId.get(op.getId())).map(id -> Op.var(op.getType(), id));
        }

        Apply apply = op.cast();
        List<Pair<Op, Optional<Op>>> argPairs = apply.getArgs()
                .stream()
                .map(arg -> Pair.create(arg, substIdImpl(arg, idToId)))
                .collect(Collectors.toList());

        if (argPairs.stream().noneMatch(ap -> ap.second.isPresent())) {
            return Optional.empty();
        } else {
            List<Op> newArgs = argPairs
                    .stream()
                    .map(p -> p.second.orElse(p.first))
                    .collect(Collectors.toList());
            Op newOp = Op.apply(apply.getType(), apply.getKind(), apply.isOrdered(), newArgs);
            return Optional.of(newOp);
        }
    }

    private static Optional<Op> subst(Op op, Map<Integer, Op> opMap, ColumnRefSet columnIds) {
        if (!columnIds.isIntersect(op.getIds())) {
            return Optional.empty();
        } else {
            return substImpl(op, opMap);
        }
    }

    public static Optional<Op> subst(Op op, Map<Integer, Op> opMap) {
        return subst(op, opMap, ColumnRefSet.createByIds(opMap.keySet()));
    }

    public static TieredList<Op> subst(Collection<Op> conjuncts, Map<Integer, Op> opMap) {
        ColumnRefSet columnIds = ColumnRefSet.createByIds(opMap.keySet());
        return conjuncts.stream().map(op -> subst(op, opMap, columnIds).orElse(op)).collect(TieredList.toList());
    }

    private static GenericColumn subst(GenericColumn column,
                                       Map<Integer, Op> idToOpMap, boolean normUnchanged) {
        if (column.isOriginal()) {
            return column;
        } else {
            DerivedColumn derivedColumn = column.cast();
            Op oldOp = derivedColumn.getOp();
            Op newOp = subst(derivedColumn.getOp(), idToOpMap).orElse(oldOp);
            GenericColumn newDerivedColumn = GenericColumn.derived(newOp);
            if (normUnchanged) {
                newDerivedColumn.setNorm(Objects.requireNonNull(derivedColumn.getNorm()));
            }
            return newDerivedColumn;
        }
    }

    public static TieredMap<Integer, GenericColumn> subst(Map<Integer, GenericColumn> columns,
                                                          Map<Integer, Op> substMap) {
        return columns.entrySet()
                .stream()
                .collect(TieredMap.toMap(Map.Entry::getKey, e -> subst(e.getValue(), substMap, false)));
    }

    public static TieredMap<Integer, GenericColumn> subst(Collection<GenericColumn> columns,
                                                          Map<Integer, Op> idToOpMap,
                                                          ColumnRefToIdConverter idConverter, boolean normUnchanged) {
        return columns.stream().collect(TieredMap.toMap(
                e -> idConverter.nextId(),
                e -> subst(e, idToOpMap, normUnchanged)
        ));
    }

    public static TieredMap<Integer, GenericColumn> subst(Map<Integer, GenericColumn> columns,
                                                          Map<String, Integer> normToOriginalColumnId,
                                                          Map<Integer, Op> idToOp,
                                                          ColumnRefToIdConverter idConverter) {
        return columns.entrySet().stream().collect(TieredMap.toMap(
                e -> e.getValue().isDerived() ? idConverter.nextId() :
                        Objects.requireNonNull(normToOriginalColumnId.get(e.getValue().getNorm().toString())),
                e -> subst(e.getValue(), idToOp, true)
        ));
    }

    public static Function<Op, Op> toSubstitute(TieredMap<Integer, Op> opMap) {
        return op -> subst(op, opMap).orElse(op);
    }

    public static TieredMap<Integer, GenericColumn> columnize(Collection<Pair<Integer, Op>> ops) {
        return ops.stream().collect(TieredMap.toMap(p -> p.first, p -> GenericColumn.derived(p.second)));
    }

    public static TieredMap<Integer, GenericColumn> columnize(TieredMap<Integer, Op> ops) {
        return ops.entrySet().stream().collect(TieredMap.toMap(
                Map.Entry::getKey,
                e -> GenericColumn.derived(e.getValue())));
    }

    private static Optional<Op> unfoldImpl(Op op, TieredMap<Integer, GenericColumn> underlyingColumns) {
        if (op.isVal()) {
            return Optional.empty();
        } else if (op.isVar()) {
            // var must be present in underlying columns
            return Optional.of(underlyingColumns.get(op.getId()).getNormalizedOp());
        } else if (op.getIds().isEmpty()) {
            return Optional.empty();
        } else {
            Apply oldOp = op.cast();
            List<Op> newArgs = op.getArgs().stream()
                    .map(arg -> unfoldImpl(arg, underlyingColumns).orElse(arg))
                    .collect(Collectors.toList());
            return Optional.of(Apply.apply(oldOp.getType(), oldOp.getKind(), oldOp.isOrdered(), newArgs));
        }
    }

    private static Op unfold(Op op, TieredMap<Integer, GenericColumn> underlyingColumns) {
        return unfoldImpl(op, underlyingColumns).orElse(op.clone());
    }

    public static Op unfoldOp(Op op, TieredMap<Integer, GenericColumn> underlyingColumns) {
        op.setNorm(unfold(op, underlyingColumns));
        return op;
    }

    public static GenericColumn unfoldDerivedColumn(DerivedColumn derivedColumn,
                                                    TieredMap<Integer, GenericColumn> underlyingColumns) {
        Op newOp = OpUtil.unfold(derivedColumn.getExpr().getOp(), underlyingColumns);
        GenericColumn normColumn = GenericColumn.derived(newOp);
        derivedColumn.setNorm(normColumn);
        return derivedColumn;
    }

    public static Op columnToOp(int id, GenericColumn column) {
        if (column.isOriginal()) {
            return toVar(id, column);
        } else {
            return column.getOp();
        }
    }

    public static Pair<Integer, GenericColumn> opToColumn(Op op, Supplier<Integer> idGen) {
        if (op.isVar()) {
            Var var = op.cast();
            return Pair.create(var.getId(), var.getTenuredColumn());
        } else {
            return Pair.create(idGen.get(), GenericColumn.derived(op));
        }
    }

    public static List<TieredList<Op>> seekForIdInOp(Op op, Integer id) {
        if (!op.getIds().contains(id)) {
            return Collections.emptyList();
        } else {
            TieredList<Op> path = TieredList.genesis();
            return seekForIdInOpImpl(op, id, path);
        }
    }

    private static List<TieredList<Op>> seekForIdInOpImpl(Op op, Integer id, TieredList<Op> path) {
        if (!op.getIds().contains(id)) {
            return Collections.emptyList();
        }
        TieredList<Op> newPath = path.concatOne(op);
        if (op.isVar()) {
            return Collections.singletonList(newPath);
        }
        return op.getArgs().stream()
                .filter(arg -> arg.getIds().contains(id))
                .map(arg -> seekForIdInOpImpl(arg, id, newPath))
                .flatMap(List::stream)
                .collect(Collectors.toList());
    }

    public static <T> Supplier<Optional<T>> getSamplingGenerator(List<Var> vars, int n,
                                                                 Function<List<Integer>, T> mapper) {
        List<Supplier<Integer>> ubGens = Lists.newArrayListWithCapacity(vars.size());
        for (int i = 0; i < vars.size(); ++i) {
            ubGens.add(Util.nextExpGenerator(2, 0));
        }
        Supplier<Optional<List<Integer>>> ubsGen = Util.nextValuesGenerator(n * 2, ubGens);
        return () -> ubsGen.get().map(mapper);
    }

    public static Supplier<Optional<List<Op>>> getSamplingConjunctsGenerator(List<Var> vars, int n) {
        return getSamplingGenerator(vars, n, ubs ->
                IntStream.range(0, vars.size())
                        .mapToObj(i -> createOneSampleConjunct(vars.get(i), n, ubs.get(i)))
                        .collect(Collectors.toList()));
    }

    public static Supplier<Optional<Double>> getSamplingRatio(List<Var> vars, int n) {
        return getSamplingGenerator(vars, n, ubs -> IntStream.range(0, vars.size())
                .mapToObj(i -> (double) ubs.get(i) / (double) (2 * n)).reduce(1.0, (a, b) -> a * b));
    }

    public static Op createOneSampleConjunct(Var var, int n, int ub) {
        // murmur_hash3_32(c)
        Op hash = Apply.apply(Type.INT, FunctionKind.of(FunctionSet.MURMUR_HASH3_32), true, ImmutableList.of(var));
        // n
        Op constN = Apply.val(ConstantOperator.createBigint(n));

        // murmur_hash3_32(c)%n
        List<Op> modArgs = ImmutableList.of(hash, constN);
        String modFunc = ArithmeticExpr.Operator.MOD.getName();
        Op modOp = Apply.apply(Type.BIGINT, FunctionKind.of(modFunc), true, modArgs);

        // murmur_hash3_32(c)%n+n
        String addFunc = ArithmeticExpr.Operator.ADD.getName();
        List<Op> addArgs = ImmutableList.of(modOp, constN);
        Op addOp = Apply.apply(Type.BIGINT, FunctionKind.of(addFunc), true, addArgs);

        // coalesce(murmur_hash3_32(c)%n+n, 0)
        Op const0 = Apply.val(ConstantOperator.createBigint(0));
        List<Op> coalesceArgs = ImmutableList.of(addOp, const0);
        Op coalesceOp = Apply.apply(Type.BIGINT, FunctionKind.of(FunctionSet.COALESCE), true, coalesceArgs);

        // coalesce(murmur_hash3_32(c)%n+n, 0) between lb and ub
        Op constUb = Apply.val(ConstantOperator.createBigint(ub));
        return Apply.le(coalesceOp, constUb);
    }

    private static Var toVar(int id, GenericColumn column) {
        Preconditions.checkArgument(column.isOriginal());
        Var var = Apply.var(column.getType(), id);
        var.getSymbol().tenured(column);
        return var;
    }

    public static Range<Val> toRange(Op op) {
        if (op.isEq()) {
            Op a = op.arg(0);
            Op b = op.arg(1);
            if (a.isVar() && b.isVal()) {
                Val v = b.cast();
                return Range.closed(v, v);
            } else {
                return Range.all();
            }
        } else if (op.isLt() || op.isLe() || op.isGt() || op.isGe()) {
            Op a = op.arg(0);
            Op b = op.arg(1);
            BoundType boundType = op.isLt() || op.isGt() ? BoundType.OPEN : BoundType.CLOSED;
            boolean isLeLt = op.isLe() || op.isLt();
            if (a.isVar() && b.isVal()) {
                Val v = b.cast();
                return isLeLt ? Range.upTo(v, boundType) : Range.downTo(v, boundType);
            } else if (a.isVal() && b.isVar()) {
                Val v = a.cast();
                return isLeLt ? Range.downTo(v, boundType) : Range.upTo(v, boundType);
            } else {
                return Range.all();
            }
        } else if (op.isIn() && op.arg(0).isVar() &&
                op.arg(1).isSetOf() &&
                !op.arg(1).getArgs().isEmpty() &&
                op.arg(1).getArgs().stream().allMatch(Op::isVal)) {
            List<Val> elms = op.arg(1).getArgs().stream()
                    .map(elm -> elm.mustCast(Val.class))
                    .collect(Collectors.toList());
            Val maxVal = Collections.max(elms, Val::compareTo);
            Val minVal = Collections.min(elms, Val::compareTo);
            return Range.closed(minVal, maxVal);
        } else {
            return Range.all();
        }
    }

    public static Range<Val> toRange(List<Op> conjuncts) {
        conjuncts = conjuncts.stream().map(Op::eliminateInRange).collect(Collectors.toList());
        if (conjuncts.isEmpty()) {
            return Range.all();
        } else if (conjuncts.size() == 1) {
            return toRange(conjuncts.get(0));
        } else {
            ColumnRefSet firstOpIds = conjuncts.get(0).getIds();
            if (firstOpIds.size() != 1) {
                return Range.all();
            }
            if (!conjuncts.stream().allMatch(op -> op.getIds().equals(firstOpIds))) {
                return Range.all();
            }
            List<Range<Val>> ranges = conjuncts.stream()
                    .map(OpUtil::toRange)
                    .collect(Collectors.toList());
            if (ranges.size() < conjuncts.size()) {
                return Range.all();
            }
            return ranges.stream().reduce(Range::span).orElse(Range.all());
        }
    }

    public static Range<Val> mergeRangeConjuncts(List<TieredList<Op>> conjunctsList) {
        List<Range<Val>> ranges = conjunctsList.stream().map(OpUtil::toRange).collect(Collectors.toList());
        //return conjunctsList.stream().map(OpUtil::toRange).reduce(Range::span).orElse(Range.all());
        return ranges.stream().reduce(Range::span).orElse(Range.all());
    }

    public static Optional<List<TieredList<Op>>> getRangeConjuncts(List<Var> varList,
                                                                   List<TieredList<Op>> conjunctsList) {
        Preconditions.checkArgument(varList.size() == conjunctsList.size());
        Range<Val> range = mergeRangeConjuncts(conjunctsList);
        if (range.equals(Range.all())) {
            return Optional.empty();
        }

        Function<Var, TieredList<Op>> rangeConjunctsBuilder = var -> {
            TieredList.Builder<Op> rangeConjuncts = TieredList.newGenesisTier();
            if (range.hasLowerBound()) {
                rangeConjuncts.add(Op.le(range.lowerEndpoint(), var));
            }
            if (range.hasUpperBound()) {
                rangeConjuncts.add(Op.le(var, range.upperEndpoint()));
            }
            return rangeConjuncts.build();
        };

        List<TieredList<Op>> rangeConjuncts = varList.stream().map(rangeConjunctsBuilder).collect(Collectors.toList());
        return Optional.of(rangeConjuncts);
    }

    public static GenericColumn getConst1Column() {
        return GenericColumn.derived(Op.val(ConstantOperator.createInt(1)));
    }

    public static boolean hasComplexOp(GenericColumn column) {
        return column.cast(DerivedColumn.class)
                .map(dColumn -> !dColumn.getOp().collect(op -> (op.isCase() || op.isFun(FunctionSet.IF))).isEmpty())
                .orElse(false);
    }

    public static List<Pair<Integer, TimeGranule>> extractPartitionByTimeGranule(
            TieredMap<Integer, GenericColumn> columns,
            ColumnRefSet partitionByColumnIds) {
        return columns.entrySet()
                .stream()
                .map(e -> Pair.create(e.getKey(), OpUtil.columnToOp(e.getKey(), e.getValue())))
                .map(p -> Pair.create(p.first, OpUtil.getPartitionByTimeGranule(p.second, partitionByColumnIds)))
                .filter(p -> p.second.isPresent())
                .map(p -> Pair.create(p.first, p.second.get()))
                .collect(Collectors.toList());
    }

    public static boolean isTrivialDerivedColumn(GenericColumn column) {
        return column.cast(DerivedColumn.class).map(GenericColumn::getOp).map(Op::isVar).orElse(false);
    }

    public static Map<Boolean, TieredMap<Integer, GenericColumn>> splitTrivialColumns(
            TieredMap<Integer, GenericColumn> columns) {
        return columns.entrySet().stream().collect(Collectors.partitioningBy(
                e -> OpUtil.isTrivialDerivedColumn(e.getValue()),
                TieredMap.toMap()));
    }

    public static ColumnsAndSubstMap eliminateDerivedVars(ColumnsAndSubstMap columnsAndSubstMap) {
        TieredMap<Integer, GenericColumn> columns = columnsAndSubstMap.getColumns();
        TieredMap<Integer, Op> substMap = columnsAndSubstMap.getSubstMap();
        Map<Boolean, TieredMap<Integer, GenericColumn>> columnGroup =
                OpUtil.splitTrivialColumns(OpUtil.subst(columns, substMap));

        TieredMap<Integer, GenericColumn> trivialColumns = columnGroup.get(true);
        TieredMap<Integer, GenericColumn> nonTrivialColumns = columnGroup.get(false);
        if (trivialColumns.isEmpty()) {
            TieredMap<Integer, Op> extraSubstMap = OpUtil.columnsToOpMap(nonTrivialColumns);
            substMap = TieredMap.mergeIntegerKey(substMap, extraSubstMap);
            return ColumnsAndSubstMap.of(nonTrivialColumns, substMap);
        }

        TieredMap<Integer, Op> extraSubstMap = OpUtil.columnsToOpMap(nonTrivialColumns);
        substMap = TieredMap.mergeIntegerKey(substMap, extraSubstMap);
        Map<Boolean, TieredMap<Integer, GenericColumn>> newColumnGroup =
                OpUtil.splitTrivialColumns(OpUtil.subst(trivialColumns, substMap));

        TieredMap<Integer, GenericColumn> newTrivialColumns = newColumnGroup.get(true);
        TieredMap<Integer, GenericColumn> newNonTrivialColumns = newColumnGroup.get(false);

        TieredMap<Integer, GenericColumn> newColumns = nonTrivialColumns.merge(newNonTrivialColumns);
        TieredMap<Integer, Op> extraTrivialSubstMap =
                OpUtil.columnsToOpMap(newTrivialColumns.merge(newNonTrivialColumns));
        substMap = TieredMap.mergeIntegerKey(substMap, extraTrivialSubstMap);
        return ColumnsAndSubstMap.of(newColumns, substMap);
    }

    public static boolean isStiffPredicate(Op op) {
        Set<String> defaultValues = ImmutableSet.of("n/a", "na", "none", "", "unknown", "tbd",
                "-1", "" + Integer.MAX_VALUE, "" + Integer.MIN_VALUE, "" + Long.MAX_VALUE, "" + Long.MIN_VALUE,
                BigInteger.ONE.negate().toString(),
                BigInteger.ONE.shiftLeft(127).subtract(BigInteger.ONE).toString(),
                BigInteger.ONE.shiftLeft(127).negate().toString(),
                "1970-01-01", "0000-00-00", "1900-01-01", "9999-12-31",
                "1970-01-01 00:00:00", "1900-01-01 00:00:00", "9999-12-31 23:59:59", "0000-00-00 00:00:00");

        if (op.isVarNeVal()) {
            return defaultValues.contains(op.arg(1).mustCast(Val.class).getValue().toString().toLowerCase());
        } else if (op.isVarNotLikeVal()) {
            return defaultValues.contains(
                    op.unmodified().arg(1).mustCast(Val.class).getValue().toString().toLowerCase());
        } else if (op.isOr()) {
            return op.getArgs().stream().allMatch(OpUtil::isStiffPredicate);
        } else {
            return op.isVarIsNotNull();
        }
    }

    public static Op exprToOp(Expr expr, ColumnRefToIdConverter idConverter,
                              TieredMap<Integer, GenericColumn> columns) {
        return OpUtil.toOpConverter(idConverter, columns).apply(SqlToScalarOperatorTranslator.translate(expr));
    }

    public static Op literalExprToOp(LiteralExpr literal) {
        return OpUtil.toOpConverter(new ColumnRefToIdConverter(), TieredMap.genesis())
                .apply(SqlToScalarOperatorTranslator.translate(literal));
    }
}
