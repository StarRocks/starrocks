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
package com.starrocks.catalog.combinator;

import com.google.api.client.util.Lists;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.starrocks.analysis.FunctionParams;
import com.starrocks.catalog.AggregateFunction;
import com.starrocks.catalog.AnyArrayType;
import com.starrocks.catalog.AnyElementType;
import com.starrocks.catalog.AnyMapType;
import com.starrocks.catalog.AnyStructType;
import com.starrocks.catalog.ArrayType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.MapType;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.StructType;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.FunctionAnalyzer;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVTestBase;
import com.starrocks.sql.optimizer.statistics.EmptyStatisticStorage;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.statistic.StatisticsMetaManager;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.apache.kudu.shaded.com.google.common.collect.Streams;
import org.assertj.core.util.Sets;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class AggStateCombinatorTest extends MVTestBase {
    private static final int MAX_AGG_FUNC_NUM_IN_TEST = 20;

    @BeforeClass
    public static void beforeClass() throws Exception {
        // set default config for async mvs
        UtFrameUtils.setDefaultConfigForAsyncMVTest(connectContext);

        Config.alter_scheduler_interval_millisecond = 1;
        FeConstants.runningUnitTest = true;
        UtFrameUtils.createMinStarRocksCluster();
        GlobalStateMgr.getCurrentState().setStatisticStorage(new EmptyStatisticStorage());

        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withEnableMV().withDatabase("test").useDatabase("test");

        // set default config for timeliness mvs
        UtFrameUtils.mockTimelinessForAsyncMVTest(connectContext);
        if (!starRocksAssert.databaseExist("_statistics_")) {
            StatisticsMetaManager m = new StatisticsMetaManager();
            m.createStatisticsTablesForTest();
        }
        Config.enable_materialized_view_text_based_rewrite = false;
        setGlobalVariableVariable("cbo_push_down_aggregate_mode", "-1");
    }

    private static final Set<String> SUPPORTED_AGG_STATE_FUNCTIONS = ImmutableSet.of(
            "ndv", "percentile_disc", "corr", "multi_distinct_sum", "var_samp", "sum", "stddev_pop",
            "array_agg_distinct", "approx_count_distinct", "variance_samp", "min", "avg", "any_value",
            "stddev", "max_by", "multi_distinct_count", "retention", "mann_whitney_u_test", "min_by",
            "var_pop", "percentile_cont", "std", "max", "covar_samp", "stddev_samp", "array_unique_agg",
            "bitmap_agg", "ds_hll_count_distinct", "percentile_approx", "variance", "bitmap_union_int",
            "variance_pop", "covar_pop");

    private List<AggregateFunction> getBuiltInAggFunctions() {
        List<AggregateFunction> builtInAggregateFunctions = Lists.newArrayList();
        var functions = GlobalStateMgr.getCurrentState().getBuiltinFunctions();
        for (var func : functions) {
            if (!(func instanceof AggregateFunction)) {
                continue;
            }
            if ((func instanceof AggStateMergeCombinator) || (func instanceof AggStateUnionCombinator)) {
                continue;
            }
            builtInAggregateFunctions.add((AggregateFunction) func);
        }
        Assert.assertTrue(builtInAggregateFunctions.size() > 0);
        return builtInAggregateFunctions;
    }

    private Type mockType(Type type) {
        if (type.isDecimalOfAnyVersion()) {
            return ScalarType.createDecimalV3NarrowestType(10, 2);
        } else if (type.isChar()) {
            return ScalarType.createCharType(100);
        } else if (type.isVarchar()) {
            return ScalarType.createVarcharType(100);
        } else if (type instanceof AnyElementType) {
            return Type.STRING;
        } else if (type instanceof AnyArrayType) {
            return Type.ARRAY_BIGINT;
        } else if (type instanceof AnyMapType) {
            return new MapType(Type.STRING, Type.STRING);
        } else if (type instanceof AnyStructType) {
            Type[] argsTypes = {Type.STRING};
            ArrayList<Type> structTypes = new ArrayList<>();
            for (Type t : argsTypes) {
                structTypes.add(new ArrayType(t));
            }
            return new StructType(structTypes);
        } else if (type.isArrayType()) {
            ArrayType arrayType = (ArrayType) type;
            return new ArrayType(mockType(arrayType.getItemType()));
        } else {
            return type;
        }
    }

    private Function getAggStateFunc(AggregateFunction aggFunc) {
        List<Type> argTypes = Stream.of(aggFunc.getArgs()).map(this::mockType).collect(Collectors.toList());
        String aggStateFuncName = FunctionSet.getAggStateName(aggFunc.functionName());
        Type[] argumentTypes = argTypes.toArray(Type[]::new);
        System.out.println("FunctionName:" + aggFunc.functionName() + ", Arg Types:" + argTypes
                + ", Intermediate Types:" + aggFunc.getIntermediateTypeOrReturnType()
                + ", ReturnType:" + aggFunc.getReturnType());
        FunctionParams params = new FunctionParams(false, Lists.newArrayList());
        Boolean[] argArgumentConstants = IntStream.range(0, aggFunc.getNumArgs())
                .mapToObj(x -> new Boolean(false)).toArray(Boolean[]::new);
        Function result = FunctionAnalyzer.getAnalyzedAggregateFunction(ConnectContext.get(),
                aggStateFuncName, params, argumentTypes, argArgumentConstants, NodePosition.ZERO);
        return result;
    }

    private String buildAggStateColumn(String fnName, List<String> argTypes, String colName) {
        String argTypesStr = argTypes.stream().collect(Collectors.joining(","));
        return String.format("%s %s(%s)", colName, fnName, argTypesStr);
    }

    private String buildAggFuncArgs(String funcName,
                                    List<String> argTypes,
                                    Map<String, String> colTypes) {
        List<String> args = Lists.newArrayList();
        switch (funcName) {
            case FunctionSet.PERCENTILE_DISC:
            case FunctionSet.LC_PERCENTILE_DISC:
            case FunctionSet.PERCENTILE_APPROX:
            case FunctionSet.PERCENTILE_CONT:
                args.add(colTypes.get(argTypes.get(0)));
                args.add("0.5");
                break;
            case FunctionSet.MANN_WHITNEY_U_TEST: {
                args.add(colTypes.get(argTypes.get(0)));
                args.add(colTypes.get(argTypes.get(1)));
                if (argTypes.size() == 3) {
                    args.add("'less'");
                } else {
                    args.add("'two-sided'");
                    args.add("0");
                }
                break;
            }
            case FunctionSet.DS_HLL_COUNT_DISTINCT: {
                args.add(colTypes.get(argTypes.get(0)));
                if (argTypes.size() == 2) {
                    args.add("18");
                } else {
                    args.add("18");
                    args.add("'hll_6'");
                }
                break;
            }
            case FunctionSet.WINDOW_FUNNEL: {
                args.add("1800");
                args.add(colTypes.get(argTypes.get(1)));
                args.add("0");
                args.add("[k1='2024-01-01']");
                break;
            }
            case FunctionSet.APPROX_TOP_K: {
                args.add(colTypes.get(argTypes.get(0)));
                if (argTypes.size() == 2) {
                    args.add("10");
                } else {
                    args.add("10");
                    args.add("100");
                }
                break;
            }
            default:
                for (String argType : argTypes) {
                    args.add(colTypes.get(argType));
                }
        }
        return Joiner.on(", ").join(args);
    }

    private void buildTableT1(List<String> funcNames,
                              Map<String, String> colTypes,
                              List<List<String>> aggArgTypes) {
        buildTableT1(funcNames, colTypes, aggArgTypes, Lists.newArrayList(), Lists.newArrayList(), -1);
    }

    private void buildTableT1(List<String> funcNames,
                              Map<String, String> colTypes,
                              List<List<String>> aggArgTypes,
                              List<String> aggStateColumns,
                              List<String> aggStateColNames) {
        buildTableT1(funcNames, colTypes, aggArgTypes, aggStateColumns, aggStateColNames, -1);
    }

    private void buildTableT1(List<String> funcNames,
                              Map<String, String> colTypes,
                              List<List<String>> aggArgTypes,
                              List<String> aggStateColumns,
                              List<String> aggStateColNames,
                              int size) {
        String define = "c0 boolean,\n" +
                "c1 tinyint(4),\n" +
                "c2 smallint(6),\n" +
                "c3 int(11),\n" +
                "c4 bigint(20),\n" +
                "c5 largeint(40),\n" +
                "c6 double,\n" +
                "c7 float,\n" +
                "c8 decimal(10, 2),\n" +
                "c9 char(100),\n" +
                "c10 date,\n" +
                "c11 datetime,\n" +
                "c12 array<boolean>,\n" +
                "c13 array<tinyint(4)>,\n" +
                "c14 array<smallint(6)>,\n" +
                "c15 array<int(11)>,\n" +
                "c16 array<bigint(20)>,\n" +
                "c17 array<largeint(40)>,\n" +
                "c18 array<double>,\n" +
                "c19 array<float>,\n" +
                "c20 array<DECIMAL64(10,2)>,\n" +
                "c21 array<char(100)>,\n" +
                "c22 array<date>,\n" +
                "c23 array<datetime>,\n" +
                "c24 varchar(100),\n" +
                "c25 json,\n" +
                "c26 varbinary,\n" +
                "c27 map<varchar(1048576),varchar(1048576)>,\n" +
                "c28 struct<col1 array<varchar(1048576)>>,\n" +
                "c29 array<varchar(100)>";
        String[] splits = define.split(",\n");
        for (String colType : splits) {
            String[] parts = colType.split(" ");
            String colName = parts[0];
            String type = colType.substring(colName.length() + 1);
            colTypes.put(type, colName);
        }
        var builtInAggregateFunctions = getBuiltInAggFunctions();
        int i = 0;
        for (AggregateFunction aggFunc : builtInAggregateFunctions) {
            if (!AggStateUtils.isSupportedAggStateFunction(aggFunc)) {
                continue;
            }

            if (size != -1 && i > size) {
                break;
            }
            List<Type> argTypes = Stream.of(aggFunc.getArgs()).map(this::mockType).collect(Collectors.toList());
            List<String> argTypeStr = argTypes.stream().map(this::mockType).map(Type::toSql).collect(Collectors.toList());
            aggArgTypes.add(argTypeStr);
            for (String argType : argTypeStr) {
                Preconditions.checkArgument(colTypes.containsKey(argType), "argType:" + argType + " not found");
            }
            String colName = "v" + i;
            aggStateColNames.add(colName);
            funcNames.add(aggFunc.functionName());
            aggStateColumns.add(buildAggStateColumn(aggFunc.functionName(), argTypeStr, colName));
            i++;
        }
        // prepare base table: t1
        try {
            String sql = "CREATE TABLE t1 ( \n" +
                    "k1  date, \n" +
                    colTypes.entrySet().stream()
                            .map(e -> String.format("%s %s", e.getValue(), e.getKey()))
                            .collect(Collectors.joining(",\n")) +
                    ") DUPLICATE KEY(k1) \n" +
                    "DISTRIBUTED BY HASH(k1) \n" +
                    "PROPERTIES (  \"replication_num\" = \"1\");";
            starRocksAssert.withTable(sql);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testCreateAggStateCombinator() {
        var builtInAggregateFunctions = getBuiltInAggFunctions();
        for (AggregateFunction aggFunc : builtInAggregateFunctions) {
            if (FunctionSet.UNSUPPORTED_AGG_STATE_FUNCTIONS.contains(aggFunc.functionName())) {
                continue;
            }
            var mergeCombinator = AggStateMergeCombinator.of(aggFunc);
            Assert.assertTrue(mergeCombinator.isPresent());
            var unionCombinator = AggStateUnionCombinator.of(aggFunc);
            Assert.assertTrue(unionCombinator.isPresent());
        }
    }

    @Test
    public void testFunctionAnalyzerStateCombinator() {
        var builtInAggregateFunctions = getBuiltInAggFunctions();
        Set<String> supportedAggFunctions = Sets.newHashSet();
        Set<String> unSupportedAggFunctions = Sets.newHashSet();
        for (AggregateFunction aggFunc : builtInAggregateFunctions) {
            if (!AggStateUtils.isSupportedAggStateFunction(aggFunc)) {
                unSupportedAggFunctions.add(aggFunc.functionName());
                continue;
            }
            supportedAggFunctions.add(aggFunc.functionName());
            Function result = getAggStateFunc(aggFunc);
            Assert.assertNotNull(result);
            Assert.assertTrue(result instanceof AggStateCombinator);
            Assert.assertFalse(result.getReturnType().isWildcardDecimal());
            Assert.assertFalse(result.getReturnType().isPseudoType());
        }
        System.out.println("Supported Agg Functions size:" + supportedAggFunctions.size());
        System.out.println("Supported Agg Functions:" + supportedAggFunctions);
        System.out.println("UnSupported Agg Functions:" + unSupportedAggFunctions);
        Assert.assertTrue(supportedAggFunctions.size() >= SUPPORTED_AGG_STATE_FUNCTIONS.size());
    }

    @Test
    public void testFunctionAnalyzerUnionCombinator() {
        var builtInAggregateFunctions = getBuiltInAggFunctions();
        Set<String> supportedAggFunctions = Sets.newHashSet();
        for (AggregateFunction aggFunc : builtInAggregateFunctions) {
            if (!AggStateUtils.isSupportedAggStateFunction(aggFunc)) {
                continue;
            }
            supportedAggFunctions.add(aggFunc.functionName());
            Function aggStateFunc = getAggStateFunc(aggFunc);
            Assert.assertNotNull(aggStateFunc);
            String aggStateFuncName = FunctionSet.getAggStateUnionName(aggFunc.functionName());
            FunctionParams params = new FunctionParams(false, Lists.newArrayList());

            Type intermediateType = aggStateFunc.getReturnType();
            // set agg_state_desc
            List<Type> argTypes = Stream.of(aggFunc.getArgs()).map(this::mockType).collect(Collectors.toList());
            intermediateType.setAggStateDesc(new AggStateDesc(aggFunc.functionName(),
                    aggFunc.getReturnType(), argTypes));
            Type[] argumentTypes = { intermediateType };
            Boolean[] argArgumentConstants = { false };

            System.out.println("Start to get func:" + aggStateFuncName + ", Arg Types:"
                    + Stream.of(argumentTypes).collect(Collectors.toList()));
            Function result = FunctionAnalyzer.getAnalyzedAggregateFunction(ConnectContext.get(),
                    aggStateFuncName, params, argumentTypes, argArgumentConstants, NodePosition.ZERO);
            Assert.assertNotNull(result);
            Assert.assertTrue(result instanceof AggStateUnionCombinator);
            Assert.assertFalse(result.getReturnType().isWildcardDecimal());
            Assert.assertFalse(result.getReturnType().isPseudoType());
        }
        System.out.println("Supported Agg Functions size:" + supportedAggFunctions.size());
        System.out.println("Supported Agg Functions:" + supportedAggFunctions);
        Assert.assertTrue(supportedAggFunctions.size() >= SUPPORTED_AGG_STATE_FUNCTIONS.size());
    }

    @Test
    public void testFunctionAnalyzerMergeCombinator() {
        var builtInAggregateFunctions = getBuiltInAggFunctions();
        Set<String> supportedAggFunctions = Sets.newHashSet();
        for (AggregateFunction aggFunc : builtInAggregateFunctions) {
            if (!AggStateUtils.isSupportedAggStateFunction(aggFunc)) {
                continue;
            }
            supportedAggFunctions.add(aggFunc.functionName());

            Function aggStateFunc = getAggStateFunc(aggFunc);
            Assert.assertNotNull(aggStateFunc);

            Type intermediateType = aggStateFunc.getReturnType();
            // set agg_state_desc
            List<Type> argTypes = Stream.of(aggFunc.getArgs()).map(this::mockType).collect(Collectors.toList());
            intermediateType.setAggStateDesc(new AggStateDesc(aggFunc.functionName(),
                    aggFunc.getReturnType(), argTypes));
            Type[] argumentTypes = { intermediateType };
            Boolean[] argArgumentConstants = { false };

            String aggStateFuncName = FunctionSet.getAggStateMergeName(aggFunc.functionName());
            FunctionParams params = new FunctionParams(false, Lists.newArrayList());
            Function result = FunctionAnalyzer.getAnalyzedAggregateFunction(ConnectContext.get(),
                    aggStateFuncName, params, argumentTypes, argArgumentConstants, NodePosition.ZERO);
            Assert.assertNotNull(result);
            Assert.assertTrue(result instanceof AggStateMergeCombinator);
            Assert.assertFalse(result.getReturnType().isWildcardDecimal());
            Assert.assertFalse(result.getReturnType().isPseudoType());
        }
        System.out.println("Supported Agg Functions size:" + supportedAggFunctions.size());
        System.out.println("Supported Agg Functions:" + supportedAggFunctions);
        Assert.assertTrue(supportedAggFunctions.size() >= SUPPORTED_AGG_STATE_FUNCTIONS.size());
    }

    @Test
    public void testCreateAggStateTableUnionWithPerTable() {
        var builtInAggregateFunctions = getBuiltInAggFunctions();
        List<String> columns = new ArrayList<>();
        List<String> colNames = new ArrayList<>();
        List<String> funcNames = new ArrayList<>();

        // prepare agg state table
        {
            int i = 0;
            for (AggregateFunction aggFunc : builtInAggregateFunctions) {
                if (!AggStateUtils.isSupportedAggStateFunction(aggFunc)) {
                    continue;
                }
                if (i >= MAX_AGG_FUNC_NUM_IN_TEST) {
                    break;
                }
                String colName = "v" + i;
                colNames.add(colName);
                funcNames.add(aggFunc.functionName());
                List<Type> argTypes = Stream.of(aggFunc.getArgs()).map(this::mockType).collect(Collectors.toList());
                columns.add(buildAggStateColumn(aggFunc.functionName(),
                        argTypes.stream().map(Type::toSql).collect(Collectors.toList()), colName));
                i++;
            }
        }
        String sql = " CREATE TABLE test_agg_state_table ( \n" +
                "k1  date, \n" +
                Joiner.on(",\n").join(columns) +
                ") DISTRIBUTED BY HASH(k1) \n" +
                "PROPERTIES (  \"replication_num\" = \"1\");";
        starRocksAssert.withTable(sql,
                () -> {
                    Table table = starRocksAssert.getCtx().getGlobalStateMgr().getLocalMetastore()
                            .getDb(connectContext.getDatabase()).getTable("test_agg_state_table");
                    List<Column> tableColumns = table.getColumns();
                    Assert.assertEquals(columns.size() + 1, tableColumns.size());
                    // test _union/_merge for per agg function
                    for (int i = 0; i < colNames.size(); i++) {
                        // test _union
                        String fnName = FunctionSet.getAggStateUnionName(funcNames.get(i));
                        String colName = colNames.get(i);
                        String unionFnName = String.format("%s(%s)", fnName, colName);
                        String sql1 = "select k1, " + unionFnName + " from test_agg_state_table group by k1";
                        String plan = UtFrameUtils.getVerboseFragmentPlan(starRocksAssert.getCtx(), sql1);
                        PlanTestBase.assertContains(plan, "  0:OlapScanNode\n" +
                                "     table: test_agg_state_table, rollup: test_agg_state_table");
                    }
                });
    }

    @Test
    public void testCreateAggStateTableMergeWithPerTable() {
        var builtInAggregateFunctions = getBuiltInAggFunctions();
        List<String> columns = new ArrayList<>();
        List<String> colNames = new ArrayList<>();
        List<String> funcNames = new ArrayList<>();

        // prepare agg state table
        {
            int i = 0;
            for (AggregateFunction aggFunc : builtInAggregateFunctions) {
                if (!AggStateUtils.isSupportedAggStateFunction(aggFunc)) {
                    continue;
                }
                if (i > MAX_AGG_FUNC_NUM_IN_TEST) {
                    break;
                }
                String colName = "v" + i;
                colNames.add(colName);
                funcNames.add(aggFunc.functionName());
                List<Type> argTypes = Stream.of(aggFunc.getArgs()).map(this::mockType).collect(Collectors.toList());
                columns.add(buildAggStateColumn(aggFunc.functionName(),
                        argTypes.stream().map(Type::toSql).collect(Collectors.toList()), colName));
                i++;
            }
        }
        String sql = " CREATE TABLE test_agg_state_table ( \n" +
                "k1  date, \n" +
                Joiner.on(",\n").join(columns) +
                ") DISTRIBUTED BY HASH(k1) \n" +
                "PROPERTIES (  \"replication_num\" = \"1\");";
        starRocksAssert.withTable(sql,
                () -> {
                    Table table = starRocksAssert.getCtx().getGlobalStateMgr().getLocalMetastore()
                            .getDb(connectContext.getDatabase()).getTable("test_agg_state_table");
                    List<Column> tableColumns = table.getColumns();
                    Assert.assertEquals(columns.size() + 1, tableColumns.size());
                    // test _union/_merge for per agg function
                    for (int i = 0; i < colNames.size(); i++) {
                        // test _merge
                        String fnName = FunctionSet.getAggStateMergeName(funcNames.get(i));
                        String colName = colNames.get(i);
                        String unionFnName = String.format("%s(%s)", fnName, colName);
                        String sql1 = "select k1, " + unionFnName + " from test_agg_state_table group by k1";
                        String plan = UtFrameUtils.getVerboseFragmentPlan(starRocksAssert.getCtx(), sql1);
                        PlanTestBase.assertContains(plan, "  0:OlapScanNode\n" +
                                "     table: test_agg_state_table, rollup: test_agg_state_table");
                    }
                });
    }



    @Test
    public void testCreateAggStateTableWithAllFunctions() {
        var builtInAggregateFunctions = getBuiltInAggFunctions();
        List<String> columns = new ArrayList<>();
        List<String> colNames = new ArrayList<>();
        List<String> funcNames = new ArrayList<>();

        // prepare agg state table
        {
            int i = 0;
            for (AggregateFunction aggFunc : builtInAggregateFunctions) {
                if (!AggStateUtils.isSupportedAggStateFunction(aggFunc)) {
                    continue;
                }
                if (i > MAX_AGG_FUNC_NUM_IN_TEST) {
                    break;
                }
                String colName = "v" + i;
                colNames.add(colName);
                funcNames.add(aggFunc.functionName());
                List<Type> argTypes = Stream.of(aggFunc.getArgs()).map(this::mockType).collect(Collectors.toList());
                columns.add(buildAggStateColumn(aggFunc.functionName(),
                        argTypes.stream().map(Type::toSql).collect(Collectors.toList()), colName));
                i++;
            }
        }
        String sql = " CREATE TABLE test_agg_state_table ( \n" +
                "k1  date, \n" +
                Joiner.on(",\n").join(columns) +
                ") DISTRIBUTED BY HASH(k1) \n" +
                "PROPERTIES (  \"replication_num\" = \"1\");";
        starRocksAssert.withTable(sql,
                () -> {
                    Table table = starRocksAssert.getCtx().getGlobalStateMgr().getLocalMetastore()
                            .getDb(connectContext.getDatabase()).getTable("test_agg_state_table");
                    List<Column> tableColumns = table.getColumns();
                    Assert.assertEquals(columns.size() + 1, tableColumns.size());
                    // test _union
                    {
                        List<String> unionColumns =
                                Streams.zip(colNames.stream(), funcNames.stream(),
                                                (c, f) -> String.format("%s(%s)", FunctionSet.getAggStateUnionName(f), c))
                                        .collect(Collectors.toList());
                        String sql1 = "select k1, " +
                                Joiner.on(",").join(unionColumns) + " from test_agg_state_table group by k1";
                        String plan = UtFrameUtils.getVerboseFragmentPlan(starRocksAssert.getCtx(), sql1);
                        PlanTestBase.assertContains(plan, "  0:OlapScanNode\n" +
                                "     table: test_agg_state_table, rollup: test_agg_state_table");
                    }

                    // test _merge
                    {
                        List<String> mergeColumns =
                                Streams.zip(colNames.stream(), funcNames.stream(),
                                                (c, f) -> String.format("%s(%s)", FunctionSet.getAggStateMergeName(f), c))
                                        .collect(Collectors.toList());
                        String sql1 = "select k1, " +
                                Joiner.on(",").join(mergeColumns) + " from test_agg_state_table group by k1";
                        String plan = UtFrameUtils.getVerboseFragmentPlan(starRocksAssert.getCtx(), sql1);
                        PlanTestBase.assertContains(plan, "  0:OlapScanNode\n" +
                                "     table: test_agg_state_table, rollup: test_agg_state_table");
                    }
                });
    }

    @Test
    public void testCreateAggStateTable2() throws Exception {
        List<String> funcNames = Lists.newArrayList();
        Map<String, String> colTypes = Maps.newLinkedHashMap();
        List<List<String>> aggArgTypes = Lists.newArrayList();
        buildTableT1(funcNames, colTypes, aggArgTypes);

        // test _state
        for (int k = 0; k < funcNames.size(); k++) {
            if (k > MAX_AGG_FUNC_NUM_IN_TEST) {
                break;
            }
            List<String> stateColumns = Lists.newArrayList();
            List<String> argTypes = aggArgTypes.get(k);
            String fnName = funcNames.get(k);
            String arg = buildAggFuncArgs(fnName, argTypes, colTypes);
            String col = String.format("%s(%s)", FunctionSet.getAggStateName(fnName), arg);
            stateColumns.add(col);
            String sql1 = "select k1, " + Joiner.on(", ").join(stateColumns) + " from t1;";
            String plan = UtFrameUtils.getVerboseFragmentPlan(starRocksAssert.getCtx(), sql1);
            PlanTestBase.assertContains(plan, "  0:OlapScanNode\n" +
                    "     table: t1, rollup: t1");
        }
        starRocksAssert.dropTable("t1");
    }

    @Test
    public void testCreateAggStateTable3() throws Exception {
        List<String> funcNames = Lists.newArrayList();
        Map<String, String> colTypes = Maps.newLinkedHashMap();
        List<List<String>> aggArgTypes = Lists.newArrayList();
        buildTableT1(funcNames, colTypes, aggArgTypes);

        // test _state
        List<String> stateColumns = Lists.newArrayList();
        for (int k = 0; k < funcNames.size(); k++) {
            if (k > MAX_AGG_FUNC_NUM_IN_TEST) {
                break;
            }
            String fnName = funcNames.get(k);
            List<String> argTypes = aggArgTypes.get(k);
            String arg = buildAggFuncArgs(fnName, argTypes, colTypes);
            String col = String.format("%s(%s)", FunctionSet.getAggStateName(fnName), arg);
            stateColumns.add(col);
        }
        String sql1 = "select k1, " + Joiner.on(", ").join(stateColumns) + " from t1";
        String plan = UtFrameUtils.getVerboseFragmentPlan(starRocksAssert.getCtx(), sql1);
        PlanTestBase.assertContains(plan, "  0:OlapScanNode\n" +
                "     table: t1, rollup: t1");
        starRocksAssert.dropTable("t1");
    }

    @Test
    public void testCreateAggStateTableWithApproxTopK() throws Exception {
        List<String> funcNames = Lists.newArrayList();
        Map<String, String> colTypes = Maps.newLinkedHashMap();
        List<List<String>> aggArgTypes = Lists.newArrayList();
        buildTableT1(funcNames, colTypes, aggArgTypes);

        // test _state
        for (int k = 0; k < funcNames.size(); k++) {
            List<String> stateColumns = Lists.newArrayList();
            List<String> argTypes = aggArgTypes.get(k);
            String fnName = funcNames.get(k);
            if (!FunctionSet.APPROX_TOP_K.equals(fnName)) {
                continue;
            }
            String arg = buildAggFuncArgs(fnName, argTypes, colTypes);
            String col = String.format("%s(%s)", FunctionSet.getAggStateName(fnName), arg);
            stateColumns.add(col);
            String sql1 = "select k1, " + Joiner.on(", ").join(stateColumns) + " from t1;";
            String plan = UtFrameUtils.getVerboseFragmentPlan(starRocksAssert.getCtx(), sql1);
            PlanTestBase.assertContains(plan, "|  31 <-> approx_top_k_state[([27: c6, DOUBLE, true], 10, 100); " +
                    "args: DOUBLE,INT,INT; result: VARBINARY; args nullable: true; result nullable: true]");
            PlanTestBase.assertContains(plan, "  0:OlapScanNode\n" +
                    "     table: t1, rollup: t1");
            break;
        }
        starRocksAssert.dropTable("t1");
    }

    @Test
    public void testCreateAggStateTableWithMultiDistinctSum() throws Exception {
        List<String> funcNames = Lists.newArrayList();
        Map<String, String> colTypes = Maps.newLinkedHashMap();
        List<List<String>> aggArgTypes = Lists.newArrayList();
        buildTableT1(funcNames, colTypes, aggArgTypes);

        // test _state
        List<String> stateColumns = Lists.newArrayList();
        List<String> unionColumns = Lists.newArrayList();
        List<String> mergeColumns = Lists.newArrayList();

        List<String> aggTableColNames = Lists.newArrayList();
        List<String> aggTableCols = Lists.newArrayList();
        int j = 0;
        for (int k = 0; k < funcNames.size(); k++) {
            List<String> argTypes = aggArgTypes.get(k);
            String fnName = funcNames.get(k);
            if (!FunctionSet.MULTI_DISTINCT_SUM.equals(fnName)) {
                continue;
            }
            String arg = buildAggFuncArgs(fnName, argTypes, colTypes);
            String stateCol = String.format("%s(%s)", FunctionSet.getAggStateName(fnName), arg);
            stateColumns.add(stateCol);
            // agg state tabll
            String aggColName = "v" + j++;
            aggTableColNames.add(aggColName);
            String aggCol = buildAggStateColumn(fnName, argTypes, aggColName);
            aggTableCols.add(aggCol);
            // union
            String unionCol = String.format("%s(%s)", FunctionSet.getAggStateUnionName(fnName), aggColName);
            unionColumns.add(unionCol);
            // merge
            String mergeCol = String.format("%s(%s)", FunctionSet.getAggStateMergeName(fnName), aggColName);
            mergeColumns.add(mergeCol);
        }

        String sql = " CREATE TABLE test_agg_state_table ( \n" +
                "k1  date, \n" +
                Joiner.on(",\n").join(aggTableCols) +
                ") DISTRIBUTED BY HASH(k1) \n" +
                "PROPERTIES (  \"replication_num\" = \"1\");";
        starRocksAssert.withTable(sql);

        // multi_distinct_sum_state
        {
            String sql1 = "select k1, " + Joiner.on(", ").join(stateColumns) + " from t1;";
            String plan = UtFrameUtils.getVerboseFragmentPlan(starRocksAssert.getCtx(), sql1);
            PlanTestBase.assertContains(plan, "  1:Project\n" +
                    "  |  output columns:\n" +
                    "  |  1 <-> [1: k1, DATE, true]\n" +
                    "  |  32 <-> multi_distinct_sum_state[([8: c6, DOUBLE, true]); args: DOUBLE; " +
                    "result: VARBINARY; args nullable: true; result nullable: true]\n" +
                    "  |  33 <-> multi_distinct_sum_state[([9: c7, FLOAT, true]); args: FLOAT; " +
                    "result: VARBINARY; args nullable: true; result nullable: true]\n" +
                    "  |  34 <-> multi_distinct_sum_state[([2: c0, BOOLEAN, true]); args: BOOLEAN; " +
                    "result: VARBINARY; args nullable: true; result nullable: true]\n" +
                    "  |  35 <-> multi_distinct_sum_state[([3: c1, TINYINT, true]); args: TINYINT; " +
                    "result: VARBINARY; args nullable: true; result nullable: true]\n" +
                    "  |  36 <-> multi_distinct_sum_state[([4: c2, SMALLINT, true]); args: SMALLINT; " +
                    "result: VARBINARY; args nullable: true; result nullable: true]\n" +
                    "  |  37 <-> multi_distinct_sum_state[([5: c3, INT, true]); args: INT; " +
                    "result: VARBINARY; args nullable: true; result nullable: true]\n" +
                    "  |  38 <-> multi_distinct_sum_state[([6: c4, BIGINT, true]); args: BIGINT; " +
                    "result: VARBINARY; args nullable: true; result nullable: true]\n" +
                    "  |  39 <-> multi_distinct_sum_state[([7: c5, LARGEINT, true]); args: LARGEINT; " +
                    "result: VARBINARY; args nullable: true; result nullable: true]\n" +
                    "  |  40 <-> multi_distinct_sum_state[([10: c8, DECIMAL64(10,2), true]); args: DECIMAL64; " +
                    "result: VARBINARY; args nullable: true; result nullable: true]\n" +
                    "  |  cardinality: 1");
            PlanTestBase.assertContains(plan, "  0:OlapScanNode\n" +
                    "     table: t1, rollup: t1");
        }

        // _union
        {
            String sql1 = "select k1, " + Joiner.on(", ").join(unionColumns)
                    + " from test_agg_state_table group by k1;";
            String plan = UtFrameUtils.getVerboseFragmentPlan(starRocksAssert.getCtx(), sql1);
            PlanTestBase.assertContains(plan, "|  aggregate: multi_distinct_sum_union[([6: v4, VARBINARY, true]); " +
                    "args: VARBINARY; result: VARBINARY; args nullable: true; " +
                    "result nullable: true], multi_distinct_sum_union[([7: v5, VARBINARY, true]); args: VARBINARY; " +
                    "result: VARBINARY; args nullable: true; result nullable: true], " +
                    "multi_distinct_sum_union[([8: v6, VARBINARY, true]); args: VARBINARY; " +
                    "result: VARBINARY; args nullable: true; result nullable: true], " +
                    "multi_distinct_sum_union[([9: v7, VARBINARY, true]); args: VARBINARY; " +
                    "result: VARBINARY; args nullable: true; result nullable: true], " +
                    "multi_distinct_sum_union[([10: v8, VARBINARY, true]); args: VARBINARY; " +
                    "result: VARBINARY; args nullable: true; result nullable: true], " +
                    "multi_distinct_sum_union[([11: v9, VARBINARY, true]); args: VARBINARY; " +
                    "result: VARBINARY; args nullable: true; result nullable: true], " +
                    "multi_distinct_sum_union[([12: v10, VARBINARY, true]); args: VARBINARY; " +
                    "result: VARBINARY; args nullable: true; result nullable: true], " +
                    "multi_distinct_sum_union[([2: v0, VARBINARY, true]); args: VARBINARY; " +
                    "result: VARBINARY; args nullable: true; result nullable: true], " +
                    "multi_distinct_sum_union[([3: v1, VARBINARY, true]); args: VARBINARY; " +
                    "result: VARBINARY; args nullable: true; result nullable: true], " +
                    "multi_distinct_sum_union[([4: v2, VARBINARY, true]); args: VARBINARY; " +
                    "result: VARBINARY; args nullable: true; result nullable: true], " +
                    "multi_distinct_sum_union[([5: v3, VARBINARY, true]); args: VARBINARY; " +
                    "result: VARBINARY; args nullable: true; result nullable: true]");
            PlanTestBase.assertContains(plan, " 0:OlapScanNode\n" +
                    "     table: test_agg_state_table, rollup: test_agg_state_table");
        }

        // _merge
        {
            String sql1 = "select k1, " + Joiner.on(", ").join(mergeColumns)
                    + " from test_agg_state_table group by k1;";
            String plan = UtFrameUtils.getVerboseFragmentPlan(starRocksAssert.getCtx(), sql1);
            PlanTestBase.assertContains(plan, "|  aggregate: multi_distinct_sum_merge[([6: v4, VARBINARY, true]); " +
                    "args: VARBINARY; result: BIGINT; args nullable: true; result nullable: true], " +
                    "multi_distinct_sum_merge[([7: v5, VARBINARY, true]); args: VARBINARY; " +
                    "result: BIGINT; args nullable: true; result nullable: true], " +
                    "multi_distinct_sum_merge[([8: v6, VARBINARY, true]); args: VARBINARY; " +
                    "result: BIGINT; args nullable: true; result nullable: true], " +
                    "multi_distinct_sum_merge[([9: v7, VARBINARY, true]); args: VARBINARY; " +
                    "result: LARGEINT; args nullable: true; result nullable: true], " +
                    "multi_distinct_sum_merge[([10: v8, VARBINARY, true]); args: VARBINARY; " +
                    "result: DECIMAL128(38,2); args nullable: true; result nullable: true], " +
                    "multi_distinct_sum_merge[([11: v9, VARBINARY, true]); args: VARBINARY; " +
                    "result: DECIMAL128(38,2); args nullable: true; result nullable: true], " +
                    "multi_distinct_sum_merge[([12: v10, VARBINARY, true]); args: VARBINARY; " +
                    "result: DECIMAL128(38,2); args nullable: true; result nullable: true], " +
                    "multi_distinct_sum_merge[([2: v0, VARBINARY, true]); args: VARBINARY; " +
                    "result: DOUBLE; args nullable: true; result nullable: true], " +
                    "multi_distinct_sum_merge[([3: v1, VARBINARY, true]); args: VARBINARY; " +
                    "result: DOUBLE; args nullable: true; result nullable: true], " +
                    "multi_distinct_sum_merge[([4: v2, VARBINARY, true]); args: VARBINARY; " +
                    "result: BIGINT; args nullable: true; result nullable: true], " +
                    "multi_distinct_sum_merge[([5: v3, VARBINARY, true]); args: VARBINARY; " +
                    "result: BIGINT; args nullable: true; result nullable: true]");
            PlanTestBase.assertContains(plan, " 0:OlapScanNode\n" +
                    "     table: test_agg_state_table, rollup: test_agg_state_table");
        }
        starRocksAssert.dropTable("t1");
        starRocksAssert.dropTable("test_agg_state_table");
    }

    @Test
    public void testCreateAggStateTableWithArrayAgg() throws Exception {
        List<String> funcNames = Lists.newArrayList();
        Map<String, String> colTypes = Maps.newLinkedHashMap();
        List<List<String>> aggArgTypes = Lists.newArrayList();
        buildTableT1(funcNames, colTypes, aggArgTypes);

        // test _state
        List<String> stateColumns = Lists.newArrayList();
        List<String> unionColumns = Lists.newArrayList();
        List<String> mergeColumns = Lists.newArrayList();

        List<String> aggTableColNames = Lists.newArrayList();
        List<String> aggTableCols = Lists.newArrayList();
        int j = 0;
        for (int k = 0; k < funcNames.size(); k++) {
            List<String> argTypes = aggArgTypes.get(k);
            String fnName = funcNames.get(k);
            if (!FunctionSet.ARRAY_AGG.equals(fnName)) {
                continue;
            }
            String arg = buildAggFuncArgs(fnName, argTypes, colTypes);
            String stateCol = String.format("%s(%s)", FunctionSet.getAggStateName(fnName), arg);
            stateColumns.add(stateCol);
            // agg state tabll
            String aggColName = "v" + j++;
            aggTableColNames.add(aggColName);
            String aggCol = buildAggStateColumn(fnName, argTypes, aggColName);
            aggTableCols.add(aggCol);
            // union
            String unionCol = String.format("%s(%s)", FunctionSet.getAggStateUnionName(fnName), aggColName);
            unionColumns.add(unionCol);
            // merge
            String mergeCol = String.format("%s(%s)", FunctionSet.getAggStateMergeName(fnName), aggColName);
            mergeColumns.add(mergeCol);
        }

        String sql = " CREATE TABLE test_agg_state_table ( \n" +
                "k1  date, \n" +
                Joiner.on(",\n").join(aggTableCols) +
                ") DISTRIBUTED BY HASH(k1) \n" +
                "PROPERTIES (  \"replication_num\" = \"1\");";
        starRocksAssert.withTable(sql);

        // multi_distinct_sum_state
        {
            String sql1 = "select k1, " + Joiner.on(", ").join(stateColumns) + " from t1;";
            String plan = UtFrameUtils.getVerboseFragmentPlan(starRocksAssert.getCtx(), sql1);
            PlanTestBase.assertContains(plan, "32 <-> array_agg_state[([26: c24, VARCHAR, true]); args: VARCHAR; " +
                    "result: struct<col1 array<varchar(100)>>; args nullable: true; result nullable: true]");
            PlanTestBase.assertContains(plan, "  0:OlapScanNode\n" +
                    "     table: t1, rollup: t1");
        }

        // _union
        {
            String sql1 = "select k1, " + Joiner.on(", ").join(unionColumns)
                    + " from test_agg_state_table group by k1;";
            String plan = UtFrameUtils.getVerboseFragmentPlan(starRocksAssert.getCtx(), sql1);
            PlanTestBase.assertContains(plan, "|  aggregate: array_agg_union[([2: v0, " +
                    "struct<col1 array<varchar(100)>>, true]); args: INVALID_TYPE; result: " +
                    "struct<col1 array<varchar(100)>>; args nullable: true; result nullable: true]");
            PlanTestBase.assertContains(plan, " 0:OlapScanNode\n" +
                    "     table: test_agg_state_table, rollup: test_agg_state_table");
        }

        // _merge
        {
            String sql1 = "select k1, " + Joiner.on(", ").join(mergeColumns)
                    + " from test_agg_state_table group by k1;";
            String plan = UtFrameUtils.getVerboseFragmentPlan(starRocksAssert.getCtx(), sql1);
            PlanTestBase.assertContains(plan, "|  aggregate: " +
                    "array_agg_merge[([2: v0, struct<col1 array<varchar(100)>>, true]); args: INVALID_TYPE; " +
                    "result: ARRAY<VARCHAR(100)>; args nullable: true; result nullable: true]");
            PlanTestBase.assertContains(plan, " 0:OlapScanNode\n" +
                    "     table: test_agg_state_table, rollup: test_agg_state_table");
        }
        starRocksAssert.dropTable("t1");
        starRocksAssert.dropTable("test_agg_state_table");
    }

    @Test
    public void testGenerateSqlTesterTestsTotal() throws Exception {
        List<String> funcNames = Lists.newArrayList();
        Map<String, String> colTypes = Maps.newLinkedHashMap();
        List<List<String>> aggArgTypes = Lists.newArrayList();
        List<String> columns = Lists.newArrayList();
        List<String> colNames = Lists.newArrayList();
        buildTableT1(funcNames, colTypes, aggArgTypes, columns, colNames, MAX_AGG_FUNC_NUM_IN_TEST);

        String sql = " CREATE TABLE test_agg_state_table ( \n" +
                "k1  date, \n" +
                Joiner.on(",\n").join(columns) +
                ") DISTRIBUTED BY HASH(k1) \n" +
                "PROPERTIES (  \"replication_num\" = \"1\");";
        starRocksAssert.withTable(sql,
                () -> {
                    Table table = starRocksAssert.getCtx().getGlobalStateMgr().getLocalMetastore()
                            .getDb(connectContext.getDatabase()).getTable("test_agg_state_table");
                    List<Column> tableColumns = table.getColumns();
                    Assert.assertEquals(columns.size() + 1, tableColumns.size());

                    // test _state
                    {
                        List<String> stateColumns = Lists.newArrayList();
                        for (int i = 0; i < funcNames.size(); i++) {
                            String fnName = funcNames.get(i);
                            List<String> argTypes = aggArgTypes.get(i);
                            String arg = buildAggFuncArgs(fnName, argTypes, colTypes);
                            String col = String.format("%s(%s)", FunctionSet.getAggStateName(fnName), arg);
                            stateColumns.add(col);
                        }
                        String sql1 = "insert into test_agg_state_table select k1, " +
                                Joiner.on(", ").join(stateColumns) + " from t1;";
                        String plan = UtFrameUtils.getVerboseFragmentPlan(starRocksAssert.getCtx(), sql1);
                        PlanTestBase.assertContains(plan, "  0:OlapScanNode\n" +
                                "     table: t1, rollup: t1");
                    }
                    // test _union
                    {
                        List<String> unionColumns =
                                Streams.zip(colNames.stream(), funcNames.stream(),
                                                (c, f) -> String.format("%s(%s)", FunctionSet.getAggStateUnionName(f), c))
                                        .collect(Collectors.toList());
                        String sql1 = "select k1, " +
                                Joiner.on(", ").join(unionColumns) + " from test_agg_state_table group by k1;";
                        String plan = UtFrameUtils.getVerboseFragmentPlan(starRocksAssert.getCtx(), sql1);
                        PlanTestBase.assertContains(plan, "  0:OlapScanNode\n" +
                                "     table: test_agg_state_table, rollup: test_agg_state_table");
                    }

                    // test _merge
                    {
                        List<String> mergeColumns =
                                Streams.zip(colNames.stream(), funcNames.stream(),
                                                (c, f) -> String.format("%s(%s)", FunctionSet.getAggStateMergeName(f), c))
                                        .collect(Collectors.toList());
                        String sql1 = "select k1, " +
                                Joiner.on(",").join(mergeColumns) + " from test_agg_state_table group by k1;";
                        String plan = UtFrameUtils.getVerboseFragmentPlan(starRocksAssert.getCtx(), sql1);
                        PlanTestBase.assertContains(plan, "  0:OlapScanNode\n" +
                                "     table: test_agg_state_table, rollup: test_agg_state_table");
                    }
                });
        starRocksAssert.dropTable("t1");
    }

    @Test
    public void testGenerateCreateSyncMVWithMultiCountDistinct() throws Exception {
        List<String> funcNames = Lists.newArrayList();
        Map<String, String> colTypes = Maps.newLinkedHashMap();
        List<List<String>> aggArgTypes = Lists.newArrayList();
        List<String> columns = Lists.newArrayList();
        List<String> colNames = Lists.newArrayList();
        buildTableT1(funcNames, colTypes, aggArgTypes, columns, colNames);
        List<String> stateColumns = Lists.newArrayList();

        List<String> queryColumns = Lists.newArrayList();
        for (int i = 0; i < funcNames.size(); i++) {
            String fnName = funcNames.get(i);
            if (!fnName.equalsIgnoreCase(FunctionSet.MULTI_DISTINCT_COUNT)) {
                continue;
            }
            List<String> argTypes = aggArgTypes.get(i);
            String arg = buildAggFuncArgs(fnName, argTypes, colTypes);
            String col = String.format("%s(%s(%s)) as agg%s",
                    FunctionSet.getAggStateUnionName(fnName), FunctionSet.getAggStateName(fnName), arg, i);
            stateColumns.add(col);

            String qCol = String.format("%s(%s) as agg%s",
                    fnName, arg, i);
            queryColumns.add(qCol);
        }

        // create sync mv with all agg functions
        String sql1 = "CREATE MATERIALIZED VIEW test_mv1 as select k1, " +
                Joiner.on(", ").join(stateColumns) + " from t1 group by k1;";
        starRocksAssert.withMaterializedView(sql1);

        // no rollup
        {
            String query = String.format("select k1, %s from t1 group by k1;", Joiner.on(", ").join(queryColumns));
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "test_mv1");
        }
        // rollup
        {
            String query = String.format("select %s from t1;", Joiner.on(", ").join(queryColumns));
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "test_mv1");
        }
        starRocksAssert.dropTable("t1");
    }

    private void testCreateSyncMVWithSpecificAggFunc(String aggFuncName) throws Exception {
        List<String> funcNames = Lists.newArrayList();
        Map<String, String> colTypes = Maps.newLinkedHashMap();
        List<List<String>> aggArgTypes = Lists.newArrayList();
        List<String> columns = Lists.newArrayList();
        List<String> colNames = Lists.newArrayList();
        buildTableT1(funcNames, colTypes, aggArgTypes, columns, colNames);
        List<String> stateColumns = Lists.newArrayList();

        List<String> queryColumns = Lists.newArrayList();
        for (int i = 0; i < funcNames.size(); i++) {
            String fnName = funcNames.get(i);
            if (!fnName.equalsIgnoreCase(aggFuncName)) {
                continue;
            }
            List<String> argTypes = aggArgTypes.get(i);
            String arg = buildAggFuncArgs(fnName, argTypes, colTypes);
            String col = String.format("%s(%s(%s)) as agg%s",
                    FunctionSet.getAggStateUnionName(fnName), FunctionSet.getAggStateName(fnName), arg, i);
            stateColumns.add(col);
            String qCol = String.format("%s(%s) as agg%s",
                    fnName, arg, i);
            queryColumns.add(qCol);
        }

        // create sync mv with all agg functions
        String sql1 = "CREATE MATERIALIZED VIEW test_mv1 as select k1, " +
                Joiner.on(", ").join(stateColumns) + " from t1 group by k1;";
        starRocksAssert.withMaterializedView(sql1);

        // no rollup
        {
            String query = String.format("select k1, %s from t1 group by k1;", Joiner.on(", ").join(queryColumns));
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "test_mv1");
        }
        // rollup
        {
            String query = String.format("select %s from t1;", Joiner.on(", ").join(queryColumns));
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "test_mv1");
        }
        starRocksAssert.dropTable("t1");
        starRocksAssert.dropMaterializedView("test_mv1");
    }

    @Test
    @Ignore
    public void testCreateSyncMVWithArrayAggDistinct() throws Exception {
        testCreateSyncMVWithSpecificAggFunc(FunctionSet.ARRAY_AGG_DISTINCT);
    }

    @Test
    public void testCreateSyncMVWithArrayUniqueAgg() throws Exception {
        testCreateSyncMVWithSpecificAggFunc(FunctionSet.ARRAY_UNIQUE_AGG);
    }

    @Test
    public void testGenerateCreateAsyncMVWithAggState() throws Exception {
        List<String> funcNames = Lists.newArrayList();
        Map<String, String> colTypes = Maps.newLinkedHashMap();
        List<List<String>> aggArgTypes = Lists.newArrayList();
        List<String> columns = Lists.newArrayList();
        List<String> colNames = Lists.newArrayList();
        buildTableT1(funcNames, colTypes, aggArgTypes, columns, colNames);
        List<String> stateColumns = Lists.newArrayList();

        List<String> queryColumns = Lists.newArrayList();
        for (int i = 0; i < funcNames.size(); i++) {
            String fnName = funcNames.get(i);
            // TODO: remove this after ARRAY_AGG_DISTINCT is supported
            if (fnName.equalsIgnoreCase(FunctionSet.ARRAY_AGG_DISTINCT)) {
                continue;
            }
            if (i > MAX_AGG_FUNC_NUM_IN_TEST) {
                break;
            }
            List<String> argTypes = aggArgTypes.get(i);
            String arg = buildAggFuncArgs(fnName, argTypes, colTypes);
            String col = String.format("%s(%s(%s)) as agg%s",
                    FunctionSet.getAggStateUnionName(fnName), FunctionSet.getAggStateName(fnName), arg, i);
            stateColumns.add(col);
            String qCol = String.format("%s(%s) as agg%s",
                    fnName, arg, i);
            queryColumns.add(qCol);
        }

        // create async mv with all agg functions
        String sql1 = "CREATE MATERIALIZED VIEW test_mv1 REFRESH MANUAL as select k1, " +
                Joiner.on(", ").join(stateColumns) + " from t1 group by k1;";
        connectContext.getSessionVariable().setOptimizerExecuteTimeout(10000);
        starRocksAssert.withMaterializedView(sql1);

        // no rollup
        {
            String query = String.format("select k1, %s from t1 group by k1;", Joiner.on(", ").join(queryColumns));
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "test_mv1");
        }
        // rollup
        {
            String query = String.format("select %s from t1;", Joiner.on(", ").join(queryColumns));
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "test_mv1");
        }
        connectContext.getSessionVariable().setOptimizerExecuteTimeout(3000);
        starRocksAssert.dropTable("t1");
        starRocksAssert.dropMaterializedView("test_mv1");
    }

    @Test
    public void testGenerateCreateAsyncMVWithCountFunctions() throws Exception {
        List<String> funcNames = Lists.newArrayList();
        Map<String, String> colTypes = Maps.newLinkedHashMap();
        List<List<String>> aggArgTypes = Lists.newArrayList();
        List<String> columns = Lists.newArrayList();
        List<String> colNames = Lists.newArrayList();
        buildTableT1(funcNames, colTypes, aggArgTypes, columns, colNames);
        List<String> stateColumns = Lists.newArrayList();

        List<String> queryColumns = Lists.newArrayList();
        for (int i = 0; i < funcNames.size(); i++) {
            String fnName = funcNames.get(i);
            if (!fnName.equalsIgnoreCase(FunctionSet.COUNT)) {
                continue;
            }
            List<String> argTypes = aggArgTypes.get(i);
            String arg = buildAggFuncArgs(fnName, argTypes, colTypes);
            String col = String.format("%s(%s(%s)) as agg%s",
                    FunctionSet.getAggStateUnionName(fnName), FunctionSet.getAggStateName(fnName), arg, i);
            stateColumns.add(col);
            String qCol = String.format("%s(%s) as agg%s",
                    fnName, arg, i);
            queryColumns.add(qCol);
        }

        // create async mv with all agg functions
        String sql1 = "CREATE MATERIALIZED VIEW test_mv1 REFRESH MANUAL as select k1, " +
                Joiner.on(", ").join(stateColumns) + " from t1 group by k1;";
        starRocksAssert.withMaterializedView(sql1);
        MaterializedView mv = starRocksAssert.getMv("test", "test_mv1");
        List<Column> mvCols = mv.getColumns();
        // count agg function's output should be always not nullable.
        for (Column col : mvCols) {
            if (col.getName().startsWith("agg")) {
                Assert.assertTrue(col.getType().isBigint());
                Assert.assertFalse(col.isAllowNull());
            }
        }
        starRocksAssert.dropTable("t1");
        starRocksAssert.dropMaterializedView("test_mv1");
    }
}

