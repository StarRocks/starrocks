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

package com.starrocks.sql.automv.lattice;

import com.starrocks.qe.GlobalVariable;
import com.starrocks.qe.SessionVariable;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.util.List;
import java.util.Map;

public class MVRecommendParams {
    private static final String USE_ARRAY_AGG_COUNT_DISTINCT = "use_array_agg_count_distinct";
    private static final String USE_BITMAP_COUNT_DISTINCT = "use_bitmap_count_distinct";
    private static final String USE_HLL_COUNT_DISTINCT = "use_hll_count_distinct";
    private static final String ENABLE_COMPLEX_DERIVED_DIMENSIONS = "enable_complex_derived_dimensions";
    private static final String ENABLE_COMPLEX_DERIVED_METRICS = "enable_complex_derived_metrics";
    private static final String DISABLE_SEMI_ANTI_JOIN = "disable_semi_anti_join";
    private static final String PRUNE_ROLLUP_UNABLE_AGGREGATE_WITH_CONJUNCTS =
            "prune_rollup_unable_aggregate_with_conjuncts";
    private static final String PUSH_DOWN_AGG_BELOW_SEMI_ANTI_JOIN = "push_down_agg_below_semi_anti_join";
    private static final String MAX_ORDER_BY_COLUMNS = "max_order_by_columns";
    private static final String PREFER_RANGE_PARTITION = "prefer_range_partition";
    private static final String STRING_TIME_FORMATS = "string_time_formats";
    private static final String COLOCATE_MV_DIMENSIONS_LIMIT = "colocate_mv_dimensions_limit";
    private boolean useArrayAggCountDistinct = false;
    private boolean useBitmapCountDistinct = true;
    private boolean useHllCountDistinct = false;
    private boolean enableComplexDerivedDimensions = true;
    private boolean enableComplexDerivedMetrics = false;
    private boolean disableSemiAntiJoin = true;
    private boolean pruneRollupUnableAggregateWithConjuncts = true;
    private boolean pushDownAggBelowSemiAntiJoin = true;
    private int maxOrderByColumns = 3;
    private boolean preferRangePartition = true;
    private String stringTimeFormats = "%Y%m%d,%Y-%m-%d";
    private int colocateMVDimensionsLimit = 6;
    private String queryDump;

    private MVRecommendParams() {
    }

    public static MVRecommendParams parseFromQueryParams(Map<String, List<String>> queryParams) {
        MVRecommendParams params = new MVRecommendParams();
        if (queryParams.containsKey(USE_ARRAY_AGG_COUNT_DISTINCT)) {
            params.useArrayAggCountDistinct = queryParams.get(USE_ARRAY_AGG_COUNT_DISTINCT).get(0)
                    .equalsIgnoreCase("true");
        }
        if (queryParams.containsKey(USE_BITMAP_COUNT_DISTINCT)) {
            params.useBitmapCountDistinct = queryParams.get(USE_BITMAP_COUNT_DISTINCT).get(0)
                    .equalsIgnoreCase("true");
        }
        if (queryParams.containsKey(USE_HLL_COUNT_DISTINCT)) {
            params.useHllCountDistinct = queryParams.get(USE_HLL_COUNT_DISTINCT).get(0)
                    .equalsIgnoreCase("true");
        }
        if (queryParams.containsKey(ENABLE_COMPLEX_DERIVED_DIMENSIONS)) {
            params.enableComplexDerivedDimensions = queryParams.get(ENABLE_COMPLEX_DERIVED_DIMENSIONS).get(0)
                    .equalsIgnoreCase("true");
        }
        if (queryParams.containsKey(ENABLE_COMPLEX_DERIVED_METRICS)) {
            params.enableComplexDerivedMetrics = queryParams.get(ENABLE_COMPLEX_DERIVED_METRICS).get(0)
                    .equalsIgnoreCase("true");
        }
        if (queryParams.containsKey(DISABLE_SEMI_ANTI_JOIN)) {
            params.disableSemiAntiJoin = queryParams.get(DISABLE_SEMI_ANTI_JOIN).get(0)
                    .equalsIgnoreCase("true");
        }
        if (queryParams.containsKey(PRUNE_ROLLUP_UNABLE_AGGREGATE_WITH_CONJUNCTS)) {
            params.pruneRollupUnableAggregateWithConjuncts =
                    queryParams.get(PRUNE_ROLLUP_UNABLE_AGGREGATE_WITH_CONJUNCTS).get(0)
                            .equalsIgnoreCase("true");
        }
        if (queryParams.containsKey(PUSH_DOWN_AGG_BELOW_SEMI_ANTI_JOIN)) {
            params.pushDownAggBelowSemiAntiJoin = queryParams.get(PUSH_DOWN_AGG_BELOW_SEMI_ANTI_JOIN).get(0)
                    .equalsIgnoreCase("true");
        }
        if (queryParams.containsKey(MAX_ORDER_BY_COLUMNS)) {
            String value = queryParams.get(MAX_ORDER_BY_COLUMNS).get(0);
            try {
                params.maxOrderByColumns = Integer.parseInt(value);
            } catch (NumberFormatException ex) {
                String errMsg = String.format("Fail to parse argument '%s' of %s, it should be a integer\n",
                        value, MAX_ORDER_BY_COLUMNS);
                throw new IllegalArgumentException(errMsg, ex);
            }
        }
        if (queryParams.containsKey(PREFER_RANGE_PARTITION)) {
            params.preferRangePartition = queryParams.get(PREFER_RANGE_PARTITION).get(0)
                    .equalsIgnoreCase("true");
        }
        if (queryParams.containsKey(STRING_TIME_FORMATS)) {
            params.stringTimeFormats = queryParams.get(STRING_TIME_FORMATS).get(0);
        }
        if (queryParams.containsKey(COLOCATE_MV_DIMENSIONS_LIMIT)) {
            String value = queryParams.get(COLOCATE_MV_DIMENSIONS_LIMIT).get(0);
            try {
                params.colocateMVDimensionsLimit = Integer.parseInt(value);
            } catch (NumberFormatException ex) {
                String errMsg = String.format("Fail to parse argument '%s' of %s, it should be a integer\n",
                        value, COLOCATE_MV_DIMENSIONS_LIMIT);
                throw new IllegalArgumentException(errMsg, ex);
            }
        }
        return params;
    }

    public static MVRecommendParams parseFromCmdLineArgs(String[] args) {
        Options options = new Options();
        options.addOption("a", USE_ARRAY_AGG_COUNT_DISTINCT, true,
                "(default false)Use array_agg to compute count distinct");
        options.addOption("b", USE_BITMAP_COUNT_DISTINCT, true,
                "(default true)Use bitmap to compute count distinct");
        options.addOption("H", USE_HLL_COUNT_DISTINCT, true, "(default false)Use hll to compute count distinct");
        options.addOption("d", ENABLE_COMPLEX_DERIVED_DIMENSIONS, true, "(default true)Allow derived dimensions");
        options.addOption("m", ENABLE_COMPLEX_DERIVED_METRICS, true, "(default false)Allow derived metrics");
        options.addOption("s", DISABLE_SEMI_ANTI_JOIN, true,
                "(default true)Do not recommend MV if sub-plan contains semi/anti join");
        options.addOption("p", PRUNE_ROLLUP_UNABLE_AGGREGATE_WITH_CONJUNCTS, true,
                "(default true)Do not recommend MV if the sub-plan contains " +
                        "rollup-unable aggregations and predicates");
        options.addOption("P", PUSH_DOWN_AGG_BELOW_SEMI_ANTI_JOIN, true,
                "(default true)Recommend MV after eliminate semi/anti join in the sub-plan");
        options.addOption("o", MAX_ORDER_BY_COLUMNS, true,
                "(default 3)The maximum of columns in short key of MV schema");
        options.addOption("r", PREFER_RANGE_PARTITION, true,
                "(default true)Partition policy that MV prefers for external tables, " +
                        "true for range and false for list");
        options.addOption("f", STRING_TIME_FORMATS, true,
                "(default '%Y%m%d,%Y-%m-%d')Acceptable time formats of str2date which is used to " +
                        "convert base tables' varchar partition column of list partition into MV's partition" +
                        " column of range partition");
        options.addOption("c", COLOCATE_MV_DIMENSIONS_LIMIT, true,
                "(default 6)Leverage colocate group to generate 1-stage aggregation to speedup " +
                        "queries if the number of MV's dimension exceeds this limit");
        options.addOption("h", "help", false, "show help info");

        DefaultParser parser = new DefaultParser();
        try {
            CommandLine commandLine = parser.parse(options, args);
            MVRecommendParams params = new MVRecommendParams();
            if (commandLine.hasOption("help")) {
                printHelpAndExit(options);
            }
            if (commandLine.hasOption(USE_ARRAY_AGG_COUNT_DISTINCT)) {
                params.useArrayAggCountDistinct = commandLine.getOptionValue(USE_ARRAY_AGG_COUNT_DISTINCT)
                        .equalsIgnoreCase("true");
            }
            if (commandLine.hasOption(USE_BITMAP_COUNT_DISTINCT)) {
                params.useBitmapCountDistinct = commandLine.getOptionValue(USE_BITMAP_COUNT_DISTINCT)
                        .equalsIgnoreCase("true");
            }
            if (commandLine.hasOption(USE_HLL_COUNT_DISTINCT)) {
                params.useHllCountDistinct = commandLine.getOptionValue(USE_HLL_COUNT_DISTINCT)
                        .equalsIgnoreCase("true");
            }
            if (commandLine.hasOption(ENABLE_COMPLEX_DERIVED_DIMENSIONS)) {
                params.enableComplexDerivedDimensions = commandLine.getOptionValue(ENABLE_COMPLEX_DERIVED_DIMENSIONS)
                        .equalsIgnoreCase("true");
            }
            if (commandLine.hasOption(ENABLE_COMPLEX_DERIVED_METRICS)) {
                params.enableComplexDerivedMetrics = commandLine.getOptionValue(ENABLE_COMPLEX_DERIVED_METRICS)
                        .equalsIgnoreCase("true");
            }
            if (commandLine.hasOption(DISABLE_SEMI_ANTI_JOIN)) {
                params.disableSemiAntiJoin = commandLine.getOptionValue(DISABLE_SEMI_ANTI_JOIN)
                        .equalsIgnoreCase("true");
            }
            if (commandLine.hasOption(PRUNE_ROLLUP_UNABLE_AGGREGATE_WITH_CONJUNCTS)) {
                params.pruneRollupUnableAggregateWithConjuncts = commandLine
                        .getOptionValue(PRUNE_ROLLUP_UNABLE_AGGREGATE_WITH_CONJUNCTS)
                        .equalsIgnoreCase("true");
            }
            if (commandLine.hasOption(PUSH_DOWN_AGG_BELOW_SEMI_ANTI_JOIN)) {
                params.pushDownAggBelowSemiAntiJoin = commandLine.getOptionValue(PUSH_DOWN_AGG_BELOW_SEMI_ANTI_JOIN)
                        .equalsIgnoreCase("true");
            }
            if (commandLine.hasOption(MAX_ORDER_BY_COLUMNS)) {
                try {
                    params.maxOrderByColumns =
                            Integer.parseInt(commandLine.getOptionValue(MAX_ORDER_BY_COLUMNS));
                } catch (NumberFormatException ex) {
                    System.out.printf("Fail to parse argument '%s' of %s, it should be a integer\n",
                            commandLine.getOptionValue(MAX_ORDER_BY_COLUMNS),
                            MAX_ORDER_BY_COLUMNS);
                    ex.printStackTrace();
                    printHelpAndExit(options);
                }
            }
            if (commandLine.hasOption(PREFER_RANGE_PARTITION)) {
                params.preferRangePartition = commandLine.getOptionValue(PREFER_RANGE_PARTITION)
                        .equalsIgnoreCase("true");
            }

            if (commandLine.hasOption(STRING_TIME_FORMATS)) {
                params.stringTimeFormats = commandLine.getOptionValue(STRING_TIME_FORMATS);
            }

            if (commandLine.hasOption(COLOCATE_MV_DIMENSIONS_LIMIT)) {
                try {
                    params.colocateMVDimensionsLimit =
                            Integer.parseInt(commandLine.getOptionValue(COLOCATE_MV_DIMENSIONS_LIMIT));
                } catch (NumberFormatException ex) {
                    System.out.printf("Fail to parse argument '%s' of %s, it should be a integer\n",
                            commandLine.getOptionValue(COLOCATE_MV_DIMENSIONS_LIMIT),
                            COLOCATE_MV_DIMENSIONS_LIMIT);
                    ex.printStackTrace();
                    printHelpAndExit(options);
                }
            }
            if (commandLine.getArgs().length != 1) {
                System.out.println("queryDump is not found");
                printHelpAndExit(options);
            }
            params.queryDump = commandLine.getArgs()[0];
            return params;
        } catch (ParseException e) {
            System.out.println("Fail to parse arguments of command line");
            e.printStackTrace();
            printHelpAndExit(options);
        }
        return null;
    }

    private static void printHelpAndExit(Options options) {
        HelpFormatter help = new HelpFormatter();
        help.setWidth(160);
        help.printHelp("FORMAT: automv_recommender [OPTIONS] queryDump.json", options);
        System.exit(-1);
    }

    public String getQueryDump() {
        return queryDump;
    }

    void setSessionVariables(SessionVariable sv) {
        sv.setAutoMVUseArrayAggCountDistinct(useArrayAggCountDistinct);
        sv.setAutoMVUseBitmapCountDistinct(useBitmapCountDistinct);
        sv.setAutoMVUseHllCountDistinct(useHllCountDistinct);
        sv.setAutoMVEnableComplexDerivedDimensions(enableComplexDerivedDimensions);
        sv.setAutoMVEnableComplexDerivedMetrics(enableComplexDerivedMetrics);
        sv.setAutomvEnableSemiAntiJoin(disableSemiAntiJoin);
        sv.setAutoMVPruneRollupUnableAggregateWithConjuncts(pruneRollupUnableAggregateWithConjuncts);
        sv.setAutoMVPushDownAggBelowSemiAntiJoin(pushDownAggBelowSemiAntiJoin);
        sv.setAutoMVMaxOrderByColumns(maxOrderByColumns);
        GlobalVariable.setAutoMVPreferRangePartition(preferRangePartition);
        GlobalVariable.setAutoMVStringTimeFormats(stringTimeFormats);
        GlobalVariable.setAutoMVColocateMVDimensionsLimit(colocateMVDimensionsLimit);
    }
}