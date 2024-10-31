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
import org.apache.commons.io.IOUtils;

import java.io.FileReader;
import java.util.Objects;

public class QueryDumpMVRecommenderCmd {
    public static void main(String[] args) throws Exception {
        CmdArgs cmdArgs = Objects.requireNonNull(CmdArgs.parseOptions(args));
        String jsonStr = IOUtils.toString(new FileReader(cmdArgs.queryDump));
        QueryDumpMVRecommender recommender = QueryDumpMVRecommender.of();
        cmdArgs.setSessionVariables(recommender.getStarRocksAssert().getCtx().getSessionVariable());
        String mv = recommender.recommend(jsonStr);
        System.out.println(mv);
        System.exit(0);
    }

    private static class CmdArgs {
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

        private static CmdArgs parseOptions(String[] args) {
            Options options = new Options();
            options.addOption("a", USE_ARRAY_AGG_COUNT_DISTINCT, false,
                    "(default false)Use array_agg to compute count distinct");
            options.addOption("b", USE_BITMAP_COUNT_DISTINCT, false,
                    "(default true)Use bitmap to compute count distinct");
            options.addOption("H", USE_HLL_COUNT_DISTINCT, false, "(default false)Use hll to compute count distinct");
            options.addOption("d", ENABLE_COMPLEX_DERIVED_DIMENSIONS, false, "(default true)Allow derived dimensions");
            options.addOption("m", ENABLE_COMPLEX_DERIVED_METRICS, false, "(default false)Allow derived metrics");
            options.addOption("s", DISABLE_SEMI_ANTI_JOIN, false,
                    "(default true)Do not recommend MV if sub-plan contains semi/anti join");
            options.addOption("p", PRUNE_ROLLUP_UNABLE_AGGREGATE_WITH_CONJUNCTS, false,
                    "(default true)Do not recommend MV if the sub-plan contains " +
                            "rollup-unable aggregations and predicates");
            options.addOption("d", PUSH_DOWN_AGG_BELOW_SEMI_ANTI_JOIN, false,
                    "(default true)Recommend MV after eliminate semi/anti join in the sub-plan");
            options.addOption("o", MAX_ORDER_BY_COLUMNS, true,
                    "(default 3)The number of columns in short key of MV schema");
            options.addOption("r", PREFER_RANGE_PARTITION, false,
                    "(default true)Partition policy that MV prefers, true for range and false for list");
            options.addOption("f", STRING_TIME_FORMATS, true,
                    "(default '%Y%m%d,%Y-%m-%d')Acceptable time formats of str2date which is used to " +
                            "convert base tables' varchar partition column of list partition into MV's partition" +
                            " column of range partition");
            options.addOption("c", COLOCATE_MV_DIMENSIONS_LIMIT, false,
                    "(default 6)Leverage colocate group to generate 1-stage aggregation to speedup " +
                            "queries if the number of MV's dimension exceeds this limit");
            options.addOption("h", "help", false, "show help info");

            DefaultParser parser = new DefaultParser();
            try {
                CommandLine commandLine = parser.parse(options, args);
                CmdArgs cmdArgs = new CmdArgs();
                if (commandLine.hasOption("help")) {
                    printHelpAndExit(options);
                }
                if (commandLine.hasOption(USE_ARRAY_AGG_COUNT_DISTINCT)) {
                    cmdArgs.useArrayAggCountDistinct = true;
                }
                if (commandLine.hasOption(USE_BITMAP_COUNT_DISTINCT)) {
                    cmdArgs.useBitmapCountDistinct = true;
                }
                if (commandLine.hasOption(USE_HLL_COUNT_DISTINCT)) {
                    cmdArgs.useHllCountDistinct = true;
                }
                if (commandLine.hasOption(ENABLE_COMPLEX_DERIVED_DIMENSIONS)) {
                    cmdArgs.enableComplexDerivedDimensions = true;
                }
                if (commandLine.hasOption(ENABLE_COMPLEX_DERIVED_METRICS)) {
                    cmdArgs.enableComplexDerivedMetrics = true;
                }
                if (commandLine.hasOption(DISABLE_SEMI_ANTI_JOIN)) {
                    cmdArgs.disableSemiAntiJoin = true;
                }
                if (commandLine.hasOption(PRUNE_ROLLUP_UNABLE_AGGREGATE_WITH_CONJUNCTS)) {
                    cmdArgs.pruneRollupUnableAggregateWithConjuncts = true;
                }
                if (commandLine.hasOption(PUSH_DOWN_AGG_BELOW_SEMI_ANTI_JOIN)) {
                    cmdArgs.pushDownAggBelowSemiAntiJoin = true;
                }
                if (commandLine.hasOption(MAX_ORDER_BY_COLUMNS)) {
                    try {
                        cmdArgs.maxOrderByColumns = Integer.parseInt(commandLine.getOptionValue(MAX_ORDER_BY_COLUMNS));
                    } catch (NumberFormatException ex) {
                        System.out.printf("Fail to parse argument '%s' of %s, it should be a integer\n",
                                commandLine.getOptionValue(MAX_ORDER_BY_COLUMNS),
                                MAX_ORDER_BY_COLUMNS);
                        ex.printStackTrace();
                        printHelpAndExit(options);
                    }
                }
                if (commandLine.hasOption(PREFER_RANGE_PARTITION)) {
                    cmdArgs.preferRangePartition = true;
                }

                if (commandLine.hasOption(STRING_TIME_FORMATS)) {
                    cmdArgs.stringTimeFormats = commandLine.getOptionValue(STRING_TIME_FORMATS);
                }

                if (commandLine.hasOption(COLOCATE_MV_DIMENSIONS_LIMIT)) {
                    try {
                        cmdArgs.colocateMVDimensionsLimit =
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
                    System.out.println("FORMAT: automv_recommender [OPTIONS] queryDump.json");
                    printHelpAndExit(options);
                }
                cmdArgs.queryDump = commandLine.getArgs()[0];
                return cmdArgs;
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
            help.printHelp("recommend MV from query dump", options);
            System.exit(-1);
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
}
