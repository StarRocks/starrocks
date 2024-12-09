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

import com.google.common.collect.ImmutableSet;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.qe.GlobalVariable;
import com.starrocks.sql.automv.estimation.CardEstimateState;
import com.starrocks.sql.automv.pn.TimeGranule;
import com.starrocks.sql.automv.util.AutoMVUtil;
import com.starrocks.sql.automv.util.TestUtil;
import com.starrocks.statistic.StatisticsMetaManager;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class AutoMVPartitionPolicyTest {
    private static final ThreadLocal<StarRocksAssert> STARROCKS_ASSERT = new ThreadLocal<>();

    private static StarRocksAssert getStarRocksAssert() {
        FeConstants.runningUnitTest = true;
        if (STARROCKS_ASSERT.get() == null) {
            STARROCKS_ASSERT.set(TestUtil.prepareTables("db0", TestUtil::getPartitionedTableSqlList));
        }
        return STARROCKS_ASSERT.get();
    }

    @BeforeClass
    public static void setUp() throws Exception {
        StarRocksAssert starRocksAssert = getStarRocksAssert();
        if (!starRocksAssert.databaseExist("_statistics_")) {
            StatisticsMetaManager m = new StatisticsMetaManager();
            m.createStatisticsTablesForTest();
        }
        UtFrameUtils.mockTimelinessForAsyncMVTest(starRocksAssert.getCtx());
    }

    @Test
    public void testPartitionByDateTruncUsingDateColumn() {
        String q0 = "select UserId, sum(M0) from hits_daily group by UserId";
        Object[][] testCases = new Object[][] {
                {q0, TimeGranule.Unit.DAY,
                        new String[] {
                                "PARTITION BY EventDate",
                                "GROUP BY\n" +
                                        "  `db0`.`hits_daily`.EventDate\n" +
                                        "  ,`db0`.`hits_daily`.UserID"
                        }
                },
                {q0, TimeGranule.Unit.MONTH,
                        new String[] {
                                "COMMENT \"MV recommended by AutoMV\"\n" +
                                        "PARTITION BY _ca0002",
                                "(date_trunc(\"month\", `db0`.`hits_daily`.EventDate)) AS _ca0002",
                                "GROUP BY\n" +
                                        "  date_trunc(\"month\", `db0`.`hits_daily`.EventDate)"

                        }
                },
                {q0, TimeGranule.Unit.HOUR,
                        new String[] {
                                "PARTITION BY EventDate",
                                "GROUP BY\n" +
                                        "  `db0`.`hits_daily`.EventDate\n" +
                                        "  ,`db0`.`hits_daily`.UserID"
                        }

                }
        };
        AutoMVUtil.testPartitionHelper(STARROCKS_ASSERT.get(), testCases);
    }

    @Test
    public void testPartitionByDateTruncFromConjunctsUsingDateColumn() {
        String q0 = "select UserId, sum(M0) from hits_daily where " +
                "date_trunc('month', EventDate) >= '2024-01-01' group by UserId";
        Object[][] testCases = new Object[][] {
                {q0, TimeGranule.Unit.DAY,
                        new String[] {
                                "COMMENT \"MV recommended by AutoMV\"\n" +
                                        "PARTITION BY _ca0002",
                                "(date_trunc(\"month\", `db0`.`hits_daily`.EventDate)) AS _ca0002",
                                "GROUP BY\n" +
                                        "  date_trunc(\"month\", `db0`.`hits_daily`.EventDate)\n" +
                                        "  ,`db0`.`hits_daily`.UserI"
                        }
                },
                {q0, TimeGranule.Unit.MONTH,
                        new String[] {
                                "PARTITION BY _ca0002",
                                "(date_trunc(\"month\", `db0`.`hits_daily`.EventDate)) AS _ca0002",
                                "GROUP BY\n" +
                                        "  date_trunc(\"month\", `db0`.`hits_daily`.EventDate)"
                        }
                },
                {q0, TimeGranule.Unit.YEAR,
                        new String[] {
                                "PARTITION BY _ca0003",
                                "(date_trunc(\"year\", `db0`.`hits_daily`.EventDate)) AS _ca0003",
                                "GROUP BY\n" +
                                        "  date_trunc(\"month\", `db0`.`hits_daily`.EventDate)\n" +
                                        "  ,date_trunc(\"year\", `db0`.`hits_daily`.EventDate)"
                        }
                }
        };
        AutoMVUtil.testPartitionHelper(STARROCKS_ASSERT.get(), testCases);
    }

    @Test
    public void testPartitionByDateTruncFromDimensionsUsingDateColumn() {
        String q0 = "select UserId, date_trunc('day', EventDate), sum(M0) from hits_daily " +
                "group by UserId, date_trunc('day', EventDate)";
        Object[][] testCases = new Object[][] {
                {q0, TimeGranule.Unit.MONTH,
                        new String[] {
                                "COMMENT \"MV recommended by AutoMV\"\n" +
                                        "PARTITION BY _ca0002",
                                "(date_trunc(\"month\", `db0`.`hits_daily`.EventDate)) AS _ca0002",
                                "GROUP BY\n" +
                                        "  date_trunc(\"month\", `db0`.`hits_daily`.EventDate)"
                        }
                },
                {q0, TimeGranule.Unit.DAY,
                        new String[] {
                                "COMMENT \"MV recommended by AutoMV\"\n" +
                                        "PARTITION BY EventDate"
                        }
                },
                {q0, TimeGranule.Unit.HOUR,
                        new String[] {
                                "COMMENT \"MV recommended by AutoMV\"\n" +
                                        "PARTITION BY EventDate"
                        }
                }
        };
        AutoMVUtil.testPartitionHelper(STARROCKS_ASSERT.get(), testCases);

    }

    @Test
    public void testPartitionByDateTruncUsingDatetimeColumn() {
        String q0 = "select UserId, sum(M0) from hits_hourly group by UserId";

        Object[][] testCases = new Object[][] {
                {q0, TimeGranule.Unit.DAY,
                        new String[] {
                                "PARTITION BY _ca0002",
                                "(date_trunc(\"day\", `db0`.`hits_hourly`.EventTime)) AS _ca0002",
                                "GROUP BY\n" +
                                        "  date_trunc(\"day\", `db0`.`hits_hourly`.EventTime"
                        }
                },
                {q0, TimeGranule.Unit.MONTH,
                        new String[] {
                                "PARTITION BY _ca0002",
                                "(date_trunc(\"month\", `db0`.`hits_hourly`.EventTime)) AS _ca0002",
                                "GROUP BY\n" +
                                        "  date_trunc(\"month\", `db0`.`hits_hourly`.EventTime"
                        }
                },
                {q0, TimeGranule.Unit.HOUR,
                        new String[] {
                                "PARTITION BY _ca0002",
                                "(date_trunc(\"hour\", `db0`.`hits_hourly`.EventTime)) AS _ca0002",
                                "GROUP BY\n" +
                                        "  date_trunc(\"hour\", `db0`.`hits_hourly`.EventTime"
                        }
                }
        };
        AutoMVUtil.testPartitionHelper(STARROCKS_ASSERT.get(), testCases);
    }

    @Test
    public void testPartitionByFineGrainedDateTruncUsingDatetimeColumn() {
        String q0 = "select UserId, date_trunc('minute', EventTime), sum(M0) " +
                "from hits_hourly group by UserId, date_trunc('minute', EventTime)";

        Object[][] testCases = new Object[][] {
                {q0, TimeGranule.Unit.DAY,
                        new String[] {
                                "PARTITION BY _ca0002",
                                "(date_trunc(\"day\", `db0`.`hits_hourly`.EventTime)) AS _ca0002",
                                "GROUP BY\n" +
                                        "  date_trunc(\"day\", `db0`.`hits_hourly`.EventTime"
                        }
                },
                {q0, TimeGranule.Unit.MONTH,
                        new String[] {
                                "PARTITION BY _ca0002",
                                "(date_trunc(\"month\", `db0`.`hits_hourly`.EventTime)) AS _ca0002",
                                "GROUP BY\n" +
                                        "  date_trunc(\"month\", `db0`.`hits_hourly`.EventTime"
                        }
                },
                {q0, TimeGranule.Unit.HOUR,
                        new String[] {
                                "PARTITION BY _ca0002",
                                "(date_trunc(\"hour\", `db0`.`hits_hourly`.EventTime)) AS _ca0002",
                                "GROUP BY\n" +
                                        "  date_trunc(\"hour\", `db0`.`hits_hourly`.EventTime"
                        }
                }
        };
        AutoMVUtil.testPartitionHelper(STARROCKS_ASSERT.get(), testCases);
    }

    @Test
    public void testPartitionByColumnSelection() {
        String q0 = "select UserId, date_trunc('hour', EventTime), sum(M0) " +
                "from hits_hourly " +
                "where date_trunc('month', EventTime) between '2024-01-01' and '2024-04-01' " +
                "group by UserId, date_trunc('hour', EventTime)";

        Object[][] testCases = new Object[][] {
                {q0, TimeGranule.Unit.DAY,
                        new String[] {
                                "PARTITION BY _ca0002",
                                "(date_trunc(\"month\", `db0`.`hits_hourly`.EventTime)) AS _ca0002",
                                "GROUP BY\n" +
                                        "  date_trunc(\"month\", `db0`.`hits_hourly`.EventTime)"
                        }
                },
                {q0, TimeGranule.Unit.MONTH,
                        new String[] {
                                "PARTITION BY _ca0002",
                                "(date_trunc(\"month\", `db0`.`hits_hourly`.EventTime)) AS _ca0002",
                                "GROUP BY\n" +
                                        "  date_trunc(\"month\", `db0`.`hits_hourly`.EventTime"
                        }
                },
                {q0, TimeGranule.Unit.HOUR,
                        new String[] {
                                "PARTITION BY _ca0002",
                                "(date_trunc(\"month\", `db0`.`hits_hourly`.EventTime)) AS _ca0002",
                                "GROUP BY\n" +
                                        "  date_trunc(\"month\", `db0`.`hits_hourly`.EventTime"
                        }
                }
        };
        AutoMVUtil.testPartitionHelper(STARROCKS_ASSERT.get(), testCases);
    }

    @Test
    public void testPartitionByDateTruncFromConjunctsUsingDatetimeColumn() {
        String q0 = "select UserId, sum(M0) from hits_hourly " +
                "where date_trunc('day', EventTime) > '2024-01-01' group by UserId";

        Object[][] testCases = new Object[][] {
                {q0, TimeGranule.Unit.DAY,
                        new String[] {
                                "PARTITION BY _ca0002",
                                "(date_trunc(\"day\", `db0`.`hits_hourly`.EventTime)) AS _ca0002",
                                "GROUP BY\n" +
                                        "  date_trunc(\"day\", `db0`.`hits_hourly`.EventTime"
                        }
                },
                {q0, TimeGranule.Unit.MONTH,
                        new String[] {
                                "PARTITION BY _ca0003",
                                "(date_trunc(\"day\", `db0`.`hits_hourly`.EventTime)) AS _ca0002",
                                "(date_trunc(\"month\", `db0`.`hits_hourly`.EventTime)) AS _ca0003",
                                "GROUP BY\n" +
                                        "  date_trunc(\"day\", `db0`.`hits_hourly`.EventTime)\n" +
                                        "  ,date_trunc(\"month\", `db0`.`hits_hourly`.EventTime)"
                        }
                },
                {q0, TimeGranule.Unit.HOUR,
                        new String[] {
                                "PARTITION BY _ca0002",
                                "(date_trunc(\"day\", `db0`.`hits_hourly`.EventTime)) AS _ca0002",
                                "GROUP BY\n" +
                                        "  date_trunc(\"day\", `db0`.`hits_hourly`.EventTime"
                        }
                }
        };
        AutoMVUtil.testPartitionHelper(STARROCKS_ASSERT.get(), testCases);
    }

    // TODO: by satanson, a MV that has list-partition base tables are not support in present,
    //  it would be supported soon in future.
    // @Test
    public void testPartitionByStr2DateUsingVarcharColumn() {
        String q0 = "select UserId, sum(M0) from hits_daily_list " +
                "where str2date(EventDateS, '%Y-%m-%d') > '2024-01-02' group by UserId";

        String q1 = "select str2date(EventDateS, '%Y-%m-%d'), UserId, sum(M0) from hits_daily_list " +
                "group by str2date(EventDateS, '%Y-%m-%d'), UserId";

        Object[][] testCases = new Object[][] {
                {q0, TimeGranule.Unit.DAY,
                        new String[] {
                                //"PARTITION BY _ca0002",
                                "(str2date(`db0`.`hits_daily_list`.EventDateS, \"%Y-%m-%d\")) AS _ca0002",
                                "GROUP BY\n" +
                                        "  str2date(`db0`.`hits_daily_list`.EventDateS, \"%Y-%m-%d\")"
                        }
                },
                {q1, TimeGranule.Unit.MONTH,
                        new String[] {
                                //"PARTITION BY _ca0002",
                                "(str2date(`db0`.`hits_daily_list`.EventDateS, \"%Y-%m-%d\")) AS _ca0002",
                                "GROUP BY\n" +
                                        "  str2date(`db0`.`hits_daily_list`.EventDateS, \"%Y-%m-%d\")"
                        }
                }
        };
        AutoMVUtil.testPartitionHelper(STARROCKS_ASSERT.get(), testCases);
    }

    @Test
    public void testRollupOtherTimeFunction() {
        String q0 = "select UserId, sum(M0) from hits_daily " +
                "where year(EventDate) = '2024' group by UserId";

        String q1 = "select UserId, sum(M0) from hits_daily " +
                "where (case when dayname(EventDate) = 'Monday' then 1 " +
                "when dayname(EventTime) = 'Sunday' then cast(substr(EventDateS,8,2) as TINYINT)" +
                "else day(EventTime) end) between 1 and 15 " +
                "group by UserId";

        Object[][] testCases = new Object[][] {
                {q0, TimeGranule.Unit.DAY,
                        new String[] {
                                "PARTITION BY EventDate"
                        }
                },
                {q0, null,
                        new String[] {
                                "COMMENT \"MV recommended by AutoMV\"\n" +
                                        "DISTRIBUTED BY HASH",
                                "(year(`db0`.`hits_daily`.EventDate)) AS _ca0002",
                                "GROUP BY\n" +
                                        "  year(`db0`.`hits_daily`.EventDate)"
                        }
                },
                {q1, TimeGranule.Unit.MONTH,
                        new String[] {
                                "PARTITION BY _ca0003",
                                "(day(`db0`.`hits_daily`.EventTime)) AS _ca0002",
                                "(date_trunc(\"month\", `db0`.`hits_daily`.EventDate)) AS _ca0003",
                                "(dayname(`db0`.`hits_daily`.EventDate)) AS _ca0004",
                                "(dayname(`db0`.`hits_daily`.EventTime)) AS _ca0005"
                        }
                }
        };
        AutoMVUtil.testPartitionHelper(STARROCKS_ASSERT.get(), testCases);
    }

    @Test
    public void testCollocateMV() {
        Set<String> queryNames = ImmutableSet.of("Q17", "Q18", "Q19");
        List<Pair<String, String>> queryList = TestUtil.getClickBenchQueryList()
                .stream()
                .filter(p -> queryNames.contains(p.first))
                .map(p -> Pair.create(p.first, p.second.replace("hits", "hits_daily")))
                .collect(Collectors.toList());
        AutoMVUtil.testHelper(getStarRocksAssert().getCtx(), queryList,
                sv -> {
                    sv.setAutoMVCardRowCountRatioLWM(0.1);
                    sv.setAutoMVCardRowCountRatioHWM(0.5);
                },
                result -> {
                    Assert.assertEquals(1, result.size());
                    String mv = result.get(0).get(2);
                    Assert.assertTrue(mv.contains("DISTRIBUTED BY HASH (EventDate, UserID, SearchPhrase)"));
                    Assert.assertTrue(mv.contains("colocate_with"));
                    String acceleratedQueries = result.get(0).get(14);
                    Assert.assertEquals("[\"Q17.part.0\", \"Q18.part.0\", \"Q19.part.0\"]", acceleratedQueries);
                });
    }

    @Test
    public void testStiffConjuncts() {
        Set<String> queryNames = ImmutableSet.of("Q29");
        List<Pair<String, String>> queryList = TestUtil.getClickBenchQueryList()
                .stream()
                .filter(p -> queryNames.contains(p.first))
                .map(p -> Pair.create(p.first, p.second.replace("hits", "hits_daily")))
                .collect(Collectors.toList());
        AutoMVUtil.testHelper(getStarRocksAssert().getCtx(), queryList,
                sv -> {
                    sv.setAutoMVCardRowCountRatioLWM(0.1);
                    sv.setAutoMVCardRowCountRatioHWM(0.5);
                },
                result -> {
                    Assert.assertEquals(1, result.size());
                    String mv = result.get(0).get(2);
                    Assert.assertTrue(mv, mv.contains("SELECT\n" +
                            "  _ta0000.EventDate\n" +
                            "  ,(regexp_replace(_ta0000.Referer, \"^https?://(?:www.)?([^/]+)/.*$\", \"1\")) AS _ca0003\n" +
                            "  ,(sum(length(_ta0000.Referer))) AS _ca0004\n" +
                            "  ,(count(length(_ta0000.Referer))) AS _ca0005\n" +
                            "  ,(count(1)) AS _ca0006\n" +
                            "  ,(min(_ta0000.Referer)) AS _ca0007\n" +
                            "FROM\n" +
                            "  (\n" +
                            "    SELECT\n" +
                            "      `db0`.`hits_daily`.EventDate\n" +
                            "      ,`db0`.`hits_daily`.Referer\n" +
                            "    FROM\n" +
                            "      `db0`.`hits_daily`\n" +
                            "    WHERE\n" +
                            "      (`db0`.`hits_daily`.Referer != \"\")\n" +
                            "  ) _ta0000\n" +
                            "GROUP BY\n" +
                            "  _ta0000.EventDate\n" +
                            "  ,regexp_replace(_ta0000.Referer, \"^https?://(?:www.)?([^/]+)/.*$\", \"1\")"));
                    String acceleratedQueries = result.get(0).get(14);
                    Assert.assertEquals("[\"Q29.part.0\"]", acceleratedQueries);
                });
    }

    @Test
    public void testMVSelection1() {
        Set<String> queryNames = ImmutableSet.of("Q17", "Q18", "Q19", "Q29");
        List<Pair<String, String>> queryList = TestUtil.getClickBenchQueryList()
                .stream()
                .filter(p -> queryNames.contains(p.first))
                .map(p -> Pair.create(p.first, p.second.replace("hits", "hits_daily")))
                .collect(Collectors.toList());
        new MockUp<CardEstimateState>() {
            @Mock
            public double getSamplingRatio() {
                return 0.6;
            }
        };
        AutoMVUtil.testHelper(getStarRocksAssert().getCtx(), queryList,
                sv -> {
                    GlobalVariable.setEnableAutoMVLifecycleKeeper(true);
                    GlobalVariable.setAutoMVPerLatticeMVLimit(1);
                    GlobalVariable.setAutoMVPerLatticeMVSelectivityRatio(0.1);
                    GlobalVariable.setAutoMVPartitionedMVCardMax(1.0E11);
                    sv.setAutoMVCardRowCountRatioHWM(1.0);
                    sv.setAutoMVCardRowCountRatioLWM(1.0);
                },
                result -> {
                    Assert.assertEquals(2, result.size());
                });

        AutoMVUtil.testHelper(getStarRocksAssert().getCtx(), queryList,
                sv -> {
                    GlobalVariable.setAutoMVPerLatticeMVLimit(10);
                    GlobalVariable.setAutoMVPerLatticeMVSelectivityRatio(0.1);
                    GlobalVariable.setAutoMVPartitionedMVCardMax(1.0E11);
                    sv.setAutoMVCardRowCountRatioHWM(1.0);
                    sv.setAutoMVCardRowCountRatioLWM(1.0);
                },
                result -> {
                    Assert.assertEquals(3, result.size());
                });
    }

    @Test
    public void testMVSelection2() {
        String sqlFmt = "select murmur_hash3_32(concat(UserId,'%d'))%%1024, sum(M0) \n" +
                "from hits_daily  \n" +
                "group by murmur_hash3_32(concat(UserId,'%d'))%%1024";
        List<Pair<String, String>> queryList = IntStream.range(0, 50)
                .mapToObj(i -> Pair.create("Q" + i, String.format(sqlFmt, i, i)))
                .collect(Collectors.toList());
        new MockUp<CardEstimateState>() {
            @Mock
            public double getSamplingRatio() {
                return 0.6;
            }
        };
        for (int n = 1; n < 50; n += 8) {
            int mvLimit = n;
            AutoMVUtil.testHelper(getStarRocksAssert().getCtx(), queryList,
                    sv -> {
                        GlobalVariable.setEnableAutoMVLifecycleKeeper(true);
                        GlobalVariable.setAutoMVPerLatticeMVLimit(mvLimit);
                        GlobalVariable.setAutoMVPerLatticeMVSelectivityRatio(0.3);
                        GlobalVariable.setAutoMVPartitionedMVCardMax(1.0E11);
                        sv.setAutoMVCardRowCountRatioHWM(1.0);
                        sv.setAutoMVCardRowCountRatioLWM(1.0);
                    },
                    result -> {
                        Assert.assertTrue(
                                (mvLimit < 30 && result.size() == mvLimit) || (mvLimit >= 30 && result.size() == 30));
                    });
        }
    }

    // @Test
    public void testMVPreferRangePartition() {
        String q0 = "select UserId, sum(M0) from hits_daily_list " +
                " group by UserId";

        Object[][] testCases = new Object[][] {
                {q0, TimeGranule.Unit.DAY,
                        new String[] {
                                "PARTITION BY _ca0002",
                                "(str2date(`db0`.`hits_daily_list`.EventDateS, \"%Y-%m-%d\")) AS _ca0002",
                                "GROUP BY\n" +
                                        "  str2date(`db0`.`hits_daily_list`.EventDateS, \"%Y-%m-%d\")"
                        }
                },
                {q0, TimeGranule.Unit.MONTH,
                        new String[] {
                                "PARTITION BY _ca0002",
                                "(str2date(`db0`.`hits_daily_list`.EventDateS, \"%Y-%m-%d\")) AS _ca0002",
                                "GROUP BY\n" +
                                        "  str2date(`db0`.`hits_daily_list`.EventDateS, \"%Y-%m-%d\")"
                        }
                }
        };

        Map<String, Object> vars = AutoMVUtil.saveGlobalVariable();
        try {
            GlobalVariable.setAutoMVPreferRangePartition(true);
            AutoMVUtil.testPartitionHelper(STARROCKS_ASSERT.get(), testCases);
        } finally {
            AutoMVUtil.restoreGlobalVariable(vars);
        }
    }

    @Test
    public void testMVPreferListPartition() {
        String q0 = "select UserId, sum(M0) from hits_daily_list " +
                " group by UserId";

        Object[][] testCases = new Object[][] {
                {q0, TimeGranule.Unit.DAY, new String[] {"PARTITION BY EventDateS"}},
                {q0, TimeGranule.Unit.MONTH, new String[] {"PARTITION BY EventDateS"}}
        };
        Map<String, Object> vars = AutoMVUtil.saveGlobalVariable();
        try {
            GlobalVariable.setAutoMVPreferRangePartition(false);
            AutoMVUtil.testPartitionHelper(STARROCKS_ASSERT.get(), testCases);
        } finally {
            AutoMVUtil.restoreGlobalVariable(vars);
        }
    }
}
