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

import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvRefreshArbiter;
import com.starrocks.catalog.MvUpdateInfo;
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

import java.util.Optional;
import java.util.stream.Stream;

public class AutoMVPartitionPolicyTest {
    private static final ThreadLocal<StarRocksAssert> STARROCKS_ASSERT = new ThreadLocal<>();

    private static StarRocksAssert getStarRocksAssert() {
        if (STARROCKS_ASSERT.get() == null) {
            STARROCKS_ASSERT.set(TestUtil.prepareTables("db0", TestUtil::getPartitionedTableSqlList));
        }
        return STARROCKS_ASSERT.get();
    }

    @BeforeClass
    public static void setUp() throws Exception {
        StarRocksAssert starRocksAssert = getStarRocksAssert();
        UtFrameUtils.setDefaultConfigForAsyncMVTest(starRocksAssert.getCtx());
        if (!starRocksAssert.databaseExist("_statistics_")) {
            StatisticsMetaManager m = new StatisticsMetaManager();
            m.createStatisticsTablesForTest();
        }

        new MockUp<MvRefreshArbiter>() {
            /**
             * {@link MvRefreshArbiter#getMVTimelinessUpdateInfo(MaterializedView, boolean)}
             */
            @Mock
            public MvUpdateInfo getMVTimelinessUpdateInfo(MaterializedView mv,
                                                          boolean isQueryRewrite) {
                return new MvUpdateInfo(MvUpdateInfo.MvToRefreshType.NO_REFRESH);
            }

            @Deprecated
            @Mock
            public MvUpdateInfo getPartitionNamesToRefreshForMv(MaterializedView mv,
                                                                boolean isQueryRewrite) {
                return new MvUpdateInfo(MvUpdateInfo.MvToRefreshType.NO_REFRESH);

            }
        };

        new MockUp<UtFrameUtils>() {
            /**
             * {@link UtFrameUtils#isPrintPlanTableNames()}
             */
            @Mock
            boolean isPrintPlanTableNames() {
                return true;
            }
        };
    }

    private void testHelper(Object[][] testCases) {
        for (Object[] tc : testCases) {
            String q = (String) tc[0];
            TimeGranule.Unit defaultGranule = (TimeGranule.Unit) tc[1];
            String granuleStr = Optional.ofNullable(defaultGranule).map(Enum::name).orElse("none");
            String[] expectLines = (String[]) tc[2];
            AutoMVUtil.testSingleQueryHelper(STARROCKS_ASSERT.get(), q,
                    sv -> sv.setAutoMVDefaultPartitionByTimeGranule(granuleStr),
                    results -> {
                        Assert.assertFalse(results.isEmpty());
                        String mv = results.get(0).get(2);
                        Stream.of(expectLines).forEach(ln -> {
                            Assert.assertTrue(mv, mv.contains(ln));
                        });
                    });
        }
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
        testHelper(testCases);
    }

    @Test
    public void testPartitionByDateTruncFromConjunctsUsingDateColumn() {
        String q0 = "select UserId, sum(M0) from hits_daily where " +
                "date_trunc('month', EventDate) > '2024-01-01' group by UserId";
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
        testHelper(testCases);
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
        testHelper(testCases);

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
        testHelper(testCases);
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
        testHelper(testCases);
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
        testHelper(testCases);
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
        testHelper(testCases);
    }

    // TODO: by satanson, a MV that has list-partition base tables are not support in present,
    //  it would be supported soon in future.
    @Test
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
        testHelper(testCases);
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
        testHelper(testCases);
    }
}
