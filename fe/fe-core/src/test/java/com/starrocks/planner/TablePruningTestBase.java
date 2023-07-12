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

package com.starrocks.planner;

import com.google.common.io.CharStreams;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import kotlin.text.Charsets;
import org.junit.Assert;

import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TablePruningTestBase {
    protected static ConnectContext ctx;
    protected static StarRocksAssert starRocksAssert;

    public static List<String> getSsbCreateTableSqlList() {
        return getSqlList("sql/ssb/", "customer", "dates", "supplier", "part", "lineorder");
    }

    public static List<String> getTPCHCreateTableSqlList() {
        return getSqlList("sql/tpch_pk_tables/",
                "nation", "region", "part", "customer", "supplier", "partsupp", "orders", "lineitem");
    }

    public static List<String> getSqlList(String directory, String... names) {
        ClassLoader loader = TablePruningTestBase.class.getClassLoader();

        List<String> sqlList = Arrays.stream(names).map(n -> {
            try {
                return CharStreams.toString(
                        new InputStreamReader(
                                Objects.requireNonNull(loader.getResourceAsStream(directory + n + ".sql")),
                                Charsets.UTF_8));
            } catch (Throwable e) {
                return null;
            }
        }).collect(Collectors.toList());
        Assert.assertFalse(sqlList.contains(null));
        return sqlList;
    }

    public static Pattern HashJoinPattern = Pattern.compile("HASH JOIN");

    String checkHashJoinCountEq(String infoMsg, String sql, int numHashJoins, Consumer<SessionVariable> svSetter) {
        return checkHashJoinCount(infoMsg, sql, (info, n) -> {
            //System.out.println(String.format("{\"%s\", %d},", info, n));
            Assert.assertEquals(info, numHashJoins, (int) n);
            return null;
        }, svSetter);
    }

    String checkHashJoinCountLessThan(String sql, int numHashJoins, Consumer<SessionVariable> svSetter) {
        return checkHashJoinCount(null, sql, (info, n) -> {
            Assert.assertTrue(info, n < numHashJoins);
            return null;
        }, svSetter);
    }

    String checkHashJoinCount(String infoMsg, String sql, BiFunction<String, Integer, Void> assertFunc,
                              Consumer<SessionVariable> svSetter) {
        try {
            svSetter.accept(ctx.getSessionVariable());
            String plan = UtFrameUtils.getVerboseFragmentPlan(ctx, sql);
            int realNumOfHashJoin =
                    (int) Arrays.stream(plan.split("\n")).filter(ln -> HashJoinPattern.matcher(ln).find()).count();
            if (infoMsg == null) {
                infoMsg = "SQL=" + sql + "\nPlan:\n" + plan;
            }
            assertFunc.apply(infoMsg, realNumOfHashJoin);
            return plan;
        } catch (Throwable err) {
            err.printStackTrace();
            Assert.fail("SQL=" + sql + "\nError:" + err.getMessage());
        } finally {
            ctx.getSessionVariable().setEnableRboTablePrune(false);
            ctx.getSessionVariable().setEnableCboTablePrune(false);
        }
        return null;
    }

    void checkHashJoinCountWithOnlyCBO(String sql, int numHashJoins) {
        checkHashJoinCountEq(null, sql, numHashJoins,
                sv -> {
                    sv.setEnableCboTablePrune(true);
                    sv.setEnableRboTablePrune(false);
                });
    }

    String checkHashJoinCountWithOnlyRBO(String sql, int numHashJoins) {
        return checkHashJoinCountEq(null, sql, numHashJoins, sv -> {
            sv.setEnableCboTablePrune(false);
            sv.setEnableRboTablePrune(true);
        });
    }

    String checkHashJoinCountWithBothRBOAndCBO(String sql, int numHashJoins) {
        return checkHashJoinCountEq(null, sql, numHashJoins, sv -> {
            sv.setEnableCboTablePrune(true);
            sv.setEnableRboTablePrune(true);
        });
    }

    String checkHashJoinCountWithBothRBOAndCBO(String info, String sql, int numHashJoins) {
        return checkHashJoinCountEq(info, sql, numHashJoins, sv -> {
            sv.setEnableCboTablePrune(true);
            sv.setEnableRboTablePrune(true);
        });
    }

    void checkHashJoinCountWithOnlyCBOLessThan(String sql, int numHashJoins) {
        checkHashJoinCountLessThan(sql, numHashJoins,
                sv -> {
                    sv.setEnableCboTablePrune(true);
                    sv.setEnableRboTablePrune(false);
                });
    }

    String checkHashJoinCountWithOnlyRBOLessThan(String sql, int numHashJoins) {
        return checkHashJoinCountLessThan(sql, numHashJoins, sv -> {
            sv.setEnableCboTablePrune(false);
            sv.setEnableRboTablePrune(true);
        });
    }

    String checkHashJoinCountWithBothRBOAndCBOLessThan(String sql, int numHashJoins) {
        return checkHashJoinCountLessThan(sql, numHashJoins, sv -> {
            sv.setEnableCboTablePrune(true);
            sv.setEnableRboTablePrune(true);
        });
    }

    public String generateSameTableJoinSql(int n, String tableAliasFmt,
                                           BiFunction<Integer, Integer, String> onClauseGenerator, String selectItems,
                                           String whereClause, String limitClauses) {
        List<String> tableAliases =
                IntStream.range(0, n).mapToObj(i -> String.format(tableAliasFmt, i))
                        .collect(Collectors.toList());
        List<String> onClauses = IntStream.range(1, n).mapToObj(i -> onClauseGenerator.apply(i - 1, i))
                .collect(Collectors.toList());
        StringBuffer fromClauseBuilder = new StringBuffer();
        Iterator<String> nextTableAlias = tableAliases.iterator();
        Iterator<String> nextOnClause = onClauses.iterator();
        fromClauseBuilder.append(nextTableAlias.next());
        while (nextTableAlias.hasNext()) {
            fromClauseBuilder.append(String.format(" INNER JOIN %s %s\n", nextTableAlias.next(), nextOnClause.next()));
        }
        return String.format("select %s from %s where %s %s", selectItems, fromClauseBuilder, whereClause,
                limitClauses);
    }
}