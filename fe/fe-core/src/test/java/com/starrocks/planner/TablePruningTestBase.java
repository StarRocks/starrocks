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
import com.starrocks.common.Pair;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import joptsimple.internal.Strings;
import kotlin.text.Charsets;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.junit.Assert;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

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

    public static List<Pair<String, String>> getQueryList(String querySet, Pattern filePat) {
        final String currentDir = System.getProperty("user.dir");
        final File queryDir = new File(currentDir + "/src/test/resources/" + querySet);
        Pattern trimTrailing = Pattern.compile("[;\n\\s]*$");
        return Arrays.stream(Objects.requireNonNull(queryDir.listFiles()))
                .flatMap(f -> {
                    Matcher mat = filePat.matcher(f.getName());
                    if (mat.matches()) {
                        return Stream.of(Pair.create(mat.group(1), f));
                    } else {
                        return Stream.empty();
                    }
                }).map(p -> {
                    try (InputStream fin = Files.newInputStream(p.second.toPath())) {
                        String content = Strings.join(IOUtils.readLines(fin, Charsets.UTF_8), "\n");
                        //content = content.replaceAll(trimTrailing.pattern(), "");
                        return Pair.create(p.first, content);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }).sorted(Comparator.comparing(p -> p.first)).collect(Collectors.toList());
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

    Pair<Integer, String> getHashJoinCount(String sql, Consumer<SessionVariable> svSetter) {
        try {
            svSetter.accept(ctx.getSessionVariable());
            ctx.getSessionVariable().setOptimizerExecuteTimeout(30000);
            String plan = UtFrameUtils.getVerboseFragmentPlan(ctx, sql);
            int realNumOfHashJoin =
                    (int) Arrays.stream(plan.split("\n")).filter(ln -> HashJoinPattern.matcher(ln).find()).count();
            String infoMsg = "SQL=" + sql + "\nPlan:\n" + plan;
            return Pair.create(realNumOfHashJoin, infoMsg);
        } catch (Throwable err) {
            err.printStackTrace();
            Assert.fail("SQL=" + sql + "\nError:" + err.getMessage());
        } finally {
            ctx.getSessionVariable().setEnableRboTablePrune(false);
            ctx.getSessionVariable().setEnableCboTablePrune(false);
        }
        return Pair.create(-1, "Error");
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
            ctx.getSessionVariable().setOptimizerExecuteTimeout(30000);
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