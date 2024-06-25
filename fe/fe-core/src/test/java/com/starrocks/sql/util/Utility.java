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

package com.starrocks.sql.util;

import com.google.common.io.CharStreams;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.qe.ConnectContext;
import com.starrocks.statistic.StatsConstants;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import joptsimple.internal.Strings;
import kotlin.text.Charsets;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.starrocks.sql.optimizer.statistics.CachedStatisticStorageTest.DEFAULT_CREATE_TABLE_TEMPLATE;

public class Utility {
    private static List<Pair<String, String>> cachedTPCDSQueryList = null;
    private static List<Pair<String, String>> cachedClickBenchQueryList = null;
    private static List<Pair<String, String>> cachedSsbQueryList = null;
    private static List<Pair<String, String>> ssbLineorderFlatQueryList = null;
    private static List<Pair<String, String>> cachedTPCHQueryList = null;

    private static List<String> getSqlList(String directory, String... names) {
        ClassLoader loader = Utility.class.getClassLoader();

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

    public static List<Pair<String, String>> getQueryList(String querySet, Pattern filePat) {
        final String currentDir = System.getProperty("user.dir");
        final File queryDir = new File(currentDir + "/src/test/resources/" + querySet);
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

    public static List<String> getClickBenchCreateTableSqlList() {
        return getSqlList("sql/click_bench/", "hits");
    }

    public static List<Pair<String, String>> getClickBenchQueryList() {
        if (cachedClickBenchQueryList == null) {
            cachedClickBenchQueryList = getQueryList("sql/click_bench/", Pattern.compile("(Q\\d+)\\.sql"));
        }
        return cachedClickBenchQueryList;
    }

    public static List<String> getSsbCreateTableSqlList() {
        return getSqlList("sql/ssb/", "customer", "dates", "supplier", "part", "lineorder", "lineorder0",
                "lineorder_flat");
    }

    public static List<Pair<String, String>> getSsbQueryList() {
        if (cachedSsbQueryList == null) {
            cachedSsbQueryList = getQueryList("sql/ssb/", Pattern.compile("(Q\\d+\\.\\d+)\\.sql"));
        }
        return cachedSsbQueryList;
    }

    public static List<Pair<String, String>> getSsbLineorderFlatQueryList() {
        if (ssbLineorderFlatQueryList == null) {
            ssbLineorderFlatQueryList =
                    getQueryList("sql/ssb_lineorder_flat/", Pattern.compile("(Q\\d+\\.\\d+)\\.sql"));
        }
        return ssbLineorderFlatQueryList;
    }

    public static List<String> getTPCHCreateTableSqlList() {
        return getSqlList("sql/tpch_tables/", "lineitem", "orders", "part", "partsupp", "supplier", "customer",
                "region",
                "nation");
    }

    public static List<Pair<String, String>> getTPCDSQueryList() {
        if (cachedTPCDSQueryList == null) {
            cachedTPCDSQueryList = getQueryList("sql/tpcds/", Pattern.compile("(query\\S+)\\.sql"));
        }
        return cachedTPCDSQueryList;
    }

    public static String getTPCDSQuery(String name) {
        String sql = Utility.getTPCDSQueryList().stream()
                .filter(p -> p.first.equals(name))
                .map(p -> p.second)
                .findFirst().orElse(null);
        return Objects.requireNonNull(sql);
    }

    public static List<String> getTPCDSCreateTableSqlList() {
        return getSqlList("sql/tpcds/", "call_center", "catalog_page", "catalog_returns", "catalog_sales",
                "customer_address", "customer_demographics", "customer", "date_dim", "household_demographics",
                "income_band", "inventory", "item", "promotion", "reason", "ship_mode", "store_returns", "store_sales",
                "store", "time_dim", "warehouse", "web_page", "web_returns", "web_sales", "web_site");
    }

    public static List<Pair<String, String>> getTPCHQueryList() {
        if (cachedTPCHQueryList == null) {
            cachedTPCHQueryList = getQueryList("/sql/tpchsql/", Pattern.compile("(q\\d+)\\.sql"));
        }
        return cachedTPCHQueryList;
    }

    public static StarRocksAssert prepareTables(String db, Supplier<List<String>> createTableSqlListGetter) {
        UtFrameUtils.createMinStarRocksCluster();
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        ctx.getSessionVariable().setEnablePipelineEngine(true);
        FeConstants.runningUnitTest = true;
        StarRocksAssert starRocksAssert = new StarRocksAssert(ctx);
        try {
            starRocksAssert.withDatabase(StatsConstants.STATISTICS_DB_NAME)
                    .useDatabase(StatsConstants.STATISTICS_DB_NAME)
                    .withTable(DEFAULT_CREATE_TABLE_TEMPLATE);
            starRocksAssert.withDatabase(db).useDatabase(db);
            createTableSqlListGetter.get().forEach(createTblSql -> {
                try {
                    starRocksAssert.withTable(createTblSql);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        FeConstants.runningUnitTest = true;
        return starRocksAssert;
    }

}