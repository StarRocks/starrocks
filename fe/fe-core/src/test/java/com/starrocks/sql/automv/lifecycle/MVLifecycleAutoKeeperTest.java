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

package com.starrocks.sql.automv.lifecycle;

import com.starrocks.common.DdlException;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.GlobalVariable;
import com.starrocks.qe.SessionVariable;
import com.starrocks.scheduler.MVLifecycleAutoKeeper;
import com.starrocks.sql.automv.generator.MVName;
import com.starrocks.sql.automv.qe.CustomizedQueryExecutor;
import com.starrocks.sql.automv.util.AutoMVUtil;
import com.starrocks.sql.automv.util.TestUtil;
import com.starrocks.statistic.StatisticsMetaManager;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MVLifecycleAutoKeeperTest {
    private static final ThreadLocal<StarRocksAssert> STARROCKS_ASSERT = new ThreadLocal<>();

    private static StarRocksAssert getStarRocksAssert() {
        FeConstants.runningUnitTest = true;
        if (STARROCKS_ASSERT.get() == null) {
            STARROCKS_ASSERT.set(TestUtil.prepareTables("ssb", TestUtil::getSsbCreateTableSqlList));
        }
        return STARROCKS_ASSERT.get();
    }

    @BeforeClass
    public static void setUp() throws Exception {
        setUpMock();
    }

    private static void setUpMock() {
        FeConstants.runningUnitTest = true;
        StarRocksAssert starRocksAssert = getStarRocksAssert();
        AutoMVUtil.mockUpTunespaceExecutor();
        AutoMVUtil.mockUpAuthorizer();
        UtFrameUtils.setDefaultConfigForAsyncMVTest(starRocksAssert.getCtx());
        if (!starRocksAssert.databaseExist("_statistics_")) {
            StatisticsMetaManager m = new StatisticsMetaManager();
            m.createStatisticsTablesForTest();
        }

        UtFrameUtils.mockTimelinessForAsyncMVTest(starRocksAssert.getCtx());
    }

    public static Stream<Arguments> nextFlatQuery() {
        return TestUtil.getSsbLineorderFlatQueryList().stream()
                .filter(p -> !p.first.equals("Q1.3"))
                .map(p -> Arguments.of(p.first));
    }

    public static Stream<Arguments> nextQuery() {
        return TestUtil.getSsbLineorderFlatQueryList().stream()
                .map(p -> Arguments.of(p.first));
    }

    private List<List<String>> showMVs(String mvDb) throws DdlException {
        CustomizedQueryExecutor executor = new CustomizedQueryExecutor();
        getStarRocksAssert().getCtx().changeCatalogDb(mvDb);
        return executor.show(getStarRocksAssert().getCtx(), "SHOW TABLES like '_mv_%'")
                .getResultRows();
    }

    private Set<String> collectMVDigests(String mvDb) throws DdlException {
        return showMVs(mvDb).stream()
                .map(row -> row.get(0))
                .map(MVName::parse)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(MVName::getDigest)
                .collect(Collectors.toSet());
    }

    private Set<String> createMVs(String catalog, String db, String mvDb, List<Pair<String, String>> queryList)
            throws Throwable {
        GlobalVariable.setAutoMVPerLatticeMVLimit(-1);
        GlobalVariable.setAutoMVPerLatticeMVSelectivityRatio(-1.0);
        AutoMVUtil.mockUpCustomizedQueryExecutor(queryList, catalog, db);
        MVLifecycleAutoKeeper keeper = new MVLifecycleAutoKeeper();
        ConnectContext ctx = getStarRocksAssert().getCtx();
        ctx.getSessionVariable().setAutoMVCardRowCountRatioLWM(1.0);
        ctx.getSessionVariable().setAutoMVCardRowCountRatioHWM(1.0);
        keeper.process(ctx, () -> true);
        return collectMVDigests(mvDb);
    }

    private void rewriteQueries(String db, List<Pair<String, String>> queryList,
                                BiFunction<String, Optional<String>, Void> check) throws Exception {
        SessionVariable sv = getStarRocksAssert().getCtx().getSessionVariable();
        sv.setEnableMaterializedViewRewrite(true);
        sv.setMaterializedViewRewriteMode("force");
        getStarRocksAssert().getCtx().changeCatalogDb(db);
        for (Pair<String, String> p : queryList) {
            String q = p.second;
            String plan = UtFrameUtils.getFragmentPlan(getStarRocksAssert().getCtx(), q);
            Optional<String> optUsedMV = MVName.getPattern().matcher(plan).results()
                    .map(matchResult -> matchResult.group(0))
                    .findFirst();
            check.apply(q, optUsedMV);
        }
    }

    private void testHelper(String catalog, String db, String mvDb, List<Pair<String, String>> queryList)
            throws Throwable {
        setUpMock();
        try {
            Set<String> mvDigests = createMVs(catalog, db, mvDb, queryList);
            Assert.assertFalse(mvDigests.isEmpty());
            rewriteQueries(db, queryList, (name, optUsedMV) -> {
                Assert.assertTrue(optUsedMV.isPresent());
                String digest = MVName.parse(optUsedMV.get()).map(MVName::getDigest).orElse("UNKNOWN");
                Assert.assertTrue(mvDigests.contains(digest));
                return null;
            });
        } finally {
            CustomizedQueryExecutor executor = new CustomizedQueryExecutor();
            executor.exec(getStarRocksAssert().getCtx(), "DROP DATABASE IF EXISTS " + mvDb);
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("nextFlatQuery")
    public void testSingleSsbFlatQuery(String queryName) throws Throwable {
        if (new Random().nextInt(5) != 0) {
            return;
        }
        List<Pair<String, String>> queryList = TestUtil.getSsbLineorderFlatQueryList().stream()
                .filter(p -> p.first.equals(queryName))
                .collect(Collectors.toList());
        testHelper("default_catalog", "ssb", "automv_db", queryList);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("nextQuery")
    public void testSingleSsbQuery(String queryName) throws Throwable {
        if (new Random().nextInt(5) != 0) {
            return;
        }
        List<Pair<String, String>> queryList = TestUtil.getSsbQueryList().stream()
                .filter(p -> p.first.equals(queryName))
                .collect(Collectors.toList());
        testHelper("default_catalog", "ssb", "automv_db", queryList);
    }

    @Test
    public void testBasic() throws Throwable {
        List<Pair<String, String>> queryList =
                TestUtil.getSsbLineorderFlatQueryList().stream().filter(p -> p.first.equals("Q1.1"))
                        .collect(Collectors.toList());
        Set<String> mvDigests = createMVs("default_catalog", "ssb", "automv_db", queryList);
        rewriteQueries("ssb", queryList, (name, optUsedMV) -> {
            if (!name.equals("Q3.1")) {
                Assert.assertTrue(optUsedMV.isPresent());
                String digest = MVName.parse(optUsedMV.get()).map(MVName::getDigest).orElse("UNKNOWN");
                Assert.assertTrue(mvDigests.contains(digest));
            }
            return null;
        });
    }

    @Test
    public void testNotCreateDuplicateMVs() throws Throwable {
        List<Pair<String, String>> queryList =
                TestUtil.getSsbLineorderFlatQueryList().stream().filter(p -> p.first.equals("Q1.1"))
                        .collect(Collectors.toList());
        createMVs("default_catalog", "ssb", "automv_db", queryList);
        Set<String> mvNames = showMVs("automv_db").stream().map(r -> r.get(0)).collect(Collectors.toSet());
        Set<String> mvDigests = createMVs("default_catalog", "ssb", "automv_db", queryList);
        Set<String> mvNames2 = showMVs("automv_db").stream().map(r -> r.get(0)).collect(Collectors.toSet());
        Assert.assertEquals(mvNames, mvNames2);
        rewriteQueries("ssb", queryList, (name, optUsedMV) -> {
            if (!name.equals("Q3.1")) {
                Assert.assertTrue(optUsedMV.isPresent());
                String digest = MVName.parse(optUsedMV.get()).map(MVName::getDigest).orElse("UNKNOWN");
                Assert.assertTrue(mvDigests.contains(digest));
            }
            return null;
        });
    }
}
