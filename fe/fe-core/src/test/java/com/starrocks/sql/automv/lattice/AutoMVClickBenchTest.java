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
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.automv.generator.MVName;
import com.starrocks.sql.automv.util.AutoMVUtil;
import com.starrocks.sql.automv.util.Result;
import com.starrocks.sql.automv.util.TestUtil;
import com.starrocks.utframe.StarRocksAssert;
import org.apache.kerby.util.IOUtil;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class AutoMVClickBenchTest {
    private static final ThreadLocal<StarRocksAssert> STARROCKS_ASSERT = new ThreadLocal<>();

    private static StarRocksAssert getStarRocksAssert() {
        FeConstants.runningUnitTest = true;
        if (STARROCKS_ASSERT.get() == null) {
            STARROCKS_ASSERT.set(TestUtil.prepareTables("click_bench", TestUtil::getClickBenchCreateTableSqlList));
        }
        return STARROCKS_ASSERT.get();
    }

    @BeforeClass
    public static void setUp() throws Exception {
        getStarRocksAssert();
    }

    public static Stream<Arguments> nextQuery() {
        return TestUtil.getClickBenchQueryList().stream()
                .map(p -> Arguments.of(p.first));
    }

    @Test
    public void testAll() {
        AutoMVUtil.defaultTestHelper(STARROCKS_ASSERT.get().getCtx(), TestUtil.getClickBenchQueryList());
    }

    @Test
    public void testQ40() {
        List<Pair<String, String>> queryList = TestUtil.getClickBenchQueryList()
                .stream()
                .filter(p -> p.first.equals("Q40"))
                .collect(Collectors.toList());

        AutoMVUtil.testHelper(STARROCKS_ASSERT.get().getCtx(),
                queryList,
                sv -> sv.setAutoMVEnableComplexDerivedDimensions(false),
                results -> Assert.assertTrue(results.isEmpty()));

        AutoMVUtil.testHelper(STARROCKS_ASSERT.get().getCtx(),
                queryList,
                sv -> {
                    sv.setAutoMVCardRowCountRatioHWM(1.0);
                    sv.setAutoMVCardRowCountRatioLWM(1.0);
                    sv.setAutoMVEnableComplexDerivedDimensions(true);
                },
                results -> Assert.assertFalse(results.isEmpty()));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("nextQuery")
    public void testSingleQuery(String name) throws Exception {
        List<Pair<String, String>> queryList = TestUtil.getClickBenchQueryList()
                .stream()
                .filter(p -> p.first.equals(name))
                .collect(Collectors.toList());
        ConnectContext ctx = getStarRocksAssert().getCtx();
        AutoMVUtil.defaultTestHelper(ctx, queryList);
    }

    private void testLattice(List<Pair<String, String>> queryList, boolean verify) {
        List<MVRecommendation> recommendations = AutoMVUtil.recommend(queryList, getStarRocksAssert().getCtx());
        String path = Optional.ofNullable(ClassLoader.getSystemClassLoader().getResource("sql"))
                .map(URL::getPath).orElse(null);

        Assert.assertTrue(path != null);
        if (!verify) {
            path = path.replace("target/test-classes/", "src/test/resources/");
        }
        File latticeDumpDir = new File(path, "lattice_dump");
        Assert.assertTrue(latticeDumpDir.exists());
        File currentDumpDir = new File(latticeDumpDir, TestUtil.getTestName());
        if (!currentDumpDir.exists()) {
            currentDumpDir.mkdir();
        }
        for (MVRecommendation rec : recommendations) {
            LatticeNode node = rec.getLatticeNode();
            File file = new File(currentDumpDir, String.format("lattice_node_%d.dump", node.getNodeOrdinal()));

            if (verify) {
                String dump = Result.wrap(() -> IOUtil.readFile(file)).unwrap().orElse(null);
                Assert.assertTrue(dump != null);
                String actual = rec.getLatticeNode().dump().getResult().replaceAll(MVName.getPattern().pattern(), "mv");
                if (!actual.equals(dump)) {
                    File file0 = new File(currentDumpDir,
                            String.format("lattice_node_%d.dump.conflict", node.getNodeOrdinal()));
                    Result.wrap(() -> IOUtil.writeFile(actual, file0));
                }
                Assert.assertEquals(file.getName(), dump, actual);
            } else {

                String dump = rec.getLatticeNode().dump().getResult().replaceAll(MVName.getPattern().pattern(), "mv");
                Assert.assertTrue(
                        Result.wrap(() -> IOUtil.writeFile(dump, file)).unwrap().isPresent());
            }
        }
    }

    @Test
    public void testClickBench1() {
        Set<String> querySet =
                ImmutableSet.of("Q01", "Q03", "Q04", "Q05", "Q06", "Q07", "Q30", "Q37", "Q38", "Q39", "Q42");
        List<Pair<String, String>> queryList = TestUtil.getClickBenchQueryList()
                .stream()
                .filter(p -> querySet.contains(p.first))
                .collect(Collectors.toList());
        for (int i = 0; i < 10; i++) {
            Collections.shuffle(queryList);
            testLattice(queryList, true);
        }
    }
}
