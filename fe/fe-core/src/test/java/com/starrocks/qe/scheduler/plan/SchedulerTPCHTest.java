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

package com.starrocks.qe.scheduler.plan;

import com.google.common.collect.Lists;
import com.starrocks.planner.TpchSQL;
import com.starrocks.qe.scheduler.SchedulerTestBase;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

public class SchedulerTPCHTest extends SchedulerTestBase {
    private static final String DIRECTORY = "scheduler/tpch/";

    @BeforeAll
    public static void beforeAll() throws Exception {
        SchedulerTestBase.beforeClass();
    }

    @AfterAll
    public static void afterAll() throws Exception {
        SchedulerTestBase.beforeClass();
    }

    @ParameterizedTest(name = "Tpch2.{0}")
    @MethodSource("listTest")
    public void testTPCHOther(String name, String file) {
        runFileUnitTest(file);
    }

    public static Stream<Arguments> listTest() {
        String sqlRootPath = Objects.requireNonNull(ClassLoader.getSystemClassLoader().getResource("sql")).getPath();
        File folder = new File(sqlRootPath + "/" + DIRECTORY);

        File[] files = folder.listFiles();
        if (files == null) {
            return Stream.empty();
        }

        return Arrays.stream(files).filter(file -> file.isFile() && file.getName().endsWith(".sql"))
                .map(file -> file.getName().replace(".sql", ""))
                .filter(s -> !TpchSQL.contains(s))
                .map(s -> Arguments.of(s, DIRECTORY + s));
    }

    @ParameterizedTest(name = "Tpch.{0}")
    @MethodSource("tpchSource")
    public void testTPCH(String name, String sql, String resultFile) {
        runFileUnitTest(sql, resultFile);
    }

    private static Stream<Arguments> tpchSource() {
        List<Arguments> cases = Lists.newArrayList();
        for (Map.Entry<String, String> entry : TpchSQL.getAllSQL().entrySet()) {
            cases.add(Arguments.of(entry.getKey(), entry.getValue(), DIRECTORY + entry.getKey()));
        }
        return cases.stream();
    }
}
