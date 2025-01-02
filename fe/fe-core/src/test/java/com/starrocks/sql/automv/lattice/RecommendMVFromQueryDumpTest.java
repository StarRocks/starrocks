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

import com.starrocks.sql.automv.util.AutoMVUtil;
import com.starrocks.sql.automv.util.Result;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.stream.Stream;

public class RecommendMVFromQueryDumpTest {

    private static String getRootPath() {
        return ClassLoader.getSystemClassLoader()
                .getResource("sql").getPath() + "/query_dump/automv";
    }

    public static Stream<Arguments> nextQueryDump() {
        File dir = new File(getRootPath());
        System.out.println(dir);
        return Stream.of(Objects.requireNonNull(dir.listFiles(File::isDirectory)))
                .flatMap(subdir -> Stream.of(Objects.requireNonNull(subdir.listFiles(File::isFile)))
                        .map(File::getName)
                        .sorted()
                        .map(fileName -> Arguments.of(subdir.getName(), fileName)));
    }

    @ParameterizedTest(name = "{0}.{1}")
    @MethodSource("nextQueryDump")
    public void test(String groupName, String queryDumpPath) throws Exception {
        if (new Random().nextInt(10) != 0) {
            return;
        }
        File queryDumpFile = new File(getRootPath() + "/" + groupName + "/" + queryDumpPath);
        String jsonStr = IOUtils.toString(new FileReader(queryDumpFile));
        QueryDumpMVRecommender recommender = QueryDumpMVRecommender.of();

        List<String> mvList = recommender.recommendNoTraceInfo(jsonStr, AutoMVUtil::configDefaultAutoMV);
        String testName = groupName + "." + queryDumpPath;
        Set<String> noMVs = Set.of(
                "tpch.tpch_1g_q02.json",
                "tpch.tpch_1g_q08.json",
                "tpch.tpch_1g_q14.json",
                "tpch.tpch_1g_q17.json");
        Assert.assertTrue(noMVs.contains(testName) || !mvList.isEmpty());
    }

    public void analyzePredicates(String dirName, String name) throws Exception {
        analyzePredicates(dirName, name, 0);
    }

    public void analyzePredicates(String dirName, String name, int start) throws Exception {
        File dir = new File(dirName);
        File[] dumpFiles = dir.listFiles(f -> f.getName().endsWith("_queryDump.json"));
        int i = start;
        while (i < dumpFiles.length) {
            File outputFile = new File(name + i + ".sql");
            FileWriter fileWriter = new FileWriter(outputFile);
            PrintWriter ps = new PrintWriter(fileWriter);
            List<File> dumpLists = Arrays.asList(dumpFiles);
            List<File> subLists = dumpLists.subList(i, Math.min(i + 100, dumpFiles.length));
            subLists.stream().flatMap(df -> {
                System.out.println(df.getAbsolutePath());
                return Result.wrap(() -> IOUtils.toString(new FileReader(df)))
                        .bind(jsonStr -> {
                            QueryDumpMVRecommender recommender = QueryDumpMVRecommender.of();
                            return recommender.analyzePredicates(jsonStr, sv -> {
                            });
                        }).unwrap().orElseGet(Collections::emptyList).stream();
            }).forEach(ps::println);
            ps.flush();
            ps.close();
        }
    }
}
