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

package com.starrocks.connector.benchmark;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.starrocks.connector.benchmark.ssb.SsbBenchmarkSuite;
import com.starrocks.connector.benchmark.tpcds.TpcdsBenchmarkSuite;
import com.starrocks.connector.benchmark.tpch.TpchBenchmarkSuite;

import java.util.List;
import java.util.Map;

public final class BenchmarkSuiteFactory {
    public static final String DEFAULT_SUITE_NAME = TpcdsBenchmarkSuite.NAME;

    private static final Map<String, BenchmarkSuite> SUITES = ImmutableMap.<String, BenchmarkSuite>builder()
            .put(TpcdsBenchmarkSuite.NAME, new TpcdsBenchmarkSuite())
            .put(TpchBenchmarkSuite.NAME, new TpchBenchmarkSuite())
            .put(SsbBenchmarkSuite.NAME, new SsbBenchmarkSuite())
            .build();

    private BenchmarkSuiteFactory() {
    }

    public static BenchmarkSuite getSuite(String dbName) {
        BenchmarkSuite suite = SUITES.get(BenchmarkNameUtils.normalizeDbName(dbName));
        if (suite == null) {
            throw new IllegalArgumentException("Unknown benchmark database: " + dbName);
        }
        return suite;
    }

    public static BenchmarkSuite getSuiteIfExists(String dbName) {
        return SUITES.get(BenchmarkNameUtils.normalizeDbName(dbName));
    }

    public static BenchmarkSuite getDefaultSuite() {
        return getSuite(DEFAULT_SUITE_NAME);
    }

    public static List<BenchmarkSuite> getSuites() {
        return ImmutableList.copyOf(SUITES.values());
    }

}
