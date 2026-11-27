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

package com.starrocks.benchmark;

import org.apache.logging.log4j.core.Logger;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

// Thread counts are supplied by the JMH CLI. High thread counts are saturation stress cases, not replays of
// a production arrival pattern.
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(3)
@State(Scope.Benchmark)
public class Log4jRollingFileAppenderBenchmark {
    @Param({"%C{1}.%M():%L", "%c{1}"})
    public String callerPattern;

    private Log4jRollingFileBenchmarkSupport.Session session;
    private Logger logger;

    public static void main(String[] args) throws IOException {
        String[] effectiveArgs = new String[args.length + 1];
        effectiveArgs[0] = Log4jRollingFileAppenderBenchmark.class.getSimpleName();
        System.arraycopy(args, 0, effectiveArgs, 1, args.length);
        org.openjdk.jmh.Main.main(effectiveArgs);
    }

    @Setup(org.openjdk.jmh.annotations.Level.Trial)
    public void setUp() throws IOException {
        session = Log4jRollingFileBenchmarkSupport.open(callerPattern);
        logger = session.logger();
    }

    @TearDown(org.openjdk.jmh.annotations.Level.Trial)
    public void tearDown() throws IOException {
        session.close();
    }

    @Benchmark
    @BenchmarkMode(Mode.SampleTime)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public void latency() {
        logMissingQueryStatus();
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void throughput() {
        logMissingQueryStatus();
    }

    private void logMissingQueryStatus() {
        logger.info("ReportExecStatus() failed, query does not exist, fragment_instance_id={}, query_id={},",
                "00000000-0000-0000-0000-000000000001", "00000000-0000-0000-0000-000000000002");
    }
}
