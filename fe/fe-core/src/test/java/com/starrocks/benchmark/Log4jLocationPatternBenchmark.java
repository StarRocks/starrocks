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

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.DefaultConfiguration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.layout.PatternLayout;
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

@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(3)
@State(Scope.Benchmark)
public class Log4jLocationPatternBenchmark {
    private static final String LOGGER_NAME = "com.starrocks.qe.QeProcessorImpl";
    private static final ThreadLocal<int[]> FORMATTED_LENGTH = ThreadLocal.withInitial(() -> new int[1]);

    @Param({"%C{1}.%M():%L", "%c{1}"})
    public String callerPattern;

    private LoggerContext context;
    private Logger logger;

    public static void main(String[] args) throws IOException {
        String[] effectiveArgs = new String[args.length + 1];
        effectiveArgs[0] = Log4jLocationPatternBenchmark.class.getSimpleName();
        System.arraycopy(args, 0, effectiveArgs, 1, args.length);
        org.openjdk.jmh.Main.main(effectiveArgs);
    }

    @Setup(org.openjdk.jmh.annotations.Level.Trial)
    public void setUp() {
        context = new LoggerContext(LOGGER_NAME + '-' + callerPattern);
        DefaultConfiguration configuration = new DefaultConfiguration();
        context.start(configuration);

        PatternLayout layout = PatternLayout.newBuilder()
                .withConfiguration(configuration)
                .withPattern('[' + callerPattern + "] %m%n")
                .build();
        FormattingAppender appender = new FormattingAppender("benchmark-appender", layout);
        appender.start();
        configuration.addAppender(appender);

        LoggerConfig loggerConfig = new LoggerConfig(LOGGER_NAME, Level.INFO, false);
        loggerConfig.addAppender(appender, Level.INFO, null);
        boolean expectedLocation = callerPattern.contains("%C");
        if (layout.requiresLocation() != expectedLocation || loggerConfig.requiresLocation() != expectedLocation) {
            throw new IllegalStateException("benchmark does not exercise the expected caller location path");
        }
        configuration.addLogger(LOGGER_NAME, loggerConfig);
        context.updateLoggers();
        logger = context.getLogger(LOGGER_NAME);
    }

    @TearDown(org.openjdk.jmh.annotations.Level.Trial)
    public void tearDown() {
        context.stop();
    }

    @Benchmark
    public int formatOnly() {
        return logMissingQueryStatus();
    }

    private int logMissingQueryStatus() {
        logger.info("ReportExecStatus() failed, query does not exist, fragment_instance_id={}, query_id={},",
                "00000000-0000-0000-0000-000000000001", "00000000-0000-0000-0000-000000000002");
        return FORMATTED_LENGTH.get()[0];
    }

    private static final class FormattingAppender extends AbstractAppender {
        private FormattingAppender(String name, PatternLayout layout) {
            super(name, null, layout, true, Property.EMPTY_ARRAY);
        }

        @Override
        public void append(LogEvent event) {
            String formattedLog = getLayout().toSerializable(event).toString();
            FORMATTED_LENGTH.get()[0] = formattedLog.length();
        }
    }
}
