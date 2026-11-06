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
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.appender.RollingFileAppender;
import org.apache.logging.log4j.core.appender.rolling.NoOpTriggeringPolicy;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.DefaultConfiguration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.layout.PatternLayout;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

final class Log4jRollingFileBenchmarkSupport {
    static final String LOGGER_NAME = "com.starrocks.qe.QeProcessorImpl";
    private static final ThreadLocal<int[]> FORMATTED_LENGTH = ThreadLocal.withInitial(() -> new int[1]);

    private Log4jRollingFileBenchmarkSupport() {
    }

    static Session open(String callerPattern) throws IOException {
        Path logDirectory = Files.createTempDirectory("starrocks-log4j-rolling-");
        Path logFile = logDirectory.resolve("fe.log");
        LoggerContext context = new LoggerContext(LOGGER_NAME + '-' + callerPattern);
        try {
            Configuration configuration = new org.apache.logging.log4j.core.config.DefaultConfiguration();
            context.start(configuration);
            PatternLayout layout = PatternLayout.newBuilder()
                    .withConfiguration(configuration)
                    .withPattern("%d{yyyy-MM-dd HH:mm:ss.SSSXXX} %p (%t|%tid) [" + callerPattern + "] %m%n")
                    .build();
            RollingFileAppender appender = RollingFileAppender.newBuilder()
                    .setName("Sys")
                    .setConfiguration(configuration)
                    .setLayout(layout)
                    .withFileName(logFile.toString())
                    .withFilePattern(logFile + ".%i")
                    // Exercise the real synchronous file write path without adding rollover as another variable.
                    .withPolicy(NoOpTriggeringPolicy.INSTANCE)
                    .withAppend(false)
                    .setBufferedIo(true)
                    .setImmediateFlush(true)
                    .build();
            if (appender == null) {
                throw new IllegalStateException("failed to create RollingFileAppender");
            }
            appender.start();
            configuration.addAppender(appender);

            LoggerConfig loggerConfig = new LoggerConfig(LOGGER_NAME, org.apache.logging.log4j.Level.INFO, false);
            loggerConfig.addAppender(appender, org.apache.logging.log4j.Level.INFO, null);
            configuration.addLogger(LOGGER_NAME, loggerConfig);
            context.updateLoggers();

            verifyConfiguration(configuration, callerPattern);
            return new Session(context, context.getLogger(LOGGER_NAME), logDirectory);
        } catch (RuntimeException e) {
            if (context.stop(30, TimeUnit.SECONDS)) {
                try {
                    deleteDirectory(logDirectory);
                } catch (IOException cleanupFailure) {
                    e.addSuppressed(cleanupFailure);
                }
            } else {
                e.addSuppressed(new IllegalStateException("logger context did not stop; temporary logs retained"));
            }
            throw e;
        }
    }

    static Session openFormatting(String callerPattern) {
        LoggerContext context = new LoggerContext(LOGGER_NAME + "-format-only-" + callerPattern);
        try {
            Configuration configuration = new DefaultConfiguration();
            context.start(configuration);
            PatternLayout layout = PatternLayout.newBuilder()
                    .withConfiguration(configuration)
                    .withPattern("%d{yyyy-MM-dd HH:mm:ss.SSSXXX} %p (%t|%tid) [" +
                            callerPattern + "] %m%n")
                    .build();
            FormattingAppender appender = new FormattingAppender("Formatting", layout);
            appender.start();
            configuration.addAppender(appender);

            LoggerConfig loggerConfig = new LoggerConfig(LOGGER_NAME, Level.INFO, false);
            loggerConfig.addAppender(appender, Level.INFO, null);
            configuration.addLogger(LOGGER_NAME, loggerConfig);
            context.updateLoggers();

            verifyLocationRequirement(layout, loggerConfig, callerPattern);
            return new Session(context, context.getLogger(LOGGER_NAME), null);
        } catch (RuntimeException e) {
            context.stop(30, TimeUnit.SECONDS);
            throw e;
        }
    }

    private static void verifyConfiguration(Configuration configuration, String callerPattern) {
        Appender appender = configuration.getAppender("Sys");
        if (!(appender instanceof RollingFileAppender)) {
            throw new IllegalStateException("benchmark must use a real RollingFileAppender");
        }
        if (!(appender.getLayout() instanceof PatternLayout)) {
            throw new IllegalStateException("benchmark must use PatternLayout");
        }
        PatternLayout layout = (PatternLayout) appender.getLayout();
        LoggerConfig loggerConfig = configuration.getLoggerConfig(LOGGER_NAME);
        verifyLocationRequirement(layout, loggerConfig, callerPattern);
    }

    private static void verifyLocationRequirement(PatternLayout layout, LoggerConfig loggerConfig,
                                                  String callerPattern) {
        boolean expectedLocation = callerPattern.contains("%C");
        if (layout.requiresLocation() != expectedLocation || loggerConfig.requiresLocation() != expectedLocation) {
            throw new IllegalStateException("benchmark does not exercise the expected caller location path");
        }
    }

    private static void deleteDirectory(Path directory) throws IOException {
        if (directory == null || !Files.exists(directory)) {
            return;
        }
        try (Stream<Path> paths = Files.walk(directory)) {
            paths.sorted(Comparator.reverseOrder()).forEach(path -> {
                try {
                    Files.deleteIfExists(path);
                } catch (IOException e) {
                    throw new DeleteFailedException(e);
                }
            });
        } catch (DeleteFailedException e) {
            throw e.getCause();
        }
    }

    static final class Session implements AutoCloseable {
        private final LoggerContext context;
        private final Logger logger;
        private final Path logDirectory;

        private Session(LoggerContext context, Logger logger, Path logDirectory) {
            this.context = context;
            this.logger = logger;
            this.logDirectory = logDirectory;
        }

        Logger logger() {
            return logger;
        }

        @Override
        public void close() throws IOException {
            if (!context.stop(30, TimeUnit.SECONDS)) {
                throw new IOException("logger context did not stop" +
                        (logDirectory == null ? "" : "; temporary logs retained at " + logDirectory));
            }
            if (logDirectory != null) {
                deleteDirectory(logDirectory);
            }
        }
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

    private static final class DeleteFailedException extends RuntimeException {
        private DeleteFailedException(IOException cause) {
            super(cause);
        }

        @Override
        public synchronized IOException getCause() {
            return (IOException) super.getCause();
        }
    }
}
