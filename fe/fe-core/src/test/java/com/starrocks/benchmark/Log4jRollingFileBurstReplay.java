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

import java.io.BufferedWriter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;

// Reconstructs the observed millisecond-bucket arrival counts and uses 122 workers as a production-scale
// concurrency cap. It does not preserve within-millisecond ordering or per-thread identities. The production
// schedule preserves the two observed waves; "uniform" is retained as a control.
// Amplification repeats each observed event at the same timestamp and is always reported separately from 1x.
// For JDK-8221393, run an isolated Linux JDK 11 JVM with ZGC and a synthetic prefill such as 1000x100 or
// 1000x500, then use JDK 17 ZGC as the negative control. These sizes are deterministic stress conditions, not
// production ResolvedMethodTable measurements. A fresh-JVM run uses prefill=0x0. The preconditioner retains
// generated StackWalker frames, so gc-before-each-burst clears transient caller-location entries while keeping
// the synthetic table load alive. rolling-file exercises the synchronous RollingFileAppender non-rollover path;
// format-only removes file-appender serialization to isolate caller-location cost. Each run writes raw CSV,
// metadata, and a summary.
public final class Log4jRollingFileBurstReplay {
    private static final String[] CALLER_PATTERNS = {"%C{1}.%M():%L", "%c{1}"};
    private static final int PRODUCTION_MESSAGES = 407;
    private static final int MAX_REPLAY_MESSAGES = 100_000;
    private static final int MAX_WORKERS = 512;
    private static final int MAX_TOTAL_CYCLES = 100;
    private static final int MAX_MEASURED_SAMPLES_PER_PATTERN = 1_000_000;
    private static final long PRODUCTION_WINDOW_MILLIS = 221;
    private static final int[][] PRODUCTION_MILLIS_COUNTS = {
            {0, 3}, {1, 5}, {2, 3}, {3, 4}, {4, 20}, {5, 16}, {6, 16}, {7, 15}, {8, 18}, {9, 15},
            {10, 16}, {11, 7}, {21, 1}, {32, 1}, {33, 4}, {34, 16}, {35, 24}, {36, 2}, {43, 13},
            {44, 3}, {46, 2}, {47, 2}, {48, 4}, {49, 1}, {54, 1}, {55, 2}, {213, 27}, {214, 32},
            {215, 55}, {216, 39}, {217, 11}, {218, 12}, {219, 12}, {220, 4}, {221, 1}
    };

    private Log4jRollingFileBurstReplay() {
    }

    public static void main(String[] args) throws Exception {
        Arguments arguments = Arguments.parse(args);
        Path output = arguments.output.toAbsolutePath();
        Path parent = output.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }

        String environment = environmentDescription(arguments);
        System.out.println(environment);
        Path preconditionCycles = Paths.get(output + ".precondition.cycles.csv");
        String precondition = Log4jStackWalkerPreconditioner.age(
                arguments.prefillMethods, arguments.prefillCycles, preconditionCycles);
        List<String> summaries;
        try (BufferedWriter samples = Files.newBufferedWriter(output, StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
            samples.write("schedule,amplification,pattern,cycle,order_in_cycle,message,scheduled_offset_ns," +
                    "actual_start_offset_ns,completion_offset_ns,start_lag_ns,logger_call_latency_ns," +
                    "end_to_end_ns\n");
            summaries = runReplay(arguments, samples);
        }
        summaries.forEach(System.out::println);

        Path metadata = Paths.get(output + ".metadata.txt");
        Path summary = Paths.get(output + ".summary.txt");
        Files.write(metadata, Arrays.asList(environment, precondition), StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        Files.write(summary, summaries, StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        System.out.println("Raw samples saved to " + output);
        System.out.println("Environment saved to " + metadata);
        System.out.println("Summary saved to " + summary);
    }

    private static List<String> runReplay(Arguments arguments, BufferedWriter output) throws Exception {
        AtomicInteger threadNumber = new AtomicInteger();
        ExecutorService executor = Executors.newFixedThreadPool(arguments.workers, runnable -> {
            Thread thread = new Thread(runnable, "log4j-burst-worker-" + threadNumber.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        });
        List<PatternRun> patternRuns = new ArrayList<>();
        Throwable primaryFailure = null;
        try {
            int measuredSamples = Math.multiplyExact(
                    arguments.scheduleOffsets.length, arguments.measurementBursts);
            for (String callerPattern : CALLER_PATTERNS) {
                patternRuns.add(new PatternRun(callerPattern,
                        openSession(callerPattern, arguments.appenderMode), measuredSamples,
                        arguments.measurementBursts));
            }
            int totalCycles = Math.addExact(arguments.warmupBursts, arguments.measurementBursts);
            int burstSequence = 0;
            for (int cycle = 0; cycle < totalCycles; cycle++) {
                boolean newPatternFirst = arguments.newPatternFirst ^ ((cycle & 1) == 1);
                int[] order = newPatternFirst ? new int[] {1, 0} : new int[] {0, 1};
                for (int orderInCycle = 0; orderInCycle < order.length; orderInCycle++) {
                    int patternIndex = order[orderInCycle];
                    if (burstSequence++ > 0 && arguments.pauseMillis > 0) {
                        Thread.sleep(arguments.pauseMillis);
                    }
                    if (arguments.gcBeforeEachBurst) {
                        Log4jStackWalkerPreconditioner.requestVerifiedGc();
                    }
                    PatternRun patternRun = patternRuns.get(patternIndex);
                    BurstResult result = runBurst(arguments, executor, patternRun.session.logger());
                    int measurement = cycle - arguments.warmupBursts;
                    if (measurement >= 0) {
                        patternRun.record(measurement, orderInCycle, result);
                    }
                }
            }
        } catch (Exception | Error t) {
            primaryFailure = t;
            throw t;
        } finally {
            try {
                closeResources(executor, patternRuns);
            } catch (Exception | Error cleanupFailure) {
                if (primaryFailure != null) {
                    primaryFailure.addSuppressed(cleanupFailure);
                } else {
                    throw cleanupFailure;
                }
            }
        }
        for (PatternRun patternRun : patternRuns) {
            patternRun.writeSamples(output, arguments);
        }
        List<String> summaries = new ArrayList<>();
        for (PatternRun patternRun : patternRuns) {
            summaries.add(patternRun.summary(arguments));
        }
        summaries.addAll(pairedSummaries(patternRuns));
        return summaries;
    }

    private static Log4jRollingFileBenchmarkSupport.Session openSession(String callerPattern, String appenderMode)
            throws IOException {
        if ("format-only".equals(appenderMode)) {
            return Log4jRollingFileBenchmarkSupport.openFormatting(callerPattern);
        }
        return Log4jRollingFileBenchmarkSupport.open(callerPattern);
    }

    private static void closeResources(ExecutorService executor, List<PatternRun> patternRuns)
            throws IOException, InterruptedException {
        executor.shutdownNow();
        if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
            throw new IllegalStateException("burst executor did not terminate; logger session left open for safety");
        }
        IOException closeFailure = null;
        for (PatternRun patternRun : patternRuns) {
            try {
                patternRun.session.close();
            } catch (IOException e) {
                if (closeFailure == null) {
                    closeFailure = e;
                } else {
                    closeFailure.addSuppressed(e);
                }
            }
        }
        if (closeFailure != null) {
            throw closeFailure;
        }
    }

    private static BurstResult runBurst(Arguments arguments, ExecutorService executor, Logger logger)
            throws InterruptedException {
        int messages = arguments.scheduleOffsets.length;
        CountDownLatch ready = new CountDownLatch(Math.min(arguments.workers, messages));
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(messages);
        long[] actualStartOffsets = new long[messages];
        long[] completionOffsets = new long[messages];
        long[] loggerCallLatencies = new long[messages];
        long[] startLags = new long[messages];
        long[] endToEndLatencies = new long[messages];
        AtomicLong burstStart = new AtomicLong();
        AtomicReference<Throwable> taskFailure = new AtomicReference<>();

        for (int message = 0; message < messages; message++) {
            int index = message;
            long scheduledOffset = arguments.scheduleOffsets[index];
            executor.execute(() -> {
                ready.countDown();
                try {
                    start.await();
                    waitUntil(burstStart.get() + scheduledOffset);
                    long logStart = System.nanoTime();
                    actualStartOffsets[index] = logStart - burstStart.get();
                    logMissingQueryStatus(logger);
                    long logEnd = System.nanoTime();
                    completionOffsets[index] = logEnd - burstStart.get();
                    loggerCallLatencies[index] = logEnd - logStart;
                    startLags[index] = actualStartOffsets[index] - scheduledOffset;
                    endToEndLatencies[index] = completionOffsets[index] - scheduledOffset;
                } catch (Throwable t) {
                    if (t instanceof InterruptedException) {
                        Thread.currentThread().interrupt();
                    }
                    taskFailure.compareAndSet(null, t);
                } finally {
                    done.countDown();
                }
            });
        }

        if (!ready.await(30, TimeUnit.SECONDS)) {
            throw new IllegalStateException("burst workers did not become ready within 30 seconds");
        }
        burstStart.set(System.nanoTime());
        start.countDown();
        if (!done.await(2, TimeUnit.MINUTES)) {
            throw new IllegalStateException("burst did not finish within two minutes");
        }
        if (taskFailure.get() != null) {
            throw new IllegalStateException("burst worker failed", taskFailure.get());
        }
        return new BurstResult(actualStartOffsets, completionOffsets, loggerCallLatencies, startLags,
                endToEndLatencies, Arrays.stream(completionOffsets).max().orElse(0));
    }

    private static void logMissingQueryStatus(Logger logger) {
        logger.info("ReportExecStatus() failed, query does not exist, fragment_instance_id={}, query_id={},",
                "00000000-0000-0000-0000-000000000001", "00000000-0000-0000-0000-000000000002");
    }

    private static void waitUntil(long deadlineNanos) throws InterruptedException {
        while (true) {
            long remaining = deadlineNanos - System.nanoTime();
            if (remaining <= 0) {
                return;
            }
            LockSupport.parkNanos(remaining);
            if (Thread.interrupted()) {
                throw new InterruptedException();
            }
        }
    }

    private static long[] sorted(List<Long> values) {
        return values.stream().mapToLong(Long::longValue).sorted().toArray();
    }

    private static long[] sorted(long[] values) {
        long[] sorted = values.clone();
        Arrays.sort(sorted);
        return sorted;
    }

    private static String percentiles(long[] sortedValues) {
        return String.format(Locale.ROOT, "p50=%.3fms p95=%.3fms p99=%.3fms max=%.3fms",
                nanosToMillis(percentile(sortedValues, 0.50)), nanosToMillis(percentile(sortedValues, 0.95)),
                nanosToMillis(percentile(sortedValues, 0.99)), nanosToMillis(percentile(sortedValues, 1.00)));
    }

    private static long percentile(long[] sortedValues, double quantile) {
        int index = (int) Math.ceil(quantile * sortedValues.length) - 1;
        return sortedValues[Math.max(0, Math.min(index, sortedValues.length - 1))];
    }

    private static double nanosToMillis(long nanos) {
        return nanos / 1_000_000.0;
    }

    private static List<String> pairedSummaries(List<PatternRun> patternRuns) {
        if (patternRuns.size() != 2 || patternRuns.get(0).recordedBursts.size() !=
                patternRuns.get(1).recordedBursts.size()) {
            throw new IllegalStateException("paired comparison requires two patterns with the same burst count");
        }
        PatternRun oldPattern = patternRuns.get(0);
        PatternRun newPattern = patternRuns.get(1);
        List<Long> loggerCallP99Deltas = new ArrayList<>();
        List<Long> endToEndP99Deltas = new ArrayList<>();
        List<Long> drainDeltas = new ArrayList<>();
        List<Long> oldFirstLoggerCallP99Deltas = new ArrayList<>();
        List<Long> newFirstLoggerCallP99Deltas = new ArrayList<>();
        double[] loggerCallP99Ratios = new double[oldPattern.recordedBursts.size()];
        List<String> perCycle = new ArrayList<>();
        for (int index = 0; index < oldPattern.recordedBursts.size(); index++) {
            RecordedBurst old = oldPattern.recordedBursts.get(index);
            RecordedBurst replacement = newPattern.recordedBursts.get(index);
            if (old.cycle != replacement.cycle || old.orderInCycle == replacement.orderInCycle) {
                throw new IllegalStateException("patterns are not a valid pair for cycle " + index);
            }
            long oldLoggerCallP99 = percentile(sorted(old.result.loggerCallLatencies), 0.99);
            long newLoggerCallP99 = percentile(sorted(replacement.result.loggerCallLatencies), 0.99);
            long oldEndToEndP99 = percentile(sorted(old.result.endToEndLatencies), 0.99);
            long newEndToEndP99 = percentile(sorted(replacement.result.endToEndLatencies), 0.99);
            long oldDrain = old.result.completionNanos - old.result.lastScheduledOffsetNanos;
            long newDrain = replacement.result.completionNanos - replacement.result.lastScheduledOffsetNanos;
            loggerCallP99Deltas.add(oldLoggerCallP99 - newLoggerCallP99);
            endToEndP99Deltas.add(oldEndToEndP99 - newEndToEndP99);
            drainDeltas.add(oldDrain - newDrain);
            (old.orderInCycle == 0 ? oldFirstLoggerCallP99Deltas : newFirstLoggerCallP99Deltas)
                    .add(oldLoggerCallP99 - newLoggerCallP99);
            loggerCallP99Ratios[index] = oldLoggerCallP99 / (double) Math.max(1, newLoggerCallP99);
            perCycle.add(String.format(Locale.ROOT,
                    "pairCycle=%d order=%s oldLoggerCallP99=%.3fms newLoggerCallP99=%.3fms " +
                            "oldMinusNewLoggerCallP99=%.3fms oldOverNewLoggerCallP99=%.3fx " +
                            "oldEndToEndP99=%.3fms newEndToEndP99=%.3fms oldDrain=%.3fms newDrain=%.3fms",
                    old.cycle, old.orderInCycle == 0 ? "old-first" : "new-first",
                    nanosToMillis(oldLoggerCallP99), nanosToMillis(newLoggerCallP99),
                    nanosToMillis(oldLoggerCallP99 - newLoggerCallP99), loggerCallP99Ratios[index],
                    nanosToMillis(oldEndToEndP99), nanosToMillis(newEndToEndP99),
                    nanosToMillis(oldDrain), nanosToMillis(newDrain)));
        }
        Arrays.sort(loggerCallP99Ratios);
        List<String> result = new ArrayList<>();
        result.add(String.format(Locale.ROOT,
                "pairedBurstComparison cycles=%d unit=burst oldMinusNewLoggerCallP99={%s} " +
                        "oldOverNewLoggerCallP99={%s} oldMinusNewEndToEndP99={%s} " +
                        "oldMinusNewDrainAfterLastArrival={%s} oldFirstLoggerCallP99Delta={%s} " +
                        "newFirstLoggerCallP99Delta={%s}",
                oldPattern.recordedBursts.size(), medianRange(sorted(loggerCallP99Deltas)),
                ratioMedianRange(loggerCallP99Ratios), medianRange(sorted(endToEndP99Deltas)),
                medianRange(sorted(drainDeltas)), medianRange(sorted(oldFirstLoggerCallP99Deltas)),
                medianRange(sorted(newFirstLoggerCallP99Deltas))));
        result.addAll(perCycle);
        return result;
    }

    private static String medianRange(long[] sortedValues) {
        return String.format(Locale.ROOT, "min=%.3fms p50=%.3fms max=%.3fms",
                nanosToMillis(sortedValues[0]), nanosToMillis(percentile(sortedValues, 0.50)),
                nanosToMillis(sortedValues[sortedValues.length - 1]));
    }

    private static String ratioMedianRange(double[] sortedValues) {
        return String.format(Locale.ROOT, "min=%.3fx p50=%.3fx max=%.3fx",
                sortedValues[0], sortedValues[(int) Math.ceil(sortedValues.length * 0.50) - 1],
                sortedValues[sortedValues.length - 1]);
    }

    private static String environmentDescription(Arguments arguments) {
        String garbageCollectors = ManagementFactory.getGarbageCollectorMXBeans().stream()
                .map(bean -> bean.getName())
                .collect(Collectors.joining("+"));
        boolean candidateJdk11Zgc = "11".equals(System.getProperty("java.specification.version")) &&
                garbageCollectors.contains("ZGC");
        return String.format(Locale.ROOT,
                "java=%s runtime=%s vendor=%s vm=%s vmVersion=%s gc=%s candidateJdk11Zgc=%s os=%s/%s " +
                        "processors=%d jvmArgs=%s schedule=%s appender=%s replayModel=millisecond-bucket-upper-bound " +
                        "amplification=%dx prefillMethods=%d prefillCycles=%d workers=%d messages=%d windowMs=%d " +
                        "warmupBursts=%d measurementBursts=%d pauseMs=%d gcBeforeEachBurst=%s " +
                        "pairedAlternating=true initialOrder=%s",
                System.getProperty("java.version"), System.getProperty("java.runtime.version"),
                System.getProperty("java.vendor"), System.getProperty("java.vm.name"),
                System.getProperty("java.vm.version"), garbageCollectors, candidateJdk11Zgc,
                System.getProperty("os.name"), System.getProperty("os.arch"),
                Runtime.getRuntime().availableProcessors(), ManagementFactory.getRuntimeMXBean().getInputArguments(),
                arguments.scheduleName, arguments.appenderMode, arguments.amplification,
                arguments.prefillMethods, arguments.prefillCycles,
                arguments.workers,
                arguments.scheduleOffsets.length, arguments.windowMillis(), arguments.warmupBursts,
                arguments.measurementBursts, arguments.pauseMillis, arguments.gcBeforeEachBurst,
                arguments.newPatternFirst ? "new-first" : "old-first");
    }

    private static final class PatternRun {
        private final String callerPattern;
        private final Log4jRollingFileBenchmarkSupport.Session session;
        private final List<Long> loggerCallLatencies;
        private final List<Long> startLags;
        private final List<Long> endToEndLatencies;
        private final List<Long> burstCompletions;
        private final List<Long> drainAfterLastArrivals;
        private final List<RecordedBurst> recordedBursts;

        private PatternRun(String callerPattern, Log4jRollingFileBenchmarkSupport.Session session,
                           int measuredSamples, int measurementBursts) {
            this.callerPattern = callerPattern;
            this.session = session;
            this.loggerCallLatencies = new ArrayList<>(measuredSamples);
            this.startLags = new ArrayList<>(measuredSamples);
            this.endToEndLatencies = new ArrayList<>(measuredSamples);
            this.burstCompletions = new ArrayList<>(measurementBursts);
            this.drainAfterLastArrivals = new ArrayList<>(measurementBursts);
            this.recordedBursts = new ArrayList<>(measurementBursts);
        }

        private void record(int cycle, int orderInCycle, BurstResult result) {
            recordedBursts.add(new RecordedBurst(cycle, orderInCycle, result));
            burstCompletions.add(result.completionNanos);
            drainAfterLastArrivals.add(result.completionNanos - result.lastScheduledOffsetNanos);
            for (int message = 0; message < result.loggerCallLatencies.length; message++) {
                loggerCallLatencies.add(result.loggerCallLatencies[message]);
                startLags.add(result.startLags[message]);
                endToEndLatencies.add(result.endToEndLatencies[message]);
            }
        }

        private void writeSamples(BufferedWriter output, Arguments arguments) throws IOException {
            for (RecordedBurst recorded : recordedBursts) {
                BurstResult result = recorded.result;
                for (int message = 0; message < result.loggerCallLatencies.length; message++) {
                    output.write(String.format(Locale.ROOT, "%s,%d,%s,%d,%d,%d,%d,%d,%d,%d,%d,%d%n",
                            arguments.scheduleName, arguments.amplification, callerPattern, recorded.cycle,
                            recorded.orderInCycle, message, arguments.scheduleOffsets[message],
                            result.actualStartOffsets[message], result.completionOffsets[message],
                            result.startLags[message], result.loggerCallLatencies[message],
                            result.endToEndLatencies[message]));
                }
            }
        }

        private String summary(Arguments arguments) {
            long[] loggerCall = sorted(loggerCallLatencies);
            long[] startLag = sorted(startLags);
            long[] endToEnd = sorted(endToEndLatencies);
            long[] completion = sorted(burstCompletions);
            long[] drain = sorted(drainAfterLastArrivals);
            return String.format(Locale.ROOT,
                    "schedule=%s amplification=%dx prefill=%dx%d pattern=%s workers=%d messages=%d " +
                            "windowMs=%d bursts=%d samples=%d loggerCall={%s} startLag={%s} endToEnd={%s} " +
                            "burstCompletion={%s} drainAfterLastArrival={%s}",
                    arguments.scheduleName, arguments.amplification, arguments.prefillMethods,
                    arguments.prefillCycles, callerPattern, arguments.workers, arguments.scheduleOffsets.length,
                    arguments.windowMillis(), arguments.measurementBursts, loggerCall.length,
                    percentiles(loggerCall), percentiles(startLag), percentiles(endToEnd),
                    percentiles(completion), percentiles(drain));
        }
    }

    private static final class BurstResult {
        private final long[] actualStartOffsets;
        private final long[] completionOffsets;
        private final long[] loggerCallLatencies;
        private final long[] startLags;
        private final long[] endToEndLatencies;
        private final long completionNanos;
        private final long lastScheduledOffsetNanos;

        private BurstResult(long[] actualStartOffsets, long[] completionOffsets, long[] loggerCallLatencies,
                            long[] startLags, long[] endToEndLatencies, long completionNanos) {
            this.actualStartOffsets = actualStartOffsets;
            this.completionOffsets = completionOffsets;
            this.loggerCallLatencies = loggerCallLatencies;
            this.startLags = startLags;
            this.endToEndLatencies = endToEndLatencies;
            this.completionNanos = completionNanos;
            this.lastScheduledOffsetNanos = completionOffsets.length == 0 ? 0 :
                    PRODUCTION_WINDOW_MILLIS * TimeUnit.MILLISECONDS.toNanos(1);
        }
    }

    private static final class RecordedBurst {
        private final int cycle;
        private final int orderInCycle;
        private final BurstResult result;

        private RecordedBurst(int cycle, int orderInCycle, BurstResult result) {
            this.cycle = cycle;
            this.orderInCycle = orderInCycle;
            this.result = result;
        }
    }

    private static final class Arguments {
        private final String scheduleName;
        private final int workers;
        private final int amplification;
        private final int prefillMethods;
        private final int prefillCycles;
        private final int warmupBursts;
        private final int measurementBursts;
        private final long pauseMillis;
        private final Path output;
        private final boolean newPatternFirst;
        private final boolean gcBeforeEachBurst;
        private final String appenderMode;
        private final long[] scheduleOffsets;

        private Arguments(String scheduleName, int workers, int amplification, int prefillMethods,
                          int prefillCycles, int warmupBursts, int measurementBursts, long pauseMillis,
                          Path output, boolean newPatternFirst, boolean gcBeforeEachBurst, String appenderMode) {
            this.scheduleName = scheduleName;
            this.workers = workers;
            this.amplification = amplification;
            this.prefillMethods = prefillMethods;
            this.prefillCycles = prefillCycles;
            this.warmupBursts = warmupBursts;
            this.measurementBursts = measurementBursts;
            this.pauseMillis = pauseMillis;
            this.output = output;
            this.newPatternFirst = newPatternFirst;
            this.gcBeforeEachBurst = gcBeforeEachBurst;
            this.appenderMode = appenderMode;
            this.scheduleOffsets = createSchedule(scheduleName, amplification);
        }

        private static Arguments parse(String[] args) {
            if (args.length != 0 && (args.length < 9 || args.length > 12)) {
                throw new IllegalArgumentException("usage: [production|uniform workers amplification " +
                        "prefillMethods prefillCycles warmupBursts measurementBursts pauseMs outputCsv " +
                        "[old-first|new-first] [gc-before-each-burst|no-gc] " +
                        "[rolling-file|format-only]]");
            }
            Arguments arguments = args.length == 0
                    ? new Arguments("production", 122, 1, 0, 0, 3, 10, 1000,
                            Paths.get("log4j-rolling-burst.csv"), false, false, "rolling-file")
                    : new Arguments(args[0], Integer.parseInt(args[1]), Integer.parseInt(args[2]),
                            Integer.parseInt(args[3]), Integer.parseInt(args[4]), Integer.parseInt(args[5]),
                            Integer.parseInt(args[6]), Long.parseLong(args[7]), Paths.get(args[8]),
                            args.length >= 10 && parseNewPatternFirst(args[9]),
                            args.length >= 11 && parseGcBeforeEachBurst(args[10]),
                            args.length == 12 ? parseAppenderMode(args[11]) : "rolling-file");
            if (arguments.workers <= 0 || arguments.amplification <= 0 || arguments.warmupBursts < 0 ||
                    arguments.prefillMethods < 0 || arguments.prefillCycles < 0 ||
                    arguments.measurementBursts <= 0 || arguments.pauseMillis < 0 ||
                    (arguments.prefillMethods == 0) != (arguments.prefillCycles == 0)) {
                throw new IllegalArgumentException("workers/amplification/measurementBursts must be positive; " +
                        "prefillMethods/prefillCycles must both be zero or positive; " +
                        "warmupBursts/pauseMs must be non-negative");
            }
            if (arguments.prefillMethods > 50_000) {
                throw new IllegalArgumentException("prefillMethods must not exceed 50000");
            }
            if ((arguments.measurementBursts & 1) != 0) {
                throw new IllegalArgumentException("measurementBursts must be even to balance pattern order");
            }
            if (arguments.workers > MAX_WORKERS) {
                throw new IllegalArgumentException("workers must not exceed " + MAX_WORKERS);
            }
            long totalCycles = (long) arguments.warmupBursts + arguments.measurementBursts;
            if (totalCycles > MAX_TOTAL_CYCLES) {
                throw new IllegalArgumentException("warmupBursts + measurementBursts must not exceed " +
                        MAX_TOTAL_CYCLES);
            }
            long measuredSamples = (long) arguments.scheduleOffsets.length * arguments.measurementBursts;
            if (measuredSamples > MAX_MEASURED_SAMPLES_PER_PATTERN) {
                throw new IllegalArgumentException("measured samples per pattern must not exceed " +
                        MAX_MEASURED_SAMPLES_PER_PATTERN);
            }
            if ((long) arguments.prefillMethods * arguments.prefillCycles > 1_000_000) {
                throw new IllegalArgumentException("prefillMethods * prefillCycles must not exceed 1000000");
            }
            return arguments;
        }

        private static long[] createSchedule(String scheduleName, int amplification) {
            int messages = checkedMessageCount(amplification);
            if ("production".equals(scheduleName)) {
                long[] offsets = new long[messages];
                int index = 0;
                for (int[] millisCount : PRODUCTION_MILLIS_COUNTS) {
                    long offset = TimeUnit.MILLISECONDS.toNanos(millisCount[0]);
                    for (int count = 0; count < millisCount[1] * amplification; count++) {
                        offsets[index++] = offset;
                    }
                }
                if (index != offsets.length) {
                    throw new IllegalStateException("production schedule must contain 407 messages");
                }
                return offsets;
            }
            if ("uniform".equals(scheduleName)) {
                long[] offsets = new long[messages];
                long windowNanos = TimeUnit.MILLISECONDS.toNanos(PRODUCTION_WINDOW_MILLIS);
                for (int index = 0; index < messages; index++) {
                    offsets[index] = messages == 1 ? 0 : windowNanos * index / (messages - 1);
                }
                return offsets;
            }
            throw new IllegalArgumentException("schedule must be production or uniform");
        }

        private static int checkedMessageCount(int amplification) {
            if (amplification <= 0) {
                throw new IllegalArgumentException("amplification must be positive");
            }
            int messages;
            try {
                messages = Math.multiplyExact(PRODUCTION_MESSAGES, amplification);
            } catch (ArithmeticException e) {
                throw new IllegalArgumentException("amplification is too large", e);
            }
            if (messages > MAX_REPLAY_MESSAGES) {
                throw new IllegalArgumentException("replay must not exceed " + MAX_REPLAY_MESSAGES + " messages");
            }
            return messages;
        }

        private static boolean parseNewPatternFirst(String order) {
            if ("new-first".equals(order)) {
                return true;
            }
            if ("old-first".equals(order)) {
                return false;
            }
            throw new IllegalArgumentException("pattern order must be old-first or new-first");
        }

        private static boolean parseGcBeforeEachBurst(String gcMode) {
            if ("gc-before-each-burst".equals(gcMode)) {
                return true;
            }
            if ("no-gc".equals(gcMode)) {
                return false;
            }
            throw new IllegalArgumentException("GC mode must be gc-before-each-burst or no-gc");
        }

        private static String parseAppenderMode(String appenderMode) {
            if ("rolling-file".equals(appenderMode) || "format-only".equals(appenderMode)) {
                return appenderMode;
            }
            throw new IllegalArgumentException("appender mode must be rolling-file or format-only");
        }

        private long windowMillis() {
            return TimeUnit.NANOSECONDS.toMillis(scheduleOffsets[scheduleOffsets.length - 1]);
        }
    }
}
