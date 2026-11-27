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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;

// Creates and retains unique StackWalker frames to keep a deterministic synthetic load in the
// ResolvedMethodTable. The requested entry count is not a production measurement. It models the large pre-cleanup
// table reported by JDK-8221393 without depending on when a particular collector unlinks dead weak entries.
final class Log4jStackWalkerPreconditioner {
    private static final String GENERATED_CLASS_PREFIX = "Log4jGeneratedStackFrames";
    private static final String PRECOMPILED_CLASS_DIRECTORY_PROPERTY = "log4j.precondition.classDir";
    private static final int MAX_PREFILL_ENTRIES = 1_000_000;
    private static final long GC_TIMEOUT_MILLIS = 10_000;
    private static final List<StackWalker.StackFrame> RETAINED_FRAMES = new ArrayList<>();

    private Log4jStackWalkerPreconditioner() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 4 && "generate".equals(args[0])) {
            int methodCount = parseMethodCount(args[1]);
            int cycles = parseCycles(args[2]);
            validateTotalEntries(methodCount, cycles);
            Path outputDirectory = Paths.get(args[3]).toAbsolutePath();
            Files.createDirectories(outputDirectory);
            compileGeneratedClasses(methodCount, cycles, outputDirectory);
            System.out.printf("Generated %,d unique StackWalker frame methods in %s sha256=%s%n",
                    (long) methodCount * cycles, outputDirectory,
                    generatedClassesSha256(outputDirectory, cycles));
            return;
        }
        if (args.length != 2 && args.length != 3) {
            throw new IllegalArgumentException("usage: prefillMethods prefillCycles [outputFile], or " +
                    "generate prefillMethods prefillCycles outputDirectory");
        }
        int methodCount = parseMethodCount(args[0]);
        int cycles = parseCycles(args[1]);
        validateTotalEntries(methodCount, cycles);
        String garbageCollectors = ManagementFactory.getGarbageCollectorMXBeans().stream()
                .map(GarbageCollectorMXBean::getName)
                .collect(Collectors.joining("+"));
        String environment = String.format(Locale.ROOT,
                "java=%s runtime=%s vendor=%s vm=%s vmVersion=%s gc=%s jvmArgs=%s",
                System.getProperty("java.version"), System.getProperty("java.runtime.version"),
                System.getProperty("java.vendor"), System.getProperty("java.vm.name"),
                System.getProperty("java.vm.version"), garbageCollectors,
                ManagementFactory.getRuntimeMXBean().getInputArguments());
        System.out.println(environment);
        Path outputFile = args.length == 3 ? Paths.get(args[2]).toAbsolutePath() : null;
        Path cycleOutputFile = outputFile == null ? null : Paths.get(outputFile + ".cycles.csv");
        String result = age(methodCount, cycles, cycleOutputFile);
        if (args.length == 3) {
            Path parent = outputFile.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            Files.write(outputFile, Arrays.asList(environment, result), StandardCharsets.UTF_8);
            System.out.println("Precondition result saved to " + outputFile);
            System.out.println("Per-cycle precondition data saved to " + cycleOutputFile);
        }
    }

    static String age(int methodCount, int cycles) throws Exception {
        return age(methodCount, cycles, null);
    }

    static String age(int methodCount, int cycles, Path cycleOutputFile) throws Exception {
        if (methodCount == 0 && cycles == 0) {
            return "StackWalker precondition disabled";
        }
        validateTotalEntries(methodCount, cycles);
        if (!RETAINED_FRAMES.isEmpty()) {
            throw new IllegalStateException("StackWalker precondition may only run once per JVM");
        }
        AgingResult agingResult;
        String precompiledDirectory = System.getProperty(PRECOMPILED_CLASS_DIRECTORY_PROPERTY);
        boolean deleteCompileDirectory = precompiledDirectory == null;
        Path compileDirectory = deleteCompileDirectory
                ? Files.createTempDirectory("starrocks-log4j-stackwalker-")
                : Paths.get(precompiledDirectory).toAbsolutePath();
        try {
            if (deleteCompileDirectory) {
                compileGeneratedClasses(methodCount, cycles, compileDirectory);
            } else if (!containsGeneratedClasses(compileDirectory, cycles)) {
                throw new IllegalStateException("precompiled StackWalker class not found in " + compileDirectory);
            }

            String generatedClassesSha256 = generatedClassesSha256(compileDirectory, cycles);
            long startNanos = System.nanoTime();
            ClassLoader parentLoader = Log4jStackWalkerPreconditioner.class.getClassLoader();
            try (URLClassLoader loader = new URLClassLoader(
                    new URL[] {compileDirectory.toUri().toURL()}, parentLoader)) {
                agingResult = fillResolvedMethodTable(loader, methodCount, cycles);
            }
            if (cycleOutputFile != null) {
                agingResult.writeCycles(cycleOutputFile);
            }
            String result = String.format(Locale.ROOT,
                    "StackWalker precondition completed: methodsPerClass=%d classes=%d retainedFrames=%d " +
                            "generatedClasses=%s generatedClassesSha256=%s fillGcCountDelta=%d " +
                            "postFillGcCountDelta=%d " +
                            "elapsed=%.3fs cycleDuration={%s} first10Mean=%.3fms last10Mean=%.3fms " +
                            "lastOverFirst=%.3fx noCompletedGcFirst10={%s} noCompletedGcLast10={%s} " +
                            "noCompletedGcLastOverFirst=%s. The retained frame count and cycle measurements do " +
                            "not by " +
                            "themselves prove table layout; confirm it with -Xlog:membername+table=trace on the " +
                            "candidate JDK 11 ZGC runtime.",
                    methodCount, cycles, agingResult.retainedFrames,
                    deleteCompileDirectory ? "in-process" : "precompiled", generatedClassesSha256,
                    agingResult.fillGcCountDelta, agingResult.postFillGcCountDelta,
                    (System.nanoTime() - startNanos) / (double) TimeUnit.SECONDS.toNanos(1),
                    agingResult.durationSummary(), nanosToMillis(agingResult.firstDecileMean()),
                    nanosToMillis(agingResult.lastDecileMean()),
                    agingResult.lastDecileMean() / (double) agingResult.firstDecileMean(),
                    agingResult.noCompletedGcFirstDecileSummary(),
                    agingResult.noCompletedGcLastDecileSummary(),
                    agingResult.noCompletedGcLastOverFirst());
            System.out.println(result);
            return result;
        } finally {
            if (deleteCompileDirectory) {
                deleteDirectory(compileDirectory);
            }
        }
    }

    static long requestVerifiedGc() {
        if (ManagementFactory.getRuntimeMXBean().getInputArguments().contains("-XX:+DisableExplicitGC")) {
            throw new IllegalStateException("explicit GC is disabled by -XX:+DisableExplicitGC");
        }
        long collectionsBefore = completedGcCycles();
        System.gc();
        return awaitCompletedGcCycle(collectionsBefore) - collectionsBefore;
    }

    private static int parseMethodCount(String value) {
        int methodCount = Integer.parseInt(value);
        if (methodCount <= 0 || methodCount > 50_000) {
            throw new IllegalArgumentException("prefillMethods must be positive and must not exceed 50000");
        }
        return methodCount;
    }

    private static int parseCycles(String value) {
        int cycles = Integer.parseInt(value);
        if (cycles <= 0) {
            throw new IllegalArgumentException("prefillCycles must be positive");
        }
        return cycles;
    }

    private static void validateTotalEntries(int methodCount, int cycles) {
        long entries = (long) methodCount * cycles;
        if (entries > MAX_PREFILL_ENTRIES) {
            throw new IllegalArgumentException("prefillMethods * prefillCycles must not exceed " +
                    MAX_PREFILL_ENTRIES);
        }
    }

    private static boolean containsGeneratedClasses(Path compileDirectory, int cycles) {
        return Files.isRegularFile(compileDirectory.resolve(generatedClassName(0) + ".class")) &&
                Files.isRegularFile(compileDirectory.resolve(generatedClassName(cycles - 1) + ".class"));
    }

    private static void compileGeneratedClasses(int methodCount, int cycles, Path compileDirectory)
            throws IOException {
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        if (compiler == null) {
            throw new IllegalStateException("a full JDK is required to generate StackWalker frame methods");
        }
        List<String> compilerArguments = new ArrayList<>();
        compilerArguments.add("--release");
        compilerArguments.add("11");
        compilerArguments.add("-d");
        compilerArguments.add(compileDirectory.toString());
        for (int cycle = 0; cycle < cycles; cycle++) {
            String className = generatedClassName(cycle);
            Path sourceFile = compileDirectory.resolve(className + ".java");
            Files.write(sourceFile, generatedSource(className, methodCount).getBytes(StandardCharsets.UTF_8));
            compilerArguments.add(sourceFile.toString());
        }
        ByteArrayOutputStream compilerOutput = new ByteArrayOutputStream();
        int exitCode = compiler.run(null, compilerOutput, compilerOutput,
                compilerArguments.toArray(new String[0]));
        if (exitCode != 0) {
            throw new IllegalStateException("failed to compile StackWalker precondition class: " +
                    compilerOutput.toString(StandardCharsets.UTF_8.name()));
        }
    }

    private static String generatedClassName(int cycle) {
        return GENERATED_CLASS_PREFIX + cycle;
    }

    private static String generatedClassesSha256(Path compileDirectory, int cycles) throws IOException {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            for (int cycle = 0; cycle < cycles; cycle++) {
                Path classFile = compileDirectory.resolve(generatedClassName(cycle) + ".class");
                digest.update(classFile.getFileName().toString().getBytes(StandardCharsets.UTF_8));
                digest.update(Files.readAllBytes(classFile));
            }
            StringBuilder result = new StringBuilder(64);
            for (byte value : digest.digest()) {
                result.append(String.format(Locale.ROOT, "%02x", value & 0xff));
            }
            return result.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }

    private static String generatedSource(String className, int methodCount) {
        StringBuilder source = new StringBuilder(methodCount * 64);
        source.append("public final class ").append(className).append(" {\n");
        for (int method = 0; method < methodCount; method++) {
            source.append("  public static void frame").append(method)
                    .append("(Runnable capture) { capture.run(); }\n");
        }
        return source.append("}\n").toString();
    }

    private static AgingResult fillResolvedMethodTable(URLClassLoader loader, int expectedMethodCount, int cycles)
            throws Exception {
        StackWalker stackWalker = StackWalker.getInstance(StackWalker.Option.RETAIN_CLASS_REFERENCE);
        long[] cycleDurations = new long[cycles];
        long[] cycleGcCountDeltas = new long[cycles];
        int[] retainedFramesAfterCycles = new int[cycles];
        int progressInterval = Math.max(1, cycles / 10);
        long fillGcCountBefore = completedGcCycles();
        long agingStartNanos = System.nanoTime();
        for (int cycle = 0; cycle < cycles; cycle++) {
            long cycleGcCountBefore = completedGcCycles();
            long cycleStartNanos = System.nanoTime();
            Class<?> generatedClass = Class.forName(generatedClassName(cycle), true, loader);
            Runnable capture = () -> retainGeneratedFrame(stackWalker, generatedClass);
            Method[] generatedMethods = Stream.of(generatedClass.getDeclaredMethods())
                    .filter(method -> method.getName().startsWith("frame"))
                    .toArray(Method[]::new);
            if (generatedMethods.length != expectedMethodCount) {
                throw new IllegalStateException("generated class does not contain the requested number of methods");
            }
            for (Method method : generatedMethods) {
                method.invoke(null, capture);
            }
            cycleDurations[cycle] = System.nanoTime() - cycleStartNanos;
            cycleGcCountDeltas[cycle] = completedGcCycles() - cycleGcCountBefore;
            retainedFramesAfterCycles[cycle] = RETAINED_FRAMES.size();
            if ((cycle + 1) % progressInterval == 0 || cycle + 1 == cycles) {
                System.out.printf(Locale.ROOT, "StackWalker precondition progress: cycle=%d/%d elapsed=%.3fs%n",
                        cycle + 1, cycles,
                        (System.nanoTime() - agingStartNanos) / (double) TimeUnit.SECONDS.toNanos(1));
            }
        }
        int expectedFrames = Math.multiplyExact(expectedMethodCount, cycles);
        if (RETAINED_FRAMES.size() != expectedFrames) {
            throw new IllegalStateException("did not retain the requested number of StackWalker frames");
        }
        long fillGcCountDelta = completedGcCycles() - fillGcCountBefore;
        long postFillGcCountDelta = requestVerifiedGc();
        return new AgingResult(fillGcCountDelta, postFillGcCountDelta, RETAINED_FRAMES.size(),
                cycleDurations, cycleGcCountDeltas, retainedFramesAfterCycles);
    }

    private static void retainGeneratedFrame(StackWalker stackWalker, Class<?> generatedClass) {
        List<StackWalker.StackFrame> generatedFrames = stackWalker.walk(frames -> frames
                .filter(frame -> frame.getDeclaringClass() == generatedClass)
                .collect(Collectors.toList()));
        if (generatedFrames.size() != 1) {
            throw new IllegalStateException("generated StackWalker frame not found");
        }
        RETAINED_FRAMES.add(generatedFrames.get(0));
    }

    private static long completedGcCycles() {
        boolean hasCycleCollector = ManagementFactory.getGarbageCollectorMXBeans().stream()
                .anyMatch(collector -> collector.getName().contains("Cycles"));
        long completed = 0;
        boolean supported = false;
        for (GarbageCollectorMXBean collector : ManagementFactory.getGarbageCollectorMXBeans()) {
            if (hasCycleCollector && !collector.getName().contains("Cycles")) {
                continue;
            }
            long collectionCount = collector.getCollectionCount();
            if (collectionCount >= 0) {
                completed += collectionCount;
                supported = true;
            }
        }
        if (!supported) {
            throw new IllegalStateException("GC collection counters are unavailable");
        }
        return completed;
    }

    private static long awaitCompletedGcCycle(long collectionsBefore) {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(GC_TIMEOUT_MILLIS);
        while (completedGcCycles() <= collectionsBefore) {
            if (System.nanoTime() >= deadline) {
                throw new IllegalStateException("explicit GC did not complete; check -XX:+DisableExplicitGC");
            }
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
        }
        return completedGcCycles();
    }

    private static double nanosToMillis(long nanos) {
        return nanos / 1_000_000.0;
    }

    private static void deleteDirectory(Path directory) throws IOException {
        if (!Files.exists(directory)) {
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

    private static final class DeleteFailedException extends RuntimeException {
        private DeleteFailedException(IOException cause) {
            super(cause);
        }

        @Override
        public synchronized IOException getCause() {
            return (IOException) super.getCause();
        }
    }

    private static final class AgingResult {
        private final long fillGcCountDelta;
        private final long postFillGcCountDelta;
        private final int retainedFrames;
        private final long[] cycleDurations;
        private final long[] cycleGcCountDeltas;
        private final int[] retainedFramesAfterCycles;

        private AgingResult(long fillGcCountDelta, long postFillGcCountDelta, int retainedFrames,
                            long[] cycleDurations, long[] cycleGcCountDeltas, int[] retainedFramesAfterCycles) {
            this.fillGcCountDelta = fillGcCountDelta;
            this.postFillGcCountDelta = postFillGcCountDelta;
            this.retainedFrames = retainedFrames;
            this.cycleDurations = cycleDurations;
            this.cycleGcCountDeltas = cycleGcCountDeltas;
            this.retainedFramesAfterCycles = retainedFramesAfterCycles;
        }

        private void writeCycles(Path outputFile) throws IOException {
            Path parent = outputFile.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            try (java.io.BufferedWriter output = Files.newBufferedWriter(outputFile, StandardCharsets.UTF_8)) {
                output.write("cycle,duration_ns,gc_count_delta,retained_frames_after_cycle\n");
                for (int cycle = 0; cycle < cycleDurations.length; cycle++) {
                    output.write(String.format(Locale.ROOT, "%d,%d,%d,%d%n", cycle, cycleDurations[cycle],
                            cycleGcCountDeltas[cycle], retainedFramesAfterCycles[cycle]));
                }
            }
        }

        private String durationSummary() {
            long[] sorted = cycleDurations.clone();
            Arrays.sort(sorted);
            return String.format(Locale.ROOT, "p50=%.3fms p95=%.3fms p99=%.3fms max=%.3fms",
                    nanosToMillis(sorted[sorted.length / 2]),
                    nanosToMillis(sorted[(int) Math.ceil(sorted.length * 0.95) - 1]),
                    nanosToMillis(sorted[(int) Math.ceil(sorted.length * 0.99) - 1]),
                    nanosToMillis(sorted[sorted.length - 1]));
        }

        private long firstDecileMean() {
            return mean(0, decileSize());
        }

        private long lastDecileMean() {
            return mean(cycleDurations.length - decileSize(), cycleDurations.length);
        }

        private String noCompletedGcFirstDecileSummary() {
            return noCompletedGcSummary(0, decileSize());
        }

        private String noCompletedGcLastDecileSummary() {
            return noCompletedGcSummary(cycleDurations.length - decileSize(), cycleDurations.length);
        }

        private String noCompletedGcLastOverFirst() {
            long first = noCompletedGcMean(0, decileSize());
            long last = noCompletedGcMean(cycleDurations.length - decileSize(), cycleDurations.length);
            return first < 0 || last < 0 ? "n/a" : String.format(Locale.ROOT, "%.3fx", last / (double) first);
        }

        private int decileSize() {
            return Math.max(1, cycleDurations.length / 10);
        }

        private long mean(int startInclusive, int endExclusive) {
            long total = 0;
            for (int index = startInclusive; index < endExclusive; index++) {
                total += cycleDurations[index];
            }
            return total / (endExclusive - startInclusive);
        }

        private String noCompletedGcSummary(int startInclusive, int endExclusive) {
            int count = noCompletedGcCount(startInclusive, endExclusive);
            long mean = noCompletedGcMean(startInclusive, endExclusive);
            return count == 0 ? "cycles=0 mean=n/a" :
                    String.format(Locale.ROOT, "cycles=%d mean=%.3fms", count, nanosToMillis(mean));
        }

        private int noCompletedGcCount(int startInclusive, int endExclusive) {
            int count = 0;
            for (int index = startInclusive; index < endExclusive; index++) {
                if (cycleGcCountDeltas[index] == 0) {
                    count++;
                }
            }
            return count;
        }

        private long noCompletedGcMean(int startInclusive, int endExclusive) {
            long total = 0;
            int count = 0;
            for (int index = startInclusive; index < endExclusive; index++) {
                if (cycleGcCountDeltas[index] == 0) {
                    total += cycleDurations[index];
                    count++;
                }
            }
            return count == 0 ? -1 : total / count;
        }
    }
}
