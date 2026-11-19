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

package com.starrocks.memory;

import com.starrocks.common.Config;
import one.profiler.AsyncProfiler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.InOrder;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Date;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ProcProfileCollectorTest {
    private static final long START_MS = Instant.parse("2026-01-02T00:00:00Z").toEpochMilli();
    /** Mirrors ProcProfileCollector.BACKOFF_MAX_MS; used to step the fake clock past any backoff window. */
    private static final long BACKOFF_MAX_MS = 900 * 1000L;

    @TempDir
    private Path tempDir;

    private String oldSysLogDir;
    private boolean oldProcProfileCpuEnable;
    private boolean oldProcProfileMemEnable;
    private long oldProcProfileCollectTimeS;

    @BeforeEach
    public void setUp() {
        oldSysLogDir = Config.sys_log_dir;
        oldProcProfileCpuEnable = Config.proc_profile_cpu_enable;
        oldProcProfileMemEnable = Config.proc_profile_mem_enable;
        oldProcProfileCollectTimeS = Config.proc_profile_collect_time_s;

        Config.sys_log_dir = tempDir.toString();
        Config.proc_profile_cpu_enable = true;
        Config.proc_profile_mem_enable = false;
        // Zero keeps the collector from actually sleeping; the fake clock drives every deadline instead.
        Config.proc_profile_collect_time_s = 0;
    }

    @AfterEach
    public void tearDown() {
        Config.sys_log_dir = oldSysLogDir;
        Config.proc_profile_cpu_enable = oldProcProfileCpuEnable;
        Config.proc_profile_mem_enable = oldProcProfileMemEnable;
        Config.proc_profile_collect_time_s = oldProcProfileCollectTimeS;
        Thread.interrupted();
    }

    /**
     * Profile file names embed a timestamp that ProcProfileCollector formats in the JVM default time zone,
     * and FE unit tests run with -Duser.timezone=Asia/Shanghai (fe-core/pom.xml). Expected names must
     * therefore be derived from the same formatter and clock reading, never hardcoded as a UTC literal.
     */
    private static Path profilePath(ProcProfileCollector collector, String prefix, long nowMs, String suffix) {
        return Path.of(collector.getProfileLogDir(),
                prefix + new SimpleDateFormat(ProcProfileFiles.TIME_FORMAT).format(new Date(nowMs)) + suffix);
    }

    /**
     * Makes a mocked {@code stop,file=X} command actually create X. async-profiler writes the profile when
     * the session is stopped, and the collector then compresses that file, so a success path that does not
     * produce it would fail in compression instead of exercising the code under test.
     */
    private static Answer<String> writesProfileFile(String content) {
        return invocation -> {
            String command = invocation.getArgument(0);
            Files.writeString(Path.of(command.substring("stop,file=".length())), content);
            return null;
        };
    }

    private static boolean isStartOf(String command, String event) {
        return command != null && command.startsWith("start,") && command.contains("event=" + event + ",");
    }

    @Test
    public void testNativeLibraryLoadErrorEntersBackoffInsteadOfRetryingEveryCycle() throws Exception {
        AtomicLong nowMs = new AtomicLong(START_MS);
        ProcProfileCollector collector = new ProcProfileCollector(nowMs::get);

        try (MockedStatic<AsyncProfiler> mockedProfiler = Mockito.mockStatic(AsyncProfiler.class)) {
            // AsyncProfiler.getInstance() rethrows this when libasyncProfiler.so cannot be loaded, e.g. when
            // the extraction directory is mounted noexec. It is an Error, so catching Exception is not enough
            // and it would otherwise escape to Daemon.run() and be retried on every 1s tick forever.
            mockedProfiler.when(AsyncProfiler::getInstance)
                    .thenThrow(new UnsatisfiedLinkError("failed to map segment from shared object"));

            collector.runAfterCatalogReady();
            mockedProfiler.verify(AsyncProfiler::getInstance, times(1));

            collector.runAfterCatalogReady();
            mockedProfiler.verify(AsyncProfiler::getInstance, times(1));
        }
    }

    @Test
    public void testCpuFailureDoesNotPreventMemoryProfileCollection() throws Exception {
        AtomicLong nowMs = new AtomicLong(START_MS);
        ProcProfileCollector collector = new ProcProfileCollector(nowMs::get);
        AsyncProfiler profiler = Mockito.mock(AsyncProfiler.class);
        Config.proc_profile_mem_enable = true;

        try (MockedStatic<AsyncProfiler> mockedProfiler = Mockito.mockStatic(AsyncProfiler.class)) {
            mockedProfiler.when(AsyncProfiler::getInstance).thenReturn(profiler);
            // Only the cpu event fails. This is the common container case: perf_events is denied while
            // alloc sampling works fine, and a permanently broken cpu profile must not disable the other.
            when(profiler.execute(argThat(command -> isStartOf(command, "cpu"))))
                    .thenThrow(new IllegalStateException("Perf events unavailable"));
            when(profiler.execute(startsWith("stop,file=")))
                    .thenAnswer(writesProfileFile("<html>profile</html>"));

            collector.runAfterCatalogReady();

            verify(profiler).execute(argThat(command -> isStartOf(command, "cpu")));
            verify(profiler).execute(argThat(command -> isStartOf(command, "alloc")));

            Path memArchive = profilePath(collector, ProcProfileFiles.MEM_FILE_NAME_PREFIX, nowMs.get(),
                    ProcProfileFiles.PUBLISHED_SUFFIX);
            Path cpuArchive = profilePath(collector, ProcProfileFiles.CPU_FILE_NAME_PREFIX, nowMs.get(),
                    ProcProfileFiles.PUBLISHED_SUFFIX);
            assertTrue(Files.exists(memArchive), "memory profile must be published when cpu fails: " + memArchive);
            assertTrue(Files.notExists(cpuArchive), "failed cpu profile must not be published: " + cpuArchive);
        }
    }

    @Test
    public void testSuccessfulCollectionPublishesArchiveAndRemovesTheRawProfile() throws Exception {
        AtomicLong nowMs = new AtomicLong(START_MS);
        ProcProfileCollector collector = new ProcProfileCollector(nowMs::get);
        AsyncProfiler profiler = Mockito.mock(AsyncProfiler.class);

        try (MockedStatic<AsyncProfiler> mockedProfiler = Mockito.mockStatic(AsyncProfiler.class)) {
            mockedProfiler.when(AsyncProfiler::getInstance).thenReturn(profiler);
            when(profiler.execute(startsWith("stop,file=")))
                    .thenAnswer(writesProfileFile("<html>profile</html>"));

            collector.runAfterCatalogReady();

            verify(profiler).execute(argThat(command -> isStartOf(command, "cpu")));
            verify(profiler).execute(startsWith("stop,file="));

            Path archive = profilePath(collector, ProcProfileFiles.CPU_FILE_NAME_PREFIX, nowMs.get(),
                    ProcProfileFiles.PUBLISHED_SUFFIX);
            Path rawProfile = profilePath(collector, ProcProfileFiles.CPU_FILE_NAME_PREFIX, nowMs.get(), ".html");
            assertTrue(Files.exists(archive), "expected published archive " + archive);
            assertTrue(Files.notExists(rawProfile), "raw profile must be removed after compression: " + rawProfile);
        }
    }

    @Test
    public void testStartFailureIsContainedAndBacksOffInsteadOfRetryingEveryCycle() throws Exception {
        AtomicLong nowMs = new AtomicLong(START_MS);
        ProcProfileCollector collector = new ProcProfileCollector(nowMs::get);
        AsyncProfiler profiler = Mockito.mock(AsyncProfiler.class);

        try (MockedStatic<AsyncProfiler> mockedProfiler = Mockito.mockStatic(AsyncProfiler.class)) {
            mockedProfiler.when(AsyncProfiler::getInstance).thenReturn(profiler);
            when(profiler.execute(startsWith("start,")))
                    .thenThrow(new IllegalStateException("Profiler already started"));

            // Must not throw: the previous code let this escape as a RuntimeException, which skipped the
            // retention pass and had Daemon.run() retry on the very next 1s tick.
            collector.runAfterCatalogReady();

            verify(profiler, times(1)).execute(argThat(command -> isStartOf(command, "cpu")));
            verify(profiler, never()).execute(startsWith("stop,file="));
            // The running session belongs to whatever this collector did not start - a manual jcmd or
            // agentpath profile - and must not be torn down.
            verify(profiler, never()).stop();

            collector.runAfterCatalogReady();
            verify(profiler, times(1)).execute(startsWith("start,"));
        }
    }

    @Test
    public void testProfilerLeftRunningByAFailedStopIsReclaimedBeforeTheNextStart() throws Exception {
        AtomicLong nowMs = new AtomicLong(START_MS);
        ProcProfileCollector collector = new ProcProfileCollector(nowMs::get);
        AsyncProfiler profiler = Mockito.mock(AsyncProfiler.class);

        try (MockedStatic<AsyncProfiler> mockedProfiler = Mockito.mockStatic(AsyncProfiler.class)) {
            mockedProfiler.when(AsyncProfiler::getInstance).thenReturn(profiler);
            // execute("stop,file=X") throws from the JNI layer before Profiler::stop() runs when the output
            // file cannot be opened, so the session this attempt started is left running.
            when(profiler.execute(startsWith("stop,file=")))
                    .thenThrow(new IOException("cannot open output file"));

            collector.runAfterCatalogReady();

            nowMs.addAndGet(BACKOFF_MAX_MS);
            collector.runAfterCatalogReady();

            // The orphaned session must be reclaimed before a new one is started, otherwise every later
            // attempt reports "Profiler already started" against this collector's own leftover session and
            // profiling never recovers without an FE restart - the state reported in #77156.
            InOrder inOrder = Mockito.inOrder(profiler);
            inOrder.verify(profiler).execute(startsWith("start,"));
            inOrder.verify(profiler).execute(startsWith("stop,file="));
            inOrder.verify(profiler).stop();
            inOrder.verify(profiler).execute(startsWith("start,"));
        }
    }

    @Test
    public void testInterruptedCollectionDoesNotEnterBackoff() throws Exception {
        AtomicLong nowMs = new AtomicLong(START_MS);
        ProcProfileCollector collector = new ProcProfileCollector(nowMs::get);
        AsyncProfiler profiler = Mockito.mock(AsyncProfiler.class);
        // A real sleep duration, so the pending interrupt is observed by Thread.sleep() itself.
        Config.proc_profile_collect_time_s = 120;

        try (MockedStatic<AsyncProfiler> mockedProfiler = Mockito.mockStatic(AsyncProfiler.class)) {
            mockedProfiler.when(AsyncProfiler::getInstance).thenReturn(profiler);

            Thread.currentThread().interrupt();
            collector.runAfterCatalogReady();

            verify(profiler, times(1)).execute(startsWith("start,"));
            verify(profiler, never()).execute(startsWith("stop,file="));
            assertTrue(Thread.currentThread().isInterrupted(), "the interrupt flag must be preserved");

            // An interrupt means the FE is shutting down, not that profiling is broken, so it must not leave
            // a backoff window behind: the next cycle attempts a collection again.
            collector.runAfterCatalogReady();
            verify(profiler, times(2)).execute(startsWith("start,"));
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    public void testBackoffDoublesUntilItReachesTheMaximum() throws Exception {
        AtomicLong nowMs = new AtomicLong(START_MS);
        ProcProfileCollector collector = new ProcProfileCollector(nowMs::get);
        AsyncProfiler profiler = Mockito.mock(AsyncProfiler.class);

        try (MockedStatic<AsyncProfiler> mockedProfiler = Mockito.mockStatic(AsyncProfiler.class)) {
            mockedProfiler.when(AsyncProfiler::getInstance).thenReturn(profiler);
            when(profiler.execute(startsWith("start,")))
                    .thenThrow(new IllegalStateException("Perf events unavailable"));

            // BACKOFF_BASE_MS doubling into the BACKOFF_MAX_MS ceiling, which then holds: 2, 4, 8, 15, 15 min.
            long[] expectedBackoffSeconds = {120, 240, 480, 900, 900};
            int expectedStarts = 0;
            for (long backoffSeconds : expectedBackoffSeconds) {
                collector.runAfterCatalogReady();
                expectedStarts++;
                verify(profiler, times(expectedStarts)).execute(startsWith("start,"));

                // One millisecond before the window closes the attempt is still suppressed.
                nowMs.addAndGet(backoffSeconds * 1000L - 1);
                collector.runAfterCatalogReady();
                verify(profiler, times(expectedStarts)).execute(startsWith("start,"));
                nowMs.addAndGet(1);
            }
        }
    }

    @Test
    public void testSuccessfulCollectionClearsTheBackoff() throws Exception {
        AtomicLong nowMs = new AtomicLong(START_MS);
        ProcProfileCollector collector = new ProcProfileCollector(nowMs::get);
        AsyncProfiler profiler = Mockito.mock(AsyncProfiler.class);

        try (MockedStatic<AsyncProfiler> mockedProfiler = Mockito.mockStatic(AsyncProfiler.class)) {
            mockedProfiler.when(AsyncProfiler::getInstance).thenReturn(profiler);
            // Fail the first start, then recover.
            when(profiler.execute(startsWith("start,")))
                    .thenThrow(new IllegalStateException("Perf events unavailable"))
                    .thenReturn(null);
            when(profiler.execute(startsWith("stop,file=")))
                    .thenAnswer(writesProfileFile("<html>profile</html>"));

            collector.runAfterCatalogReady();
            verify(profiler, times(1)).execute(startsWith("start,"));

            nowMs.addAndGet(BACKOFF_MAX_MS);
            collector.runAfterCatalogReady();
            verify(profiler, times(2)).execute(startsWith("start,"));
            Path archive = profilePath(collector, ProcProfileFiles.CPU_FILE_NAME_PREFIX, nowMs.get(),
                    ProcProfileFiles.PUBLISHED_SUFFIX);
            assertTrue(Files.exists(archive), "recovered cycle must publish an archive: " + archive);

            // The backoff is cleared, so the very next cycle collects again without waiting.
            collector.runAfterCatalogReady();
            verify(profiler, times(3)).execute(startsWith("start,"));
        }
    }

    @Test
    public void testDisablingProfilingClearsTheBackoffSoReEnablingRetriesImmediately() throws Exception {
        AtomicLong nowMs = new AtomicLong(START_MS);
        ProcProfileCollector collector = new ProcProfileCollector(nowMs::get);
        AsyncProfiler profiler = Mockito.mock(AsyncProfiler.class);

        try (MockedStatic<AsyncProfiler> mockedProfiler = Mockito.mockStatic(AsyncProfiler.class)) {
            mockedProfiler.when(AsyncProfiler::getInstance).thenReturn(profiler);
            when(profiler.execute(startsWith("start,")))
                    .thenThrow(new IllegalStateException("Perf events unavailable"));

            collector.runAfterCatalogReady();
            verify(profiler, times(1)).execute(startsWith("start,"));

            // Toggling the feature off and on again is the only lever an operator has to force a retry after
            // fixing the root cause, so it must not be defeated by a backoff window set before the fix.
            Config.proc_profile_cpu_enable = false;
            collector.runAfterCatalogReady();
            Config.proc_profile_cpu_enable = true;
            collector.runAfterCatalogReady();

            verify(profiler, times(2)).execute(startsWith("start,"));
        }
    }
}
