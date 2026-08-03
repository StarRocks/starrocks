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
import com.starrocks.common.util.FrontendDaemon;
import one.profiler.AsyncProfiler;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

public class ProcProfileCollector extends FrontendDaemon {
    private static final Logger LOG = LogManager.getLogger(ProcProfileCollector.class);
    private static final long LOG_INTERVAL = 3600 * 1000L;
    private static final long BACKOFF_BASE_MS = 120 * 1000L;
    private static final long BACKOFF_MAX_MS = 900 * 1000L;
    private static final int MAX_BACKOFF_DOUBLINGS = 10;

    private final SimpleDateFormat profileTimeFormat = new SimpleDateFormat(ProcProfileFiles.TIME_FORMAT);
    private final String profileLogDir;
    private final LongSupplier currentTimeMillisSupplier;

    // CPU and memory profiling fail for independent reasons - 'cpu' needs perf_events, which containers
    // often deny, while 'alloc' sampling works almost everywhere - so each carries its own retry state and
    // neither may gate the other.
    private final ProfileTarget cpuProfile =
            new ProfileTarget(ProcProfileFiles.CPU_FILE_NAME_PREFIX, "cpu", "cpu");
    private final ProfileTarget memProfile =
            new ProfileTarget(ProcProfileFiles.MEM_FILE_NAME_PREFIX, "alloc,alloc=2m", "memory");

    private long lastLogTime = -1;
    private boolean profilerStopPending = false;

    public ProcProfileCollector() {
        this(System::currentTimeMillis);
    }

    ProcProfileCollector(LongSupplier currentTimeMillisSupplier) {
        super("proc-profile-collector", 1000L);
        profileLogDir = Config.sys_log_dir + "/proc_profile";
        this.currentTimeMillisSupplier = currentTimeMillisSupplier;
    }

    @Override
    protected void runAfterCatalogReady() {
        File file = new File(profileLogDir);
        file.mkdirs();
        prepareLibrary();

        long nowMs = currentTimeMillis();
        collectIfEnabledAndDue(cpuProfile, Config.proc_profile_cpu_enable, nowMs);
        collectIfEnabledAndDue(memProfile, Config.proc_profile_mem_enable, nowMs);
    }

    private void collectIfEnabledAndDue(ProfileTarget target, boolean enabled, long nowMs) {
        if (!enabled) {
            // Toggling a profile off and on again is the only lever an operator has to force a retry after
            // fixing the root cause, so a disabled profile must not keep its backoff. See
            // AutovacuumDaemon#nextCollectTimeMs, which resets on the corresponding teardown.
            clearProfileBackoff(target);
            return;
        }
        if (nowMs < target.nextCollectTimeMs) {
            return;
        }

        CollectionResult result;
        try {
            result = collectProfile(target);
        } catch (Throwable t) {
            // Profiling is a best-effort diagnostic facility, so nothing it throws may escape to
            // Daemon.run(): that would retry on the next interval and skip the cleanup below.
            // Errors are possible here, not just exceptions - see collectProfile().
            result = CollectionResult.START_FAILED;
            checkAndLog(() -> LOG.warn("collect {} profile failed, reason: {}", target.displayName, t.toString()));
        }

        if (result == CollectionResult.SUCCESS) {
            clearProfileBackoff(target);
        } else if (result != CollectionResult.INTERRUPTED) {
            // An interrupt means the FE is shutting down, not that profiling is broken: counting it
            // would leave a stale failure count and a backoff window behind for the next startup.
            scheduleProfileBackoff(target, result);
        }
    }

    public String getProfileLogDir() {
        return profileLogDir;
    }

    // AsyncProfiler depends on the native library libasyncProfiler.so, which is bundled inside the JAR.
    // By default, it extracts this library to the /tmp directory in order to load it.
    // However, if /tmp is mounted with the "noexec" option, loading the library will fail.
    // To avoid this issue, we explicitly set 'one.profiler.extractPath' to a directory with execute permissions.
    // See prepareLibrary() for how the extraction path is set to a safer default under STARROCKS_HOME.
    // See https://github.com/StarRocks/starrocks/issues/64502
    private void prepareLibrary() {
        final String libPathProperty = "one.profiler.extractPath";
        String value = System.getProperty(libPathProperty);
        if (StringUtils.isEmpty(value)) {
            String dir = Config.STARROCKS_HOME_DIR + "/bin/";
            if (StringUtils.isNotEmpty(Config.STARROCKS_HOME_DIR) && new File(dir).exists()) {
                System.setProperty(libPathProperty, dir);
                LOG.info("change the system property {} to {}", libPathProperty, dir);
            }
        }
    }

    // No 'file=' is passed here because the profile is written by an explicit 'dump' command below, and no
    // 'timeout=' either: on expiry async-profiler stops the profiler and only writes a file when 'file=' was
    // given, so a timeout would silently discard the whole collection. The unconditional stop in the finally
    // block of collectProfile(), together with reclaimProfilerIfStopPending(), is the watchdog instead.
    private static String genStartCommand(String event, int jstackDepth) {
        return String.format("start,quiet,event=%s,loglevel=error,cstack=vm,jstackdepth=%d",
                event, jstackDepth);
    }

    private CollectionResult collectProfile(ProfileTarget target) {
        String event = target.event;
        String profileName = target.displayName;
        AsyncProfiler profiler;
        try {
            profiler = AsyncProfiler.getInstance();
        } catch (Throwable t) {
            // getInstance() loads the bundled libasyncProfiler.so and rethrows the UnsatisfiedLinkError -
            // an Error, not an Exception - when the library cannot be loaded, which is exactly what happens
            // when the extraction directory is mounted noexec (see the comment on prepareLibrary()).
            // It also caches nothing on failure, so every retry re-extracts the library.
            logCollectionFailure(profileName, CollectionResult.START_FAILED, t);
            return CollectionResult.START_FAILED;
        }

        reclaimProfilerIfStopPending(profiler);

        String fileName = target.fileNamePrefix + currentTimeString() + ".html";
        File rawTempFile = new File(profileLogDir + "/" + fileName + ".tmp.html");
        CollectionResult result = CollectionResult.SUCCESS;
        Exception failure = null;
        boolean startedByThisAttempt = false;
        boolean dumpCommandSucceeded = false;

        try {
            profiler.execute(genStartCommand(event, Config.proc_profile_jstack_depth));
            startedByThisAttempt = true;
            Thread.sleep(Config.proc_profile_collect_time_s * 1000L);
            profiler.execute("dump,file=" + rawTempFile.getPath());
            dumpCommandSucceeded = true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            result = CollectionResult.INTERRUPTED;
            failure = e;
        } catch (Exception e) {
            if (!startedByThisAttempt && isProfilerAlreadyStarted(e)) {
                result = CollectionResult.BUSY;
            } else if (!startedByThisAttempt) {
                result = CollectionResult.START_FAILED;
            } else {
                result = CollectionResult.COLLECTION_FAILED;
            }
            failure = e;
        } finally {
            if (startedByThisAttempt) {
                try {
                    profiler.stop();
                } catch (Exception e) {
                    // The profiler this attempt started may still be running and still sampling. Remember
                    // that, so the next attempt reclaims it instead of reporting BUSY against itself forever.
                    // The returned result deliberately describes only the collection: an untidy teardown must
                    // not be reported as "no profile was produced", or every successful cycle whose stop()
                    // fails would grow the backoff until profiling stops for good.
                    profilerStopPending = true;
                    recordStopFailure(e, failure);
                }
            }
        }

        if (dumpCommandSucceeded && !isValidProfileFile(rawTempFile)) {
            result = CollectionResult.DUMP_FAILED;
        }

        if (result != CollectionResult.SUCCESS) {
            deleteQuietly(rawTempFile);
            logCollectionFailure(profileName, result, failure);
            return result;
        }

        try {
            compressFile(rawTempFile, fileName, fileName);
        } catch (IOException e) {
            checkAndLog(() -> LOG.warn("compress {} profile file {} failed, reason: {}",
                    profileName, fileName, e.getMessage()));
            deleteQuietly(rawTempFile);
            return CollectionResult.COLLECTION_FAILED;
        }

        return CollectionResult.SUCCESS;
    }

    private void compressFile(File sourceFile, String archiveEntryName, String targetFileName) throws IOException {
        File targetFile = new File(profileLogDir + "/" + targetFileName + ".tar.gz");
        // Write to a temporary file first and atomically rename it to the final name on success,
        // so consumers (e.g. /proc_profile listing/download endpoints) never observe a
        // partially-written .tar.gz file.
        File tmpFile = new File(profileLogDir + "/" + targetFileName + ".tar.gz.tmp");
        try {
            try (FileOutputStream fileOutputStream = new FileOutputStream(tmpFile);
                    GzipCompressorOutputStream gzipOutputStream = new GzipCompressorOutputStream(fileOutputStream);
                    TarArchiveOutputStream tarArchive = new TarArchiveOutputStream(gzipOutputStream);
                    FileInputStream fileInputStream = new FileInputStream(sourceFile)) {
                TarArchiveEntry tarEntry = new TarArchiveEntry(sourceFile, archiveEntryName);
                tarArchive.putArchiveEntry(tarEntry);

                byte[] buffer = new byte[1024];
                int len;
                while ((len = fileInputStream.read(buffer)) > 0) {
                    tarArchive.write(buffer, 0, len);
                }
                tarArchive.closeArchiveEntry();
                tarArchive.finish();
            }

            try {
                Files.move(tmpFile.toPath(), targetFile.toPath(),
                        StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException e) {
                Files.move(tmpFile.toPath(), targetFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
            }
        } finally {
            if (tmpFile.exists()) {
                tmpFile.delete();
            }
        }

        sourceFile.delete();
    }

    private String currentTimeString() {
        return profileTimeFormat.format(new Date(currentTimeMillis()));
    }

    private void checkAndLog(Runnable runnable) {
        if (currentTimeMillis() - lastLogTime > LOG_INTERVAL) {
            runnable.run();
            lastLogTime = currentTimeMillis();
        }
    }

    private long currentTimeMillis() {
        return currentTimeMillisSupplier.getAsLong();
    }

    private void clearProfileBackoff(ProfileTarget target) {
        if (target.consecutiveFailures > 0) {
            LOG.info("{} profile collection recovered after {} consecutive failures",
                    target.displayName, target.consecutiveFailures);
        }
        target.consecutiveFailures = 0;
        target.nextCollectTimeMs = 0;
    }

    private void scheduleProfileBackoff(ProfileTarget target, CollectionResult result) {
        target.consecutiveFailures++;
        long backoffMs = profileFailureBackoffMs(target.consecutiveFailures);
        target.nextCollectTimeMs = currentTimeMillis() + backoffMs;

        // Log the transitions, not the steady state: entering backoff once, then at most one reminder per
        // LOG_INTERVAL. Recovery is logged by clearProfileBackoff().
        if (target.consecutiveFailures == 1) {
            LOG.warn("{} profile collection failed ({}), backing off, next attempt in {}s",
                    target.displayName, result, TimeUnit.MILLISECONDS.toSeconds(backoffMs));
        } else {
            checkAndLog(() -> LOG.warn("{} profile collection still failing ({}), consecutive_failures={}",
                    target.displayName, result, target.consecutiveFailures));
        }
    }

    // Bounded exponential backoff, 2 to 15 minutes. Retry policy for a debug facility is not something an
    // operator needs to tune, so it lives here rather than in Config - the same call
    // MVActiveChecker.MvActiveInfo makes for the same problem (MAX_BACKOFF_MINUTES / BACKOFF_BASE).
    private static long profileFailureBackoffMs(int consecutiveFailures) {
        long multiplier = 1L << Math.min(consecutiveFailures - 1, MAX_BACKOFF_DOUBLINGS);
        return Math.min(BACKOFF_BASE_MS * multiplier, BACKOFF_MAX_MS);
    }

    private boolean isValidProfileFile(File file) {
        return file.isFile() && file.length() > 0;
    }

    private boolean isProfilerAlreadyStarted(Exception e) {
        return e.getMessage() != null && e.getMessage().contains("Profiler already started");
    }

    // A previous attempt started the profiler but failed to stop it, so it may still be running and burning
    // sampling overhead on the FE. Reclaim it once. Only a session this collector started is ever stopped
    // here: a profiler owned by something else (a manual jcmd/agentpath session) must not be torn down, and
    // shows up as BUSY instead.
    private void reclaimProfilerIfStopPending(AsyncProfiler profiler) {
        if (!profilerStopPending) {
            return;
        }
        // Clear first: if this stop also fails there is nothing further this collector can do, and retrying
        // every cycle would only add noise. The start below then reports BUSY, which is the honest signal.
        profilerStopPending = false;
        try {
            profiler.stop();
            LOG.info("stopped the proc profiler left running by a previous failed stop");
        } catch (Exception e) {
            checkAndLog(() -> LOG.warn("could not reclaim the proc profiler, reason: {}", e.getMessage()));
        }
    }

    private void recordStopFailure(Exception stopError, Exception collectionFailure) {
        if (collectionFailure != null) {
            collectionFailure.addSuppressed(stopError);
        }
        checkAndLog(() -> LOG.warn("stop proc profiler failed, reason: {}", stopError.getMessage()));
    }

    private void logCollectionFailure(String profileName, CollectionResult result, Throwable failure) {
        if (result == CollectionResult.SUCCESS || result == CollectionResult.INTERRUPTED) {
            // An interrupt is a normal part of FE shutdown, so it must not warn on the way down.
            return;
        }
        String message = failure == null ? result.name() : failure.toString();
        checkAndLog(() -> LOG.warn("collect {} profile failed, result: {}, reason: {}",
                profileName, result, message));
    }

    private void deleteQuietly(File file) {
        if (file.exists()) {
            file.delete();
        }
    }

    // One profile the collector produces: which async-profiler event to record, which file-name prefix its
    // profiles carry, and the failure backoff for that event alone. Kept mutable and per-instance so that a
    // permanently broken event cannot throttle a healthy one.
    private static class ProfileTarget {
        private final String fileNamePrefix;
        private final String event;
        private final String displayName;

        private long nextCollectTimeMs = 0;
        private int consecutiveFailures = 0;

        private ProfileTarget(String fileNamePrefix, String event, String displayName) {
            this.fileNamePrefix = fileNamePrefix;
            this.event = event;
            this.displayName = displayName;
        }
    }

    private enum CollectionResult {
        SUCCESS,
        BUSY,
        START_FAILED,
        COLLECTION_FAILED,
        DUMP_FAILED,
        INTERRUPTED
    }
}
