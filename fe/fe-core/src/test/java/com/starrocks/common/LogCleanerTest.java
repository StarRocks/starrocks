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

package com.starrocks.common;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LogCleanerTest {
    // testRoot plays the role of STARROCKS_HOME: the log dir lives under it and
    // any sibling directories created by a test live next to the log dir.
    private File testRoot;
    private File testLogDir;
    private boolean originalLogCleanerEnable;
    private int originalThreshold;
    private int originalTarget;
    private int originalRetentionDays;
    private int originalCheckInterval;

    @BeforeEach
    public void setUp() throws IOException {
        // Save original config values
        originalLogCleanerEnable = Config.log_cleaner_disk_util_based_enable;
        originalThreshold = Config.log_cleaner_disk_usage_threshold;
        originalTarget = Config.log_cleaner_disk_usage_target;
        originalRetentionDays = Config.log_cleaner_audit_log_min_retention_days;
        originalCheckInterval = Config.log_cleaner_check_interval_second;

        // Create temporary test directory
        testRoot = Files.createTempDirectory("log_cleaner_test").toFile();
        testLogDir = new File(testRoot, "log");
        assertTrue(testLogDir.mkdir());
        Config.sys_log_dir = testLogDir.getAbsolutePath();
        Config.audit_log_dir = testLogDir.getAbsolutePath();
        Config.internal_log_dir = testLogDir.getAbsolutePath();
        Config.big_query_log_dir = testLogDir.getAbsolutePath();
        Config.profile_log_dir = testLogDir.getAbsolutePath();
        Config.feature_log_dir = testLogDir.getAbsolutePath();

        // Enable log cleaner for tests
        Config.log_cleaner_disk_util_based_enable = true;
        Config.log_cleaner_disk_usage_threshold = 0;
        Config.log_cleaner_disk_usage_target = 0;
        Config.log_cleaner_audit_log_min_retention_days = 3;
        Config.log_cleaner_check_interval_second = 300;
    }

    @AfterEach
    public void tearDown() throws IOException {
        // Restore original config values
        Config.log_cleaner_disk_util_based_enable = originalLogCleanerEnable;
        Config.log_cleaner_disk_usage_threshold = originalThreshold;
        Config.log_cleaner_disk_usage_target = originalTarget;
        Config.log_cleaner_audit_log_min_retention_days = originalRetentionDays;
        Config.log_cleaner_check_interval_second = originalCheckInterval;

        // Clean up test directory
        if (testRoot != null && testRoot.exists()) {
            deleteDirectory(testRoot);
        }
    }

    private void deleteDirectory(File directory) throws IOException {
        if (directory.exists()) {
            Files.walk(Paths.get(directory.getAbsolutePath()))
                    .sorted((a, b) -> b.compareTo(a))
                    .forEach(path -> {
                        try {
                            Files.delete(path);
                        } catch (IOException e) {
                            // Ignore
                        }
                    });
        }
    }

    @Test
    public void testLogCleanerDisabled() throws IOException {
        createLogFile("fe.log.20240101-1", 1000);
        createLogFile("fe.audit.log.20240101-1", 1000);
        Config.log_cleaner_disk_util_based_enable = false;

        LogCleaner cleaner = new LogCleaner();
        cleaner.runAfterCatalogReady();

        // Files should not be deleted when cleaner is disabled
        assertTrue(new File(testLogDir, "fe.log.20240101-1").exists());
        assertTrue(new File(testLogDir, "fe.audit.log.20240101-1").exists());
    }

    @Test
    public void testLogCleanerWithLowDiskUsage() throws IOException {
        // Create some log files
        createLogFile("fe.log.20240101-1", 1000);
        createLogFile("fe.audit.log.20240101-1", 1000);

        // Set high threshold so current disk usage is below it
        Config.log_cleaner_disk_usage_threshold = 99;

        LogCleaner cleaner = new LogCleaner();
        cleaner.runAfterCatalogReady();

        // Files should not be deleted when disk usage is below threshold
        assertTrue(new File(testLogDir, "fe.log.20240101-1").exists());
        assertTrue(new File(testLogDir, "fe.audit.log.20240101-1").exists());
    }

    @Test
    public void testLogCleanerDeletesOldFiles() throws IOException {
        // Create old log files (older than retention period)
        long oldTime = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(10);
        createLogFileWithTime("fe.log.20240101-1", 1000, oldTime);
        createLogFileWithTime("fe.log.20240102-1", 1000, oldTime);
        createLogFileWithTime("fe.dump.log.20240101-1", 1000, oldTime);

        LogCleaner cleaner = new LogCleaner();
        cleaner.runAfterCatalogReady();

        assertFalse(new File(testLogDir, "fe.log.20240101-1").exists());
        assertFalse(new File(testLogDir, "fe.log.20240102-1").exists());
        assertFalse(new File(testLogDir, "fe.dump.log.20240101-1").exists());
    }

    @Test
    public void testAuditLogRetention() throws IOException {
        // Create recent audit log file (within retention period)
        long recentTime = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(1);
        createLogFileWithTime("fe.audit.log.20240101-1", 1000, recentTime);

        // Create old audit log file (beyond retention period)
        long oldTime = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(5);
        createLogFileWithTime("fe.audit.log.20240102-1", 1000, oldTime);

        LogCleaner cleaner = new LogCleaner();
        cleaner.runAfterCatalogReady();

        // Recent audit log should still exist (protected)
        File recentFile = new File(testLogDir, "fe.audit.log.20240101-1");
        // Old audit log may be deleted if disk usage is high
        File oldFile = new File(testLogDir, "fe.audit.log.20240102-1");

        assertTrue(recentFile.exists());
        assertFalse(oldFile.exists());
    }

    @Test
    public void testLogFilePatternMatching() throws IOException {
        // Create files with different patterns
        createLogFile("fe.log.20240101-1", 1000);
        createLogFile("fe.audit.log.20240101-1", 1000);
        createLogFile("fe.dump.log.20240101-1", 1000);
        createLogFile("fe.big_query.log.20240101-1", 1000);
        createLogFile("fe.profile.log.20240101-1", 1000);
        createLogFile("fe.features.log.20240101-1", 1000);
        createLogFile("fe.internal.log.20240101-1", 1000);
        createLogFile("fe.warn.log.20240101-1", 1000);
        // Non-log file should not be matched
        createLogFile("other.log.20240101-1", 1000);

        LogCleaner cleaner = new LogCleaner();
        cleaner.runAfterCatalogReady();

        assertFalse(new File(testLogDir, "fe.log.20240101-1").exists());
        assertFalse(new File(testLogDir, "fe.dump.log.20240101-1").exists());
        assertFalse(new File(testLogDir, "fe.big_query.log.20240101-1").exists());
        assertFalse(new File(testLogDir, "fe.profile.log.20240101-1").exists());
        assertFalse(new File(testLogDir, "fe.features.log.20240101-1").exists());
        assertFalse(new File(testLogDir, "fe.internal.log.20240101-1").exists());
        assertFalse(new File(testLogDir, "fe.warn.log.20240101-1").exists());

        assertTrue(new File(testLogDir, "other.log.20240101-1").exists());
    }

    @Test
    public void testNonExistentDirectory() {
        Config.sys_log_dir = "/non/existent/directory";

        LogCleaner cleaner = new LogCleaner();
        // Should not throw exception
        cleaner.runAfterCatalogReady();
    }

    @Test
    public void testEmptyDirectory() {
        LogCleaner cleaner = new LogCleaner();
        // Should not throw exception with empty directory
        cleaner.runAfterCatalogReady();
    }

    @Test
    public void testCurrentLogFileNotDeleted() throws IOException {
        // Create current log file (without suffix, should not be deleted)
        createLogFile("fe.log", 1000);
        createLogFile("fe.log.20240101-1", 1000);

        LogCleaner cleaner = new LogCleaner();
        cleaner.runAfterCatalogReady();

        // Current log file should not be deleted (doesn't match pattern)
        assertTrue(new File(testLogDir, "fe.log").exists());
        // Old log file should be deleted
        assertFalse(new File(testLogDir, "fe.log.20240101-1").exists());
    }

    // ---------------------------------------------------------------------
    // Per-cycle summary log
    // ---------------------------------------------------------------------

    @Test
    public void testSummaryCountsCurrentAndRolledFilesPerType() throws IOException {
        createLogFile("fe.log", 100);
        createLogFile("fe.log.20240101-1", 2048);
        createLogFile("fe.audit.log.20240101-1", 1024 * 1024);

        String summary = new LogCleaner().buildSummary();

        assertTrue(summary.contains("log dir " + testLogDir.getCanonicalPath() + " (disk usage "), summary);
        assertTrue(summary.contains("fe.log: 2 files, 2.1KB"), summary);
        assertTrue(summary.contains("fe.audit.log: 1 file, 1.0MB"), summary);
    }

    @Test
    public void testSummaryReportsDirectoryTotalIncludingUnmatchedFiles() throws IOException {
        createLogFile("fe.log", 1024);
        createLogFile("hs_err_pid123.log", 1024);

        String summary = new LogCleaner().buildSummary();

        assertTrue(summary.contains("other: 1 file, 1.0KB"), summary);
        assertTrue(summary.contains("total 2.0KB"), summary);
    }

    @Test
    public void testSummaryOmitsLogTypesWithoutFiles() throws IOException {
        createLogFile("fe.log", 100);

        String summary = new LogCleaner().buildSummary();

        assertTrue(summary.contains("fe.log: 1 file"), summary);
        assertFalse(summary.contains("fe.warn.log"), summary);
        assertFalse(summary.contains("fe.audit.log"), summary);
        assertFalse(summary.contains("other:"), summary);
    }

    @Test
    public void testSummaryListsSiblingEntriesWithRecursiveSizes() throws IOException {
        File meta = new File(testRoot, "meta");
        File bdb = new File(meta, "bdb");
        assertTrue(bdb.mkdirs());
        createFile(bdb, "00000001.jdb", 1024);
        createFile(bdb, "00000002.jdb", 1024);
        createFile(meta, "image", 512);
        createFile(testRoot, "heap.hprof", 3000);

        String summary = new LogCleaner().buildSummary();

        assertTrue(summary.contains("sibling entries of " + testRoot.getCanonicalPath() + ":"), summary);
        assertTrue(summary.contains("meta/: 2.5KB {bdb/: 2.0KB, image: 512B}"), summary);
        assertTrue(summary.contains("heap.hprof: 2.9KB"), summary);
    }

    @Test
    public void testSummaryDoesNotListLogDirectoriesAsSiblings() throws IOException {
        File audit = new File(testRoot, "audit");
        assertTrue(audit.mkdir());
        createFile(audit, "fe.audit.log", 1024);
        Config.audit_log_dir = audit.getAbsolutePath();

        String summary = new LogCleaner().buildSummary();

        assertTrue(summary.contains("log dir " + audit.getCanonicalPath() + " (disk usage "), summary);
        assertFalse(summary.contains("audit/: "), summary);
    }

    @Test
    public void testSummaryReportsSymlinkedLogDirOnce() throws IOException {
        // ${STARROCKS_HOME}/log is commonly a symlink to the real log dir on a data disk, and log dirs that are
        // left unset in fe.conf default to ${STARROCKS_HOME}/log. The same directory must not be summarized twice.
        File link = new File(testRoot, "loglink");
        Files.createSymbolicLink(link.toPath(), testLogDir.toPath());
        createLogFile("fe.log", 1024);
        Config.audit_log_dir = link.getAbsolutePath();

        String summary = new LogCleaner().buildSummary();

        assertEquals(1, countOccurrences(summary, "log dir "), summary);
        assertTrue(summary.contains("log dir " + testLogDir.getCanonicalPath() + " (disk usage "), summary);
        assertFalse(summary.contains("loglink"), summary);
    }

    private static int countOccurrences(String text, String needle) {
        int count = 0;
        for (int from = text.indexOf(needle); from >= 0; from = text.indexOf(needle, from + needle.length())) {
            count++;
        }
        return count;
    }

    @Test
    public void testSummaryCapsEntriesListedPerSiblingDirectory() throws IOException {
        File lib = new File(testRoot, "lib");
        assertTrue(lib.mkdir());
        int total = LogCleaner.SUMMARY_MAX_ENTRIES_PER_DIR + 2;
        for (int i = 1; i <= total; i++) {
            // sizes are distinct so the two smallest (1KB and 2KB) are the ones folded away
            createFile(lib, "lib-" + i + ".jar", 1024L * i);
        }

        String summary = new LogCleaner().buildSummary();

        assertTrue(summary.contains("lib-" + total + ".jar: "), summary);
        assertTrue(summary.contains("lib-3.jar: 3.0KB"), summary);
        assertFalse(summary.contains("lib-1.jar"), summary);
        assertFalse(summary.contains("lib-2.jar"), summary);
        assertTrue(summary.contains("+2 more: 3.0KB"), summary);
    }

    @Test
    public void testSummarySkipsSiblingsWhenLogDirIsUnderFileSystemRoot() {
        // /usr's parent is the file system root (and unlike /tmp it is not a symlink on macOS); walking every
        // top-level directory of the root file system is never acceptable
        Config.sys_log_dir = "/usr";

        String summary = new LogCleaner().buildSummary();

        assertTrue(summary.contains("log dir /usr (disk usage "), summary);
        assertFalse(summary.contains("sibling entries of /:"), summary);
    }

    @Test
    public void testDirectorySizeWalkStopsAtFileBudget() throws IOException {
        File dir = new File(testRoot, "big");
        assertTrue(dir.mkdir());
        createFile(dir, "a", 100);
        createFile(dir, "b", 100);
        createFile(dir, "c", 100);

        LogCleaner.SizeInfo exact = LogCleaner.sizeOf(dir.toPath(), 10);
        assertFalse(exact.truncated);
        assertEquals(300, exact.bytes);

        LogCleaner.SizeInfo capped = LogCleaner.sizeOf(dir.toPath(), 2);
        assertTrue(capped.truncated);
        assertTrue(LogCleaner.formatSize(capped).startsWith(">="), LogCleaner.formatSize(capped));
    }

    @Test
    public void testHumanReadableBytes() {
        assertEquals("0B", LogCleaner.humanReadableBytes(0));
        assertEquals("1023B", LogCleaner.humanReadableBytes(1023));
        assertEquals("1.0KB", LogCleaner.humanReadableBytes(1024));
        assertEquals("1.5KB", LogCleaner.humanReadableBytes(1536));
        assertEquals("1.0MB", LogCleaner.humanReadableBytes(1024L * 1024));
        assertEquals("2.5GB", LogCleaner.humanReadableBytes(5L * 1024 * 1024 * 1024 / 2));
        assertEquals("1.0TB", LogCleaner.humanReadableBytes(1024L * 1024 * 1024 * 1024));
    }

    private void createLogFile(String fileName, long size) throws IOException {
        createFile(testLogDir, fileName, size);
    }

    private void createFile(File dir, String fileName, long size) throws IOException {
        File file = new File(dir, fileName);
        try (java.io.FileWriter writer = new java.io.FileWriter(file)) {
            for (long i = 0; i < size; i++) {
                writer.write('a');
            }
        }
    }

    private void createLogFileWithTime(String fileName, long size, long modificationTime) throws IOException {
        createLogFile(fileName, size);
        // Set modification time
        Files.setLastModifiedTime(Paths.get(new File(testLogDir, fileName).getAbsolutePath()),
                FileTime.fromMillis(modificationTime));
    }
}
