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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LogCleanerTest {
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
        testLogDir = Files.createTempDirectory("log_cleaner_test").toFile();
        Config.sys_log_dir = testLogDir.getAbsolutePath();
        Config.audit_log_dir = testLogDir.getAbsolutePath();
        Config.internal_log_dir = testLogDir.getAbsolutePath();
        Config.dump_log_dir = testLogDir.getAbsolutePath();
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
        if (testLogDir != null && testLogDir.exists()) {
            deleteDirectory(testLogDir);
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

    private void createLogFile(String fileName, long size) throws IOException {
        File file = new File(testLogDir, fileName);
        try (java.io.FileWriter writer = new java.io.FileWriter(file)) {
            for (long i = 0; i < size; i++) {
                writer.write('a');
            }
        }
    }

    private void createLogFileWithTime(String fileName, long size, long modificationTime) throws IOException {
        File file = new File(testLogDir, fileName);
        try (java.io.FileWriter writer = new java.io.FileWriter(file)) {
            for (long i = 0; i < size; i++) {
                writer.write('a');
            }
        }
        // Set modification time
        Files.setLastModifiedTime(Paths.get(file.getAbsolutePath()), FileTime.fromMillis(modificationTime));
    }
}
