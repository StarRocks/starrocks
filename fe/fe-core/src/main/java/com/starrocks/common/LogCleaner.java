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

import com.starrocks.common.util.FrontendDaemon;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * LogCleaner periodically checks disk usage and cleans log files when disk usage exceeds threshold.
 * 
 * Cleaning strategy:
 * 1. When disk usage exceeds log_cleaner_disk_usage_threshold (default 80%), start cleaning
 * 2. For each log type, sort files by modification time (oldest first)
 * 3. Delete one oldest file from each log type per iteration
 * 4. Continue until disk usage drops below log_cleaner_disk_usage_target (default 60%)
 * 5. Audit log files must retain at least log_cleaner_audit_log_min_retention_days (default 3 days)
 */
public class LogCleaner extends FrontendDaemon {
    private static final Logger LOG = LogManager.getLogger(LogCleaner.class);

    // All log file patterns that should be cleaned
    private static final String[] ALL_LOG_PATTERNS = {
            "fe.log",
            "fe.warn.log",
            "fe.audit.log",
            "fe.internal.log",
            "fe.dump.log",
            "fe.big_query.log",
            "fe.profile.log",
            "fe.features.log"
    };

    /**
     * Represents a log directory
     */
    private static class LogDirectory {
        private final String dirPath;

        public LogDirectory(String dirPath) {
            this.dirPath = dirPath;
        }

        public String getDirPath() {
            return dirPath;
        }
    }

    // Get all log directories from config, merging directories with the same path
    private LogDirectory[] getLogDirectories() {
        // Use LinkedHashSet to preserve insertion order and remove duplicates
        Set<String> uniquePaths = new LinkedHashSet<>();
        uniquePaths.add(Config.sys_log_dir);
        uniquePaths.add(Config.audit_log_dir);
        uniquePaths.add(Config.internal_log_dir);
        uniquePaths.add(Config.dump_log_dir);
        uniquePaths.add(Config.big_query_log_dir);
        uniquePaths.add(Config.profile_log_dir);
        uniquePaths.add(Config.feature_log_dir);
        
        // Convert to array of LogDirectory objects
        LogDirectory[] directories = new LogDirectory[uniquePaths.size()];
        int index = 0;
        for (String path : uniquePaths) {
            directories[index++] = new LogDirectory(path);
        }
        return directories;
    }

    public LogCleaner() {
        super("LogCleaner", Config.log_cleaner_check_interval_second * 1000L);
    }

    @Override
    protected void runAfterCatalogReady() {
        if (!Config.log_cleaner_enable) {
            return;
        }

        if (!GlobalStateMgr.getCurrentState().isLeader()) {
            return;
        }

        // Update interval from config
        setInterval(Config.log_cleaner_check_interval_second * 1000L);

        try {
            cleanLogsIfNeeded();
        } catch (Exception e) {
            LOG.warn("Error in log cleaner", e);
        }
    }

    private void cleanLogsIfNeeded() {
        LogDirectory[] logDirs = getLogDirectories();
        int totalCleanedCount = 0;

        // Check each log directory separately
        for (LogDirectory logDir : logDirs) {
            File dir = new File(logDir.getDirPath());
            if (!dir.exists() || !dir.isDirectory()) {
                LOG.debug("Log directory does not exist: {}", logDir.getDirPath());
                continue;
            }

            // Get disk usage for this specific log directory
            double diskUsagePercent = getDiskUsagePercent(dir);
            LOG.debug("Current disk usage for {}: {}%", logDir.getDirPath(), 
                    String.format("%.2f", diskUsagePercent));

            if (diskUsagePercent < Config.log_cleaner_disk_usage_threshold) {
                // Disk usage is below threshold for this directory, skip
                continue;
            }

            LOG.info("Disk usage {}% for directory {} exceeds threshold {}%, starting log cleanup",
                    String.format("%.2f", diskUsagePercent), logDir.getDirPath(), 
                    Config.log_cleaner_disk_usage_threshold);

            // Clean logs in this directory until disk usage drops below target
            int cleanedCount = cleanLogsInDirectory(logDir, dir);
            totalCleanedCount += cleanedCount;
        }

        if (totalCleanedCount > 0) {
            LOG.info("Log cleanup completed. Total deleted {} files across all directories", 
                    totalCleanedCount);
        }
    }

    private int cleanLogsInDirectory(LogDirectory logDir, File dir) {
        int cleanedCount = 0;
        int maxIterations = 1000; // Safety limit to prevent infinite loop
        int iteration = 0;
        
        double diskUsagePercent = getDiskUsagePercent(dir);
        
        while (diskUsagePercent >= Config.log_cleaner_disk_usage_target && iteration < maxIterations) {
            iteration++;
            
            // Collect all log files in this directory (scan all log types)
            List<LogFileInfo> logFiles = collectLogFilesInDirectory(dir);
            
            if (logFiles.isEmpty()) {
                LOG.debug("No log files found in directory: {}", logDir.getDirPath());
                break;
            }

            // Sort by modification time (oldest first)
            logFiles.sort(Comparator.comparingLong(LogFileInfo::getModificationTime));

            boolean deletedAny = false;

            // Delete the oldest file
            for (LogFileInfo fileInfo : logFiles) {
                File fileToDelete = fileInfo.getFile();
                
                // Verify file still exists (may have been deleted externally)
                if (!fileToDelete.exists()) {
                    continue;
                }

                // Check audit log retention policy (for any audit log file, regardless of directory)
                if (isAuditLogFile(fileToDelete.getName()) && !canDeleteAuditLog(fileInfo)) {
                    LOG.debug("Skipping audit log file {} (within retention period)",
                            fileToDelete.getName());
                    continue;
                }

                // Delete the file
                long fileSize = fileToDelete.length();
                if (fileToDelete.delete()) {
                    LOG.info("Deleted log file: {} (size: {} bytes)", 
                            fileToDelete.getAbsolutePath(), fileSize);
                    deletedAny = true;
                    cleanedCount++;
                    break; // Only delete one file per iteration
                } else {
                    LOG.warn("Failed to delete log file: {}", fileToDelete.getAbsolutePath());
                }
            }

            if (!deletedAny) {
                // No more files can be deleted (all are protected or empty)
                LOG.warn("Cannot delete more log files in directory {}. Disk usage: {}%, Target: {}%",
                        logDir.getDirPath(), String.format("%.2f", diskUsagePercent), 
                        Config.log_cleaner_disk_usage_target);
                break;
            }

            // Recheck disk usage for this directory
            diskUsagePercent = getDiskUsagePercent(dir);
            LOG.debug("Disk usage after cleanup iteration {} for directory {}: {}%", 
                    iteration, logDir.getDirPath(), String.format("%.2f", diskUsagePercent));
        }
        
        if (iteration >= maxIterations) {
            LOG.warn("Reached maximum iterations ({}) in log cleanup for directory {}", 
                    maxIterations, logDir.getDirPath());
        }

        if (cleanedCount > 0) {
            LOG.info("Log cleanup completed for directory {}. Deleted {} files. Final disk usage: {}%",
                    logDir.getDirPath(), cleanedCount, String.format("%.2f", diskUsagePercent));
        }

        return cleanedCount;
    }

    private double getDiskUsagePercent(File path) {
        try {
            long totalSpace = path.getTotalSpace();
            long usableSpace = path.getUsableSpace();
            long usedSpace = totalSpace - usableSpace;

            if (totalSpace == 0) {
                LOG.warn("Cannot get disk space information for path: {}", path.getAbsolutePath());
                return 0.0;
            }

            return (usedSpace * 100.0) / totalSpace;
        } catch (Exception e) {
            LOG.warn("Error getting disk usage for path: {}", path.getAbsolutePath(), e);
            return 0.0;
        }
    }

    private List<LogFileInfo> collectLogFilesInDirectory(File dir) {
        List<LogFileInfo> logFiles = new ArrayList<>();
        
        File[] files = dir.listFiles();
        if (files == null) {
            return logFiles;
        }

        for (File file : files) {
            if (!file.isFile()) {
                continue;
            }

            String fileName = file.getName();
            
            // Check if file matches any log pattern (scan all log types)
            for (String pattern : ALL_LOG_PATTERNS) {
                if (matchesLogPattern(fileName, pattern)) {
                    try {
                        Path filePath = Paths.get(file.getAbsolutePath());
                        BasicFileAttributes attrs = Files.readAttributes(filePath, BasicFileAttributes.class);
                        logFiles.add(new LogFileInfo(file, attrs.lastModifiedTime().toMillis()));
                    } catch (Exception e) {
                        LOG.warn("Error reading file attributes: {}", file.getAbsolutePath(), e);
                    }
                    break;
                }
            }
        }

        return logFiles;
    }

    private boolean matchesLogPattern(String fileName, String pattern) {
        // Match exact name or name with suffix (e.g., fe.log, fe.log.20240101-1, fe.log.20240101-1.gz)
        return fileName.equals(pattern) || fileName.startsWith(pattern + ".");
    }

    private boolean isAuditLogFile(String fileName) {
        return matchesLogPattern(fileName, "fe.audit.log");
    }

    private boolean canDeleteAuditLog(LogFileInfo fileInfo) {
        long fileAge = System.currentTimeMillis() - fileInfo.getModificationTime();
        long minRetentionMs = TimeUnit.DAYS.toMillis(Config.log_cleaner_audit_log_min_retention_days);
        return fileAge >= minRetentionMs;
    }

    /**
     * Represents a single log file with its metadata
     */
    private static class LogFileInfo {
        private final File file;
        private final long modificationTime;

        public LogFileInfo(File file, long modificationTime) {
            this.file = file;
            this.modificationTime = modificationTime;
        }

        public File getFile() {
            return file;
        }

        public long getModificationTime() {
            return modificationTime;
        }
    }
}

