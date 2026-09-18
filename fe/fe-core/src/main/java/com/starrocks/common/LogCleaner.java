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

import com.google.common.annotations.VisibleForTesting;
import com.starrocks.common.util.FrontendDaemon;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * LogCleaner is a daemon that runs on every FE node. It watches the disk usage of the FE log directories and deletes
 * rolled log files when the disk a directory lives on fills up.
 *
 * Every cycle (log_cleaner_check_interval_second, default 1 minute):
 * 1. Log one summary line, whether or not cleaning is enabled: for each log directory its disk usage and the file
 *    count and total size of every log type found there, and for the entries next to each log directory (for
 *    example meta/ and lib/ under STARROCKS_HOME) their total sizes and their largest first-level children.
 * 2. If log_cleaner_disk_util_based_enable is on, check each configured log directory separately. A directory whose
 *    disk usage is below log_cleaner_disk_usage_threshold (default 80%) is left alone.
 * 3. Otherwise collect every rolled log file in that directory. Only files named "&lt;type&gt;.&lt;suffix&gt;"
 *    (e.g. fe.log.20240101-1) qualify, so the file currently being written (e.g. fe.log) is never a candidate.
 *    All candidates of all types are sorted together by modification time and deleted oldest first, regardless
 *    of type. Disk usage is re-checked after each deletion and the loop stops once it drops below
 *    log_cleaner_disk_usage_target (default 60%).
 * 4. Audit log files younger than log_cleaner_audit_log_min_retention_days (default 3 days) are never deleted.
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
            "fe.features.log",
            "fe.gc.log",
            "fe.plan.log"
    };

    // Bucket in the summary for files in a log directory that match none of ALL_LOG_PATTERNS (e.g. hs_err_pid*.log)
    private static final String OTHER_LOG_TYPE = "other";

    // How many first-level children of a sibling directory the summary lists; the rest are folded into "+N more"
    @VisibleForTesting
    static final int SUMMARY_MAX_ENTRIES_PER_DIR = 10;

    // Upper bound on files visited while sizing one sibling entry, so a huge tree next to the log dir cannot stall
    // the daemon. A size that hit the bound is reported as a lower bound (">=").
    private static final int SUMMARY_MAX_FILES_PER_ENTRY = 100_000;

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
        // Update interval from config
        setInterval(Config.log_cleaner_check_interval_second * 1000L);

        try {
            LOG.info(buildSummary());
        } catch (Exception e) {
            LOG.warn("Error building log cleaner summary", e);
        }

        if (!Config.log_cleaner_disk_util_based_enable) {
            return;
        }

        try {
            cleanLogsIfNeeded();
        } catch (Exception e) {
            LOG.warn("Error in log cleaner", e);
        }
    }

    // ---------------------------------------------------------------------
    // Per-cycle summary
    // ---------------------------------------------------------------------

    /**
     * Builds the one-line summary logged at the start of every cycle. Example:
     * <pre>
     * Log cleaner summary: log dir /sr/fe/log (disk usage 71.35%, total 1.8GB) [fe.log: 4 files, 800.0MB;
     * fe.audit.log: 3 files, 1.0GB] | sibling entries of /sr/fe: meta/: 3.2GB {bdb/: 3.1GB, image/: 120.5MB},
     * lib/: 410.2MB {starrocks-fe.jar: 90.1MB, ..., +140 more: 200.7MB}, bin/: 40.3KB {...}
     * </pre>
     */
    @VisibleForTesting
    String buildSummary() {
        // Deduplicate by canonical path: ${STARROCKS_HOME}/log is often a symlink to the real log dir on a data
        // disk, and every *_log_dir left unset in fe.conf defaults to ${STARROCKS_HOME}/log.
        List<File> logDirs = new ArrayList<>();
        for (LogDirectory logDir : getLogDirectories()) {
            File dir = new File(logDir.getDirPath());
            if (!dir.isDirectory()) {
                continue;
            }
            File canonical = canonicalOrAbsolute(dir);
            if (!logDirs.contains(canonical)) {
                logDirs.add(canonical);
            }
        }
        if (logDirs.isEmpty()) {
            return "Log cleaner summary: no log directory exists";
        }

        List<String> segments = new ArrayList<>();
        for (File dir : logDirs) {
            segments.add(summarizeLogDirectory(dir));
        }

        // Everything that sits next to a log directory, e.g. meta/ and lib/ under STARROCKS_HOME.
        Set<File> parents = new LinkedHashSet<>();
        for (File dir : logDirs) {
            File parent = dir.getParentFile();
            // Never enumerate the children of the file system root: that would walk /proc, /data, ... every cycle.
            if (parent != null && parent.getParentFile() != null) {
                parents.add(parent);
            }
        }
        for (File parent : parents) {
            segments.add(summarizeSiblings(parent, logDirs));
        }

        return "Log cleaner summary: " + String.join(" | ", segments);
    }

    private String summarizeLogDirectory(File dir) {
        // log type -> {file count, total bytes}
        Map<String, long[]> statsByType = new LinkedHashMap<>();
        long totalBytes = 0;
        File[] files = dir.listFiles();
        if (files != null) {
            for (File file : files) {
                if (!file.isFile()) {
                    continue;
                }
                long[] stats = statsByType.computeIfAbsent(logTypeOf(file.getName()), k -> new long[2]);
                long length = file.length();
                stats[0]++;
                stats[1] += length;
                totalBytes += length;
            }
        }

        List<String> parts = new ArrayList<>();
        for (String type : ALL_LOG_PATTERNS) {
            appendTypeStats(parts, type, statsByType.get(type));
        }
        appendTypeStats(parts, OTHER_LOG_TYPE, statsByType.get(OTHER_LOG_TYPE));

        return "log dir " + dir.getPath()
                + " (disk usage " + String.format("%.2f", getDiskUsagePercent(dir)) + "%"
                + ", total " + humanReadableBytes(totalBytes) + ")"
                + " [" + (parts.isEmpty() ? "no files" : String.join("; ", parts)) + "]";
    }

    private static void appendTypeStats(List<String> parts, String type, long[] stats) {
        if (stats == null) {
            return;
        }
        parts.add(type + ": " + stats[0] + (stats[0] == 1 ? " file, " : " files, ") + humanReadableBytes(stats[1]));
    }

    private static File canonicalOrAbsolute(File file) {
        try {
            return file.getCanonicalFile();
        } catch (IOException e) {
            return file.getAbsoluteFile();
        }
    }

    private static String logTypeOf(String fileName) {
        for (String pattern : ALL_LOG_PATTERNS) {
            if (fileName.equals(pattern) || fileName.startsWith(pattern + ".")) {
                return pattern;
            }
        }
        return OTHER_LOG_TYPE;
    }

    private String summarizeSiblings(File parent, List<File> logDirs) {
        String header = "sibling entries of " + parent.getPath() + ": ";
        File[] children = parent.listFiles();
        if (children == null) {
            return header + "(unreadable)";
        }

        List<SizedEntry> siblings = new ArrayList<>();
        for (File child : children) {
            // logDirs holds canonical paths, so a symlink to a log dir is recognized as well
            if (logDirs.contains(canonicalOrAbsolute(child))) {
                continue;
            }
            siblings.add(SizedEntry.of(child, true));
        }
        if (siblings.isEmpty()) {
            return header + "(none)";
        }
        siblings.sort(SizedEntry.LARGEST_FIRST);

        List<String> parts = new ArrayList<>();
        for (SizedEntry sibling : siblings) {
            parts.add(sibling.describe());
        }
        return header + String.join(", ", parts);
    }

    /**
     * A file or directory next to a log directory, with its total size and (for directories) its first-level
     * children, largest first.
     */
    private static final class SizedEntry {
        private static final Comparator<SizedEntry> LARGEST_FIRST =
                Comparator.comparingLong((SizedEntry e) -> e.size.bytes).reversed()
                        .thenComparing(e -> e.name);

        private final String name;
        private final boolean isDirectory;
        private final SizeInfo size;
        // null unless this entry is a directory whose children were enumerated
        private final List<SizedEntry> children;

        private SizedEntry(String name, boolean isDirectory, SizeInfo size, List<SizedEntry> children) {
            this.name = name;
            this.isDirectory = isDirectory;
            this.size = size;
            this.children = children;
        }

        static SizedEntry of(File file, boolean enumerateChildren) {
            Path path = file.toPath();
            boolean isDirectory = Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS);
            if (!isDirectory || !enumerateChildren) {
                return new SizedEntry(file.getName(), isDirectory, sizeOf(path, SUMMARY_MAX_FILES_PER_ENTRY), null);
            }

            List<SizedEntry> children = new ArrayList<>();
            SizeInfo total = new SizeInfo();
            File[] childFiles = file.listFiles();
            if (childFiles != null) {
                for (File child : childFiles) {
                    SizedEntry entry = SizedEntry.of(child, false);
                    children.add(entry);
                    total.add(entry.size);
                }
            }
            children.sort(LARGEST_FIRST);
            return new SizedEntry(file.getName(), true, total, children);
        }

        String describe() {
            StringBuilder sb = new StringBuilder(name);
            if (isDirectory) {
                sb.append('/');
            }
            sb.append(": ").append(formatSize(size));
            if (children == null || children.isEmpty()) {
                return sb.toString();
            }

            List<String> parts = new ArrayList<>();
            int shown = Math.min(children.size(), SUMMARY_MAX_ENTRIES_PER_DIR);
            for (int i = 0; i < shown; i++) {
                parts.add(children.get(i).describe());
            }
            if (children.size() > shown) {
                SizeInfo rest = new SizeInfo();
                for (int i = shown; i < children.size(); i++) {
                    rest.add(children.get(i).size);
                }
                parts.add("+" + (children.size() - shown) + " more: " + formatSize(rest));
            }
            return sb.append(" {").append(String.join(", ", parts)).append('}').toString();
        }
    }

    /**
     * Total bytes of a file or directory tree. {@code truncated} is set when the walk stopped early because it
     * visited more than the allowed number of files, in which case {@code bytes} is a lower bound.
     */
    @VisibleForTesting
    static final class SizeInfo {
        long bytes;
        boolean truncated;

        void add(SizeInfo other) {
            bytes += other.bytes;
            truncated |= other.truncated;
        }
    }

    /**
     * Sizes a file, or a directory tree without following symbolic links. Visiting more than {@code maxFiles}
     * files stops the walk and marks the result as truncated.
     */
    @VisibleForTesting
    static SizeInfo sizeOf(Path path, int maxFiles) {
        SizeInfo info = new SizeInfo();
        try {
            BasicFileAttributes attrs = Files.readAttributes(path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
            if (!attrs.isDirectory()) {
                info.bytes = attrs.size();
                return info;
            }
            Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
                private int visited = 0;

                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes fileAttrs) {
                    if (++visited > maxFiles) {
                        info.truncated = true;
                        return FileVisitResult.TERMINATE;
                    }
                    if (fileAttrs.isRegularFile()) {
                        info.bytes += fileAttrs.size();
                    }
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFileFailed(Path file, IOException exc) {
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException | RuntimeException e) {
            LOG.debug("Error computing size of {}", path, e);
        }
        return info;
    }

    @VisibleForTesting
    static String formatSize(SizeInfo info) {
        return (info.truncated ? ">=" : "") + humanReadableBytes(info.bytes);
    }

    @VisibleForTesting
    static String humanReadableBytes(long bytes) {
        if (bytes < 1024) {
            return bytes + "B";
        }
        String[] units = {"KB", "MB", "GB", "TB", "PB"};
        double value = bytes;
        int unit = -1;
        while (value >= 1024 && unit < units.length - 1) {
            value /= 1024;
            unit++;
        }
        return String.format("%.1f%s", value, units[unit]);
    }

    // ---------------------------------------------------------------------
    // Cleaning
    // ---------------------------------------------------------------------

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

        List<LogFileInfo> logFiles = collectLogFilesInDirectory(dir);

        if (logFiles.isEmpty()) {
            LOG.debug("No log files found in directory: {}", logDir.getDirPath());
        }

        // Sort by modification time (oldest first)
        logFiles.sort(Comparator.comparingLong(LogFileInfo::getModificationTime));
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
                cleanedCount++;
            } else {
                LOG.warn("Failed to delete log file: {}", fileToDelete.getAbsolutePath());
            }

            double diskUsagePercent = getDiskUsagePercent(dir);
            if (diskUsagePercent < Config.log_cleaner_disk_usage_target) {
                break;
            }
        }

        if (cleanedCount > 0) {
            LOG.info("Log cleanup completed for directory {}. Deleted {} files. Final disk usage: {}%",
                    logDir.getDirPath(), cleanedCount, String.format("%.2f", getDiskUsagePercent(dir)));
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
        // do not remove the current writing log file.
        return fileName.startsWith(pattern + ".");
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
