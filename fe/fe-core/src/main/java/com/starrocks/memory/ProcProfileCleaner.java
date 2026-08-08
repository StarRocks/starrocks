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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Comparator;
import java.util.List;
import java.util.function.LongSupplier;

/**
 * Enforces the retention policy on the profile files that {@link ProcProfileCollector} produces under
 * {@code sys_log_dir/proc_profile}.
 *
 * <p>This runs as its own daemon rather than at the end of a collection cycle, so retention holds even while
 * collection is failing or is blocked in the middle of a several-minute capture. Its interval *is* the
 * cleanup interval, re-read every cycle, which is how the FE runs periodic cleanup elsewhere - see
 * {@link com.starrocks.common.LogCleaner}.
 */
public class ProcProfileCleaner extends FrontendDaemon {
    private static final Logger LOG = LogManager.getLogger(ProcProfileCleaner.class);
    private static final long LOG_INTERVAL = 3600 * 1000L;

    private final SimpleDateFormat profileTimeFormat = new SimpleDateFormat(ProcProfileFiles.TIME_FORMAT);
    private final String profileLogDir;
    private final LongSupplier currentTimeMillisSupplier;

    private long lastLogTime = -1;

    public ProcProfileCleaner() {
        this(System::currentTimeMillis);
    }

    ProcProfileCleaner(LongSupplier currentTimeMillisSupplier) {
        super("proc-profile-cleaner", cleanupIntervalMs());
        this.profileLogDir = Config.sys_log_dir + "/proc_profile";
        this.currentTimeMillisSupplier = currentTimeMillisSupplier;
    }

    @Override
    protected void runAfterCatalogReady() {
        setInterval(cleanupIntervalMs());
        try {
            deleteExpiredFiles();
        } catch (Exception e) {
            checkAndLog(() -> LOG.warn("delete expired proc profile files failed, reason: {}", e.getMessage()));
        }
    }

    private static long cleanupIntervalMs() {
        return Math.max(Config.proc_profile_cleanup_interval_s, 1) * 1000L;
    }

    private void deleteExpiredFiles() {
        File[] files = new File(profileLogDir).listFiles();
        if (files == null) {
            return;
        }

        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(currentTimeMillisSupplier.getAsLong());
        calendar.add(Calendar.DAY_OF_MONTH, -Config.proc_profile_file_retained_days);
        String timeToDelete = profileTimeFormat.format(calendar.getTime());

        List<ProfileFile> retainedFiles = new ArrayList<>();
        long totalSize = 0;
        for (File file : files) {
            String timePart = ProcProfileFiles.profileTimePart(file.getName());
            if (timePart == null) {
                continue;
            }

            if (timePart.compareTo(timeToDelete) < 0 && file.delete()) {
                continue;
            }

            if (!ProcProfileFiles.isPublishedArchive(file.getName())) {
                // Either an intermediate file of a collection that may still be running, or a leftover from
                // one that was killed. Not servable, so it is not part of the operator-visible size budget,
                // and the age check above is the only safe way for another thread to reclaim it.
                continue;
            }

            // Files whose delete() failed are still on disk, so they keep counting against the budget -
            // otherwise the size pass would under-count and evict live profiles to compensate.
            long fileLength = file.length();
            retainedFiles.add(new ProfileFile(file, timePart, fileLength));
            totalSize += fileLength;
        }

        if (totalSize <= Config.proc_profile_file_retained_size_bytes) {
            return;
        }

        // Evict oldest first until the total is back inside the budget.
        retainedFiles.sort(Comparator.comparing(ProfileFile::getTimePart));
        for (ProfileFile profileFile : retainedFiles) {
            if (totalSize <= Config.proc_profile_file_retained_size_bytes) {
                break;
            }
            if (profileFile.getFile().delete()) {
                totalSize -= profileFile.getLength();
            }
        }
    }

    private void checkAndLog(Runnable runnable) {
        long nowMs = currentTimeMillisSupplier.getAsLong();
        if (nowMs - lastLogTime > LOG_INTERVAL) {
            runnable.run();
            lastLogTime = nowMs;
        }
    }

    private static class ProfileFile {
        private final File file;
        private final String timePart;
        private final long length;

        private ProfileFile(File file, String timePart, long length) {
            this.file = file;
            this.timePart = timePart;
            this.length = length;
        }

        private File getFile() {
            return file;
        }

        private String getTimePart() {
            return timePart;
        }

        private long getLength() {
            return length;
        }
    }
}
