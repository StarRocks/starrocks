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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ProcProfileCleanerTest {
    private static final long START_MS = Instant.parse("2026-01-02T00:00:00Z").toEpochMilli();

    @TempDir
    private Path tempDir;

    private String oldSysLogDir;
    private int oldProcProfileFileRetainedDays;
    private long oldProcProfileFileRetainedSizeBytes;
    private long oldProcProfileCleanupIntervalS;

    @BeforeEach
    public void setUp() {
        oldSysLogDir = Config.sys_log_dir;
        oldProcProfileFileRetainedDays = Config.proc_profile_file_retained_days;
        oldProcProfileFileRetainedSizeBytes = Config.proc_profile_file_retained_size_bytes;
        oldProcProfileCleanupIntervalS = Config.proc_profile_cleanup_interval_s;

        Config.sys_log_dir = tempDir.toString();
        Config.proc_profile_file_retained_days = 1;
        Config.proc_profile_file_retained_size_bytes = Long.MAX_VALUE;
        Config.proc_profile_cleanup_interval_s = 300;
    }

    @AfterEach
    public void tearDown() {
        Config.sys_log_dir = oldSysLogDir;
        Config.proc_profile_file_retained_days = oldProcProfileFileRetainedDays;
        Config.proc_profile_file_retained_size_bytes = oldProcProfileFileRetainedSizeBytes;
        Config.proc_profile_cleanup_interval_s = oldProcProfileCleanupIntervalS;
    }

    private Path profileDir() {
        return Path.of(Config.sys_log_dir, "proc_profile");
    }

    private Path writeProfile(String name, String content) throws Exception {
        Path dir = profileDir();
        Files.createDirectories(dir);
        Path profile = dir.resolve(name);
        Files.writeString(profile, content);
        return profile;
    }

    @Test
    public void testProfilesOlderThanTheRetentionCutoffAreDeleted() throws Exception {
        // Well before the cutoff (now minus proc_profile_file_retained_days) in any time zone.
        Path expired = writeProfile("cpu-profile-20000101-000000.html.tar.gz", "expired");
        Path fresh = writeProfile("mem-profile-20260102-000000.html.tar.gz", "fresh");
        Path unrelated = writeProfile("fe.audit.log", "not a profile");

        new ProcProfileCleaner(() -> START_MS).runAfterCatalogReady();

        assertTrue(Files.notExists(expired), "expired profile must be deleted: " + expired);
        assertTrue(Files.exists(fresh), "profile inside the retention window must be kept: " + fresh);
        assertTrue(Files.exists(unrelated), "non-profile files must never be touched: " + unrelated);
    }

    @Test
    public void testOldestProfilesAreEvictedWhenTheRetainedSizeIsExceeded() throws Exception {
        Config.proc_profile_file_retained_size_bytes = 15;

        Path oldest = writeProfile("cpu-profile-20260102-000001.html.tar.gz", "0123456789");
        Path middle = writeProfile("cpu-profile-20260102-000002.html.tar.gz", "0123456789");
        Path newest = writeProfile("mem-profile-20260102-000003.html.tar.gz", "0123456789");

        new ProcProfileCleaner(() -> START_MS).runAfterCatalogReady();

        assertTrue(Files.notExists(oldest), "oldest profile must be evicted first: " + oldest);
        assertTrue(Files.notExists(middle), "eviction must continue until under the budget: " + middle);
        assertTrue(Files.exists(newest), "newest profile must be retained: " + newest);
    }

    @Test
    public void testInProgressFilesAreNeitherServedNorEvictedBySizeBudget() throws Exception {
        // A collection that is still running owns these two names. The cleaner runs on its own thread, so
        // evicting them would corrupt an in-flight capture; they also are not downloadable, so they do not
        // belong to the operator-visible size budget.
        Config.proc_profile_file_retained_size_bytes = 5;

        Path rawProfile = writeProfile("cpu-profile-20260102-000001.html", "0123456789");
        Path halfArchive = writeProfile("cpu-profile-20260102-000001.html.tar.gz.tmp", "0123456789");
        Path published = writeProfile("cpu-profile-20260102-000002.html.tar.gz", "0123456789");

        new ProcProfileCleaner(() -> START_MS).runAfterCatalogReady();

        assertTrue(Files.exists(rawProfile),
                "profile still being written must not be evicted by the size budget: " + rawProfile);
        assertTrue(Files.exists(halfArchive),
                "half-written archive must not be evicted by the size budget: " + halfArchive);
        assertTrue(Files.notExists(published), "the published archive is over budget and must go: " + published);
        // Guards the naming contract the HTTP endpoints rely on: a temp name must never look publishable.
        assertTrue(!ProcProfileFiles.isPublishedArchive(halfArchive.getFileName().toString()),
                "a half-written archive must not match the published suffix");
    }

    @Test
    public void testStaleInProgressFilesAreStillReapedByAge() throws Exception {
        // Left behind by an FE that was killed mid-collection. Old enough that no collection can own it.
        Path staleDump = writeProfile("cpu-profile-20000101-000000.html", "orphan");
        Path staleArchive = writeProfile("mem-profile-20000101-000000.html.tar.gz.tmp", "orphan");

        new ProcProfileCleaner(() -> START_MS).runAfterCatalogReady();

        assertTrue(Files.notExists(staleDump), "stale raw profile must be reclaimed: " + staleDump);
        assertTrue(Files.notExists(staleArchive), "stale archive must be reclaimed: " + staleArchive);
    }

    @Test
    public void testMissingProfileDirectoryIsNotAnError() {
        // The collector creates the directory; the cleaner may well run first.
        assertTrue(Files.notExists(profileDir()), "precondition: the directory does not exist yet");
        new ProcProfileCleaner(() -> START_MS).runAfterCatalogReady();
    }

    @Test
    public void testIntervalTracksTheConfigAndIsNeverZero() {
        Config.proc_profile_cleanup_interval_s = 42;
        ProcProfileCleaner cleaner = new ProcProfileCleaner(() -> START_MS);
        assertEquals(42_000L, cleaner.getInterval(), "the daemon interval is the cleanup interval");

        // A zero or negative interval would turn the daemon into a busy loop over the directory.
        Config.proc_profile_cleanup_interval_s = 0;
        cleaner.runAfterCatalogReady();
        assertEquals(1_000L, cleaner.getInterval(), "a non-positive configured interval is clamped to 1s");
    }
}
