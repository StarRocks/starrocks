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
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProcProfileCollectorTest {

    @ParameterizedTest
    @CsvSource({"0,0", "0,115680513", "115680513,0", "115680513,115680513"})
    void testCompressFilePreservesOwnerIds(long userId, long groupId, @TempDir Path directory) throws Exception {
        String originalLogDir = Config.sys_log_dir;
        try {
            Config.sys_log_dir = directory.toString();
            ProcProfileCollector collector = new ProcProfileCollector();
            Path profileDir = Files.createDirectories(Path.of(collector.getProfileLogDir()));
            String fileName = "cpu-profile-20260101-000000.html";
            String content = "<html>profile</html>";
            Path source = profileDir.resolve(fileName);
            Files.writeString(source, content);

            // Inject filesystem ownership without requiring privileged chown or a platform-specific filesystem.
            new MockUp<TarArchiveOutputStream>() {
                @Mock
                public void putArchiveEntry(Invocation invocation, TarArchiveEntry entry) throws IOException {
                    if (entry.getName().equals(fileName)) {
                        entry.setUserId(userId);
                        entry.setGroupId(groupId);
                    }
                    invocation.proceed(entry);
                }
            };

            Method compressFile = ProcProfileCollector.class.getDeclaredMethod("compressFile", String.class);
            compressFile.setAccessible(true);
            compressFile.invoke(collector, fileName);

            assertFalse(Files.exists(source));
            assertFalse(Files.exists(profileDir.resolve(fileName + ".tar.gz.tmp")));
            try (TarArchiveInputStream archive = new TarArchiveInputStream(
                    new GzipCompressorInputStream(Files.newInputStream(profileDir.resolve(fileName + ".tar.gz"))))) {
                TarArchiveEntry entry = archive.getNextEntry();
                assertNotNull(entry);
                assertEquals(fileName, entry.getName());
                assertEquals(userId, entry.getLongUserId());
                assertEquals(groupId, entry.getLongGroupId());
                assertEquals(content, new String(archive.readAllBytes(), StandardCharsets.UTF_8));
                assertNull(archive.getNextEntry());
            }
        } finally {
            Config.sys_log_dir = originalLogDir;
        }
    }

    @Test
    public void runAfterCatalogReady() throws IOException, InterruptedException {
        Config.proc_profile_collect_time_s = 1;
        ProcProfileCollector collector = new ProcProfileCollector();
        String dir = collector.getProfileLogDir();
        File profileDir = new File(dir);

        // count the number of files in the directory
        Function<Void, Integer> countFiles = (x) -> {
            if (!profileDir.exists() || !profileDir.isDirectory()) {
                return 0;
            }
            int count = 0;
            String[] files = profileDir.list();
            if (files == null) {
                return 0;
            }
            for (String filename : files) {
                if (filename.startsWith("cpu-profile-") && filename.endsWith(".tar.gz")) {
                    count++;
                }
            }
            return count;
        };

        int prevCount = countFiles.apply(null);

        collector.runAfterCatalogReady();
        assertTrue(profileDir.exists(), "Profile log directory should exist");
        assertTrue(profileDir.isDirectory(), "Profile log directory should be a directory");

        collector.runAfterCatalogReady();
        int afterCount = countFiles.apply(null);
        assertTrue(afterCount > prevCount, String.format("prev: %d, after: %d", prevCount, afterCount));
    }
}
