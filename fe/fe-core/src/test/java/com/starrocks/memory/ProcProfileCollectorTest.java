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
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertTrue;

class ProcProfileCollectorTest {

    @Test
    public void runAfterCatalogReady() throws IOException, InterruptedException {
        Config.proc_profile_collect_time_s = 1;
        ProcProfileCollector collector = new ProcProfileCollector();
        String dir = collector.getProfileLogDir();
        File profileDir = new File(dir);

        // count the number of files in the directory
        Function<Void, Integer> countFiles = (x) -> {
            int count = 0;
            String[] files = profileDir.list();
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
        assertTrue(afterCount > prevCount);
    }
}