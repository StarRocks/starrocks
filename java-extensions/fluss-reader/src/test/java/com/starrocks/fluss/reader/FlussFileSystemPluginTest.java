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

package com.starrocks.fluss.reader;

import org.apache.fluss.fs.FileSystemPlugin;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.TreeSet;

public class FlussFileSystemPluginTest {

    @Test
    public void testRemoteStorageFileSystemsAreBundled() {
        // Fluss moves older log segments to remote storage, and the client downloads them itself. Without a
        // file system for the scheme, the client retries the download forever and the read never ends.
        Set<String> schemes = new TreeSet<>();
        for (FileSystemPlugin plugin : ServiceLoader.load(FileSystemPlugin.class)) {
            schemes.add(plugin.getScheme());
        }
        Assertions.assertTrue(schemes.containsAll(List.of("s3", "s3a", "oss")), "Bundled schemes: " + schemes);
    }
}
