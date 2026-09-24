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

package com.starrocks.epack.connector.lakeformation;

import com.starrocks.common.FeConstants;
import com.starrocks.connector.hive.HiveRemoteFileIO;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Lists files with one table's vended credentials, on unshared file systems it closes itself: Hadoop's cache
 * never evicts, so one cached instance per credential would accumulate for the life of the process.
 */
public final class LakeFormationRemoteFileIO extends HiveRemoteFileIO implements AutoCloseable {
    private static final Logger LOG = LogManager.getLogger(LakeFormationRemoteFileIO.class);

    private final Configuration lakeFormationConfiguration;

    // One per (scheme, authority).
    private final Map<String, FileSystem> openedFileSystems = new ConcurrentHashMap<>();

    public LakeFormationRemoteFileIO(Configuration configuration) {
        super(configuration);
        this.lakeFormationConfiguration = configuration;
    }

    @Override
    protected FileSystem fileSystemFor(URI uri) throws IOException {
        // Unit tests inject a file system into the base class.
        if (FeConstants.runningUnitTest) {
            return super.fileSystemFor(uri);
        }
        String key = uri.getScheme() + "://" + uri.getAuthority();
        FileSystem cached = openedFileSystems.get(key);
        if (cached != null) {
            return cached;
        }
        // A race opens one extra instance, which the loser closes.
        FileSystem opened = FileSystem.newInstance(uri, lakeFormationConfiguration);
        FileSystem raced = openedFileSystems.putIfAbsent(key, opened);
        if (raced != null) {
            closeQuietly(opened);
            return raced;
        }
        return opened;
    }

    /** Called when the listing is done, not on metadata eviction, which says nothing about a running listing. */
    @Override
    public void close() {
        openedFileSystems.values().forEach(LakeFormationRemoteFileIO::closeQuietly);
        openedFileSystems.clear();
    }

    private static void closeQuietly(FileSystem fileSystem) {
        try {
            fileSystem.close();
        } catch (IOException | RuntimeException e) {
            // Logged without the configuration, which holds keys.
            LOG.warn("Failed to close a Lake Formation file system: {}", e.toString());
        }
    }
}
