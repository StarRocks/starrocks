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

package com.starrocks.connector.iceberg;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.FileIO;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class TableFileIOCache {
    public static final org.slf4j.Logger LOG = LoggerFactory.getLogger(AbstractIcebergMetadataScanner.class);

    private final Cache<String, FileIO> cache;

    public TableFileIOCache(long expireSeconds, long capacity) {
        // note: reason why not use expireAfterAccess is that
        // fileio could be bound to credentials, and credentials could be expired.
        // some fileio implementations may not handle this well
        // so for safety, we use expireAfterWrite
        this.cache = CacheBuilder.newBuilder()
                .expireAfterWrite(expireSeconds, TimeUnit.SECONDS)
                .maximumSize(capacity)
                .build();
    }

    public synchronized FileIO get(Table table) {
        String cacheKey = generateCacheKey(table);
        FileIO fileIO = cache.getIfPresent(cacheKey);
        if (fileIO == null) {
            LOG.debug(String.format("TableFileIOCache miss for table: %s", table.name()));
            fileIO = table.io();
            cache.put(cacheKey, fileIO);
        } else {
            LOG.debug(String.format("TableFileIOCache hit for table: %s", table.name()));
        }
        return fileIO;
    }

    private static String generateCacheKey(Table table) {
        return table.name() + "_" + table.uuid();
    }
}
