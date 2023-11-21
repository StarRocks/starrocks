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


package com.starrocks.connector.jdbc;

import com.github.benmanes.caffeine.cache.AsyncCache;
import com.starrocks.connector.exception.StarRocksConnectorException;
import org.checkerframework.checker.nullness.qual.NonNull;
import com.starrocks.common.Config;

import java.util.concurrent.ExecutionException;
import java.util.function.Function;

public class JDBCAsyncCache<K, V> {

    private final AsyncCache<K, V> asyncCache;

    public JDBCAsyncCache(AsyncCache<K, V> asyncCache) {
        this.asyncCache = asyncCache;
    }

    public @NonNull V get(@NonNull K key, @NonNull Function<K, V> function) {
        try {
            if (Config.jdbc_meta_cache_enable){
                return asyncCache.get(key, function).get();
            } else {
                return function.apply(key);
            }
        } catch (InterruptedException | ExecutionException e) {
            throw new StarRocksConnectorException(e.getMessage());
        }
    }
}
