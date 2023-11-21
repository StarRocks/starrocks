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

import com.github.benmanes.caffeine.cache.Caffeine;
import com.starrocks.common.Config;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.concurrent.TimeUnit;

public class JDBCCacheBuilder {

    public static <K, V> @NonNull JDBCAsyncCache<K, V> buildAsync() {
        return new JDBCAsyncCache<>(Caffeine.newBuilder()
                .expireAfterWrite(Config.jdbc_meta_cache_expire_sec, TimeUnit.SECONDS)
                .buildAsync());
    }
}
