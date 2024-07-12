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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/iceberg/blob/main/hive-metastore/src/main/java/org/apache/iceberg/hive/CachedClientPool.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.iceberg.hive;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Scheduler;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.ClientPool;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.ThreadPools;
import org.apache.thrift.TException;
import org.immutables.value.Value;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * A ClientPool that caches the underlying HiveClientPool instances.
 *
 * <p>The following key elements are supported and can be specified via {@link
 * CatalogProperties#CLIENT_POOL_CACHE_KEYS}:
 *
 * <ul>
 *   <li>ugi - the Hadoop UserGroupInformation instance that represents the current user using the
 *       cache.
 *   <li>user_name - similar to UGI but only includes the user's name determined by
 *       UserGroupInformation#getUserName.
 *   <li>conf - name of an arbitrary configuration. The value of the configuration will be extracted
 *       from catalog properties and added to the cache key. A conf element should start with a
 *       "conf:" prefix which is followed by the configuration name. E.g. specifying "conf:a.b.c"
 *       will add "a.b.c" to the key, and so that configurations with different default catalog
 *       wouldn't share the same client pool. Multiple conf elements can be specified.
 * </ul>
 */
public class CachedClientPool implements ClientPool<IMetaStoreClient, TException> {

    private static final String CONF_ELEMENT_PREFIX = "conf:";

    private static Cache<Key, HiveClientPool> clientPoolCache;

    private final Configuration conf;
    private final int clientPoolSize;
    private final long evictionInterval;
    private final Key key;
    private final Map<String, String> properties;

    CachedClientPool(Configuration conf, Map<String, String> properties) {
        this.conf = conf;
        this.properties = properties;
        this.clientPoolSize =
                PropertyUtil.propertyAsInt(
                        properties,
                        CatalogProperties.CLIENT_POOL_SIZE,
                        CatalogProperties.CLIENT_POOL_SIZE_DEFAULT);
        this.evictionInterval =
                PropertyUtil.propertyAsLong(
                        properties,
                        CatalogProperties.CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS,
                        CatalogProperties.CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS_DEFAULT);
        this.key = extractKey(properties.get(CatalogProperties.CLIENT_POOL_CACHE_KEYS), conf);
        init();
    }

    @VisibleForTesting
    HiveClientPool clientPool() {
        return clientPoolCache.get(key, k -> new HiveClientPool(clientPoolSize, conf, properties));
    }

    private synchronized void init() {
        if (clientPoolCache == null) {
            // Since Caffeine does not ensure that removalListener will be involved after expiration
            // We use a scheduler with one thread to clean up expired clients.
            clientPoolCache =
                    Caffeine.newBuilder()
                            .expireAfterAccess(evictionInterval, TimeUnit.MILLISECONDS)
                            .removalListener((ignored, value, cause) -> ((HiveClientPool) value).close())
                            .scheduler(
                                    Scheduler.forScheduledExecutorService(
                                            ThreadPools.newScheduledPool("hive-metastore-cleaner", 1)))
                            .build();
        }
    }

    @VisibleForTesting
    static Cache<Key, HiveClientPool> clientPoolCache() {
        return clientPoolCache;
    }

    @Override
    public <R> R run(Action<R, IMetaStoreClient, TException> action)
            throws TException, InterruptedException {
        return clientPool().run(action);
    }

    @Override
    public <R> R run(Action<R, IMetaStoreClient, TException> action, boolean retry)
            throws TException, InterruptedException {
        return clientPool().run(action, retry);
    }

    @VisibleForTesting
    static Key extractKey(String cacheKeys, Configuration conf) {
        // generate key elements in a certain order, so that the Key instances are comparable
        List<Object> elements = Lists.newArrayList();
        elements.add(conf.get(HiveConf.ConfVars.METASTOREURIS.varname, ""));
        elements.add(conf.get(HiveCatalog.HIVE_CONF_CATALOG, "hive"));
        if (cacheKeys == null || cacheKeys.isEmpty()) {
            return Key.of(elements);
        }

        Set<KeyElementType> types = Sets.newTreeSet(Comparator.comparingInt(Enum::ordinal));
        Map<String, String> confElements = Maps.newTreeMap();
        for (String element : cacheKeys.split(",", -1)) {
            String trimmed = element.trim();
            if (trimmed.toLowerCase(Locale.ROOT).startsWith(CONF_ELEMENT_PREFIX)) {
                String key = trimmed.substring(CONF_ELEMENT_PREFIX.length());
                ValidationException.check(
                        !confElements.containsKey(key), "Conf key element %s already specified", key);
                confElements.put(key, conf.get(key));
            } else {
                KeyElementType type = KeyElementType.valueOf(trimmed.toUpperCase());
                switch (type) {
                    case UGI:
                    case USER_NAME:
                        ValidationException.check(
                                !types.contains(type), "%s key element already specified", type.name());
                        types.add(type);
                        break;
                    default:
                        throw new ValidationException("Unknown key element %s", trimmed);
                }
            }
        }
        for (KeyElementType type : types) {
            switch (type) {
                case UGI:
                    try {
                        elements.add(UserGroupInformation.getCurrentUser());
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                    break;
                case USER_NAME:
                    try {
                        elements.add(UserGroupInformation.getCurrentUser().getUserName());
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                    break;
                default:
                    throw new RuntimeException("Unexpected key element " + type.name());
            }
        }
        for (String key : confElements.keySet()) {
            elements.add(ConfElement.of(key, confElements.get(key)));
        }
        return Key.of(elements);
    }

    @Value.Immutable
    abstract static class Key {

        abstract List<Object> elements();

        private static Key of(Iterable<?> elements) {
            return ImmutableKey.builder().elements(elements).build();
        }
    }

    @Value.Immutable
    abstract static class ConfElement {
        abstract String key();

        @Nullable
        abstract String value();

        static ConfElement of(String key, String value) {
            return ImmutableConfElement.builder().key(key).value(value).build();
        }
    }

    private enum KeyElementType {
        UGI,
        USER_NAME,
        CONF
    }
}
