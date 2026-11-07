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

package com.starrocks.jdbcbridge;

import com.zaxxer.hikari.HikariDataSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.regex.Pattern;

// Caches one HikariCP pool per user/password/jdbc_url on this BE/CN.
//
// A pool that no scanner has used for its idle timeout is closed and removed. FE does not tell
// BE/CN when a JDBC catalog or resource is dropped or altered, so without this a pool for retired
// credentials would live until restart: with minimumIdle > 0 it keeps reopening a connection
// every maxLifetime, and once the credentials are revoked it retries the login every few seconds
// forever.
public class DataSourceCache {
    private static final Logger LOG = LogManager.getLogger(DataSourceCache.class);
    private static final long EVICT_INTERVAL_MS = 30_000L;

    // HikariCP's default idleTimeout, used when a caller gives no expiration.
    private static final long DEFAULT_EXPIRE_AFTER_IDLE_MS = 600_000L;
    // Masks the value of password-like URL parameters (as HikariCP does when it logs a URL) and
    // the password of user:password@ credentials in the authority.
    private static final Pattern URL_PASSWORD_PARAM =
            Pattern.compile("(?i)([?&;][^&#;=]*(?:password|pwd)=)[^&#;]*");
    private static final Pattern URL_USER_INFO_PASSWORD = Pattern.compile("(//[^/?#@:]*:)[^/?#@]*@");

    public static final class DataSourceCacheItem {
        private final HikariDataSource hikariDataSource;
        private final ClassLoader classLoader;
        // Kept apart from the pool's own idleTimeout: HikariCP sets that to 0 when it is close to or
        // above maxLifetime, which is the case with the BE defaults (600 s idle, 300 s maxLifetime).
        private final long expireAfterIdleMs;
        // Guarded by the cache map's per-key compute.
        private int activeScanners = 0;
        private long lastUsedMs = System.currentTimeMillis();

        public DataSourceCacheItem(HikariDataSource hikariDataSource, ClassLoader classLoader) {
            this(hikariDataSource, classLoader, DEFAULT_EXPIRE_AFTER_IDLE_MS);
        }

        public DataSourceCacheItem(HikariDataSource hikariDataSource, ClassLoader classLoader,
                                   long expireAfterIdleMs) {
            if (expireAfterIdleMs <= 0) {
                throw new IllegalArgumentException("expireAfterIdleMs must be positive: " + expireAfterIdleMs);
            }
            this.hikariDataSource = hikariDataSource;
            this.classLoader = classLoader;
            this.expireAfterIdleMs = expireAfterIdleMs;
        }

        public HikariDataSource getHikariDataSource() {
            return hikariDataSource;
        }

        public ClassLoader getClassLoader() {
            return classLoader;
        }

        private boolean isExpired(long nowMs) {
            return activeScanners == 0 && nowMs - lastUsedMs >= expireAfterIdleMs;
        }
    }

    private final Map<String, DataSourceCacheItem> sources = new ConcurrentHashMap<>();
    // Driver class loaders are keyed by the driver jar, not by the pool, and are never dropped:
    // drivers register themselves in DriverManager, which pins their loader, so creating a new
    // loader every time a pool is re-created would leak metaspace.
    private final Map<String, ClassLoader> driverClassLoaders = new ConcurrentHashMap<>();
    private static final DataSourceCache INSTANCE = new DataSourceCache();

    private DataSourceCache() {
        ScheduledExecutorService evictor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "jdbc-datasource-evictor");
            t.setDaemon(true);
            return t;
        });
        evictor.scheduleWithFixedDelay(() -> {
            try {
                evictExpired(System.currentTimeMillis());
            } catch (Throwable t) {
                LOG.warn("Failed to evict idle JDBC connection pools", t);
            }
        }, EVICT_INTERVAL_MS, EVICT_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    public static DataSourceCache getInstance() {
        return INSTANCE;
    }

    public ClassLoader getDriverClassLoader(String driverLocation, Supplier<ClassLoader> provider) {
        return driverClassLoaders.computeIfAbsent(driverLocation, k -> provider.get());
    }

    // Returns the cached pool without marking it in use. Only for tests and callers that hold
    // no connection from it.
    public DataSourceCacheItem getSource(String key, Supplier<DataSourceCacheItem> provider) {
        checkKey(key);
        return sources.computeIfAbsent(key, k -> create(provider));
    }

    // Returns the pool for `key`, creating it if needed, and marks it in use until release().
    public DataSourceCacheItem acquire(String key, Supplier<DataSourceCacheItem> provider) {
        checkKey(key);
        return sources.compute(key, (k, item) -> {
            if (item == null) {
                item = create(provider);
            }
            item.activeScanners++;
            item.lastUsedMs = System.currentTimeMillis();
            return item;
        });
    }

    public void release(String key, DataSourceCacheItem item) {
        sources.computeIfPresent(key, (k, current) -> {
            if (current == item && current.activeScanners > 0) {
                current.activeScanners--;
                current.lastUsedMs = System.currentTimeMillis();
            }
            return current;
        });
    }

    // Closes and removes every pool that is not in use and has been idle for its idle timeout.
    // Returns the number of pools closed.
    int evictExpired(long nowMs) {
        List<HikariDataSource> expired = new ArrayList<>();
        for (String key : sources.keySet()) {
            sources.computeIfPresent(key, (k, item) -> {
                if (item.isExpired(nowMs)) {
                    expired.add(item.getHikariDataSource());
                    return null;
                }
                return item;
            });
        }
        for (HikariDataSource dataSource : expired) {
            try {
                dataSource.close();
                LOG.info("Closed idle JDBC connection pool {} (url={}, user={})", dataSource.getPoolName(),
                        redactJdbcUrl(dataSource.getJdbcUrl()), dataSource.getUsername());
            } catch (Exception e) {
                LOG.warn("Failed to close idle JDBC connection pool {} (url={}, user={})",
                        dataSource.getPoolName(), redactJdbcUrl(dataSource.getJdbcUrl()), dataSource.getUsername(), e);
            }
        }
        return expired.size();
    }

    static String redactJdbcUrl(String jdbcUrl) {
        if (jdbcUrl == null) {
            return null;
        }
        String redacted = URL_PASSWORD_PARAM.matcher(jdbcUrl).replaceAll("$1<masked>");
        return URL_USER_INFO_PASSWORD.matcher(redacted).replaceAll("$1<masked>@");
    }

    private static void checkKey(String key) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("Driver ID cannot be null or empty");
        }
    }

    private static DataSourceCacheItem create(Supplier<DataSourceCacheItem> provider) {
        DataSourceCacheItem item = provider.get();
        if (item == null) {
            throw new NullPointerException("DataSourceCacheItem supplier returned null");
        }
        return item;
    }
}
