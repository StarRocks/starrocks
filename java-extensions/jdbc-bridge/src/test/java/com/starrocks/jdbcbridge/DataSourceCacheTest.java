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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class DataSourceCacheTest {
    // Long enough that the background evictor (which uses the wall clock) never fires during a test;
    // the tests drive eviction with an explicit clock instead.
    private static final long IDLE_TIMEOUT_MS = TimeUnit.HOURS.toMillis(1);

    private static HikariDataSource newSource(SQLException connectError) {
        HikariDataSource source = new HikariDataSource() {
            @Override
            public Connection getConnection() throws SQLException {
                throw connectError;
            }
        };
        // HikariCP resets idleTimeout to 0 when it is close to or above maxLifetime (the BE defaults are
        // 600 s idle and 300 s maxLifetime). Expiration must not depend on it.
        source.setIdleTimeout(0);
        return source;
    }

    private static DataSourceCache.DataSourceCacheItem newItem(HikariDataSource source) {
        return new DataSourceCache.DataSourceCacheItem(source, DataSourceCacheTest.class.getClassLoader(),
                IDLE_TIMEOUT_MS);
    }

    private static long afterIdleTimeout() {
        return System.currentTimeMillis() + IDLE_TIMEOUT_MS + TimeUnit.MINUTES.toMillis(1);
    }

    @Test
    public void testIdlePoolIsClosedAndRecreated() {
        DataSourceCache cache = DataSourceCache.getInstance();
        String key = "u/p/jdbc:test:" + UUID.randomUUID();
        HikariDataSource source = newSource(null);
        DataSourceCache.DataSourceCacheItem item = cache.acquire(key, () -> newItem(source));
        cache.release(key, item);

        Assertions.assertEquals(0, cache.evictExpired(System.currentTimeMillis()),
                "a pool used within its idle timeout must be kept");
        Assertions.assertFalse(source.isClosed());

        Assertions.assertTrue(cache.evictExpired(afterIdleTimeout()) >= 1);
        Assertions.assertTrue(source.isClosed(), "an idle pool must be closed, not just forgotten");

        HikariDataSource next = newSource(null);
        DataSourceCache.DataSourceCacheItem recreated = cache.acquire(key, () -> newItem(next));
        Assertions.assertSame(next, recreated.getHikariDataSource());
        cache.release(key, recreated);
        cache.evictExpired(afterIdleTimeout());
        Assertions.assertTrue(next.isClosed());
    }

    @Test
    public void testPoolInUseIsNeverEvicted() {
        DataSourceCache cache = DataSourceCache.getInstance();
        String key = "u/p/jdbc:test:" + UUID.randomUUID();
        HikariDataSource source = newSource(null);
        DataSourceCache.DataSourceCacheItem first = cache.acquire(key, () -> newItem(source));
        DataSourceCache.DataSourceCacheItem second = cache.acquire(key, () -> newItem(newSource(null)));
        Assertions.assertSame(first, second);

        cache.release(key, first);
        cache.evictExpired(afterIdleTimeout());
        Assertions.assertFalse(source.isClosed(), "one scanner still holds the pool");

        cache.release(key, second);
        cache.evictExpired(afterIdleTimeout());
        Assertions.assertTrue(source.isClosed());
    }

    @Test
    public void testReleaseOfEvictedItemDoesNotTouchNewPool() {
        DataSourceCache cache = DataSourceCache.getInstance();
        String key = "u/p/jdbc:test:" + UUID.randomUUID();
        DataSourceCache.DataSourceCacheItem old = cache.acquire(key, () -> newItem(newSource(null)));
        cache.release(key, old);
        cache.evictExpired(afterIdleTimeout());

        HikariDataSource current = newSource(null);
        DataSourceCache.DataSourceCacheItem item = cache.acquire(key, () -> newItem(current));
        // A late or duplicate release of the evicted item must not mark the new pool as unused.
        cache.release(key, old);
        cache.evictExpired(afterIdleTimeout());
        Assertions.assertFalse(current.isClosed());

        cache.release(key, item);
        cache.evictExpired(afterIdleTimeout());
        Assertions.assertTrue(current.isClosed());
    }

    @Test
    public void testExpirationUsesBeIdleTimeoutEvenWhenHikariDisabledIt() {
        DataSourceCache cache = DataSourceCache.getInstance();
        String key = "u/p/jdbc:test:" + UUID.randomUUID();
        HikariDataSource source = newSource(null);
        Assertions.assertEquals(0, source.getIdleTimeout());
        DataSourceCache.DataSourceCacheItem item = cache.acquire(key, () -> newItem(source));
        cache.release(key, item);

        cache.evictExpired(System.currentTimeMillis() + IDLE_TIMEOUT_MS - TimeUnit.MINUTES.toMillis(1));
        Assertions.assertFalse(source.isClosed(), "idle for less than the BE idle timeout");
        cache.evictExpired(afterIdleTimeout());
        Assertions.assertTrue(source.isClosed(), "idle for longer than the BE idle timeout");
    }

    @Test
    public void testRedactJdbcUrl() {
        Assertions.assertEquals("jdbc:postgresql://h:5432/db?user=u&password=<masked>&ssl=true",
                DataSourceCache.redactJdbcUrl("jdbc:postgresql://h:5432/db?user=u&password=s3cret&ssl=true"));
        Assertions.assertEquals("jdbc:sqlserver://h:1433;databaseName=db;Password=<masked>;encrypt=true",
                DataSourceCache.redactJdbcUrl("jdbc:sqlserver://h:1433;databaseName=db;Password=s3cret;encrypt=true"));
        Assertions.assertEquals("jdbc:mysql://h:3306/db?trustStorePassword=<masked>&pwd=<masked>",
                DataSourceCache.redactJdbcUrl("jdbc:mysql://h:3306/db?trustStorePassword=a&pwd=b"));
        Assertions.assertEquals("jdbc:clickhouse://u:<masked>@h:8123/db",
                DataSourceCache.redactJdbcUrl("jdbc:clickhouse://u:s3cret@h:8123/db"));
        Assertions.assertEquals("jdbc:postgresql://h:5432/db", DataSourceCache.redactJdbcUrl("jdbc:postgresql://h:5432/db"));
        Assertions.assertNull(DataSourceCache.redactJdbcUrl(null));
    }

    @Test
    public void testFailedOpenReleasesPool() throws Exception {
        JDBCScanContext context = new JDBCScanContext();
        context.setDriverClassName("org.postgresql.Driver");
        context.setJdbcURL("jdbc:test:" + UUID.randomUUID());
        context.setUser("test");
        context.setPassword("");
        context.setSql("SELECT 1");
        context.setQueryTimeZone("UTC");
        context.setStatementFetchSize(1);
        String key = context.getUser() + "/" + context.getPassword() + "/" + context.getJdbcURL();
        SQLException loginError = new SQLException("role \"test\" is not permitted to log in");
        HikariDataSource source = newSource(loginError);
        DataSourceCache cache = DataSourceCache.getInstance();
        cache.getSource(key, () -> newItem(source));

        JDBCScanner scanner = new JDBCScanner("unused", context);
        SQLException thrown = Assertions.assertThrows(SQLException.class, scanner::open);
        Assertions.assertSame(loginError, thrown);
        // BE also calls close() after a failed open; the pool must be released exactly once.
        scanner.close();

        cache.evictExpired(afterIdleTimeout());
        Assertions.assertTrue(source.isClosed(), "a failed open must not leave the pool marked in use");
    }
}
