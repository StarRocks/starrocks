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

package com.starrocks.connector.delta.unity;

import com.google.common.base.Ticker;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * Decorator that sits in front of a {@link UnityCatalogApi} delegate (normally
 * {@link UnityCatalogClient}) and caches every read-path response with a shared TTL.
 *
 * <p>Four independent Guava caches back the interface:
 * <ul>
 *   <li>schemas per UC catalog,</li>
 *   <li>tables per (catalog, schema),</li>
 *   <li>{@link UnityCatalogTypes.TableInfo} per full-name (also answers {@link #tableExists}),</li>
 *   <li>{@link UnityCatalogTypes.TemporaryTableCredentials} per table-id.</li>
 * </ul>
 *
 * <p>Credentials are cached with the same {@code expireAfterWrite} TTL as the metadata, but every
 * cache hit additionally checks the UC-server-side {@code expiration_time} minus a configurable
 * safety margin, and re-vends if the lease is about to expire. This keeps stale credentials from
 * being handed out even when the TTL is configured longer than a typical UC lease.</p>
 *
 * <p>{@link #getMetastoreSummary()} is intentionally not cached here -- {@link UnityMetastore}
 * memoizes the metastore region on the catalog handle (see {@code resolveAwsRegion}).</p>
 */
public class CachingUnityCatalogClient implements UnityCatalogApi {

    private final UnityCatalogApi delegate;
    private final long credentialsSafetyMarginMs;
    private final LongSupplier clockMillis;

    private final Cache<String, List<UnityCatalogTypes.Schema>> schemasCache;
    private final Cache<SchemaKey, List<UnityCatalogTypes.TableSummary>> tablesCache;
    private final Cache<String, UnityCatalogTypes.TableInfo> tableInfoCache;
    private final Cache<String, UnityCatalogTypes.TemporaryTableCredentials> credentialsCache;

    public CachingUnityCatalogClient(UnityCatalogApi delegate, UnityCatalogProperties properties) {
        this(delegate, properties, Ticker.systemTicker(), System::currentTimeMillis);
    }

    // Visible for testing: lets tests advance a FakeTicker for TTL assertions and a separate
    // wall-clock supplier for credential-expiration assertions.
    CachingUnityCatalogClient(UnityCatalogApi delegate,
                              UnityCatalogProperties properties,
                              Ticker ticker,
                              LongSupplier clockMillis) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        Objects.requireNonNull(properties, "properties");
        this.credentialsSafetyMarginMs =
                TimeUnit.SECONDS.toMillis(properties.getCredentialsSafetyMarginSec());
        this.clockMillis = Objects.requireNonNull(clockMillis, "clockMillis");

        long ttlSec = properties.getCacheTtlSec();
        this.schemasCache = newCache(ticker, ttlSec);
        this.tablesCache = newCache(ticker, ttlSec);
        this.tableInfoCache = newCache(ticker, ttlSec);
        this.credentialsCache = newCache(ticker, ttlSec);
    }

    private static <K, V> Cache<K, V> newCache(Ticker ticker, long ttlSec) {
        CacheBuilder<Object, Object> builder = CacheBuilder.newBuilder().ticker(ticker);
        if (ttlSec > 0) {
            builder.expireAfterWrite(ttlSec, TimeUnit.SECONDS);
        } else {
            // ttlSec == 0 means "do not cache": zero-duration expireAfterWrite evicts entries
            // before a second caller can read them, effectively bypassing the cache.
            builder.expireAfterWrite(0, TimeUnit.NANOSECONDS);
        }
        return builder.build();
    }

    @Override
    public List<UnityCatalogTypes.Schema> listSchemas(String ucCatalog) {
        try {
            return schemasCache.get(ucCatalog, () -> delegate.listSchemas(ucCatalog));
        } catch (ExecutionException e) {
            throw unwrap(e);
        }
    }

    @Override
    public List<UnityCatalogTypes.TableSummary> listTables(String ucCatalog, String schemaName) {
        SchemaKey key = new SchemaKey(ucCatalog, schemaName);
        try {
            return tablesCache.get(key, () -> delegate.listTables(ucCatalog, schemaName));
        } catch (ExecutionException e) {
            throw unwrap(e);
        }
    }

    @Override
    public UnityCatalogTypes.TableInfo getTable(String fullName) {
        try {
            return tableInfoCache.get(fullName, () -> delegate.getTable(fullName));
        } catch (ExecutionException e) {
            throw unwrap(e);
        }
    }

    @Override
    public boolean tableExists(String fullName) {
        // Positive hit: if we already have the TableInfo cached we know the table exists.
        // Negative responses are not cached in v1; always delegate on a miss.
        if (tableInfoCache.getIfPresent(fullName) != null) {
            return true;
        }
        return delegate.tableExists(fullName);
    }

    @Override
    public UnityCatalogTypes.MetastoreSummary getMetastoreSummary() {
        return delegate.getMetastoreSummary();
    }

    @Override
    public UnityCatalogTypes.TemporaryTableCredentials getTemporaryTableCredentials(String tableId,
                                                                                    String operation) {
        UnityCatalogTypes.TemporaryTableCredentials cached = credentialsCache.getIfPresent(tableId);
        if (cached != null && !isNearExpiry(cached)) {
            return cached;
        }
        UnityCatalogTypes.TemporaryTableCredentials fresh =
                delegate.getTemporaryTableCredentials(tableId, operation);
        if (fresh != null) {
            credentialsCache.put(tableId, fresh);
        }
        return fresh;
    }

    private boolean isNearExpiry(UnityCatalogTypes.TemporaryTableCredentials creds) {
        Long expirationTime = creds.expirationTime;
        if (expirationTime == null) {
            // UC did not tell us when these creds expire; safest to treat every hit as fresh --
            // the TTL on the cache entry still bounds reuse to cacheTtlSec.
            return false;
        }
        return expirationTime - credentialsSafetyMarginMs <= clockMillis.getAsLong();
    }

    /**
     * Drop every cached entry referencing {@code fullName} (table info + any credentials for its
     * table id). Called from {@code REFRESH EXTERNAL TABLE} via
     * {@link UnityMetastore#invalidateTable(String, String)}.
     */
    @Override
    public void invalidate(String fullName) {
        UnityCatalogTypes.TableInfo cached = tableInfoCache.getIfPresent(fullName);
        tableInfoCache.invalidate(fullName);
        if (cached != null && cached.tableId != null) {
            credentialsCache.invalidate(cached.tableId);
        }
    }

    private static RuntimeException unwrap(ExecutionException e) {
        Throwable cause = e.getCause();
        if (cause instanceof RuntimeException) {
            return (RuntimeException) cause;
        }
        if (cause instanceof Error) {
            throw (Error) cause;
        }
        return new RuntimeException(cause == null ? e : cause);
    }

    private static final class SchemaKey {
        private final String catalogName;
        private final String schemaName;

        SchemaKey(String catalogName, String schemaName) {
            this.catalogName = catalogName;
            this.schemaName = schemaName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof SchemaKey)) {
                return false;
            }
            SchemaKey that = (SchemaKey) o;
            return Objects.equals(catalogName, that.catalogName)
                    && Objects.equals(schemaName, that.schemaName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(catalogName, schemaName);
        }
    }
}
