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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class CachingUnityCatalogClientTest {

    private static UnityCatalogProperties propsWith(long ttlSec, long credentialsSafetyMarginSec) {
        return new UnityCatalogProperties(ImmutableMap.of(
                "unity.catalog.host", "https://example.cloud.databricks.com",
                "unity.catalog.token", "dapiTEST",
                "unity.catalog.name", "main",
                "unity.catalog.cache.ttl-sec", Long.toString(ttlSec),
                "unity.catalog.cache.credentials.safety-margin-sec", Long.toString(credentialsSafetyMarginSec)));
    }

    private static final class AdvanceableTicker extends Ticker {
        private final AtomicLong nanos = new AtomicLong();

        @Override
        public long read() {
            return nanos.get();
        }

        void advance(long duration, TimeUnit unit) {
            nanos.addAndGet(unit.toNanos(duration));
        }
    }

    private static CachingUnityCatalogClient newClient(UnityCatalogApi delegate,
                                                       UnityCatalogProperties props,
                                                       Ticker ticker,
                                                       AtomicLong clockMillis) {
        return new CachingUnityCatalogClient(delegate, props, ticker, clockMillis::get);
    }

    @Test
    public void testListSchemasCachesAcrossCalls(@Mocked UnityCatalogApi delegate) {
        UnityCatalogTypes.Schema s = new UnityCatalogTypes.Schema();
        s.name = "sales";
        new Expectations() {
            {
                delegate.listSchemas("main");
                result = ImmutableList.of(s);
                times = 1;
            }
        };

        CachingUnityCatalogClient client = newClient(delegate, propsWith(60, 60),
                new AdvanceableTicker(), new AtomicLong());

        List<UnityCatalogTypes.Schema> first = client.listSchemas("main");
        List<UnityCatalogTypes.Schema> second = client.listSchemas("main");
        Assertions.assertEquals(1, first.size());
        Assertions.assertSame(first, second, "second call must be a cache hit");
    }

    @Test
    public void testListTablesCachedPerSchema(@Mocked UnityCatalogApi delegate) {
        UnityCatalogTypes.TableSummary t = new UnityCatalogTypes.TableSummary();
        t.name = "orders";
        t.dataSourceFormat = "DELTA";
        new Expectations() {
            {
                delegate.listTables("main", "sales");
                result = ImmutableList.of(t);
                times = 1;
                delegate.listTables("main", "marketing");
                result = ImmutableList.<UnityCatalogTypes.TableSummary>of();
                times = 1;
            }
        };

        CachingUnityCatalogClient client = newClient(delegate, propsWith(60, 60),
                new AdvanceableTicker(), new AtomicLong());

        client.listTables("main", "sales");
        client.listTables("main", "sales");
        client.listTables("main", "marketing");
        client.listTables("main", "marketing");
    }

    @Test
    public void testGetTableCachesByFullName(@Mocked UnityCatalogApi delegate) {
        UnityCatalogTypes.TableInfo info = new UnityCatalogTypes.TableInfo();
        info.fullName = "main.sales.orders";
        info.tableId = "abc-123";
        info.dataSourceFormat = "DELTA";
        info.storageLocation = "s3://bucket/orders";

        new Expectations() {
            {
                delegate.getTable("main.sales.orders");
                result = info;
                times = 1;
            }
        };

        CachingUnityCatalogClient client = newClient(delegate, propsWith(60, 60),
                new AdvanceableTicker(), new AtomicLong());

        Assertions.assertSame(info, client.getTable("main.sales.orders"));
        Assertions.assertSame(info, client.getTable("main.sales.orders"));
    }

    @Test
    public void testTableExistsShortCircuitsWhenTableInfoCached(@Mocked UnityCatalogApi delegate) {
        UnityCatalogTypes.TableInfo info = new UnityCatalogTypes.TableInfo();
        info.fullName = "main.sales.orders";
        info.dataSourceFormat = "DELTA";
        info.storageLocation = "s3://bucket/orders";

        new Expectations() {
            {
                delegate.getTable("main.sales.orders");
                result = info;
                times = 1;
                // No call to delegate.tableExists should happen when TableInfo is cached.
                delegate.tableExists(anyString);
                times = 0;
            }
        };

        CachingUnityCatalogClient client = newClient(delegate, propsWith(60, 60),
                new AdvanceableTicker(), new AtomicLong());
        client.getTable("main.sales.orders");
        Assertions.assertTrue(client.tableExists("main.sales.orders"));
    }

    @Test
    public void testTableExistsDelegatesWhenNotCached(@Mocked UnityCatalogApi delegate) {
        new Expectations() {
            {
                delegate.tableExists("main.sales.missing");
                result = false;
                times = 1;
            }
        };

        CachingUnityCatalogClient client = newClient(delegate, propsWith(60, 60),
                new AdvanceableTicker(), new AtomicLong());
        Assertions.assertFalse(client.tableExists("main.sales.missing"));
    }

    @Test
    public void testTableExistsDoesNotNegativeCache(@Mocked UnityCatalogApi delegate) {
        // v1 explicitly skips negative caching: two calls -> two delegate hits.
        new Expectations() {
            {
                delegate.tableExists("main.sales.missing");
                result = false;
                times = 2;
            }
        };

        CachingUnityCatalogClient client = newClient(delegate, propsWith(60, 60),
                new AdvanceableTicker(), new AtomicLong());
        Assertions.assertFalse(client.tableExists("main.sales.missing"));
        Assertions.assertFalse(client.tableExists("main.sales.missing"));
    }

    @Test
    public void testCredentialsCacheHitWhenWellBeforeExpiry(@Mocked UnityCatalogApi delegate) {
        UnityCatalogTypes.TemporaryTableCredentials creds = new UnityCatalogTypes.TemporaryTableCredentials();
        creds.expirationTime = 10_000_000L; // 10s
        AtomicLong wallClock = new AtomicLong(1_000L); // 1s -- well inside the 60s safety margin
        new Expectations() {
            {
                delegate.getTemporaryTableCredentials("abc-123", "READ");
                result = creds;
                times = 1;
            }
        };

        CachingUnityCatalogClient client = newClient(delegate, propsWith(60, 60),
                new AdvanceableTicker(), wallClock);
        Assertions.assertSame(creds, client.getTemporaryTableCredentials("abc-123", "READ"));
        Assertions.assertSame(creds, client.getTemporaryTableCredentials("abc-123", "READ"));
    }

    @Test
    public void testCredentialsBypassedWhenInsideSafetyMargin(@Mocked UnityCatalogApi delegate) {
        UnityCatalogTypes.TemporaryTableCredentials first = new UnityCatalogTypes.TemporaryTableCredentials();
        first.expirationTime = 10_000L; // 10s
        UnityCatalogTypes.TemporaryTableCredentials second = new UnityCatalogTypes.TemporaryTableCredentials();
        second.expirationTime = 200_000L;

        AtomicLong wallClock = new AtomicLong(1_000L);
        new Expectations() {
            {
                delegate.getTemporaryTableCredentials("abc-123", "READ");
                returns(first, second);
                times = 2;
            }
        };

        // safety margin 60s => creds that expire at 10s are always "near expiry".
        CachingUnityCatalogClient client = newClient(delegate, propsWith(60, 60),
                new AdvanceableTicker(), wallClock);
        Assertions.assertSame(first, client.getTemporaryTableCredentials("abc-123", "READ"));
        // Second call: cache hit, but expirationTime - safetyMargin (10_000 - 60_000 = -50_000)
        // is already <= now (1_000), so bypass and re-vend.
        Assertions.assertSame(second, client.getTemporaryTableCredentials("abc-123", "READ"));
    }

    @Test
    public void testCredentialsWithoutExpirationTimeStayCached(@Mocked UnityCatalogApi delegate) {
        UnityCatalogTypes.TemporaryTableCredentials creds = new UnityCatalogTypes.TemporaryTableCredentials();
        creds.expirationTime = null;

        new Expectations() {
            {
                delegate.getTemporaryTableCredentials("abc-123", "READ");
                result = creds;
                times = 1;
            }
        };

        CachingUnityCatalogClient client = newClient(delegate, propsWith(60, 60),
                new AdvanceableTicker(), new AtomicLong());
        Assertions.assertSame(creds, client.getTemporaryTableCredentials("abc-123", "READ"));
        Assertions.assertSame(creds, client.getTemporaryTableCredentials("abc-123", "READ"));
    }

    @Test
    public void testTtlExpiryReloadsEntry(@Mocked UnityCatalogApi delegate) {
        UnityCatalogTypes.TableInfo info1 = new UnityCatalogTypes.TableInfo();
        info1.fullName = "main.sales.orders";
        UnityCatalogTypes.TableInfo info2 = new UnityCatalogTypes.TableInfo();
        info2.fullName = "main.sales.orders";

        new Expectations() {
            {
                delegate.getTable("main.sales.orders");
                returns(info1, info2);
                times = 2;
            }
        };

        AdvanceableTicker ticker = new AdvanceableTicker();
        CachingUnityCatalogClient client = newClient(delegate, propsWith(60, 60), ticker, new AtomicLong());

        Assertions.assertSame(info1, client.getTable("main.sales.orders"));
        ticker.advance(61, TimeUnit.SECONDS);
        Assertions.assertSame(info2, client.getTable("main.sales.orders"));
    }

    @Test
    public void testInvalidateClearsTableInfoAndCredentials(@Mocked UnityCatalogApi delegate) {
        UnityCatalogTypes.TableInfo info = new UnityCatalogTypes.TableInfo();
        info.fullName = "main.sales.orders";
        info.tableId = "abc-123";
        info.dataSourceFormat = "DELTA";
        info.storageLocation = "s3://bucket/orders";

        UnityCatalogTypes.TemporaryTableCredentials creds = new UnityCatalogTypes.TemporaryTableCredentials();
        creds.expirationTime = Long.MAX_VALUE;

        new Expectations() {
            {
                delegate.getTable("main.sales.orders");
                result = info;
                times = 2; // initial + post-invalidate
                delegate.getTemporaryTableCredentials("abc-123", "READ");
                result = creds;
                times = 2; // initial + post-invalidate
            }
        };

        CachingUnityCatalogClient client = newClient(delegate, propsWith(60, 60),
                new AdvanceableTicker(), new AtomicLong());
        client.getTable("main.sales.orders");
        client.getTemporaryTableCredentials("abc-123", "READ");

        client.invalidate("main.sales.orders");

        client.getTable("main.sales.orders");
        client.getTemporaryTableCredentials("abc-123", "READ");
    }

    @Test
    public void testZeroTtlBypassesCache(@Mocked UnityCatalogApi delegate) {
        UnityCatalogTypes.Schema s = new UnityCatalogTypes.Schema();
        s.name = "sales";

        new Expectations() {
            {
                delegate.listSchemas("main");
                result = ImmutableList.of(s);
                times = 2;
            }
        };

        CachingUnityCatalogClient client = newClient(delegate, propsWith(0, 60),
                new AdvanceableTicker(), new AtomicLong());
        client.listSchemas("main");
        client.listSchemas("main");
    }

    /**
     * {@link CachingUnityCatalogClient#getMetastoreSummary()} is intentionally a pure pass-through
     * since {@link UnityMetastore} memoizes the region on the catalog handle. This test pins the
     * pass-through contract: every call must reach the delegate.
     */
    @Test
    public void testGetMetastoreSummaryDelegatesEveryCall(@Mocked UnityCatalogApi delegate) {
        UnityCatalogTypes.MetastoreSummary summary = new UnityCatalogTypes.MetastoreSummary();
        summary.region = "eu-central-1";
        new Expectations() {
            {
                delegate.getMetastoreSummary();
                result = summary;
                times = 3;
            }
        };

        CachingUnityCatalogClient client = newClient(delegate, propsWith(60, 60),
                new AdvanceableTicker(), new AtomicLong());
        Assertions.assertSame(summary, client.getMetastoreSummary());
        Assertions.assertSame(summary, client.getMetastoreSummary());
        Assertions.assertSame(summary, client.getMetastoreSummary());
    }
}
