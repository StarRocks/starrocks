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

import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.credential.CloudType;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.FileIO;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.function.Supplier;

import static com.starrocks.connector.iceberg.IcebergVendedCredentials.ASSUMED_LIFETIME_MS;
import static com.starrocks.connector.iceberg.IcebergVendedCredentials.S3_SESSION_TOKEN_EXPIRES_AT_MS;
import static com.starrocks.connector.iceberg.IcebergVendedCredentials.refreshAfterMs;
import static com.starrocks.credential.CredentialRefreshPolicy.MAX_REFRESH_MARGIN_MS;

public class IcebergVendedCredentialsTest {
    private static final long NOW = 1_000_000_000L;
    private static final Supplier<OptionalLong> LOADED_NOW = () -> OptionalLong.of(NOW);
    private static final Supplier<OptionalLong> UNKNOWN_LOAD = OptionalLong::empty;

    private static Table tableWithIoProperties(Map<String, String> properties) {
        FileIO io = Mockito.mock(FileIO.class);
        Mockito.when(io.properties()).thenReturn(properties);
        Table table = Mockito.mock(Table.class);
        Mockito.when(table.io()).thenReturn(io);
        Mockito.when(table.location()).thenReturn("s3://bucket/db/t1");
        return table;
    }

    private static Map<String, String> s3Credentials() {
        Map<String, String> properties = new HashMap<>();
        properties.put("s3.access-key-id", "ak");
        properties.put("s3.secret-access-key", "sk");
        properties.put("s3.session-token", "token");
        return properties;
    }

    @Test
    public void testNoVendedCredentials() {
        Assertions.assertNull(IcebergVendedCredentials.of(tableWithIoProperties(new HashMap<>()), NOW, LOADED_NOW));
    }

    @Test
    public void testVendedS3CredentialsWithExpiry() {
        Map<String, String> properties = s3Credentials();
        properties.put(S3_SESSION_TOKEN_EXPIRES_AT_MS, String.valueOf(NOW + 3600_000L));
        IcebergVendedCredentials credentials = IcebergVendedCredentials.of(tableWithIoProperties(properties), NOW, UNKNOWN_LOAD);
        Assertions.assertNotNull(credentials);
        Assertions.assertEquals(CloudType.AWS, credentials.cloudConfiguration().getCloudType());
        Assertions.assertEquals(NOW + 3600_000L - MAX_REFRESH_MARGIN_MS, credentials.refreshAfterMs());
    }

    @Test
    public void testRefreshAfterWithoutExpiryAssumesDefaultLifetime() {
        Assertions.assertEquals(NOW + ASSUMED_LIFETIME_MS - MAX_REFRESH_MARGIN_MS, refreshAfterMs(Map.of(), NOW, LOADED_NOW));
        Assertions.assertEquals(NOW + ASSUMED_LIFETIME_MS - MAX_REFRESH_MARGIN_MS,
                refreshAfterMs(Map.of(S3_SESSION_TOKEN_EXPIRES_AT_MS, "not-a-number"), NOW, LOADED_NOW));
    }

    @Test
    public void testRefreshAfterShortLifetimeUsesHalf() {
        // A 10-minute credential refreshes after 5 minutes, not 15 minutes before it expires.
        Assertions.assertEquals(NOW + 300_000L,
                refreshAfterMs(Map.of(S3_SESSION_TOKEN_EXPIRES_AT_MS, String.valueOf(NOW + 600_000L)), NOW, UNKNOWN_LOAD));
    }

    @Test
    public void testRefreshAfterTakesEarliestExpiry() {
        Map<String, String> properties = new HashMap<>();
        properties.put("adls.sas-token-expires-at-ms.account1", String.valueOf(NOW + 7200_000L));
        properties.put("adls.sas-token-expires-at-ms.account2", String.valueOf(NOW + 3600_000L));
        Assertions.assertEquals(NOW + 3600_000L - MAX_REFRESH_MARGIN_MS, refreshAfterMs(properties, NOW, UNKNOWN_LOAD));
    }

    @Test
    public void testCachedTableWithoutExpiryRefreshesSoon() {
        // Its credentials may be as old as the table cache allows; renew from a fresh load early.
        Assertions.assertEquals(NOW + IcebergVendedCredentials.CACHED_UNKNOWN_EXPIRY_REFRESH_MS,
                refreshAfterMs(Map.of(), NOW, UNKNOWN_LOAD));
    }

    @Test
    public void testRefreshAfterAlreadyExpiredIsNow() {
        Assertions.assertEquals(NOW,
                refreshAfterMs(Map.of(S3_SESSION_TOKEN_EXPIRES_AT_MS, String.valueOf(NOW - 1)), NOW, UNKNOWN_LOAD));
    }

    private static Table vendingTable(UUID uuid) {
        Table table = tableWithIoProperties(s3Credentials());
        Mockito.when(table.uuid()).thenReturn(uuid);
        return table;
    }

    @Test
    public void testRenewedFromTheSameTable() {
        UUID uuid = UUID.randomUUID();
        IcebergVendedCredentials renewed =
                IcebergVendedCredentials.renewedFrom(vendingTable(uuid), vendingTable(uuid), NOW, "c.db.t1");
        Assertions.assertEquals(NOW + ASSUMED_LIFETIME_MS - MAX_REFRESH_MARGIN_MS, renewed.refreshAfterMs());
    }

    @Test
    public void testNotRenewedFromAReplacedTable() {
        StarRocksConnectorException e = Assertions.assertThrows(StarRocksConnectorException.class,
                () -> IcebergVendedCredentials.renewedFrom(vendingTable(UUID.randomUUID()),
                        vendingTable(UUID.randomUUID()), NOW, "c.db.t1"));
        Assertions.assertTrue(e.getMessage().contains("replaced"), e.getMessage());
    }

    @Test
    public void testRenewedWhenAUuidIsUnknown() {
        Table planned = vendingTable(null);
        Table reloaded = vendingTable(null);
        Mockito.when(reloaded.uuid()).thenThrow(new NullPointerException("no uuid in metadata"));
        Assertions.assertNotNull(IcebergVendedCredentials.renewedFrom(planned, reloaded, NOW, "c.db.t1"));
    }

    @Test
    public void testNotRenewedWhenTheTableStopsVending() {
        Table reloaded = tableWithIoProperties(new HashMap<>());
        Assertions.assertThrows(StarRocksConnectorException.class,
                () -> IcebergVendedCredentials.renewedFrom(vendingTable(null), reloaded, NOW, "c.db.t1"));
    }

    @Test
    public void testNotRenewedForAMovedTable() {
        UUID uuid = UUID.randomUUID();
        Table reloaded = vendingTable(uuid);
        Mockito.when(reloaded.location()).thenReturn("s3://bucket/db/t1_moved");
        StarRocksConnectorException e = Assertions.assertThrows(StarRocksConnectorException.class,
                () -> IcebergVendedCredentials.renewedFrom(vendingTable(uuid), reloaded, NOW, "c.db.t1"));
        Assertions.assertTrue(e.getMessage().contains("moved"), e.getMessage());
    }

    @Test
    public void testRefreshAfterWithoutExpiryCountsFromTheLoad() {
        // A cache entry loaded 30 minutes ago: an hour from the load, less the margin.
        long loadedAt = NOW - 30 * 60_000L;
        Assertions.assertEquals(loadedAt + ASSUMED_LIFETIME_MS - MAX_REFRESH_MARGIN_MS,
                refreshAfterMs(Map.of(), NOW, () -> OptionalLong.of(loadedAt)));
    }

    @Test
    public void testLoadTimeIsNotAskedWhenTheCredentialsSayWhenTheyExpire() {
        Supplier<OptionalLong> mustNotAsk = () -> {
            throw new AssertionError("asked for the load time");
        };
        Assertions.assertEquals(NOW + 3600_000L - MAX_REFRESH_MARGIN_MS,
                refreshAfterMs(Map.of(S3_SESSION_TOKEN_EXPIRES_AT_MS, String.valueOf(NOW + 3600_000L)), NOW, mustNotAsk));
        Assertions.assertNull(IcebergVendedCredentials.of(tableWithIoProperties(new HashMap<>()), NOW, mustNotAsk));
    }
}
