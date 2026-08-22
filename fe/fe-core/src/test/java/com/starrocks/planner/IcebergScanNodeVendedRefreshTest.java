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

package com.starrocks.planner;

import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.Table;
import com.starrocks.connector.iceberg.IcebergMORParams;
import com.starrocks.connector.iceberg.IcebergTableMORParams;
import com.starrocks.connector.iceberg.IcebergUtil;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.gcp.GCPCloudConfiguration;
import com.starrocks.credential.gcp.GCPCloudConfigurationProvider;
import com.starrocks.credential.gcp.GCPCloudCredential;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.MetadataMgr;
import com.starrocks.thrift.TCloudConfiguration;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * Pins the near-expiry re-vend contract of
 * {@link IcebergScanNode#refreshVendedCloudConfigurationIfNearExpiry(long)}: re-vend only inside
 * the window, rate-limit reloads, skip unchanged credentials, and keep the current token on error.
 */
public class IcebergScanNodeVendedRefreshTest {

    private static final long HOUR_MS = 3600_000L;

    private IcebergTable icebergTable;
    private int reloadCalls;
    private CloudConfiguration freshConfig;

    private static CloudConfiguration vendedConfig(String token, long expiresAtMs) {
        GCPCloudCredential credential = new GCPCloudCredential("", false, "", "", "", "",
                token, String.valueOf(expiresAtMs));
        return new GCPCloudConfiguration(credential);
    }

    private static Long expirationOf(TCloudConfiguration tCloudConfiguration) {
        String v = tCloudConfiguration.getCloud_properties()
                .get(GCPCloudConfigurationProvider.TOKEN_EXPIRATION_KEY);
        return v == null ? null : Long.parseLong(v);
    }

    private IcebergScanNode newNode(String initialToken, long initialExpiryMs) {
        // Constructed with a null catalog name so the constructor's credential setup is skipped;
        // the catalog identity is mocked afterwards for the refresh path.
        new MockUp<IcebergTable>() {
            @Mock
            public String getCatalogName() {
                return null;
            }
        };
        icebergTable = new IcebergTable();
        TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
        desc.setTable(icebergTable);
        IcebergScanNode node = new IcebergScanNode(new PlanNodeId(0), desc, "IcebergScanNode",
                IcebergTableMORParams.EMPTY, IcebergMORParams.EMPTY, null);

        new MockUp<IcebergTable>() {
            @Mock
            public String getCatalogName() {
                return "cat";
            }

            @Mock
            public String getCatalogDBName() {
                return "db";
            }

            @Mock
            public String getCatalogTableName() {
                return "tbl";
            }
        };
        new MockUp<MetadataMgr>() {
            @Mock
            public void refreshTable(String catalogName, String srDbName, Table table,
                                     List<String> partitionNames, boolean onlyCachedPartitions) {
                reloadCalls++;
            }

            @Mock
            public Table getTable(ConnectContext context, String catalogName, String dbName, String tblName) {
                return icebergTable;
            }
        };
        new MockUp<IcebergUtil>() {
            @Mock
            public CloudConfiguration getVendedCloudConfiguration(String catalogName, IcebergTable table) {
                return freshConfig;
            }
        };
        node.setCloudConfiguration(vendedConfig(initialToken, initialExpiryMs));
        return node;
    }

    @BeforeEach
    public void setUp() {
        reloadCalls = 0;
    }

    @Test
    public void testNoVendedCredentialDoesNotReload() {
        IcebergScanNode node = newNode("tok1", System.currentTimeMillis() + 60_000L);
        node.setCloudConfiguration(null);
        Assertions.assertNull(node.refreshVendedCloudConfigurationIfNearExpiry(3000_000L));
        Assertions.assertEquals(0, reloadCalls);
    }

    @Test
    public void testMissingExpirationDoesNotReload() {
        IcebergScanNode node = newNode("tok1", System.currentTimeMillis() + 60_000L);
        // An empty access token serializes no expiration property.
        node.setCloudConfiguration(vendedConfig("", 0));
        Assertions.assertNull(node.refreshVendedCloudConfigurationIfNearExpiry(3000_000L));
        Assertions.assertEquals(0, reloadCalls);
    }

    @Test
    public void testMalformedExpirationDoesNotReload() {
        IcebergScanNode node = newNode("tok1", System.currentTimeMillis() + 60_000L);
        GCPCloudCredential credential = new GCPCloudCredential("", false, "", "", "", "", "tok1", "soon");
        node.setCloudConfiguration(new GCPCloudConfiguration(credential));
        Assertions.assertNull(node.refreshVendedCloudConfigurationIfNearExpiry(3000_000L));
        Assertions.assertEquals(0, reloadCalls);
    }

    @Test
    public void testReloadReturningNonIcebergTableKeepsCurrentToken() {
        IcebergScanNode node = newNode("tok1", System.currentTimeMillis() + 60_000L);
        new MockUp<MetadataMgr>() {
            @Mock
            public void refreshTable(String catalogName, String srDbName, Table table,
                                     List<String> partitionNames, boolean onlyCachedPartitions) {
            }

            @Mock
            public Table getTable(ConnectContext context, String catalogName, String dbName, String tblName) {
                return null;
            }
        };
        Assertions.assertNull(node.refreshVendedCloudConfigurationIfNearExpiry(3000_000L));
    }

    @Test
    public void testFarFromExpiryDoesNotReload() {
        long expiry = System.currentTimeMillis() + 10 * HOUR_MS;
        IcebergScanNode node = newNode("tok1", expiry);
        Assertions.assertNull(node.refreshVendedCloudConfigurationIfNearExpiry(3000_000L));
        Assertions.assertEquals(0, reloadCalls);
    }

    @Test
    public void testNearExpiryRevendsFreshToken() {
        long oldExpiry = System.currentTimeMillis() + 60_000L;
        long newExpiry = System.currentTimeMillis() + HOUR_MS;
        IcebergScanNode node = newNode("tok1", oldExpiry);
        freshConfig = vendedConfig("tok2", newExpiry);

        TCloudConfiguration refreshed = node.refreshVendedCloudConfigurationIfNearExpiry(3000_000L);
        Assertions.assertNotNull(refreshed);
        Assertions.assertEquals(newExpiry, expirationOf(refreshed));
        Assertions.assertEquals(1, reloadCalls);
        // The node now carries the fresh credential for subsequent delivery rounds.
        Assertions.assertSame(freshConfig, node.getCloudConfiguration());
    }

    @Test
    public void testSameTokenReservedIsNotShipped() {
        long expiry = System.currentTimeMillis() + 60_000L;
        IcebergScanNode node = newNode("tok1", expiry);
        // The catalog re-serves the same token until it is really about to die (observed with
        // Polaris): shipping it would be a no-op on the BE, so it must be skipped.
        freshConfig = vendedConfig("tok1", expiry);
        Assertions.assertNull(node.refreshVendedCloudConfigurationIfNearExpiry(3000_000L));
        Assertions.assertEquals(1, reloadCalls);
    }

    @Test
    public void testNewTokenWithSameExpiryIsShipped() {
        long expiry = System.currentTimeMillis() + 60_000L;
        IcebergScanNode node = newNode("tok1", expiry);
        // Catalogs can vend a different token with the same (e.g. rounded) expiry; the BE
        // filesystem-handle cache keys on the token value, so it must still be shipped.
        freshConfig = vendedConfig("tok2", expiry);
        TCloudConfiguration refreshed = node.refreshVendedCloudConfigurationIfNearExpiry(3000_000L);
        Assertions.assertNotNull(refreshed);
        Assertions.assertEquals(expiry, expirationOf(refreshed));
        Assertions.assertEquals(1, reloadCalls);
    }

    @Test
    public void testReloadAttemptsAreRateLimited() {
        long expiry = System.currentTimeMillis() + 60_000L;
        IcebergScanNode node = newNode("tok1", expiry);
        freshConfig = vendedConfig("tok2", System.currentTimeMillis() + HOUR_MS);

        Assertions.assertNotNull(node.refreshVendedCloudConfigurationIfNearExpiry(3000_000L));
        // Immediately asking again (e.g. the next delivery round, milliseconds later) must not
        // reload the table again: the 30s cooldown does the rate limiting.
        Assertions.assertNull(node.refreshVendedCloudConfigurationIfNearExpiry(3000_000L));
        Assertions.assertEquals(1, reloadCalls);
    }

    @Test
    public void testReloadFailureKeepsCurrentToken() {
        long expiry = System.currentTimeMillis() + 60_000L;
        IcebergScanNode node = newNode("tok1", expiry);
        new MockUp<MetadataMgr>() {
            @Mock
            public void refreshTable(String catalogName, String srDbName, Table table,
                                     List<String> partitionNames, boolean onlyCachedPartitions) {
                throw new RuntimeException("catalog unreachable");
            }
        };
        // Best-effort: a failed re-vend returns null and the scan continues on the current token.
        Assertions.assertNull(node.refreshVendedCloudConfigurationIfNearExpiry(3000_000L));
    }
}
