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

package com.starrocks.connector.iceberg.rest;

import com.google.common.collect.ImmutableMap;
import com.starrocks.catalog.Database;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.ha.FrontendNodeType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.NodeMgr;
import com.starrocks.system.Frontend;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import mockit.Verifications;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import org.apache.iceberg.exceptions.RESTException;
import org.apache.iceberg.rest.RESTSessionCatalog;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class IcebergRESTCatalogAuthRecoveryTest {

    @Mocked
    private RESTSessionCatalog restCatalog;

    private ConnectContext connectContext;

    @BeforeEach
    public void setUp() {
        new MockUp<NodeMgr>() {
            @Mock
            public Frontend getMySelf() {
                return new Frontend(FrontendNodeType.FOLLOWER, "fe1", "127.0.0.1", 9010);
            }
        };
        connectContext = new ConnectContext();
    }

    private IcebergRESTCatalog newCatalog(Map<String, String> properties) {
        return new IcebergRESTCatalog("rest_catalog", new Configuration(), new HashMap<>(properties));
    }

    private IcebergRESTCatalog newOAuth2CredentialCatalog() {
        return newCatalog(ImmutableMap.of(
                "iceberg.catalog.security", "oauth2",
                "iceberg.catalog.oauth2.credential", "id:secret"));
    }

    @Test
    public void testNotAuthorizedTriggersRebuildAndRetry() {
        new Expectations() {
            {
                restCatalog.loadNamespaceMetadata((SessionCatalog.SessionContext) any, (Namespace) any);
                result = new NotAuthorizedException("Not authorized: %s", "token expired");
                result = ImmutableMap.of("location", "s3://bucket/db1");
            }
        };

        Database db = newOAuth2CredentialCatalog().getDB(connectContext, "db1");

        Assertions.assertEquals("s3://bucket/db1", db.getLocation());
        new Verifications() {
            {
                restCatalog.initialize(anyString, (Map<String, String>) any);
                times = 2; // constructor + one auth-recovery rebuild
            }
        };
    }

    @Test
    public void testFailedRetryPropagatesAfterSingleRebuild() {
        new Expectations() {
            {
                restCatalog.loadNamespaceMetadata((SessionCatalog.SessionContext) any, (Namespace) any);
                result = new NotAuthorizedException("Not authorized: %s", "token expired");
            }
        };

        IcebergRESTCatalog catalog = newOAuth2CredentialCatalog();
        Assertions.assertThrows(StarRocksConnectorException.class, () -> catalog.getDB(connectContext, "db1"));
        new Verifications() {
            {
                restCatalog.initialize(anyString, (Map<String, String>) any);
                times = 2;
            }
        };
    }

    @Test
    public void testRebuildIsRateLimited() {
        new Expectations() {
            {
                restCatalog.loadNamespaceMetadata((SessionCatalog.SessionContext) any, (Namespace) any);
                result = new NotAuthorizedException("Not authorized: %s", "token expired");
            }
        };

        IcebergRESTCatalog catalog = newOAuth2CredentialCatalog();
        Assertions.assertThrows(StarRocksConnectorException.class, () -> catalog.getDB(connectContext, "db1"));
        Assertions.assertThrows(StarRocksConnectorException.class, () -> catalog.getDB(connectContext, "db1"));
        new Verifications() {
            {
                restCatalog.initialize(anyString, (Map<String, String>) any);
                times = 2; // the second NotAuthorized arrives within the cooldown and must not rebuild again
            }
        };
    }

    @Test
    public void testNoRecoveryWithoutClientCredential() {
        new Expectations() {
            {
                restCatalog.loadNamespaceMetadata((SessionCatalog.SessionContext) any, (Namespace) any);
                result = new NotAuthorizedException("Not authorized: %s", "bad token");
            }
        };

        IcebergRESTCatalog catalog = newCatalog(ImmutableMap.of());
        Assertions.assertThrows(StarRocksConnectorException.class, () -> catalog.getDB(connectContext, "db1"));
        new Verifications() {
            {
                restCatalog.initialize(anyString, (Map<String, String>) any);
                times = 1;
            }
        };
    }

    @Test
    public void testNoRecoveryForJwtSecurity() {
        new Expectations() {
            {
                restCatalog.loadNamespaceMetadata((SessionCatalog.SessionContext) any, (Namespace) any);
                result = new NotAuthorizedException("Not authorized: %s", "expired user token");
            }
        };

        IcebergRESTCatalog catalog = newCatalog(ImmutableMap.of(
                "iceberg.catalog.security", "jwt",
                "iceberg.catalog.oauth2.credential", "id:secret"));
        Assertions.assertThrows(StarRocksConnectorException.class, () -> catalog.getDB(connectContext, "db1"));
        new Verifications() {
            {
                restCatalog.initialize(anyString, (Map<String, String>) any);
                times = 1;
            }
        };
    }

    @Test
    public void testWrappedOperationsPassThroughOnSuccess() {
        new Expectations() {
            {
                restCatalog.loadNamespaceMetadata((SessionCatalog.SessionContext) any, (Namespace) any);
                result = ImmutableMap.of("location", "s3://bucket/db1");
                minTimes = 0;
            }
        };

        IcebergRESTCatalog catalog = newOAuth2CredentialCatalog();
        Assertions.assertNotNull(catalog.getTable(connectContext, "db1", "t1"));
        Assertions.assertFalse(catalog.tableExists(connectContext, "db1", "t1"));
        Assertions.assertTrue(catalog.listAllDatabases(connectContext).isEmpty());
        catalog.createDB(connectContext, "db2", null);
        Assertions.assertEquals("s3://bucket/db1", catalog.getDB(connectContext, "db1").getLocation());
        Assertions.assertDoesNotThrow(() -> catalog.dropDB(connectContext, "db1"));
        Assertions.assertTrue(catalog.listTables(connectContext, "db1").isEmpty());
        Assertions.assertTrue(catalog.createTable(connectContext, "db1", "t1", null, null, null, null, ImmutableMap.of()));
        Assertions.assertFalse(catalog.dropTable(connectContext, "db1", "t1", true));
        catalog.renameTable(connectContext, "db1", "t1", "t2");
        Assertions.assertNotNull(catalog.getView(connectContext, "db1", "v1"));
        Assertions.assertFalse(catalog.dropView(connectContext, "db1", "v1"));
        Assertions.assertEquals("s3://bucket/db1",
                catalog.loadNamespaceMetadata(connectContext, Namespace.of("db1")).get("location"));
        new Verifications() {
            {
                restCatalog.initialize(anyString, (Map<String, String>) any);
                times = 1; // no NotAuthorized -> no rebuild
            }
        };
    }

    @Test
    public void testOtherRestExceptionsDoNotRebuild() {
        new Expectations() {
            {
                restCatalog.loadNamespaceMetadata((SessionCatalog.SessionContext) any, (Namespace) any);
                result = new RESTException("server error");
            }
        };

        IcebergRESTCatalog catalog = newOAuth2CredentialCatalog();
        Assertions.assertThrows(StarRocksConnectorException.class, () -> catalog.getDB(connectContext, "db1"));
        new Verifications() {
            {
                restCatalog.initialize(anyString, (Map<String, String>) any);
                times = 1;
            }
        };
    }
}
