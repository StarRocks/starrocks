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

package com.starrocks.catalog;

import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReportException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class ChangeCatalogDBTest {
    private static ConnectContext ctx;

    @BeforeAll
    public static void setup() throws Exception {
        ctx = UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT);
    }

    @Test
    void testChangeCatalog(@Mocked CatalogMgr catalogMgr) throws DdlException {
        new Expectations() {
            {
                catalogMgr.catalogExists("default_catalog");
                result = true;

                catalogMgr.catalogExists("hive_catalog");
                result = true;

                catalogMgr.catalogExists("nonexistent_catalog");
                result = false;

                catalogMgr.catalogExists("");
                result = false;
            }
        };

        ctx.changeCatalog("default_catalog");
        Assertions.assertEquals("default_catalog", ctx.getCurrentCatalog());
        Assertions.assertEquals("", ctx.getDatabase());

        ctx.changeCatalog("hive_catalog");
        Assertions.assertEquals("hive_catalog", ctx.getCurrentCatalog());
        Assertions.assertEquals("", ctx.getDatabase());

        Assertions.assertThrows(DdlException.class, () -> {
            ctx.changeCatalog("nonexistent_catalog");
        });

        Assertions.assertThrows(DdlException.class, () -> {
            ctx.changeCatalog("");
        });
    }

    @Test
    void testChangeDB(@Mocked MetadataMgr metadataMgr) throws DdlException {
        new Expectations() {
            {
                metadataMgr.getDb(ctx, "default_catalog", "db");
                result = new Database(101, "db");

                metadataMgr.getDb(ctx, "default_catalog", "nonexistent_db");
                result = null;
            }
        };

        ctx.setCurrentCatalog("default_catalog");
        ctx.changeCatalogDb("db");
        Assertions.assertEquals("db", ctx.getDatabase());
        Assertions.assertThrows(DdlException.class, () -> {
            ctx.changeCatalogDb("nonexistent_db");
        });
    }

    @Test
    void testChangeCatalogDB(@Mocked CatalogMgr catalogMgr, @Mocked MetadataMgr metadataMgr) throws DdlException {
        new Expectations() {
            {
                catalogMgr.catalogExists("default_catalog");
                result = true;

                catalogMgr.catalogExists("hive_catalog");
                result = true;
            }

            {
                metadataMgr.getDb(ctx, "default_catalog", "db");
                result = new Database(101, "db");

                metadataMgr.getDb(ctx, "default_catalog", "nonexistent_db");
                result = null;

                metadataMgr.getDb(ctx, "hive_catalog", "db");
                result = new Database(101, "db");

                metadataMgr.getDb(ctx, "hive_catalog", "nonexistent_db");
                result = null;
            }
        };

        ctx.changeCatalogDb("default_catalog.db");
        Assertions.assertEquals("default_catalog", ctx.getCurrentCatalog());
        Assertions.assertEquals("db", ctx.getDatabase());

        ctx.changeCatalogDb("hive_catalog.db");
        Assertions.assertEquals("hive_catalog", ctx.getCurrentCatalog());
        Assertions.assertEquals("db", ctx.getDatabase());

        Assertions.assertThrows(DdlException.class, () -> {
            ctx.changeCatalogDb("default_catalog.nonexistent_db");
        });

        Assertions.assertThrows(DdlException.class, () -> {
            ctx.changeCatalogDb("hive_catalog.nonexistent_db");
        });
    }

    // Reproduces the root cause of MySQL ERROR 2013 (Lost connection at 'reading authorization packet').
    // When a user has no privilege on the target DB, changeCatalogDb() throws ErrorReportException
    // (a RuntimeException) via AccessDeniedException.reportAccessDenied(), NOT a DdlException.
    // In MysqlProto.negotiate(), only DdlException is caught for the "set database" step,
    // so this RuntimeException bubbles up uncaught, causing FE to close the connection
    // without sending a MySQL ERR packet — the client then sees ERROR 2013.
    @Test
    void testChangeCatalogDbAccessDeniedThrowsRuntimeException(@Mocked MetadataMgr metadataMgr) {
        new Expectations() {
            {
                metadataMgr.getDb((ConnectContext) any, "default_catalog", "secret_db");
                result = new Database(201, "secret_db");
            }
        };

        // Mock Authorizer.checkAnyActionOnOrInDb to always throw AccessDeniedException,
        // simulating a user with zero privileges on the target database.
        new MockUp<Authorizer>() {
            @Mock
            public void checkAnyActionOnOrInDb(ConnectContext ctx,
                                               String catalogName, String db) throws AccessDeniedException {
                throw new AccessDeniedException();
            }
        };

        ctx.setCurrentCatalog("default_catalog");
        ctx.setThreadLocalInfo();

        // The key assertion: changeCatalogDb throws ErrorReportException (RuntimeException),
        // NOT DdlException. This is exactly why MysqlProto.negotiate() fails to catch it.
        ErrorReportException ex = Assertions.assertThrows(ErrorReportException.class, () -> {
            ctx.changeCatalogDb("secret_db");
        });

        // Assert the exception carries the correct ErrorCode (most robust check)
        Assertions.assertEquals(ErrorCode.ERR_ACCESS_DENIED, ex.getErrorCode(),
                "Expected ERR_ACCESS_DENIED error code on exception");

        // Assert that ConnectContext.state has been set to ERR with the correct ErrorCode.
        // This is the prerequisite for MysqlProto.sendResponsePacket() to send a MySQL ERR packet.
        Assertions.assertTrue(ctx.getState().isError(),
                "QueryState should be ERR after access denied");
        Assertions.assertEquals(ErrorCode.ERR_ACCESS_DENIED, ctx.getState().getErrorCode(),
                "QueryState should carry ERR_ACCESS_DENIED error code");
    }
}
