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

package com.starrocks.server;

import com.starrocks.catalog.Database;
import com.starrocks.common.DdlException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mocked;
import org.apache.thrift.TException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class GlobalStateMgrTest {
    private static ConnectContext ctx;
    private static final GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();

    @BeforeAll
    public static void setup() throws Exception {
        ctx = UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT);
    }

    @Test
    void TestChangeCatalog(@Mocked CatalogMgr catalogMgr, @Mocked MetadataMgr metadataMgr) throws TException, DdlException {
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

        globalStateMgr.changeCatalogDb(ctx, "default_catalog.");
        Assertions.assertEquals("default_catalog", ctx.getCurrentCatalog());
        Assertions.assertEquals("", ctx.getDatabase());

        globalStateMgr.changeCatalogDb(ctx, "hive_catalog.");
        Assertions.assertEquals("hive_catalog", ctx.getCurrentCatalog());
        Assertions.assertEquals("", ctx.getDatabase());

        Assertions.assertThrows(DdlException.class, () -> {
            globalStateMgr.changeCatalogDb(ctx, "nonexistent_catalog.");
        });

        Assertions.assertThrows(DdlException.class, () -> {
            globalStateMgr.changeCatalogDb(ctx, ".");
        });
    }

    @Test
    void TestChangeDB(@Mocked MetadataMgr metadataMgr) throws DdlException {
        new Expectations() {
            {
                metadataMgr.getDb("default_catalog", "db");
                result = new Database(101, "db");

                metadataMgr.getDb("default_catalog", "nonexistent_db");
                result = null;
            }
        };

        ctx.setCurrentCatalog("default_catalog");
        globalStateMgr.changeCatalogDb(ctx, "db");
        Assertions.assertEquals("db", ctx.getDatabase());
        Assertions.assertThrows(DdlException.class, () -> {
            globalStateMgr.changeCatalogDb(ctx, "nonexistent_db");
        });
    }

    @Test
    void TestChangeCatalogDB(@Mocked CatalogMgr catalogMgr, @Mocked MetadataMgr metadataMgr) throws DdlException {
        new Expectations() {
            {
                catalogMgr.catalogExists("default_catalog");
                result = true;

                catalogMgr.catalogExists("hive_catalog");
                result = true;;
            }
            {
                metadataMgr.getDb("default_catalog", "db");
                result = new Database(101, "db");

                metadataMgr.getDb("default_catalog", "nonexistent_db");
                result = null;

                metadataMgr.getDb("hive_catalog", "db");
                result = new Database(101, "db");

                metadataMgr.getDb("hive_catalog", "nonexistent_db");
                result = null;
            }
        };

        globalStateMgr.changeCatalogDb(ctx, "default_catalog.db");
        Assertions.assertEquals("default_catalog", ctx.getCurrentCatalog());
        Assertions.assertEquals("db", ctx.getDatabase());

        globalStateMgr.changeCatalogDb(ctx, "hive_catalog.db");
        Assertions.assertEquals("hive_catalog", ctx.getCurrentCatalog());
        Assertions.assertEquals("db", ctx.getDatabase());

        Assertions.assertThrows(DdlException.class, () -> {
            globalStateMgr.changeCatalogDb(ctx, "default_catalog.nonexistent_db");
        });

        Assertions.assertThrows(DdlException.class, () -> {
            globalStateMgr.changeCatalogDb(ctx, "hive_catalog.nonexistent_db");
        });
    }

    @AfterAll
    public static void teardown() {}
}
