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

import com.starrocks.authorization.AccessController;
import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.authorization.PrivilegeType;
import com.starrocks.authorization.ranger.hive.RangerHiveAccessController;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.connector.Connector;
import com.starrocks.connector.ConnectorContext;
import com.starrocks.connector.ConnectorFactory;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.persist.AlterCatalogLog;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import mockit.Verifications;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AlterCatalogTest {
    public static ConnectContext connectContext;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        UtFrameUtils.setUpForPersistTest();

        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        ConnectorPlanTestBase.mockHiveCatalog(connectContext);
    }

    @AfterAll
    public static void teardown() throws Exception {
        UtFrameUtils.tearDownForPersisTest();
    }

    @Test
    public void testAlter(@Mocked RangerBasePlugin rangerPlugin) throws Exception {

        new Expectations() {
            {
                rangerPlugin.init();
                minTimes = 0;
            }
        };

        try {
            DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                    "alter catalog hive0 set (\"type\"  =  \"hudi\");",
                    connectContext), connectContext);
            Assertions.fail();
        } catch (AnalysisException e) {
        }

        try {
            DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                    "alter catalog hive0 set (\"hive.metastore.uris\"  =  \"xx\");",
                    connectContext), connectContext);
            Assertions.fail();
        } catch (DdlException e) {
        }

        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "alter catalog hive0 set (\"ranger.plugin.hive.service.name\"  =  \"hive0\");",
                connectContext), connectContext);
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "alter catalog hive0 set (\"enable_cache_list_names\"  =  \"true\");",
                connectContext), connectContext);

        Map<String, Catalog> catalogMap = connectContext.getGlobalStateMgr().getCatalogMgr().getCatalogs();
        Catalog catalog = catalogMap.get("hive0");
        Map<String, String> properties = catalog.getConfig();
        Assertions.assertEquals("hive0", properties.get("ranger.plugin.hive.service.name"));
        Assertions.assertEquals("true", properties.get("enable_cache_list_names"));
    }

    @Test
    public void testReplay(@Mocked RangerBasePlugin rangerPlugin) throws Exception {
        new Expectations() {
            {
                rangerPlugin.init();
                minTimes = 0;
            }
        };

        Map<String, String> properties = new HashMap<>();
        properties.put("ranger.plugin.hive.service.name", "hive0");
        properties.put("enable_cache_list_names", "true");
        AlterCatalogLog log = new AlterCatalogLog("hive0", properties);
        GlobalStateMgr.getCurrentState().getCatalogMgr().replayAlterCatalog(log);

        Map<String, Catalog> catalogMap = connectContext.getGlobalStateMgr().getCatalogMgr().getCatalogs();
        Catalog catalog = catalogMap.get("hive0");
        properties = catalog.getConfig();
        Assertions.assertEquals("hive0", properties.get("ranger.plugin.hive.service.name"));
        Assertions.assertEquals("true", properties.get("enable_cache_list_names"));
    }

    @Test
    public void testPriv() throws Exception {
        StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(
                "alter catalog hive0 set (\"ranger.plugin.hive.service.name\"  =  \"hive0\");",
                connectContext);
        Authorizer.check(stmt, connectContext);

        connectContext.setCurrentUserIdentity(new UserIdentity("u1", "%"));
        try {
            Authorizer.check(stmt, connectContext);
            Assertions.fail();
        } catch (Exception e) {
            Assertions.assertTrue(e.getMessage().contains("Access denied"));
        }
    }

    // ---- ranger.plugin.hive.catalog_prefix.enabled (POST-1852) ----

    @Test
    public void testCreateCatalogRejectsInvalidPrefixValue() {
        Exception e = Assertions.assertThrows(Exception.class, () -> UtFrameUtils.parseStmtWithNewParser(
                "create external catalog bad_prefix properties(\"type\"=\"hive\", " +
                        "\"hive.metastore.uris\"=\"thrift://127.0.0.1:9083\", " +
                        "\"ranger.plugin.hive.service.name\"=\"hive0\", " +
                        "\"ranger.plugin.hive.catalog_prefix.enabled\"=\"yes\")",
                connectContext));
        Assertions.assertTrue(e.getMessage().contains("ranger.plugin.hive.catalog_prefix.enabled"), e.getMessage());
        Assertions.assertNull(connectContext.getGlobalStateMgr().getCatalogMgr().getCatalogByName("bad_prefix"));
    }

    @Test
    public void testAlterCatalogRejectsInvalidPrefixValue() {
        Catalog catalog = connectContext.getGlobalStateMgr().getCatalogMgr().getCatalogByName("hive0");
        String before = catalog.getConfig().get("ranger.plugin.hive.catalog_prefix.enabled");
        Exception e = Assertions.assertThrows(Exception.class, () -> UtFrameUtils.parseStmtWithNewParser(
                "alter catalog hive0 set (\"ranger.plugin.hive.catalog_prefix.enabled\" = \"ture\");",
                connectContext));
        Assertions.assertTrue(e.getMessage().contains("ranger.plugin.hive.catalog_prefix.enabled"), e.getMessage());
        Assertions.assertEquals(before, catalog.getConfig().get("ranger.plugin.hive.catalog_prefix.enabled"),
                "a rejected ALTER must leave the catalog untouched");
    }

    /** ALTER CATALOG rebuilds the controller; on the leader the new value is live when the statement returns. */
    @Test
    public void testAlterCatalogRebuildsHiveControllerWithPrefix(@Mocked RangerBasePlugin rangerPlugin) throws Exception {
        new Expectations() {
            {
                rangerPlugin.init();
                minTimes = 0;
            }
        };

        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "alter catalog hive0 set (\"ranger.plugin.hive.service.name\" = \"hive0\", " +
                        "\"ranger.plugin.hive.catalog_prefix.enabled\" = \"true\");",
                connectContext), connectContext);
        AccessController enabled = Authorizer.getInstance().getAccessControlOrDefault("hive0");
        Assertions.assertInstanceOf(RangerHiveAccessController.class, enabled);
        Assertions.assertEquals("hive0_user", databaseSentToRanger(enabled, rangerPlugin));

        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "alter catalog hive0 set (\"ranger.plugin.hive.catalog_prefix.enabled\" = \"false\");",
                connectContext), connectContext);
        AccessController disabled = Authorizer.getInstance().getAccessControlOrDefault("hive0");
        Assertions.assertNotSame(enabled, disabled, "ALTER CATALOG must install a new controller instance");
        Assertions.assertEquals("user", databaseSentToRanger(disabled, rangerPlugin));
    }

    /** An ALTER the connector rejects must leave the catalog's registered controller untouched. */
    @Test
    public void testRejectedAlterKeepsRegisteredController(@Mocked RangerBasePlugin rangerPlugin) throws Exception {
        new Expectations() {
            {
                rangerPlugin.init();
                minTimes = 0;
            }
        };
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "alter catalog hive0 set (\"ranger.plugin.hive.service.name\" = \"hive0\", " +
                        "\"ranger.plugin.hive.catalog_prefix.enabled\" = \"false\");",
                connectContext), connectContext);
        AccessController before = Authorizer.getInstance().getAccessControlOrDefault("hive0");

        // e.g. a connector with a strict property list refusing a key; JMockit reverts this after the test
        new MockUp<ConnectorFactory>() {
            @Mock
            public Connector createRealConnector(ConnectorContext context) {
                throw new StarRocksConnectorException("property rejected by the connector");
            }
        };
        Assertions.assertThrows(Exception.class, () -> DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "alter catalog hive0 set (\"ranger.plugin.hive.catalog_prefix.enabled\" = \"true\");",
                connectContext), connectContext));
        AccessController after = Authorizer.getInstance().getAccessControlOrDefault("hive0");
        Assertions.assertSame(before, after, "a failed ALTER must not install the new controller");
        Assertions.assertEquals("user", databaseSentToRanger(after, rangerPlugin));
    }

    /** A follower that already initialized the catalog must pick the replayed property up immediately. */
    @Test
    public void testReplayAlterCatalogRefreshesRegisteredController(@Mocked RangerBasePlugin rangerPlugin)
            throws Exception {
        new Expectations() {
            {
                rangerPlugin.init();
                minTimes = 0;
            }
        };
        // non-replay ALTER registers a controller, as a follower that has touched the catalog would have
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "alter catalog hive0 set (\"ranger.plugin.hive.service.name\" = \"hive0\", " +
                        "\"ranger.plugin.hive.catalog_prefix.enabled\" = \"false\");",
                connectContext), connectContext);
        AccessController before = Authorizer.getInstance().getAccessControlOrDefault("hive0");
        Assertions.assertEquals("user", databaseSentToRanger(before, rangerPlugin));

        Map<String, String> properties = new HashMap<>();
        properties.put("ranger.plugin.hive.catalog_prefix.enabled", "true");
        GlobalStateMgr.getCurrentState().getCatalogMgr().replayAlterCatalog(new AlterCatalogLog("hive0", properties));

        AccessController after = Authorizer.getInstance().getAccessControlOrDefault("hive0");
        Assertions.assertNotSame(before, after, "replay must replace the registered controller");
        Assertions.assertEquals("hive0_user", databaseSentToRanger(after, rangerPlugin));
    }

    /** A follower that never initialized the catalog keeps the lazy behavior: nothing registered by replay. */
    @Test
    public void testReplayAlterCatalogStaysLazyWhenNothingRegistered(@Mocked RangerBasePlugin rangerPlugin)
            throws Exception {
        new Expectations() {
            {
                rangerPlugin.init();
                minTimes = 0;
            }
        };
        Authorizer.getInstance().removeAccessControl("hive0");
        Map<String, String> properties = new HashMap<>();
        properties.put("ranger.plugin.hive.service.name", "hive0");
        properties.put("ranger.plugin.hive.catalog_prefix.enabled", "true");
        GlobalStateMgr.getCurrentState().getCatalogMgr().replayAlterCatalog(new AlterCatalogLog("hive0", properties));
        Assertions.assertFalse(Authorizer.getInstance().catalogToAccessControl.containsKey("hive0"),
                "replay must not eagerly create a controller for a catalog this FE never initialized");
    }

    /** Runs one check as a non-root user and returns the `database` resource the plugin received. */
    private static String databaseSentToRanger(AccessController controller, RangerBasePlugin rangerPlugin) {
        ConnectContext ctx = new ConnectContext();
        ctx.setCurrentUserIdentity(new UserIdentity("u1", "%"));
        // the fully mocked plugin returns null from isAccessAllowed, which the controller reports as denied
        Assertions.assertThrows(AccessDeniedException.class,
                () -> controller.checkDbAction(ctx, "hive0", "user", PrivilegeType.CREATE_TABLE));
        List<RangerAccessRequest> requests = new ArrayList<>();
        new Verifications() {
            {
                rangerPlugin.isAccessAllowed(withCapture(requests));
            }
        };
        Assertions.assertFalse(requests.isEmpty(), "the check must reach the Ranger plugin");
        return requests.get(requests.size() - 1).getResource().getValue("database").toString();
    }
}
