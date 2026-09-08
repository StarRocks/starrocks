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

package com.starrocks.scheduler;

import com.google.common.collect.Maps;
import com.starrocks.authentication.MockTokenUtils;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.common.Config;
import com.starrocks.connector.MockedMetadataMgr;
import com.starrocks.connector.iceberg.MockIcebergMetadata;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVTestBase;
import com.starrocks.utframe.UtFrameUtils;
import okhttp3.mockwebserver.Dispatcher;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.Map;

public class IcebergBotRefreshTest extends MVTestBase {

    private static MockWebServer mockWebServer;
    private static ConnectContext connectContext;

    @BeforeAll
    public static void beforeClass() throws Exception {
        MVTestBase.beforeClass();
        mockWebServer = new MockWebServer();
        mockWebServer.start();
        Config.use_bot_for_background_tasks = true;
        Config.background_task_client_token_issuer_url = mockWebServer.url("/").toString();
        Config.background_task_client_id = "test_id";
        Config.background_task_client_password = "test_secret";
        Config.background_task_client_scope = "test_scope";
        Config.background_task_client_audience = "test_audience";
        Config.background_task_client_token_jwks_url = mockWebServer.url("/jwks").toString();
        Config.background_task_client_principal_field = "sub";
        GlobalStateMgr.getCurrentState().initTokenProvider();

        connectContext = UtFrameUtils.createDefaultCtx();
        GlobalStateMgr gsmMgr = connectContext.getGlobalStateMgr();
        MockedMetadataMgr metadataMgr = new MockedMetadataMgr(gsmMgr.getLocalMetastore(), gsmMgr.getConnectorMgr());
        gsmMgr.setMetadataMgr(metadataMgr);
        mockIcebergCatalog(metadataMgr);
    }

    @AfterAll
    public static void afterClass() throws Exception {
        if (mockWebServer != null) {
            mockWebServer.shutdown();
        }
    }

    private static void mockIcebergCatalog(MockedMetadataMgr metadataMgr) throws Exception {
        Map<String, String> properties = Maps.newHashMap();
        properties.put("type", "iceberg");
        properties.put("iceberg.catalog.type", "rest");
        properties.put("iceberg.catalog.uri", mockWebServer.url("/").toString());
        properties.put("iceberg.catalog.security", "JWT");
        GlobalStateMgr.getCurrentState().getCatalogMgr().createCatalog("iceberg", "iceberg0", "", properties);
        MockIcebergMetadata mockIcebergMetadata = new MockIcebergMetadata();
        metadataMgr.registerMockedMetadata(MockIcebergMetadata.MOCKED_ICEBERG_CATALOG_NAME, mockIcebergMetadata);
    }

    @Test
    public void testIcebergBotRefresh() throws Exception {
        // 1. Create bot user and grant permissions
        UserIdentity botUser = new UserIdentity("test_bot", "%");
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "CREATE USER 'test_bot'@'%' IDENTIFIED BY ''", connectContext), connectContext);
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "GRANT ALL ON ALL MATERIALIZED VIEWS IN DATABASE test TO 'test_bot'@'%'", connectContext), connectContext);
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "GRANT USAGE ON CATALOG iceberg0 TO 'test_bot'@'%'", connectContext), connectContext);

        // 2. Mock responses for MV creation and refresh
        String icebergTableResponse = "{\n" +
                "  \"metadata-location\": \"file:///tmp/warehouse/partitioned_db/t1/metadata/00000-abc.metadata.json\",\n" +
                "  \"metadata\": {\n" +
                "    \"format-version\": 2,\n" +
                "    \"table-uuid\": \"uuid\",\n" +
                "    \"location\": \"file:///tmp/warehouse/partitioned_db/t1\",\n" +
                "    \"last-updated-ms\": 1620000000000,\n" +
                "    \"last-column-id\": 3,\n" +
                "    \"schema\": {\n" +
                "      \"type\": \"struct\",\n" +
                "      \"fields\": [\n" +
                "        {\"id\": 1, \"name\": \"id\", \"required\": true, \"type\": \"int\"},\n" +
                "        {\"id\": 2, \"name\": \"data\", \"required\": false, \"type\": \"string\"},\n" +
                "        {\"id\": 3, \"name\": \"date\", \"required\": true, \"type\": \"date\"}\n" +
                "      ]\n" +
                "    },\n" +
                "    \"partition-spec\": [],\n" +
                "    \"current-snapshot-id\": -1,\n" +
                "    \"snapshots\": []\n" +
                "  }\n" +
                "}";
        MockTokenUtils mockTokenUtils = new MockTokenUtils();
        String token = mockTokenUtils.generateTestOIDCToken(100000, mockWebServer.url("/").toString(), "test_audience",
                botUser.getUser());
        String path = ClassLoader.getSystemClassLoader().getResource("auth").getPath();
        File file = new File(path + "/" + "signer-jwks.json");
        String jwksContent = new String(Files.readAllBytes(file.toPath()));

        final Dispatcher dispatcher = new Dispatcher() {
            @NotNull
            @Override
            public MockResponse dispatch(@NotNull RecordedRequest request) {
                switch (request.getPath()) {
                    case "/v1/config":
                        return new MockResponse().setResponseCode(200).setBody("{\"defaults\":{},\"overrides\":{}}");
                    case "/":
                        return new MockResponse().setResponseCode(200).setBody("{\"warehouse\":\"/tmp/warehouse\"}");
                    case "/v1/namespaces":
                        return new MockResponse().setResponseCode(200).setBody("{\"namespaces\": [[\"partitioned_db\"]]}");
                    case "/v1/namespaces/partitioned_db":
                        return new MockResponse().setResponseCode(200).setBody(
                                "{\"namespace\": [\"partitioned_db\"], \"properties\": {}}");
                    case "/v1/tables/partitioned_db/t1":
                        return new MockResponse().setBody(icebergTableResponse).setResponseCode(200);
                    case "/oauth2/token":
                        return new MockResponse().setBody("{\"id_token\":\"" + token + "\"}").setResponseCode(200);
                    case "/jwks":
                        return new MockResponse().setBody(jwksContent)
                                .setResponseCode(200).setHeader("Content-Type", "application/json");
                }
                return new MockResponse().setResponseCode(404);
            }
        };
        mockWebServer.setDispatcher(dispatcher);

        // 3. Create Materialized View
        String createMvSql = "create materialized view test.mv_iceberg_bot_2 " +
                "partition by date " +
                "distributed by hash(id) " +
                "refresh deferred manual " +
                "as select id, data, date from iceberg0.partitioned_db.t1;";
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(createMvSql, connectContext), connectContext);

        // 4. Refresh MV
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        MaterializedView mv = (MaterializedView) testDb.getTable("mv_iceberg_bot_2");
        Task task = TaskBuilder.buildMvTask(mv, "test");
        task.setUserIdentity(botUser);
        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
        taskRun.setConnectContext(connectContext);
        initAndExecuteTaskRun(taskRun);
    }
}