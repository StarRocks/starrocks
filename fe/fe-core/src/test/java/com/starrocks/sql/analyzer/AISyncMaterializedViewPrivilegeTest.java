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

package com.starrocks.sql.analyzer;

import com.starrocks.common.Config;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.PlanTestNoneDBBase;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class AISyncMaterializedViewPrivilegeTest extends PlanTestNoneDBBase {
    private static final String DATABASE = "ai_sync_mv_auth";
    private static String previousEndpoint;
    private static String previousModel;
    private static String previousProvider;

    @BeforeAll
    public static void setUpTable() throws Exception {
        previousEndpoint = Config.ai_default_chat_endpoint;
        previousModel = Config.ai_default_chat_model;
        previousProvider = Config.ai_default_chat_provider;
        Config.ai_default_chat_endpoint = "https://models.example.test/v1/chat/completions";
        Config.ai_default_chat_model = "sync-mv-test-model";
        Config.ai_default_chat_provider = "openai_compatible";
        starRocksAssert.withDatabase(DATABASE).useDatabase(DATABASE);
        starRocksAssert.withTable("CREATE TABLE prompts (id BIGINT, prompt VARCHAR(100)) "
                + "DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')");
    }

    @AfterAll
    public static void restoreAIConfiguration() {
        Config.ai_default_chat_endpoint = previousEndpoint;
        Config.ai_default_chat_model = previousModel;
        Config.ai_default_chat_provider = previousProvider;
    }

    @Test
    public void testCreateRejectsAIWithoutAIProjectExecution() {
        SemanticException error = Assertions.assertThrows(SemanticException.class,
                () -> analyze("CREATE MATERIALIZED VIEW ai_mv AS "
                        + "SELECT id, ai_complete(prompt) AS answer FROM prompts"));
        Assertions.assertTrue(error.getMessage().contains("Synchronous materialized view"), error.getMessage());
    }

    @Test
    public void testOrdinarySyncMVDefinitionsRemainAllowed() {
        Assertions.assertDoesNotThrow(() -> analyze("CREATE MATERIALIZED VIEW ordinary_mv AS "
                + "SELECT id, upper(prompt) AS answer FROM prompts WHERE prompt != ''"));
    }

    private static void analyze(String sql) {
        Analyzer.analyze(SqlParser.parse(sql, connectContext.getSessionVariable()).get(0), connectContext);
    }
}
