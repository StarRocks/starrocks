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

package com.starrocks.sql.plan;

import com.starrocks.common.Config;
import com.starrocks.context.ai.AIProviderType;
import com.starrocks.qe.AIQueryAdmission;
import com.starrocks.server.AIProviderMgr;
import com.starrocks.sql.common.AIModelConfigs;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.util.Map;

public class AIInputTokenPlanTest extends PlanTestBase {
    private static String oldEndpoint;
    private static String oldModel;
    private static String oldProvider;

    @BeforeAll
    public static void configureAI() {
        oldEndpoint = Config.ai_default_chat_endpoint;
        oldModel = Config.ai_default_chat_model;
        oldProvider = Config.ai_default_chat_provider;
        Config.ai_default_chat_endpoint = "https://unit.test.example/v1/chat/completions";
        Config.ai_default_chat_model = "unit-test-model";
        Config.ai_default_chat_provider = AIModelConfigs.OPENAI_COMPATIBLE_PROVIDER;
    }

    @AfterAll
    public static void restoreAI() {
        Config.ai_default_chat_endpoint = oldEndpoint;
        Config.ai_default_chat_model = oldModel;
        Config.ai_default_chat_provider = oldProvider;
    }

    @Test
    public void testLiteralEstimateInExplainCosts() throws Exception {
        ExecPlan plan = getExecPlan("select ai_complete('hello')");
        Assertions.assertTrue(plan.getExplainString(TExplainLevel.COSTS).contains("AI INPUT TOKENS: 21"));
        Assertions.assertFalse(plan.getExplainString(TExplainLevel.NORMAL).contains("AI INPUT TOKENS"));
    }

    @Test
    public void testUnknownDoesNotExposePartialEstimate() throws Exception {
        ExecPlan plan = getExecPlan("select ai_complete('hello'), ai_summarize('hello')");
        Assertions.assertTrue(plan.getExplainString(TExplainLevel.COSTS).contains("AI INPUT TOKENS: UNKNOWN"));
    }

    @Test
    public void testOrdinaryQueriesAreUnchanged() throws Exception {
        Assertions.assertFalse(getExecPlan("select 1").getExplainString(TExplainLevel.COSTS)
                .contains("AI INPUT TOKENS"));
    }

    @Test
    public void testProviderUsesTheSamePromptEstimate() throws Exception {
        AIProviderMgr manager = connectContext.getGlobalStateMgr().getAIProviderMgr();
        manager.createProvider("token_test_chat", AIProviderType.CHAT,
                Map.of("endpoint", "https://unit.test.example/v1/chat/completions", "model", "chat-model"), "");
        manager.createProvider("token_test_embed", AIProviderType.EMBEDDING,
                Map.of("endpoint", "https://unit.test.example/v1/embeddings", "model", "embed-model"), "");
        try {
            Assertions.assertEquals(BigInteger.valueOf(26), getExecPlan("select ai_custom_query('token_test_chat', '中🙂abc')")
                    .getAIInputTokenEstimate().getTokens());
            Assertions.assertEquals(BigInteger.TEN, getExecPlan("select ai_custom_embedding('token_test_embed', '中🙂abc')")
                    .getAIInputTokenEstimate().getTokens());
            Assertions.assertEquals(BigInteger.valueOf(26), getExecPlan("select ai_complete('other-model', '中🙂abc')")
                    .getAIInputTokenEstimate().getTokens());
        } finally {
            manager.dropProvider("token_test_chat", true);
            manager.dropProvider("token_test_embed", true);
        }
    }

    @Test
    public void testBudgetIsCapturedPerPhysicalPlan() throws Exception {
        long oldLimit = Config.ai_query_admission_max_estimated_input_tokens;
        try {
            Config.ai_query_admission_max_estimated_input_tokens = 10;
            ExecPlan original = getExecPlan("select ai_complete('hello')");
            Config.ai_query_admission_max_estimated_input_tokens = 21;
            ExecPlan rebuilt = getExecPlan("select ai_complete('hello')");
            Assertions.assertEquals(10, original.getAIInputTokenLimit());
            Assertions.assertEquals(21, rebuilt.getAIInputTokenLimit());
            Assertions.assertThrows(com.starrocks.common.StarRocksException.class, () -> AIQueryAdmission.check(original));
            Assertions.assertDoesNotThrow(() -> AIQueryAdmission.check(rebuilt));
        } finally {
            Config.ai_query_admission_max_estimated_input_tokens = oldLimit;
        }
    }

    @Test
    public void testActualGlobalAndLocalTopNScopes() throws Exception {
        long oldThreshold = connectContext.getSessionVariable().getAiTopnPushdownMaxGlobalLimit();
        boolean oldEnabled = connectContext.getSessionVariable().isEnableAiTopnPushdown();
        UtFrameUtils.addMockBackend(10002);
        try {
            connectContext.getSessionVariable().setEnableAiTopnPushdown(true);
            connectContext.getSessionVariable().setAiTopnPushdownMaxGlobalLimit(1000);
            String sql = "select k1, ai_complete('hello') from t7 order by k1 limit 5";
            AIInputTokenEstimate global = getExecPlan(sql).getAIInputTokenEstimate();
            Assertions.assertEquals(AIInputTokenEstimate.Status.ESTIMATED, global.getStatus(), global.toString());
            Assertions.assertEquals(BigInteger.valueOf(105), global.getTokens());
            connectContext.getSessionVariable().setAiTopnPushdownMaxGlobalLimit(0);
            AIInputTokenEstimate local = getExecPlan(sql).getAIInputTokenEstimate();
            Assertions.assertEquals(AIInputTokenEstimate.Status.UNKNOWN, local.getStatus(), local.toString());
        } finally {
            connectContext.getSessionVariable().setAiTopnPushdownMaxGlobalLimit(oldThreshold);
            connectContext.getSessionVariable().setEnableAiTopnPushdown(oldEnabled);
            UtFrameUtils.dropMockBackend(10002);
        }
    }

    @Test
    public void testOptionsAndNestedAIOutputsAreUnknown() throws Exception {
        for (String sql : new String[] {"select ai_complete('hello', map{'temperature':0.5})",
                "select ai_complete(ai_complete('hello'))"}) {
            Assertions.assertEquals(AIInputTokenEstimate.Status.UNKNOWN, getExecPlan(sql).getAIInputTokenEstimate().getStatus());
        }
    }
}
