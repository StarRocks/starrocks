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

import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.GlobalFunctionMgr;
import com.starrocks.context.ai.AIProvider;
import com.starrocks.context.ai.AIProviderType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.server.AIProviderMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.ExprToSql;
import com.starrocks.sql.ast.expression.ExprUtils;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.common.AIModelConfigs;
import com.starrocks.sql.parser.AstBuilder;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.type.FileType;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

// Exercise real parsing, overload resolution and argument analysis without starting an FE cluster.
public class AIFunctionArgumentsTest {
    private ConnectContext context;
    private AIProviderMgr providers;
    private MockedStatic<GlobalStateMgr> global;
    private MockedStatic<Authorizer> authorizer;

    @BeforeEach
    public void setUp() {
        FunctionSet functions = new FunctionSet();
        functions.init();
        GlobalStateMgr state = mock(GlobalStateMgr.class);
        context = mock(ConnectContext.class);
        when(context.getSessionVariable()).thenReturn(new SessionVariable());
        when(context.getGlobalStateMgr()).thenReturn(state);
        ConnectContext.set(context);
        when(state.getSqlParser()).thenReturn(new SqlParser(AstBuilder.getInstance()));
        when(state.getGlobalFunctionMgr()).thenReturn(mock(GlobalFunctionMgr.class));
        when(state.getLocalMetastore()).thenReturn(mock(LocalMetastore.class));
        when(state.getBuiltinFunctions()).thenReturn(functions.getBuiltinFunctions());
        when(state.getFunction(any(Function.class), any(Function.CompareMode.class)))
                .thenAnswer(call -> functions.getFunction(call.getArgument(0), call.getArgument(1)));
        global = mockStatic(GlobalStateMgr.class);
        global.when(GlobalStateMgr::getCurrentState).thenReturn(state);
        authorizer = mockStatic(Authorizer.class);
        providers = mock(AIProviderMgr.class);
        AIProvider chat = new AIProvider("chat-id", "chat_provider", AIProviderType.CHAT,
                Map.of("model", "chat-default", "endpoint", "https://example.test/v1/chat/completions",
                        "protocol", "openai"), "");
        AIProvider embedding = new AIProvider("embed-id", "embed_provider", AIProviderType.EMBEDDING,
                Map.of("model", "embed-default", "endpoint", "https://example.test/v1/embeddings",
                        "protocol", "openai"), "");
        when(providers.getDefaultProvider(AIProviderType.CHAT)).thenReturn(chat);
        when(providers.getDefaultProvider(AIProviderType.EMBEDDING)).thenReturn(embedding);
        when(providers.getProvider("chat_provider")).thenReturn(chat);
        when(providers.getProvider("embed_provider")).thenReturn(embedding);
        when(state.getAIProviderMgr()).thenReturn(providers);
    }

    @AfterEach
    public void tearDown() {
        authorizer.close();
        global.close();
        ConnectContext.remove();
    }

    private void typeSlots(Expr expression) {
        if (expression instanceof SlotRef slot) {
            slot.setType(slot.getColumnName().equals("file_column") ? FileType.FILE : VarcharType.VARCHAR);
        }
        expression.getChildren().forEach(this::typeSlots);
    }

    private FunctionCallExpr analyze(String sql) {
        FunctionCallExpr call = (FunctionCallExpr) SqlParser.parseSqlToExpr(sql, 0);
        typeSlots(call);
        ExpressionAnalyzer.analyzeExpressionIgnoreSlot(call, context);
        assertTrue(call.getFn().isAI(), sql);
        assertEquals(call.getFn().getNumArgs(), call.getChildren().size(), sql);
        return call;
    }

    private void reject(String sql, String message) {
        SemanticException error = assertThrows(SemanticException.class, () -> analyze(sql), sql);
        assertTrue(error.getMessage().contains(message), sql + ": " + error.getMessage());
    }

    private String stringArgument(FunctionCallExpr call, int index) {
        return ((StringLiteral) call.getChild(index)).getValue();
    }

    @Test
    public void testAllSignaturesAndPositionalPrefixes() {
        String[] calls = {"ai_complete('x')", "ai_complete('x',file_column)",
                "ai_embed('x')", "ai_embed(file_column)", "ai_sentiment('x')", "ai_classify('x',['a'])",
                "ai_extract('x',['a'])", "ai_fix_grammar('x')", "ai_redact('x',['a'])",
                "ai_translate('x','English','Chinese')", "ai_similarity('x','y')",
                "ai_summarize('x')", "ai_filter('x','condition')"};
        long[] ids = {200000, 200001, 200002, 200003, 200004, 200005, 200006, 200007, 200008,
                200009, 200010, 200011, 200012};
        for (int i = 0; i < calls.length; i++) {
            FunctionCallExpr defaults = analyze(calls[i]);
            assertEquals(ids[i], defaults.getFn().getFunctionId());
            assertTrue(AIModelConfigs.shouldUseDefaultProvider(defaults));
            int providerIndex = AIModelConfigs.getProviderArgument(defaults.getFn());
            int modelIndex = AIModelConfigs.getModelArgument(defaults.getFn());
            assertEquals(providerIndex + 1, modelIndex);
            assertEquals("provider", defaults.getFn().getArgNames()[providerIndex]);
            assertEquals("model", defaults.getFn().getArgNames()[modelIndex]);
            String provider = i == 2 || i == 3 ? "embed_provider" : "chat_provider";
            String prefix = calls[i].substring(0, calls[i].length() - 1);
            FunctionCallExpr withProvider = analyze(prefix + ", '" + provider + "')");
            assertFalse(AIModelConfigs.shouldUseDefaultProvider(withProvider));
            assertEquals(provider, AIModelConfigs.resolveProvider(providers, withProvider).getName());
            FunctionCallExpr withModel = analyze(prefix + ", '" + provider + "', 'explicit-model')");
            assertEquals("explicit-model", stringArgument(withModel, modelIndex));
            assertEquals(provider, stringArgument(withModel, providerIndex));
            if (withModel.getFn().getArgNames()[withModel.getFn().getNumArgs() - 1].equals("options")) {
                analyze(prefix + ", '" + provider + "', 'explicit-model', json_object())");
            }
            List<String> named = new ArrayList<>();
            for (int j = 0; j < withModel.getChildren().size(); j++) {
                named.add("`" + withModel.getFn().getArgNames()[j] + "` => "
                        + ExprToSql.toSql(withModel.getChild(j)));
            }
            Collections.reverse(named);
            FunctionCallExpr reordered = analyze(withModel.getFunctionName() + "(" + String.join(",", named) + ")");
            assertEquals(ids[i], reordered.getFn().getFunctionId());
            assertEquals(provider, stringArgument(reordered, providerIndex));
            assertEquals("explicit-model", stringArgument(reordered, modelIndex));
            List<String> nullDefaults = new ArrayList<>();
            for (int j = 0; j < defaults.getChildren().size(); j++) {
                String name = defaults.getFn().getArgNames()[j];
                String value = List.of("provider", "model", "options").contains(name)
                        ? "NULL" : ExprToSql.toSql(defaults.getChild(j));
                nullDefaults.add("`" + name + "` => " + value);
            }
            FunctionCallExpr withNullDefaults = analyze(defaults.getFunctionName()
                    + "(" + String.join(",", nullDefaults) + ")");
            assertEquals(ids[i], withNullDefaults.getFn().getFunctionId());
            assertTrue(AIModelConfigs.shouldUseDefaultProvider(withNullDefaults));
            assertEquals("", stringArgument(withNullDefaults, providerIndex));
            assertEquals("", stringArgument(withNullDefaults, modelIndex));
        }
    }

    @Test
    public void testExtractLabelsNamedArgumentsAndDefaults() {
        for (String sql : List.of("ai_extract(input => 'Alice', labels => ['name'])",
                "ai_extract(labels => ['name'], input => 'Alice')", "ai_extract('Alice', ['name'])")) {
            FunctionCallExpr call = analyze(sql);
            assertEquals(200006, call.getFn().getFunctionId());
            assertEquals("labels", call.getFn().getArgNames()[1]);
            assertTrue(AIModelConfigs.shouldUseDefaultProvider(call));
            assertEquals("chat_provider", AIModelConfigs.resolveProvider(providers, call).getName());
        }
        FunctionCallExpr explicit = analyze("ai_extract(model => 'explicit-model', labels => ['name'], provider => "
                + "'chat_provider', input => 'Alice')");
        assertFalse(AIModelConfigs.shouldUseDefaultProvider(explicit));
        assertEquals("chat_provider", stringArgument(explicit, 2));
        assertEquals("explicit-model", stringArgument(explicit, 3));
        reject("ai_extract(input => 'Alice', `keys` => ['name'])", "does not support parameter 'keys'");
        reject("ai_extract(labels => ['name'])", "No matching function");
        reject("ai_extract(input => 'Alice', labels => ['name'], labels => ['age'])", "No matching function");
        for (String array : List.of("[]", "['name', NULL]", "[' ']")) {
            reject("ai_extract(input => 'Alice', labels => " + array + ")", "nonempty ARRAY");
        }
    }

    @Test
    public void testRenamedArgumentsAndFileOverloads() {
        assertEquals(200000, analyze("ai_complete(prompt => 'hello')").getFn().getFunctionId());
        assertEquals(200001, analyze("ai_complete(file => file_column, prompt => 'hello')").getFn().getFunctionId());
        assertEquals(200002, analyze("ai_embed(input => 'hello')").getFn().getFunctionId());
        FunctionCallExpr file = analyze("ai_embed(input => file_column)");
        assertEquals(200003, file.getFn().getFunctionId());
        reject("ai_complete(input => 'hello')", "does not support parameter");
        reject("ai_embed(file => file_column)", "does not support parameter");
        reject("ai_complete()", "Missing required parameter");
    }

    @Test
    public void testExplicitEmptySelectorsAndOptionsUseDefaults() {
        for (String parameter : List.of("provider", "model")) {
            for (String value : List.of("NULL", "''", "cast(NULL as varchar)",
                    "concat('', '')", "if(true, NULL, 'x')")) {
                FunctionCallExpr call = analyze("ai_complete(prompt => 'x', " + parameter + " => " + value + ")");
                assertTrue(AIModelConfigs.shouldUseDefaultProvider(call));
                assertEquals("chat_provider", AIModelConfigs.resolveProvider(providers, call).getName());
                Expr model = ExprUtils.analyzeAndCastFold(call.getChild(AIModelConfigs.getModelArgument(call.getFn())));
                assertEquals("", ((StringLiteral) model).getValue());
            }
            reject("ai_complete(prompt => 'x', " + parameter + " => '   ')", "whitespace");
        }
        for (String value : List.of("NULL", "json_object()", "cast(NULL as json)", "parse_json('{}')",
                "if(true, cast(NULL as json), json_object('a',1))")) {
            analyze("ai_complete(prompt => 'x', options => " + value + ")");
        }
        for (String value : List.of("''", "'   '", "cast(NULL as varchar)", "concat('', '')",
                "map{}", "map{'guardrails':true}", "'{\"guardrails\":true}'")) {
            assertThrows(SemanticException.class, () -> analyze("ai_complete(prompt => 'x', options => " + value + ")"));
        }
        analyze("ai_complete('x', '', 'model')");
        analyze("ai_complete('x', 'chat_provider', '')");
        analyze("ai_complete('x', 'chat_provider', 'model', NULL)");
        analyze("ai_complete('x', NULL, NULL, json_object())");
        analyze("ai_sentiment('x', NULL, NULL)");
        reject("ai_complete('x', 'chat_provider', 'model', '')", "constant JSON");
    }

    @Test
    public void testConstantSelectorsAndOptions() {
        reject("ai_complete(prompt => 'x', model => model_column)", "constant model");
        reject("ai_complete(prompt => 'x', provider => provider_column)", "constant provider");
        reject("ai_complete(prompt => 'x', options => json_object('value', value_column))", "constant options");
        FunctionCallExpr call = analyze("ai_complete(prompt => prompt_column, "
                + "model => concat('my-', 'model'), provider => concat('chat_', 'provider'), "
                + "options => json_object('temperature',0.5))");
        assertEquals("chat_provider", AIModelConfigs.resolveProvider(providers, call).getName());
        analyze("ai_complete(prompt => 'x', options => json_object())");
        analyze("ai_complete(prompt => 'x', options => parse_json('{\"guardrails\":true,\"dimensions\":512}'))");
        analyze("ai_complete(prompt => 'x', options => to_json(map{'guardrails':true}))");
        analyze("ai_complete(prompt => 'x', options => parse_json('invalid json'))");
        analyze("ai_complete(prompt => 'x', options => parse_json('null'))");
        analyze("ai_complete(prompt => 'x', options => json_object('nested', json_object('value',NULL)))");
    }

    @Test
    public void testDefaultsSurviveCloneAndReanalysis() {
        for (String sql : List.of("ai_complete('hello')", "ai_complete(prompt => 'hello')",
                "ai_complete(model => 'chosen', prompt => 'hello')", "ai_embed(input => file_column)")) {
            FunctionCallExpr call = analyze(sql);
            FunctionCallExpr copy = (FunctionCallExpr) call.clone();
            ExpressionAnalyzer.analyzeExpressionIgnoreSlot(copy, context);
            assertEquals(call.getChildren(), copy.getChildren());
            assertTrue(AIModelConfigs.shouldUseDefaultProvider(copy));
            assertTrue(ExprUtils.analyzeAndCastFold(call.clone()) instanceof FunctionCallExpr);
        }
    }

    @Test
    public void testViewSqlRoundTripPreservesDefaultSemantics() {
        for (String sql : List.of("ai_complete('hello')", "ai_complete(prompt => 'hello')",
                "ai_complete(prompt => 'hello', model => 'chosen')",
                "ai_complete(prompt => 'hello', options => json_object('temperature',0.5))",
                "ai_complete(prompt => 'hello', provider => NULL, model => NULL, options => NULL)",
                "ai_complete(prompt => 'hello', provider => '', model => '', options => NULL)",
                "ai_embed(input => file_column, provider => 'embed_provider')")) {
            FunctionCallExpr call = analyze(sql);
            String storedSql = AstToSQLBuilder.toSQL(call);
            FunctionCallExpr restored = analyze(storedSql);
            assertEquals(call.getFn().getFunctionId(), restored.getFn().getFunctionId(), storedSql);
            for (int i = 0; i < call.getChildren().size(); i++) {
                Expr original = call.getChild(i);
                Expr reparsed = restored.getChild(i);
                assertEquals(AstToSQLBuilder.toSQL(original), AstToSQLBuilder.toSQL(reparsed), storedSql);
            }
            assertEquals(AIModelConfigs.resolveProvider(providers, call).getName(),
                    AIModelConfigs.resolveProvider(providers, restored).getName());
        }
    }

    @Test
    public void testInputNullUsesNamedArgumentConstraints() {
        reject("ai_complete(prompt => NULL)", "required parameter 'prompt' cannot be NULL");
        analyze("ai_complete(prompt => '', file => file_column)");
        analyze("ai_embed(input => cast(NULL as varchar))");
        reject("ai_translate(input => NULL, source_language => '', target_language => 'Chinese')",
                "required parameter 'input' cannot be NULL");
        reject("ai_translate(input => 'hello', source_language => NULL, target_language => 'Chinese')",
                "required parameter 'source_language' cannot be NULL");
        analyze("ai_translate(input => 'hello', source_language => '', target_language => 'Chinese')");
        reject("ai_embed(input => NULL)", "required parameter 'input' cannot be NULL");
        FunctionCallExpr call = analyze("ai_complete('x', NULL)");
        assertEquals(200000, call.getFn().getFunctionId());
        assertEquals("", stringArgument(call, AIModelConfigs.getProviderArgument(call.getFn())));
        // JSON contents, including reserved keys, are checked by BE before dispatch.
        analyze("ai_complete(prompt => 'x', options => json_object('model','bad'))");
    }
}
