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

import com.starrocks.catalog.Database;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionName;
import com.starrocks.catalog.SqlFunction;
import com.starrocks.common.Config;
import com.starrocks.context.ai.AIProviderType;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.expression.CastExpr;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.NullLiteral;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.type.ArrayType;
import com.starrocks.type.FileType;
import com.starrocks.type.FloatType;
import com.starrocks.type.JsonType;
import com.starrocks.type.Type;
import com.starrocks.type.VarcharType;
import com.starrocks.utframe.AIProviderTestUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Map;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class AIFunctionAnalyzerTest {
    @BeforeAll
    public static void beforeClass() throws Exception {
        AnalyzeTestUtil.init();
    }

    @BeforeEach
    public void setUpDefaultProvider() {
        AIProviderTestUtil.setChatProperty("endpoint", "https://models.example.test/v1/chat/completions");
        AIProviderTestUtil.setChatProperty("model", "default-model");
        AIProviderTestUtil.setChatProperty("protocol", "openai");
    }

    @AfterEach
    public void clearDefaultProviders() {
        AIProviderTestUtil.clearDefaults();
    }

    @Test
    public void testEmbeddingSignaturesAndOptions() {
        AIProviderTestUtil.setEmbeddingProperty("endpoint", "https://models.example.test/v1/embeddings");
        AIProviderTestUtil.setEmbeddingProperty("model", "embedding-model");
        AIProviderTestUtil.setEmbeddingProperty("protocol", "openai");
        for (String args : new String[] {"'text'", "input => 'text', options => json_object('dimensions', 3)",
                "'text', '', 'model'", "'text', '', 'model', json_object('dimensions', 3)"}) {
            QueryStatement statement = (QueryStatement) analyzeSuccess("select ai_embed(" + args + ")");
            Assertions.assertEquals(new ArrayType(FloatType.FLOAT),
                    ((SelectRelation) statement.getQueryRelation()).getOutputExpression().get(0).getType());
        }
        analyzeSuccess("select ai_embed(ta, '', 'model') from tall");
        analyzeSuccess("select ai_embed(input => 'text', options => cast(null as json))");
        analyzeFail("select ai_embed(input => 'text', options => json_object('value', ta)) from tall", "constant options");
        analyzeFail("select ai_embed(to_bitmap(1))", "No matching function");
    }

    @Test
    public void testEmbeddingRequiresIndependentEndpoint() {
        analyzeFail("select ai_embed('prompt')", "SET <provider_name> AS DEFAULT AI PROVIDER");
    }

    @Test
    public void testTypedFileOverloadsAndOptions() {
        AIProviderTestUtil.setEmbeddingProperty("endpoint", "https://models.example.test/v1/embeddings");
        AIProviderTestUtil.setEmbeddingProperty("model", "embedding-model");
        AIProviderTestUtil.setEmbeddingProperty("protocol", "openai");
        Expr prompt = new StringLiteral("describe");
        Expr model = new StringLiteral("model");
        Expr defaults = new StringLiteral("");
        Expr options = ((SelectRelation) ((QueryStatement) analyzeSuccess("select json_object('temperature', 0.5)"))
                .getQueryRelation()).getOutputExpression().get(0);
        assertTypedFunctionId("ai_complete", 200001, prompt, fileSlot());
        assertTypedFunctionId("ai_complete", 200001, prompt, fileSlot(), defaults, model, options);
        assertTypedFunctionId("ai_complete", 200001, prompt, fileSlot(), defaults, model, new NullLiteral());
        assertTypedFunctionId("ai_embed", 200003, fileSlot());
        assertTypedFunctionId("ai_embed", 200003, fileSlot(), defaults, model, options);
    }

    @Test
    public void testFileOptionsRequireConstantJson() {
        AIProviderTestUtil.setEmbeddingProperty("endpoint", "https://models.example.test/v1/embeddings");
        AIProviderTestUtil.setEmbeddingProperty("model", "embedding-model");
        AIProviderTestUtil.setEmbeddingProperty("protocol", "openai");
        SlotRef options = new SlotRef(null, "options", "options");
        options.setType(JsonType.JSON);
        Expr defaults = new StringLiteral("");
        SemanticException completeError = Assertions.assertThrows(SemanticException.class,
                () -> analyzeTypedFunction("ai_complete", new StringLiteral("prompt"), fileSlot(), defaults, defaults, options));
        Assertions.assertTrue(completeError.getMessage().contains("constant options"));
        SemanticException embeddingError = Assertions.assertThrows(SemanticException.class,
                () -> analyzeTypedFunction("ai_embed", fileSlot(), defaults, defaults, options));
        Assertions.assertTrue(embeddingError.getMessage().contains("constant options"));
    }

    private static SlotRef fileSlot() {
        SlotRef slot = new SlotRef(null, "file", "file");
        slot.setType(FileType.FILE);
        return slot;
    }

    private static FunctionCallExpr analyzeTypedFunction(String name, Expr... arguments) {
        FunctionCallExpr call = new FunctionCallExpr(name, Arrays.asList(arguments));
        ExpressionAnalyzer.analyzeExpressionIgnoreSlot(call, AnalyzeTestUtil.getConnectContext());
        return call;
    }

    private static void assertTypedFunctionId(String name, long expectedId, Expr... arguments) {
        Function function = analyzeTypedFunction(name, arguments).getFn();
        Assertions.assertEquals(expectedId, function.getFunctionId());
        Assertions.assertTrue(function.isAI());
        Assertions.assertTrue(Arrays.asList(function.getArgs()).contains(FileType.FILE));
    }

    @ParameterizedTest
    @ValueSource(strings = {"ai_complete", "ai_embed"})
    public void testAIProviderSelectorRequiresConstant(String function) throws Exception {
        analyzeFail("select " + function + "('prompt', ta, 'model') from tall", "constant provider");
        for (String provider : new String[] {"'  '", "concat(' ', ' ')"}) {
            analyzeFail("select " + function + "('prompt', " + provider + ", 'model')", "whitespace");
        }
        analyzeFail("select " + function + "('prompt', 'missing_provider', 'model')", "does not exist");
        String name = "provider_for_analysis";
        AIProviderType type = function.equals("ai_embed") ? AIProviderType.EMBEDDING : AIProviderType.CHAT;
        GlobalStateMgr.getCurrentState().getAIProviderMgr().createProvider(name, type,
                Map.of("endpoint", "https://models.example.test/v1/inference", "model", "provider-model"), "");
        try {
            for (String provider : new String[] {"'provider_for_analysis'", "concat('provider_', 'for_analysis')"}) {
                analyzeSuccess("select " + function + "('prompt', " + provider + ", 'model')");
                analyzeSuccess("select " + function + "('prompt', " + provider + ", 'model', json_object())");
            }
        } finally {
            GlobalStateMgr.getCurrentState().getAIProviderMgr().dropProvider(name, true);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"[]", "['a', NULL]", "['  ']", "cast([] as array<varchar>)",
            "cast(['a', NULL] as array<varchar>)", "cast(['  '] as array<varchar>)"})
    public void testHelperArraysRejectInvalidConstantContents(String array) {
        for (String function : new String[] {"ai_classify", "ai_extract", "ai_redact"}) {
            Assertions.assertAll(
                    () -> analyzeFail("select " + function + "('p', " + array + ")", "nonempty ARRAY"),
                    () -> analyzeFail("select " + function + "('p', " + array + ", '', 'model')", "nonempty ARRAY"));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"ai_classify", "ai_extract", "ai_redact"})
    public void testHelperArrayLiteralsRejectJavaWhitespace(String function) {
        // Keep this set aligned with BE helper-array validation, including Java's control-character whitespace.
        int[] whitespace = {0x0009, 0x000A, 0x000B, 0x000C, 0x000D, 0x0020, 0x001C, 0x001D, 0x001E, 0x001F,
                0x1680, 0x2000, 0x2001, 0x2002, 0x2003, 0x2004, 0x2005, 0x2006, 0x2008, 0x2009, 0x200A,
                0x2028, 0x2029, 0x205F, 0x3000};
        for (int codePoint : whitespace) {
            String array = "['" + Character.toString(codePoint) + "']";
            Assertions.assertAll(String.format("%s U+%04X", function, codePoint),
                    () -> analyzeFail("select " + function + "('p', " + array + ")", "nonblank elements"),
                    () -> analyzeFail("select " + function + "('p', " + array + ", '', 'model')", "nonblank elements"));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"ai_classify", "ai_extract", "ai_redact"})
    public void testHelperArrayLiteralsAcceptNonblankUnicode(String function) {
        // Nonbreaking spaces and zero-width characters are not blank according to String.isBlank().
        for (String value : new String[] {"\u00A0", "\u2007", "\u202F", "\u200B", "\uFEFF", "\u2003label\u3000"}) {
            String array = "['" + value + "']";
            Assertions.assertAll(
                    () -> analyzeSuccess("select " + function + "('p', " + array + ")"),
                    () -> analyzeSuccess("select " + function + "('p', " + array + ", '', 'model')"));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"\u2003", "\u3000"})
    public void testHelperArrayStringCastsDeferBlankValidationToBackend(String whitespace) {
        String value = "[\"" + whitespace + "\"]";
        for (String function : new String[] {"ai_classify", "ai_extract", "ai_redact"}) {
            for (String suffix : new String[] {"", ", '', 'model'"}) {
                QueryStatement statement = (QueryStatement) analyzeSuccess(
                        "select " + function + "('p', cast('" + value + "' as array<varchar>)" + suffix + ")");
                FunctionCallExpr call = (FunctionCallExpr) ((SelectRelation) statement.getQueryRelation())
                        .getOutputExpression().get(0);
                CastExpr cast = Assertions.assertInstanceOf(CastExpr.class, call.getChild(1));
                Assertions.assertEquals(new ArrayType(VarcharType.VARCHAR), cast.getType());
                Assertions.assertEquals(value, Assertions.assertInstanceOf(StringLiteral.class, cast.getChild(0)).getValue());
                analyzeSuccess("select " + function + "('p', parse_json('" + value + "')" + suffix + ")");
            }
        }
    }

    @Test
    public void testHelperArraysMustBeConstant() {
        analyzeFail("select ai_classify('p', [ta]) from tall", "constant ARRAY");
        analyzeFail("select ai_extract('p', [ta]) from tall", "constant ARRAY");
        analyzeFail("select ai_redact('p', [ta]) from tall", "constant ARRAY");
    }

    @ParameterizedTest
    @CsvSource({
            "ai_classify, ta, tall",
            "ai_classify, v_json, tjson",
            "ai_extract, ta, tall",
            "ai_extract, v_json, tjson",
            "ai_redact, ta, tall",
            "ai_redact, v_json, tjson"
    })
    public void testHelperArrayArgumentsRequireConstantsBeforeImplicitCasts(
            String function, String column, String table) {
        Assertions.assertAll(
                () -> analyzeFail("select " + function + "('p', " + column + ") from " + table, "constant ARRAY"),
                () -> analyzeFail("select " + function + "('p', " + column + ", '', 'model') from " + table,
                        "constant ARRAY"));
    }

    @Test
    public void testHelperArrayArgumentsAcceptConstantCastsAndExpressions() {
        for (String function : new String[] {"ai_classify", "ai_extract", "ai_redact"}) {
            for (String argument : new String[] {"['a']", "cast(['a'] as array<varchar>)",
                    "'[\"a\"]'", "parse_json('[\"a\"]')",
                    "array_concat(['a'], ['b'])"}) {
                analyzeSuccess("select " + function + "('p', " + argument + ")");
                analyzeSuccess("select " + function + "('p', " + argument + ", '', 'model')");
            }
        }
    }

    @Test
    public void testTextHelperSignatures() {
        String[] names = {"ai_sentiment", "ai_classify", "ai_extract", "ai_fix_grammar", "ai_redact",
                "ai_translate", "ai_similarity", "ai_summarize", "ai_filter"};
        String[] args = {"'p'", "'p', ['a']", "'p', ['a']", "'p'", "'p', ['a']",
                "'p', 'en', 'zh'", "'p', 'q'", "'p'", "'p', 'condition'"};
        for (int i = 0; i < names.length; i++) {
            assertFunctionId("select " + names[i] + "(" + args[i] + ")", 200004 + i);
            assertFunctionId("select " + names[i] + "(" + args[i] + ", '', 'model')", 200004 + i);
        }
        for (int i = 0; i < names.length; i++) {
            assertFunctionId("select " + names[i] + "(" + args[i] + ", 'test_default_chat', 'model')", 200004 + i);
        }

    }

    private static void assertFunctionId(String sql, long id) {
        QueryStatement statement = (QueryStatement) analyzeSuccess(sql);
        FunctionCallExpr call = (FunctionCallExpr) ((SelectRelation) statement.getQueryRelation()).getOutputExpression().get(0);
        Assertions.assertEquals(id, call.getFn().getFunctionId());
        Assertions.assertTrue(call.getFn().isAI());
    }

    @Test
    public void testConcreteJsonOptionsType() {
        QueryStatement statement = (QueryStatement) analyzeSuccess(
                "select ai_complete('prompt', '', 'model', json_object('guardrails', true))");
        FunctionCallExpr call = (FunctionCallExpr) ((SelectRelation) statement.getQueryRelation())
                .getOutputExpression().get(0);
        Assertions.assertTrue(call.getFn().isAI());
        Assertions.assertEquals(JsonType.JSON, call.getFn().getArgs()[3]);
        Assertions.assertEquals(JsonType.JSON, call.getChild(3).getType());
    }

    @Test
    public void testExplicitModelMustBeConstant() {
        analyzeFail("select ai_complete(prompt => ta, model => cast(tb as varchar)) from tall", "constant model");
    }

    @Test
    public void testJsonOptions() {
        for (String options : new String[] {"json_object()", "NULL", "cast(NULL as json)",
                "json_object('guardrails', true, 'temperature', 0.7)",
                "json_object('payload', json_object('nested', parse_json('[1,true,null]')))",
                "parse_json('{\"dimensions\":512}')", "to_json(map{'dimensions':512})"}) {
            analyzeSuccess("select ai_complete(prompt => 'p', options => " + options + ")");
        }
        analyzeFail("select ai_complete(prompt => 'p', options => json_object('temperature', te)) from tall",
                "constant options");
        for (String options : new String[] {"map{}", "map{'guardrails':true}", "'{}'", "cast(NULL as varchar)"}) {
            analyzeFail("select ai_complete(prompt => 'p', options => " + options + ")");
        }
    }

    @Test
    public void testJsonContentValidationIsDeferredToExecution() {
        for (String options : new String[] {"json_object('model', 'override')", "parse_json('null')",
                "parse_json('[1,2]')", "parse_json('invalid json')"}) {
            analyzeSuccess("select ai_complete(prompt => 'p', options => " + options + ")");
        }
    }

    @Test
    public void testEmptyModelUsesDefaultAndBlankModelIsRejected() {
        analyzeFail("select ai_complete(prompt => 'p', model => '   ')", "whitespace");
        analyzeSuccess("select ai_complete(prompt => 'p', model => '')");
        analyzeSuccess("select ai_complete(prompt => 'p', model => NULL)");
        analyzeSuccess("select ai_complete(prompt => cast(null as varchar), model => 'model')");
        analyzeSuccess("select ai_complete(prompt => '', model => 'model')");
    }

    @Test
    public void testUnsupportedRelationalContexts() {
        Assertions.assertAll(
                () -> analyzeFail("select ai_complete(ta) from tall group by ai_complete(ta)", "GROUP BY"),
                () -> analyzeFail("select distinct ai_complete(ta) from tall", "SELECT DISTINCT"),
                () -> analyzeFail("select max(ai_complete(ta)) from tall", "aggregate function"),
                () -> analyzeFail("select row_number() over (partition by ai_complete(ta)) from tall",
                        "window function"));
    }

    @Test
    public void testConditionalEvaluationContextsRejectAI() {
        Assertions.assertAll(
                () -> analyzeFail("select if(true, ai_complete('p'), 'fallback')",
                        "conditional expression cannot contain AI function ai_complete"),
                () -> analyzeFail("select ifnull(ai_complete('p'), 'fallback')",
                        "conditional expression cannot contain AI function ai_complete"),
                () -> analyzeFail("select nullif(ai_complete('p'), 'x')",
                        "conditional expression cannot contain AI function ai_complete"),
                () -> analyzeFail("select coalesce(null, ai_complete('p'), 'fallback')",
                        "conditional expression cannot contain AI function ai_complete"),
                () -> analyzeFail("select case when true then ai_complete('p') else 'fallback' end",
                        "conditional expression cannot contain AI function ai_complete"),
                () -> analyzeFail("select case 'x' when ai_complete('p') then 'match' else 'fallback' end",
                        "conditional expression cannot contain AI function ai_complete"),
                () -> analyzeFail("select if(true, (select ai_complete('p')), 'fallback')",
                        "conditional expression cannot contain AI function ai_complete"),
                () -> analyzeFail("select case when true then (select ai_complete('p')) else 'fallback' end",
                        "conditional expression cannot contain AI function ai_complete"));

        analyzeSuccess("select if(true, 'value', 'fallback')");
        analyzeSuccess("select ifnull(null, 'fallback')");
        analyzeSuccess("select nullif('value', 'other')");
        analyzeSuccess("select coalesce(null, 'value', 'fallback')");
        analyzeSuccess("select case when true then 'value' else 'fallback' end");
        analyzeSuccess("select case 'x' when 'x' then 'match' else 'fallback' end");
        analyzeSuccess("select if(true, (select 'value'), 'fallback')");
        analyzeSuccess("select case when true then (select 'value') else 'fallback' end");
        analyzeSuccess("select ai_complete(if(true, 'prompt', 'fallback'))");
    }

    @Test
    public void testTableFunctionArgumentsRejectAI() {
        analyzeFail("select * from t0, unnest([ai_complete('p')])",
                "Table Function argument cannot contain AI function ai_complete");
        analyzeSuccess("select * from t0, unnest([1, 2, 3])");
    }

    @Test
    public void testLambdaBodiesRejectAIFunctions() {
        Assertions.assertAll(
                () -> analyzeFail("select array_map(x -> ai_complete('p'), ['a', 'b'])",
                        "Lambda function body cannot contain AI function ai_complete"),
                () -> analyzeFail("select array_map(x -> ai_complete(x), ['a', 'b'])",
                        "Lambda function body cannot contain AI function ai_complete"),
                () -> analyzeFail("select array_map(x -> concat(ai_complete(x), '!'), ['a', 'b'])",
                        "Lambda function body cannot contain AI function ai_complete"));
    }

    @Test
    public void testSqlUdfDefinitionRejectsAIFunctions() {
        boolean originalEnableUdf = Config.enable_udf;
        try {
            Config.enable_udf = true;
            analyzeFail("create function ai_wrapper_pr1(x string) returns ai_complete(x)",
                    "SQL UDF body cannot contain AI function ai_complete");
        } finally {
            Config.enable_udf = originalEnableUdf;
        }
    }

    @Test
    public void testLegacySqlUdfCannotReintroduceAIFunctions() {
        boolean originalEnableUdf = Config.enable_udf;
        SqlFunction function = new SqlFunction(
                new FunctionName("test", "legacy_ai_wrapper_pr1"),
                new Type[] {VarcharType.VARCHAR},
                VarcharType.VARCHAR,
                new String[] {"x"},
                "ai_complete(x)");
        Database database = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        try {
            Config.enable_udf = true;
            Database.replayCreateFunctionLog(function);
            Assertions.assertAll(
                    () -> analyzeFail("select legacy_ai_wrapper_pr1(ta) from tall",
                            "SQL UDF body cannot contain AI function ai_complete"),
                    () -> analyzeFail("select legacy_ai_wrapper_pr1(ta), legacy_ai_wrapper_pr1(ta) from tall",
                            "SQL UDF body cannot contain AI function ai_complete"),
                    () -> analyzeFail("select max(legacy_ai_wrapper_pr1(ta)) from tall",
                            "SQL UDF body cannot contain AI function ai_complete"),
                    () -> analyzeFail("select count(*) from tall group by legacy_ai_wrapper_pr1(ta)",
                            "SQL UDF body cannot contain AI function ai_complete"),
                    () -> analyzeFail("create materialized view legacy_ai_mv_pr1 distributed by random "
                                    + "as select legacy_ai_wrapper_pr1(ta) as answer from tall",
                            "SQL UDF body cannot contain AI function ai_complete"),
                    () -> analyzeFail("create table legacy_ai_generated_pr1 (k bigint, prompt varchar(20), "
                                    + "answer varchar as (legacy_ai_wrapper_pr1(prompt))) duplicate key(k) "
                                    + "distributed by hash(k) buckets 1 properties('replication_num'='1')",
                            "SQL UDF body cannot contain AI function ai_complete"));
        } finally {
            database.dropFunctionForRestore(function);
            Config.enable_udf = originalEnableUdf;
        }
    }

    @Test
    public void testMaterializedViewAndGeneratedColumnContexts() {
        Assertions.assertAll(
                () -> analyzeFail("create materialized view ai_mv distributed by random "
                        + "as select ai_complete(ta) as answer from tall", "nondeterministic function"),
                () -> analyzeFail("create table ai_generated (k bigint, prompt varchar(20), "
                        + "answer varchar as (ai_complete(prompt))) duplicate key(k) "
                        + "distributed by hash(k) buckets 1 properties('replication_num'='1')",
                        "Generated Column expression cannot contain AI function ai_complete"),
                () -> analyzeFail("alter table tall add column answer varchar as (ai_complete(ta))",
                        "Generated Column expression cannot contain AI function ai_complete"),
                () -> analyzeFail("alter table tall modify column tf varchar as (ai_complete(ta))",
                        "Generated Column expression cannot contain AI function ai_complete"));
    }

    @Test
    public void testMaterializedViewRejectsNestedNondeterministicFunctions() {
        Assertions.assertAll(
                () -> analyzeFail("create materialized view nested_ai_mv distributed by random "
                                + "as select concat(ai_complete(ta), '!') as answer from tall",
                        "nondeterministic function"),
                () -> analyzeFail("create materialized view nested_rand_mv distributed by random "
                                + "as select concat(rand(), '!') as answer from tall",
                        "nondeterministic function"));
    }

    @Test
    public void testMaterializedViewRejectsNondeterministicFunctionsInSetOperations() {
        Assertions.assertAll(
                () -> analyzeFail("create materialized view set_order_ai_mv distributed by random "
                                + "as select ta as x from tall union all select ta as x from tall "
                                + "order by ai_complete(x)",
                        "nondeterministic function"),
                () -> analyzeFail("create materialized view set_order_rand_mv distributed by random "
                                + "as select ta as x from tall union all select ta as x from tall "
                                + "order by concat(cast(rand() as varchar), x)",
                        "nondeterministic function"),
                () -> analyzeFail("create materialized view values_nested_ai_mv distributed by random "
                                + "as select ta as x from tall union all "
                                + "select x from (values (concat(ai_complete('p'), '!'))) v(x)",
                        "nondeterministic function"),
                () -> analyzeFail("create materialized view values_nested_rand_mv distributed by random "
                                + "as select ta as x from tall union all "
                                + "select x from (values (concat(cast(rand() as varchar), '!'))) v(x)",
                        "nondeterministic function"));
    }

    @Test
    public void testCorrelatedAIFunctionPredicatesAreRejectedWithoutFalsePositives() {
        Assertions.assertAll(
                () -> analyzeSuccess("select count(*) from t0 having ai_complete('p') = 'x'"),
                () -> analyzeSuccess("select * from t0 where exists "
                        + "(select 1 from t1 where ai_complete(cast(t1.v4 as varchar)) = 'x')"),
                () -> analyzeSuccess("select * from t0 join t1 "
                        + "on ai_complete(cast(t0.v1 as varchar)) = cast(t1.v4 as varchar)"),
                () -> analyzeSuccess("select * from t0 join t1 on t0.v1 = t1.v4 "
                        + "where ai_complete(cast(t0.v2 as varchar)) = 'x'"),
                () -> analyzeFail("select * from t0 where exists "
                        + "(select 1 from t1 where ai_complete(cast(t0.v1 as varchar)) = 'x')",
                        "correlated AI function ai_complete"),
                () -> analyzeFail("select * from t0 where exists (select 1 from t1 "
                        + "where ai_complete(cast(t1.v4 as varchar)) = cast(t0.v1 as varchar))",
                        "correlated AI function ai_complete"),
                () -> analyzeFail("select * from t0 where exists (select 1 from t1 "
                        + "where ai_complete(cast(t1.v4 as varchar)) = 'x' and t1.v5 = t0.v1)",
                        "correlated AI function ai_complete"),
                () -> analyzeFail("select * from t0 where exists (select 1 from t1 join t2 "
                        + "on ai_complete(cast(t0.v1 as varchar)) = cast(t1.v4 as varchar))",
                        "correlated AI function ai_complete"),
                () -> analyzeFail("select * from t0 where exists (select t1.v4 from t1 group by t1.v4 "
                        + "having ai_complete(cast(t0.v1 as varchar)) = 'x')",
                        "HAVING clause cannot contain correlated AI function ai_complete"),
                () -> analyzeFail("select * from t0 where exists (select t1.v4 from t1 group by t1.v4 "
                        + "having ai_complete(cast(t1.v4 as varchar)) = cast(t0.v1 as varchar))",
                        "HAVING clause cannot contain correlated AI function ai_complete"));
    }

    @Test
    public void testCorrelatedSelectAndOrderByAIFunctionsAreRejected() {
        Assertions.assertAll(
                () -> analyzeFail("select * from t0 where exists (select "
                                + "concat(ai_complete(cast(t1.v4 as varchar)), cast(t0.v1 as varchar)) from t1)",
                        "SELECT list cannot contain correlated AI function ai_complete"),
                () -> analyzeFail("select * from t0 where exists (select t1.v4 from t1 order by "
                                + "concat(ai_complete(cast(t1.v4 as varchar)), cast(t0.v1 as varchar)))",
                        "ORDER BY clause cannot contain correlated AI function ai_complete"),
                () -> analyzeSuccess("select * from t0 where exists (select "
                        + "concat(ai_complete(cast(t1.v4 as varchar)), cast(t1.v5 as varchar)) from t1)"),
                () -> analyzeSuccess("select * from t0 where exists (select t1.v4 from t1 order by "
                        + "concat(ai_complete(cast(t1.v4 as varchar)), cast(t1.v5 as varchar)))"),
                () -> analyzeSuccess("select * from t0 where exists (select t1.v4 as inner_value from t1 order by "
                        + "concat(ai_complete(cast(t1.v5 as varchar)), cast(inner_value as varchar)))"));
    }

    @Test
    public void testCorrelatedQueryBlockRejectsAIAcrossClauses() {
        Assertions.assertAll(
                () -> analyzeFail("select * from t0 where exists (select "
                                + "ai_complete(cast(t1.v4 as varchar)) from t1 where t1.v5 = t0.v1)",
                        "SELECT list cannot contain correlated AI function ai_complete"),
                () -> analyzeFail("select * from t0 where exists (select t0.v1 from t1 "
                                + "where ai_complete(cast(t1.v4 as varchar)) = 'x')",
                        "WHERE clause cannot contain correlated AI function ai_complete"),
                () -> analyzeFail("select * from t0 where exists (select t1.v4 from t1 "
                                + "where t1.v5 = t0.v1 group by t1.v4 "
                                + "having ai_complete(cast(t1.v4 as varchar)) = 'x')",
                        "HAVING clause cannot contain correlated AI function ai_complete"),
                () -> analyzeFail("select * from t0 where exists (select t1.v4 from t1 "
                                + "where t1.v5 = t0.v1 order by ai_complete(cast(t1.v4 as varchar)))",
                        "ORDER BY clause cannot contain correlated AI function ai_complete"),
                () -> analyzeFail("select * from t0 where exists (select 1 from t1 join t2 "
                                + "on ai_complete(cast(t1.v4 as varchar)) = cast(t2.v7 as varchar) "
                                + "where t1.v5 = t0.v1)",
                        "JOIN ON clause cannot contain correlated AI function ai_complete"),
                () -> analyzeFail("select * from t0 where exists (select "
                                + "ai_complete(cast(t1.v4 as varchar)) from t1 join t2 "
                                + "on t1.v4 = t2.v7 and t2.v8 = t0.v1)",
                        "SELECT list cannot contain correlated AI function ai_complete"));
    }

    @Test
    public void testCorrelatedQueryBlockKeepsIndependentAIBoundaries() {
        Assertions.assertAll(
                () -> analyzeSuccess("select * from t0 where ai_complete(cast(t0.v1 as varchar)) in "
                        + "(select cast(t1.v4 as varchar) from t1 where t1.v5 = t0.v2)"),
                () -> analyzeSuccess("select * from t0 where ai_complete(cast(t0.v1 as varchar)) not in "
                        + "(select cast(t1.v4 as varchar) from t1 where t1.v5 = t0.v2)"),
                () -> analyzeSuccess("select * from t0 where exists (select "
                        + "concat(cast(t0.v1 as varchar), (select ai_complete('p'))) from t1)"),
                () -> analyzeSuccess("select * from t0 where exists (select t0.v1 from "
                        + "(select ai_complete(cast(t1.v4 as varchar)) as answer from t1) d)"),
                () -> analyzeSuccess("select * from t0 where exists (with w as "
                        + "(select ai_complete(cast(t1.v4 as varchar)) as answer from t1) "
                        + "select t0.v1 from w)"),
                () -> analyzeFail("select * from t0 where exists (with w as "
                                + "(select ai_complete(cast(t0.v1 as varchar)) as answer) select * from w)",
                        "SELECT list cannot contain correlated AI function ai_complete"),
                () -> analyzeSuccess("select ai_complete(ta) as answer from tall order by answer"),
                () -> analyzeSuccess("select ai_complete('p'), array_map(x -> x + 1, [1, 2])"),
                () -> analyzeFail("select * from t0 where exists (select 1 from t1 where exists "
                                + "(select ai_complete(cast(t1.v4 as varchar)) from t2))",
                        "SELECT list cannot contain correlated AI function ai_complete"));
    }

    @ParameterizedTest
    @CsvSource(value = {"ai_complete('p', NULL)|200000", "ai_complete('p', NULL, NULL)|200000",
            "ai_embed(NULL)|200002"}, delimiter = '|')
    public void testUntypedNullUsesBuiltinOverloadResolution(String expression, long expectedId) {
        AIProviderTestUtil.setEmbeddingProperty("endpoint", "https://models.example.test/v1/embeddings");
        AIProviderTestUtil.setEmbeddingProperty("model", "embedding-model");
        AIProviderTestUtil.setEmbeddingProperty("protocol", "openai");
        assertFunctionId("select " + expression, expectedId);
    }

    @Test
    public void testTypedNullDisambiguatesTextFileAndOptions() {
        AIProviderTestUtil.setEmbeddingProperty("endpoint", "https://models.example.test/v1/embeddings");
        AIProviderTestUtil.setEmbeddingProperty("model", "embedding-model");
        AIProviderTestUtil.setEmbeddingProperty("protocol", "openai");
        assertFunctionId("select ai_complete('p', '', 'm', cast(NULL as json))", 200000);
        assertTypedFunctionId("ai_complete", 200001, new StringLiteral("p"), NullLiteral.create(FileType.FILE));
        assertFunctionId("select ai_embed(cast(NULL as varchar))", 200002);
        assertFunctionId("select ai_embed('p', '', 'm', cast(NULL as json))", 200002);
        assertTypedFunctionId("ai_embed", 200003, NullLiteral.create(FileType.FILE));
    }

    @ParameterizedTest
    @ValueSource(strings = {"NULL", "cast(NULL as varchar)", "nullif('x', 'x')", "if(true, NULL, 'x')"})
    public void testHelperNullInputsKeepTheirOriginalSemantics(String value) {
        String[] names = {"ai_sentiment", "ai_classify", "ai_extract", "ai_fix_grammar", "ai_redact",
                "ai_translate", "ai_similarity", "ai_summarize", "ai_filter"};
        String[] suffixes = {"", ", ['a']", ", ['a']", "", ", ['a']", ", 'en', 'zh'", ", 'q'", "", ", 'q'"};
        for (int i = 0; i < names.length; i++) {
            analyzeSuccess("select " + names[i] + "(" + value + suffixes[i] + ")");
            analyzeSuccess("select " + names[i] + "(" + value + suffixes[i] + ", '', 'model')");
            analyzeSuccess("select " + names[i] + "(" + value + suffixes[i] + ", 'test_default_chat', 'model')");
        }
        analyzeSuccess("select ai_translate('hello', " + value + ", 'zh')");
        analyzeSuccess("select ai_translate('hello', 'en', " + value + ")");
        analyzeFail("select ai_classify('text', cast(NULL as array<varchar>))", "nonempty ARRAY");
        analyzeSuccess("select ai_complete(cast(NULL as varchar), '', 'model')");
    }

    @Test
    public void testTranslationAndNestedNullOptions() {
        analyzeSuccess("select ai_translate('hello', '', 'zh')");
        analyzeSuccess("select ai_translate(ta, '', 'zh') from tall");
        analyzeSuccess("select ai_translate('hello', ta, 'zh') from tall");
        analyzeSuccess("select ai_complete(prompt => 'p', options => json_object('nested', json_object('value', NULL)))");
    }
    @Test
    public void testRemovedCustomFunctionsHaveNoOverloads() {
        analyzeFail("select ai_custom_query('provider', 'prompt')", "No matching function");
        analyzeFail("select ai_custom_embedding('provider', 'text')", "No matching function");
    }
    @Test
    public void testNullResolutionDoesNotDependOnConstantFolding() {
        var session = AnalyzeTestUtil.getConnectContext().getSessionVariable();
        boolean previous = session.isDisableFunctionFoldConstants();
        session.setDisableFunctionFoldConstants(true);
        try {
            analyzeSuccess("select ai_complete(nullif('x', 'x'))");
            assertFunctionId("select ai_complete('a', NULL)", 200000);
            Assertions.assertTrue(session.isDisableFunctionFoldConstants());
        } finally {
            session.setDisableFunctionFoldConstants(previous);
        }
    }
}
