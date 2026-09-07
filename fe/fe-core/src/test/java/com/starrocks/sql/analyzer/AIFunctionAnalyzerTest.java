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
import com.starrocks.catalog.FunctionName;
import com.starrocks.catalog.SqlFunction;
import com.starrocks.common.Config;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.type.MapType;
import com.starrocks.type.Type;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class AIFunctionAnalyzerTest {
    @BeforeAll
    public static void beforeClass() throws Exception {
        AnalyzeTestUtil.init();
    }

    @BeforeEach
    public void setUpSystemChat() {
        Config.ai_default_chat_endpoint = "https://models.example.test/v1/chat/completions";
        Config.ai_default_chat_model = "default-model";
        Config.ai_default_chat_provider = "openai_compatible";
    }

    @AfterEach
    public void clearSystemChat() {
        Config.ai_default_chat_endpoint = "";
        Config.ai_default_chat_model = "";
        Config.ai_default_chat_provider = "";
    }

    @Test
    public void testAllOverloadsAndConcreteMapType() {
        analyzeSuccess("select ai_complete('prompt')");
        analyzeSuccess("select ai_complete('prompt', map{'temperature': 0.5})");
        analyzeSuccess("select ai_complete('model', 'prompt')");
        // A bare NULL resolves to the (model, prompt) overload, not the option MAP overload.
        analyzeSuccess("select ai_complete('model', NULL)");
        QueryStatement statement = (QueryStatement) analyzeSuccess(
                "select ai_complete('model', 'prompt', map{'flag': true})");

        FunctionCallExpr call = (FunctionCallExpr) ((SelectRelation) statement.getQueryRelation())
                .getOutputExpression().get(0);
        Assertions.assertTrue(call.getFn().isAi());
        Assertions.assertInstanceOf(MapType.class, call.getFn().getArgs()[2]);
        Assertions.assertEquals(VarcharType.VARCHAR, ((MapType) call.getFn().getArgs()[2]).getKeyType());
    }

    @Test
    public void testExplicitModelMayVaryAndDoesNotRequireDefaultModel() {
        Config.ai_default_chat_model = "";
        analyzeSuccess("select ai_complete(ta, cast(tb as varchar)) from tall");
        analyzeSuccess("select ai_complete(ta, cast(tb as varchar), map{'temperature': 1}) from tall");
        analyzeFail("select ai_complete(cast(tb as varchar)) from tall", "ai_default_chat_model");
        analyzeFail("select ai_complete(cast(tb as varchar), map{'temperature': 1}) from tall",
                "ai_default_chat_model");
    }

    @Test
    public void testSystemEndpointAndProviderAreRequired() {
        Config.ai_default_chat_endpoint = "";
        analyzeFail("select ai_complete('prompt')", "ai_default_chat_endpoint");

        Config.ai_default_chat_endpoint = "https://models.example.test/v1/chat/completions";
        Config.ai_default_chat_provider = "";
        analyzeFail("select ai_complete('prompt')", "ai_default_chat_provider");

        Config.ai_default_chat_provider = "unsupported";
        analyzeFail("select ai_complete('prompt')", "openai_compatible");
    }

    @Test
    public void testValidJsonCompatibleOptions() {
        analyzeSuccess("select ai_complete('p', map{})");
        analyzeSuccess("select ai_complete('p', cast(map{} as map<varchar,json>))");
        analyzeSuccess("select ai_complete('p', "
                + "cast(map{cast('key' as char(3)): 1} as map<varchar,int>))");
        analyzeSuccess("select ai_complete('p', "
                + "cast(map{1: 2} as map<varchar,int>))");
        analyzeSuccess("select ai_complete('p', map{'text': '123'})");
        analyzeSuccess("select ai_complete('p', map{'boolean': true})");
        analyzeSuccess("select ai_complete('p', map{'number': 1.25})");
        analyzeSuccess("select ai_complete('p', map{'array': [1, 2]})");
        analyzeSuccess("select ai_complete('p', map{'nested': map{'key': 'value'}})");
        analyzeSuccess("select ai_complete('p', map{'nested': map{}})");
        analyzeSuccess("select ai_complete('p', map{'nullable': null})");
        analyzeSuccess("select ai_complete('p', map{'empty_array': []})");
        analyzeSuccess("select ai_complete('p', map{'array': [map{}]})");
        analyzeSuccess("select ai_complete('p', map{'nested': "
                + "cast(map{cast('key' as char(3)): 1} as map<varchar,int>)})");
        analyzeSuccess("select ai_complete('p', map{'nested': "
                + "map{'level2': map{'level3': map{'level4': [1, 2, 3]}}}})");
        analyzeSuccess("select ai_complete('p', map{'struct': "
                + "cast(row(map{cast('key' as char(3)): 1}) "
                + "as struct<m map<varchar,int>>)})");
        analyzeSuccess("select ai_complete('p', map{'named_struct': "
                + "cast(named_struct('m', map{cast('key' as char(3)): 1}) "
                + "as struct<m map<varchar,int>>)})");
        analyzeSuccess("select ai_complete('p', map{'struct': "
                + "cast(row(map{cast('key' as char(3)): 1}, "
                + "[map{cast('nested' as char(6)): 2}]) "
                + "as struct<m map<varchar,int>, a array<map<varchar,int>>>)})");
        analyzeSuccess("select ai_complete('p', map{'row': row(map{})})");
        analyzeSuccess("select ai_complete('p', map{'named_struct': named_struct('m', map{})})");
        analyzeSuccess("select ai_complete('p', map_concat(map{}, map{}))");
        analyzeSuccess("select ai_complete('p', map_concat(map{'x': map{}}, map{}))");
        analyzeSuccess("select ai_complete('p', map_concat(map{'x': [map{}]}, map{}))");
        analyzeSuccess("select ai_complete('p', map_concat(map{'a': null}, map{'b': map{}}))");
        analyzeSuccess("select ai_complete('p', "
                + "map{'text': cast(cast('NaN' as double) as varchar)})");
        analyzeSuccess("select ai_complete('p', map_concat(map{'a': 1}, map{'b': 2}))");
        analyzeSuccess("select ai_complete('p', map{md5('a'): 1})");
        analyzeSuccess("select ai_complete('p', map{'nullable': cast(null as json)})");
        analyzeSuccess("select ai_complete('p', cast(null as map<varchar,json>))");
    }

    @Test
    public void testOptionMapMustBeConstantWithStringKeys() {
        Assertions.assertAll(
                () -> analyzeFail("select ai_complete('p', map{'temperature': te}) from tall",
                        "constant option MAP"),
                () -> analyzeFail("select ai_complete('p', cast(map{1: 2} as map<int,int>))", "VARCHAR keys"),
                () -> analyzeFail("select ai_complete('p', map{'nested': map{1: 2}})", "nested MAP keys"),
                () -> analyzeFail("select ai_complete('p', cast(map{'key': 1} as map<char(3),int>))",
                        "VARCHAR keys"),
                () -> analyzeFail("select ai_complete('p', "
                        + "map{'nested': cast(map{'key': 1} as map<char(3),int>)})", "nested MAP keys"));
        analyzeFail("select ai_complete('p', map{'struct': "
                + "cast(row(map{'key': 1}) as struct<m map<char(3),int>>)})", "nested MAP keys");
    }

    @Test
    public void testOptionMapRejectsInvalidKeys() {
        analyzeFail("select ai_complete('p', map{'x': 1, 'x': 2})", "duplicate option key");
        analyzeFail("select ai_complete('p', "
                + "cast(map{1: 'a', cast('1' as int): 'b'} as map<varchar,varchar>))",
                "duplicate option key");
        analyzeFail("select ai_complete('p', map{cast(null as varchar): 1})", "NULL option key");
        analyzeFail("select ai_complete('p', "
                + "cast(map{cast(null as char(3)): 1} as map<varchar,int>))", "NULL option key");
        analyzeFail("select ai_complete('p', map{'': 1})", "empty option key");
        analyzeFail("select ai_complete('p', map{'model': 1})", "reserved option key");
        analyzeFail("select ai_complete('p', "
                + "cast(map{cast('model' as char(5)): 1} as map<varchar,int>))",
                "reserved option key");
        analyzeFail("select ai_complete('p', map{'messages': 1})", "reserved option key");
        analyzeFail("select ai_complete('p', map{'stream': 1})", "reserved option key");
    }

    @Test
    public void testOptionMapRejectsUnsupportedAndNonFiniteValues() {
        Assertions.assertAll(
                () -> analyzeFail("select ai_complete('p', map{'opaque': bitmap_empty()})", "JSON-compatible"),
                () -> analyzeFail("select ai_complete('p', map{'number': cast('NaN' as double)})", "finite"),
                () -> analyzeFail("select ai_complete('p', map{'number': cast('Infinity' as double)})", "finite"),
                () -> analyzeFail("select ai_complete('p', "
                        + "map{'number': cast(1.0e200 as double) * cast(1.0e200 as double)})", "finite"));
    }

    @Test
    public void testTypedEmptyAndNullValuesStillEnforceJsonTypeContract() {
        Assertions.assertAll(
                () -> analyzeFail("select ai_complete('p', "
                        + "map{'nested': cast(map{} as map<char(3),int>)})", "nested MAP keys"),
                () -> analyzeFail("select ai_complete('p', cast(map{'nested': map{}} "
                        + "as map<varchar,map<char(3),int>>))", "nested MAP keys"),
                () -> analyzeFail("select ai_complete('p', cast(map{'nested': map{}} "
                        + "as map<varchar,map<boolean,int>>))", "nested MAP keys"),
                () -> analyzeFail("select ai_complete('p', map_concat("
                        + "map{'nested': cast(map{} as map<boolean,int>)}, map{}))", "nested MAP keys"),
                () -> analyzeFail("select ai_complete('p', map_concat("
                        + "cast(map{} as map<boolean,int>), map{}))", "nested MAP keys"),
                () -> analyzeFail("select ai_complete('p', map_concat("
                        + "map{'a': cast(null as map<boolean,int>)}, map{'b': map{}}))", "nested MAP keys"),
                () -> analyzeFail("select ai_complete('p', map{'opaque': cast(null as bitmap)})",
                        "JSON-compatible"),
                () -> analyzeFail("select ai_complete('p', map{'opaque': cast([] as array<bitmap>)})",
                        "JSON-compatible"),
                () -> analyzeFail("select ai_complete('p', map{'struct': "
                        + "cast(row(cast(null as bitmap)) as struct<b bitmap>)})", "JSON-compatible"),
                () -> analyzeFail("select ai_complete('p', cast(null as map<varchar,bitmap>))",
                        "JSON-compatible"),
                () -> analyzeFail("select ai_complete('p', cast(map{} as map<varchar,bitmap>))",
                        "JSON-compatible"));
    }

    @Test
    public void testFiniteOptionDefersWhenConstantFoldingIsDisabled() {
        boolean original = AnalyzeTestUtil.getConnectContext().getSessionVariable()
                .isDisableFunctionFoldConstants();
        try {
            AnalyzeTestUtil.getConnectContext().getSessionVariable().setDisableFunctionFoldConstants(true);
            analyzeSuccess("select ai_complete('p', map{'number': "
                    + "cast(1.25 as double) + cast(2.5 as double)})");
        } finally {
            AnalyzeTestUtil.getConnectContext().getSessionVariable().setDisableFunctionFoldConstants(original);
        }
    }

    @Test
    public void testEmptyExplicitModelIsRejectedButEmptyPromptIsValid() {
        Assertions.assertAll(
                () -> analyzeFail("select ai_complete('', 'prompt')", "model must not be empty"),
                () -> analyzeFail("select ai_complete('   ', 'prompt')", "model must not be empty"));
        analyzeSuccess("select ai_complete(cast(null as varchar), 'p')");
        analyzeSuccess("select ai_complete('model', '')");
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
}
