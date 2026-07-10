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

package com.starrocks.context.sql;

import com.google.common.collect.ImmutableMap;
import com.starrocks.context.ContextMgr;
import com.starrocks.context.ContextReadExecutor;
import com.starrocks.context.SnapshotResolver;
import com.starrocks.context.retrieval.ContextPacker;
import com.starrocks.context.retrieval.ContextSearchExecutor;
import com.starrocks.context.retrieval.ReferenceExpander;
import com.starrocks.context.retrieval.TextSearchExecutor;
import com.starrocks.context.retrieval.VectorSearchExecutor;
import com.starrocks.persist.ContextOpLog;
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.SubqueryRelation;
import com.starrocks.sql.ast.TableFunctionRelation;
import com.starrocks.sql.ast.ValuesRelation;
import com.starrocks.sql.ast.expression.ArrayExpr;
import com.starrocks.sql.ast.expression.DecimalLiteral;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FloatLiteral;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.ast.expression.NamedArgument;
import com.starrocks.sql.ast.expression.StringLiteral;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class ContextTvfRelationResolverTest {

    @Test
    public void testVectorSearchNamedArgsRewritesToSubqueryRelation() {
        ContextMgr mgr = newMgr();
        StubReadExecutor read = new StubReadExecutor(Map.of(
                7L, new ContextReadExecutor.EntityMeta(7L, "doc_7", "page", 3L, 9L,
                        "preview seven", 0.8, "Doc Seven", null)));
        StubVectorSearch vector = new StubVectorSearch(
                new VectorSearchExecutor.EntityHit(7L, 0.91, "preview", "preview seven"));
        ContextTvfRelationResolver resolver = newResolver(mgr, read, new StubTextSearch(),
                vector, new StubReferenceExpander(), new StubPacker(),
                new ContextSearchExecutor(mgr, new StubTextSearch(), new StubReferenceExpander(), vector));

        TableFunctionRelation relation = new TableFunctionRelation(new FunctionCallExpr("vector_search", Arrays.asList(
                new NamedArgument("scope", new StringLiteral("cb.docs")),
                new NamedArgument("query_text", new StringLiteral("deal scoring")))));
        Relation resolved = resolver.resolve(relation);
        Assertions.assertInstanceOf(SubqueryRelation.class, resolved);
        Assertions.assertNotNull(((SubqueryRelation) resolved).getQueryStatement());
        Assertions.assertNotNull(vector.lastRequest);
        Assertions.assertEquals("deal scoring", vector.lastRequest.queryText);
    }

    @Test
    public void testVectorSearchDeepOptionFlowsThroughFeResolver() {
        ContextMgr mgr = newMgr();
        StubReadExecutor read = new StubReadExecutor(Map.of(
                7L, new ContextReadExecutor.EntityMeta(7L, "doc_7", "page", 3L, 9L,
                        "preview seven", 0.8, "Doc Seven", null)));
        StubVectorSearch vector = new StubVectorSearch(
                new VectorSearchExecutor.EntityHit(7L, 0.91, "section", "section snippet"));
        ContextTvfRelationResolver resolver = newResolver(mgr, read, new StubTextSearch(),
                vector, new StubReferenceExpander(), new StubPacker(),
                new ContextSearchExecutor(mgr, new StubTextSearch(), new StubReferenceExpander(), vector));

        TableFunctionRelation relation = new TableFunctionRelation(new FunctionCallExpr("vector_search", Arrays.asList(
                new NamedArgument("scope", new StringLiteral("cb.docs")),
                new NamedArgument("query_text", new StringLiteral("deal scoring")),
                new NamedArgument("options", new StringLiteral("-d")))));
        resolver.resolve(relation);

        Assertions.assertNotNull(vector.lastRequest);
        Assertions.assertTrue(vector.lastRequest.deepMode);
    }

    @Test
    public void testTextSearchNamedArgsRewritesToSubqueryRelation() {
        ContextMgr mgr = newMgr();
        StubReadExecutor read = new StubReadExecutor(Collections.emptyMap());
        StubTextSearch text = new StubTextSearch();
        ContextTvfRelationResolver resolver = newResolver(mgr, read, text, new StubVectorSearch(),
                new StubReferenceExpander(), new StubPacker(),
                new ContextSearchExecutor(mgr, text, new StubReferenceExpander(), new StubVectorSearch()));

        TableFunctionRelation relation = new TableFunctionRelation(new FunctionCallExpr("text_search", Arrays.asList(
                new NamedArgument("scope", new StringLiteral("cb.docs")),
                new NamedArgument("pattern", new StringLiteral("deal scoring")))));
        Relation resolved = resolver.resolve(relation);
        Assertions.assertInstanceOf(SubqueryRelation.class, resolved);
        SubqueryRelation subquery = (SubqueryRelation) resolved;
        String sql = com.starrocks.sql.analyzer.AstToSQLBuilder.toSQL(subquery.getQueryStatement());
        Assertions.assertTrue(sql.contains("fragment_text"), sql);
        Assertions.assertTrue(sql.contains("MATCH") || sql.contains("LIKE"), sql);
    }

    @Test
    public void testReadCollectionRewritesToScopedSubquery() {
        ContextMgr mgr = newMgr();
        StubReadExecutor read = new StubReadExecutor(Collections.emptyMap());
        ContextTvfRelationResolver resolver = newResolver(mgr, read, new StubTextSearch(), new StubVectorSearch(),
                new StubReferenceExpander(), new StubPacker(),
                new ContextSearchExecutor(mgr, new StubTextSearch(), new StubReferenceExpander(), new StubVectorSearch()));

        TableFunctionRelation relation = new TableFunctionRelation(new FunctionCallExpr("read_collection",
                Arrays.asList(new IntLiteral(2L))));
        Relation resolved = resolver.resolve(relation);
        Assertions.assertInstanceOf(SubqueryRelation.class, resolved);
        SubqueryRelation subquery = (SubqueryRelation) resolved;
        String sql = com.starrocks.sql.analyzer.AstToSQLBuilder.toSQL(subquery.getQueryStatement());
        // AstToSQLBuilder backticks identifiers and may rewrite STRING -> VARCHAR(N); compare on a
        // normalized form (drop backticks, uppercase) so the test stays robust to formatter quirks.
        String norm = sql.replace("`", "").toUpperCase();
        Assertions.assertTrue(norm.contains("__INTERNAL_CONTEXT.CONTEXT_ENTITY_VERSIONS"), sql);
        Assertions.assertTrue(norm.contains("__INTERNAL_CONTEXT.CONTEXT_ENTITY_HEADS"), sql);
        Assertions.assertTrue(norm.contains("COLLECTION_ID = 2"), sql);
        Assertions.assertTrue(norm.contains("CAST"), sql);
        Assertions.assertTrue(norm.contains("LIMIT 1000"), sql);
    }

    @Test
    public void testReadContextBaseRewritesToScopedSubquery() {
        ContextMgr mgr = newMgr();
        StubReadExecutor read = new StubReadExecutor(Collections.emptyMap());
        ContextTvfRelationResolver resolver = newResolver(mgr, read, new StubTextSearch(), new StubVectorSearch(),
                new StubReferenceExpander(), new StubPacker(),
                new ContextSearchExecutor(mgr, new StubTextSearch(), new StubReferenceExpander(), new StubVectorSearch()));

        TableFunctionRelation relation = new TableFunctionRelation(new FunctionCallExpr("read_contextbase",
                Arrays.asList(new IntLiteral(1L))));
        Relation resolved = resolver.resolve(relation);
        Assertions.assertInstanceOf(SubqueryRelation.class, resolved);
        SubqueryRelation subquery = (SubqueryRelation) resolved;
        String sql = com.starrocks.sql.analyzer.AstToSQLBuilder.toSQL(subquery.getQueryStatement());
        String norm = sql.replace("`", "").toUpperCase();
        Assertions.assertTrue(norm.contains("__INTERNAL_CONTEXT.CONTEXT_ENTITY_VERSIONS"), sql);
        Assertions.assertTrue(norm.contains("__INTERNAL_CONTEXT.CONTEXT_ENTITY_HEADS"), sql);
        Assertions.assertTrue(norm.contains("CONTEXTBASE_ID = 1"), sql);
        Assertions.assertTrue(norm.contains("CAST"), sql);
        Assertions.assertTrue(norm.contains("LIMIT 2000"), sql);
    }

    @Test
    public void testContextSearchNamedArgsMaterializesValuesRelation() {
        ContextMgr mgr = newMgr();
        StubReadExecutor read = new StubReadExecutor(Map.of(
                11L, new ContextReadExecutor.EntityMeta(11L, "doc_11", "page", 5L, 12L,
                        "preview eleven", 1.0, "Doc Eleven", null)));
        StubTextSearch text = new StubTextSearch(new TextSearchExecutor.EntityHit(11L, 2, null, 0.7));
        StubVectorSearch vector = new StubVectorSearch(
                new VectorSearchExecutor.EntityHit(11L, 0.8, "preview", "preview eleven"));
        ContextSearchExecutor search = new ContextSearchExecutor(mgr, text, new StubReferenceExpander(), vector);
        ContextTvfRelationResolver resolver = newResolver(mgr, read, text, vector,
                new StubReferenceExpander(), new StubPacker(), search);

        TableFunctionRelation relation = new TableFunctionRelation(new FunctionCallExpr("context_search", Arrays.asList(
                new NamedArgument("contextbase", new StringLiteral("cb")),
                new NamedArgument("collection", new StringLiteral("docs")),
                new NamedArgument("query_embedding", buildEmbedding("0.1", "0.2")),
                new NamedArgument("query_text", new StringLiteral("deal scoring")))));
        Relation resolved = resolver.resolve(relation);
        Assertions.assertInstanceOf(ValuesRelation.class, resolved);
        ValuesRelation values = (ValuesRelation) resolved;
        Assertions.assertEquals(14, values.getColumnOutputNames().size());
        Assertions.assertEquals("final_score", values.getColumnOutputNames().get(7));
        Assertions.assertEquals(1, values.getRows().size());
    }

    @Test
    public void testContextSearchAcceptsDecimalWeightLiterals() {
        // Regression: a SQL fractional weight such as 0.2 parses as a DecimalLiteral (not a
        // FloatLiteral), and the resolver previously rejected it with "text_weight must be a
        // literal, got: DecimalLiteral", so fractional weights were unusable from the TVF.
        // The resolver must now accept DecimalLiteral weights and pass the exact double through.
        ContextMgr mgr = newMgr();
        StubReadExecutor read = new StubReadExecutor(Collections.emptyMap());
        CapturingSearchExecutor search = new CapturingSearchExecutor(mgr);
        ContextTvfRelationResolver resolver = newResolver(mgr, read, new StubTextSearch(),
                new StubVectorSearch(), new StubReferenceExpander(), new StubPacker(), search);

        TableFunctionRelation relation = new TableFunctionRelation(new FunctionCallExpr("context_search",
                Arrays.asList(
                        new NamedArgument("contextbase", new StringLiteral("cb")),
                        new NamedArgument("query_text", new StringLiteral("kw")),
                        new NamedArgument("text_weight", new DecimalLiteral("0.2")),
                        new NamedArgument("vector_weight", new DecimalLiteral("0.7")),
                        new NamedArgument("graph_weight", new DecimalLiteral("0.1")))));
        Relation resolved = resolver.resolve(relation);

        Assertions.assertInstanceOf(ValuesRelation.class, resolved);
        Assertions.assertNotNull(search.lastRequest);
        Assertions.assertEquals(0.2, search.lastRequest.textWeight, 1e-9);
        Assertions.assertEquals(0.7, search.lastRequest.vectorWeight, 1e-9);
        Assertions.assertEquals(0.1, search.lastRequest.graphWeight, 1e-9);
        Assertions.assertTrue(search.lastRequest.explicitTextWeight);
        Assertions.assertTrue(search.lastRequest.explicitVectorWeight);
        Assertions.assertTrue(search.lastRequest.explicitGraphWeight);
    }

    @Test
    public void testContextSearchAcceptsIntegerAndScientificWeightLiterals() {
        // Integers (IntLiteral) and scientific-notation doubles (FloatLiteral) must keep working
        // alongside the new DecimalLiteral support.
        ContextMgr mgr = newMgr();
        StubReadExecutor read = new StubReadExecutor(Collections.emptyMap());
        CapturingSearchExecutor search = new CapturingSearchExecutor(mgr);
        ContextTvfRelationResolver resolver = newResolver(mgr, read, new StubTextSearch(),
                new StubVectorSearch(), new StubReferenceExpander(), new StubPacker(), search);

        TableFunctionRelation relation = new TableFunctionRelation(new FunctionCallExpr("context_search",
                Arrays.asList(
                        new NamedArgument("contextbase", new StringLiteral("cb")),
                        new NamedArgument("query_text", new StringLiteral("kw")),
                        new NamedArgument("text_weight", new IntLiteral(1L)),
                        new NamedArgument("vector_weight", new FloatLiteral("0.7e0")),
                        new NamedArgument("graph_weight", new DecimalLiteral("0")))));
        resolver.resolve(relation);

        Assertions.assertNotNull(search.lastRequest);
        Assertions.assertEquals(1.0, search.lastRequest.textWeight, 1e-9);
        Assertions.assertEquals(0.7, search.lastRequest.vectorWeight, 1e-9);
        Assertions.assertEquals(0.0, search.lastRequest.graphWeight, 1e-9);
    }

    @Test
    public void testContextSearchRejectsNonLiteralWeight() {
        // A string weight is still not a numeric literal and must be rejected loudly.
        ContextMgr mgr = newMgr();
        StubReadExecutor read = new StubReadExecutor(Collections.emptyMap());
        ContextTvfRelationResolver resolver = newResolver(mgr, read, new StubTextSearch(),
                new StubVectorSearch(), new StubReferenceExpander(), new StubPacker(),
                new CapturingSearchExecutor(mgr));

        TableFunctionRelation relation = new TableFunctionRelation(new FunctionCallExpr("context_search",
                Arrays.asList(
                        new NamedArgument("contextbase", new StringLiteral("cb")),
                        new NamedArgument("query_text", new StringLiteral("kw")),
                        new NamedArgument("text_weight", new StringLiteral("0.2")))));
        com.starrocks.context.error.ContextException ex = Assertions.assertThrows(
                com.starrocks.context.error.ContextException.class,
                () -> resolver.resolve(relation));
        Assertions.assertEquals(com.starrocks.context.error.ContextErrorCode.INVALID_ARGUMENT, ex.getCode());
        Assertions.assertTrue(ex.getMessage().contains("text_weight"), ex.getMessage());
        Assertions.assertTrue(ex.getMessage().contains("must be a literal"), ex.getMessage());
    }

    @Test
    public void testRequireDoubleLiteralAcceptsDecimalFloatInt() throws Exception {
        ContextMgr mgr = newMgr();
        ContextTvfRelationResolver resolver = newResolver(mgr,
                new StubReadExecutor(Collections.emptyMap()), new StubTextSearch(), new StubVectorSearch(),
                new StubReferenceExpander(), new StubPacker(),
                new ContextSearchExecutor(mgr, new StubTextSearch(), new StubReferenceExpander(),
                        new StubVectorSearch()));
        java.lang.reflect.Method m = ContextTvfRelationResolver.class.getDeclaredMethod(
                "requireDoubleLiteral", Expr.class, String.class);
        m.setAccessible(true);

        Assertions.assertEquals(0.2, (Double) m.invoke(resolver, new DecimalLiteral("0.2"), "w"), 1e-9);
        Assertions.assertEquals(0.2, (Double) m.invoke(resolver, new FloatLiteral("0.2e0"), "w"), 1e-9);
        Assertions.assertEquals(3.0, (Double) m.invoke(resolver, new IntLiteral(3L), "w"), 1e-9);

        // StringLiteral is not numeric -> ContextException wrapped in InvocationTargetException.
        java.lang.reflect.InvocationTargetException wrapper = Assertions.assertThrows(
                java.lang.reflect.InvocationTargetException.class,
                () -> m.invoke(resolver, new StringLiteral("0.2"), "w"));
        Assertions.assertInstanceOf(com.starrocks.context.error.ContextException.class, wrapper.getCause());
        Assertions.assertTrue(wrapper.getCause().getMessage().contains("must be a literal"),
                wrapper.getCause().getMessage());
    }

    @Test
    public void testFloatArrayArgAcceptsDecimalLiterals() throws Exception {
        // query_embedding components written as 0.1 / 0.2 parse as DecimalLiteral and must be
        // accepted just like FloatLiteral / IntLiteral components.
        ContextMgr mgr = newMgr();
        ContextTvfRelationResolver resolver = newResolver(mgr,
                new StubReadExecutor(Collections.emptyMap()), new StubTextSearch(), new StubVectorSearch(),
                new StubReferenceExpander(), new StubPacker(),
                new ContextSearchExecutor(mgr, new StubTextSearch(), new StubReferenceExpander(),
                        new StubVectorSearch()));
        java.lang.reflect.Method m = ContextTvfRelationResolver.class.getDeclaredMethod(
                "floatArrayArg", Expr.class);
        m.setAccessible(true);

        ArrayExpr array = new ArrayExpr(null, Arrays.asList(
                new DecimalLiteral("0.1"), new FloatLiteral("0.2e0"), new IntLiteral(1L)));
        float[] out = (float[]) m.invoke(resolver, array);
        Assertions.assertArrayEquals(new float[] {0.1f, 0.2f, 1.0f}, out, 1e-6f);

        ArrayExpr bad = new ArrayExpr(null, Collections.singletonList(new StringLiteral("0.1")));
        java.lang.reflect.InvocationTargetException wrapper = Assertions.assertThrows(
                java.lang.reflect.InvocationTargetException.class,
                () -> m.invoke(resolver, bad));
        Assertions.assertInstanceOf(com.starrocks.context.error.ContextException.class, wrapper.getCause());
    }

    @Test
    public void testContextSearchRejectsRemovedRequiredGraphMode() {
        // graph_mode=REQUIRED was removed when fusion gained auto-seed-derivation. The TVF
        // resolver must surface a loud INVALID_ARGUMENT instead of silently coercing to AUTO.
        ContextMgr mgr = newMgr();
        ContextSearchExecutor search = new ContextSearchExecutor(mgr, new StubTextSearch(),
                new StubReferenceExpander(), new StubVectorSearch());
        ContextTvfRelationResolver resolver = newResolver(mgr,
                new StubReadExecutor(Collections.emptyMap()), new StubTextSearch(),
                new StubVectorSearch(), new StubReferenceExpander(), new StubPacker(), search);

        TableFunctionRelation relation = new TableFunctionRelation(new FunctionCallExpr("context_search",
                Arrays.asList(
                        new NamedArgument("contextbase", new StringLiteral("cb")),
                        new NamedArgument("query_text", new StringLiteral("kw")),
                        new NamedArgument("graph_mode", new StringLiteral("REQUIRED")))));
        com.starrocks.context.error.ContextException ex = Assertions.assertThrows(
                com.starrocks.context.error.ContextException.class,
                () -> resolver.resolve(relation));
        Assertions.assertEquals(com.starrocks.context.error.ContextErrorCode.INVALID_ARGUMENT, ex.getCode());
        Assertions.assertTrue(ex.getMessage().contains("REQUIRED"), ex.getMessage());
    }

    @Test
    public void testContextSearchAcceptsGraphSeedTopkNamedArg() {
        // graph_seed_topk is the new knob for capping auto-derived seeds. The resolver must
        // accept it without rejecting "unknown parameter".
        ContextMgr mgr = newMgr();
        StubReadExecutor read = new StubReadExecutor(Collections.emptyMap());
        ContextSearchExecutor search = new ContextSearchExecutor(mgr, new StubTextSearch(),
                new StubReferenceExpander(), new StubVectorSearch());
        ContextTvfRelationResolver resolver = newResolver(mgr, read, new StubTextSearch(),
                new StubVectorSearch(), new StubReferenceExpander(), new StubPacker(), search);

        TableFunctionRelation relation = new TableFunctionRelation(new FunctionCallExpr("context_search",
                Arrays.asList(
                        new NamedArgument("contextbase", new StringLiteral("cb")),
                        new NamedArgument("query_text", new StringLiteral("kw")),
                        new NamedArgument("graph_seed_topk", new IntLiteral(3)))));
        Relation resolved = resolver.resolve(relation);
        Assertions.assertInstanceOf(ValuesRelation.class, resolved);
    }

    @Test
    public void testContextSearchAcceptsDirectionNamedArg() {
        // direction selects the reference-expansion BFS direction for fusion search (default BOTH).
        // The resolver must accept it without rejecting "unknown parameter".
        ContextMgr mgr = newMgr();
        StubReadExecutor read = new StubReadExecutor(Collections.emptyMap());
        ContextSearchExecutor search = new ContextSearchExecutor(mgr, new StubTextSearch(),
                new StubReferenceExpander(), new StubVectorSearch());
        ContextTvfRelationResolver resolver = newResolver(mgr, read, new StubTextSearch(),
                new StubVectorSearch(), new StubReferenceExpander(), new StubPacker(), search);

        TableFunctionRelation relation = new TableFunctionRelation(new FunctionCallExpr("context_search",
                Arrays.asList(
                        new NamedArgument("contextbase", new StringLiteral("cb")),
                        new NamedArgument("query_text", new StringLiteral("kw")),
                        new NamedArgument("direction", new StringLiteral("forward")))));
        Relation resolved = resolver.resolve(relation);
        Assertions.assertInstanceOf(ValuesRelation.class, resolved);
    }

    @Test
    public void testContextPackMaterializesSingleRow() {
        ContextMgr mgr = newMgr();
        ContextPacker.Result packResult = new ContextPacker.Result(
                "# packed", 12, Arrays.asList(1L, 2L), Collections.singletonList(3L),
                Collections.emptyList(), Collections.singletonList(new ContextPacker.Citation(1L, 2L, "Doc One", "ek_test")));
        StubReadExecutor read = new StubReadExecutor(Collections.emptyMap(), Map.of(1L, 1L, 2L, 1L));
        ContextTvfRelationResolver resolver = newResolver(mgr, read,
                new StubTextSearch(), new StubVectorSearch(), new StubReferenceExpander(),
                new StubPacker(packResult), new ContextSearchExecutor(mgr, new StubTextSearch(),
                        new StubReferenceExpander(), new StubVectorSearch()));

        TableFunctionRelation relation = new TableFunctionRelation(new FunctionCallExpr("context_pack", Arrays.asList(
                new NamedArgument("scope", new StringLiteral("cb.docs")),
                new NamedArgument("entity_ids", buildLongArray(1L, 2L)),
                new NamedArgument("max_tokens", new IntLiteral(4000)))));
        Relation resolved = resolver.resolve(relation);
        Assertions.assertInstanceOf(ValuesRelation.class, resolved);
        ValuesRelation values = (ValuesRelation) resolved;
        Assertions.assertEquals(5, values.getColumnOutputNames().size());
        Assertions.assertEquals(1, values.getRows().size());
    }

    @Test
    public void testGraphExpandRejectsMixedContextBaseSeeds() {
        ContextMgr mgr = newMgr();
        StubReadExecutor read = new StubReadExecutor(Collections.emptyMap(), Map.of(11L, 1L, 22L, 2L));
        ContextTvfRelationResolver resolver = newResolver(mgr, read, new StubTextSearch(), new StubVectorSearch(),
                new StubReferenceExpander(), new StubPacker(),
                new ContextSearchExecutor(mgr, new StubTextSearch(), new StubReferenceExpander(),
                        new StubVectorSearch()));

        TableFunctionRelation relation = new TableFunctionRelation(new FunctionCallExpr("graph_expand", Arrays.asList(
                new NamedArgument("contextbase", new StringLiteral("cb")),
                new NamedArgument("seed_ids", buildLongArray(11L, 22L)),
                new NamedArgument("depth", new IntLiteral(1L)))));
        com.starrocks.context.error.ContextException ex = Assertions.assertThrows(
                com.starrocks.context.error.ContextException.class,
                () -> resolver.resolve(relation));
        Assertions.assertEquals(com.starrocks.context.error.ContextErrorCode.ACCESS_DENIED, ex.getCode());
        Assertions.assertTrue(ex.getMessage().contains("single contextbase"), ex.getMessage());
    }

    @Test
    public void testContextPackRejectsMixedContextBaseEntityIds() {
        ContextMgr mgr = newMgr();
        StubReadExecutor read = new StubReadExecutor(Collections.emptyMap(), Map.of(11L, 1L, 22L, 2L));
        ContextTvfRelationResolver resolver = newResolver(mgr, read, new StubTextSearch(), new StubVectorSearch(),
                new StubReferenceExpander(), new StubPacker(),
                new ContextSearchExecutor(mgr, new StubTextSearch(), new StubReferenceExpander(),
                        new StubVectorSearch()));

        TableFunctionRelation relation = new TableFunctionRelation(new FunctionCallExpr("context_pack", Arrays.asList(
                new NamedArgument("scope", new StringLiteral("cb.docs")),
                new NamedArgument("entity_ids", buildLongArray(11L, 22L)),
                new NamedArgument("max_tokens", new IntLiteral(4000L)))));
        com.starrocks.context.error.ContextException ex = Assertions.assertThrows(
                com.starrocks.context.error.ContextException.class,
                () -> resolver.resolve(relation));
        Assertions.assertEquals(com.starrocks.context.error.ContextErrorCode.ACCESS_DENIED, ex.getCode());
        Assertions.assertTrue(ex.getMessage().contains("single contextbase"), ex.getMessage());
    }

    @Test
    public void testScopedVectorSearchAlwaysUsesIndexShape() throws Exception {
        // The scope-column rollout is complete, so the TVF vector search always emits the PRE
        // filtered-ANN shape: the scope is a residual predicate on the fragments scan
        // (f.contextbase_id) with ORDER BY ... LIMIT directly on the scan. There is no readiness
        // gate and no index-free heads-JOIN-below-TopN fallback. Whether a segment actually has an
        // HNSW .vi is transparent to the FE -- the BE uses it when present, brute-forces otherwise.
        VectorSearchExecutor.Request req = new VectorSearchExecutor.Request();
        req.contextBaseId = 1L;
        String sql = buildVectorBaseSql(req);
        Assertions.assertTrue(sql.contains("f.contextbase_id = 1"), sql);
        Assertions.assertTrue(sql.contains("ORDER BY raw_score DESC LIMIT"), sql);
        // The removed fallback joined heads directly onto the fragments scan and scoped on
        // h.contextbase_id below the TopN; the index shape joins heads onto the ANN subquery.
        Assertions.assertFalse(sql.contains("ON h.entity_id = f.entity_id"), sql);
        Assertions.assertFalse(sql.contains("h.contextbase_id"), sql);
    }

    @Test
    public void testScopedVectorSearchSnapshotFenceUsesIndexShape() throws Exception {
        // Snapshot-fenced scoped search also takes the index shape: scope on the fragments scan,
        // the version fence applied via a JOIN above the inner TopN (never the heads-JOIN fallback).
        VectorSearchExecutor.Request req = new VectorSearchExecutor.Request();
        req.contextBaseId = 1L;
        req.snapshotFence = 100L;
        String sql = buildVectorBaseSql(req);
        Assertions.assertTrue(sql.contains("f.contextbase_id = 1"), sql);
        Assertions.assertTrue(sql.contains("ORDER BY raw_score DESC LIMIT"), sql);
        Assertions.assertFalse(sql.contains("ON h.entity_id = f.entity_id"), sql);
    }

    private static String buildVectorBaseSql(VectorSearchExecutor.Request req) throws Exception {
        ContextMgr mgr = newMgr();
        ContextTvfRelationResolver resolver = newResolver(mgr,
                new StubReadExecutor(Collections.emptyMap()), new StubTextSearch(), new StubVectorSearch(),
                new StubReferenceExpander(), new StubPacker(),
                new ContextSearchExecutor(mgr, new StubTextSearch(), new StubReferenceExpander(),
                        new StubVectorSearch()));
        java.lang.reflect.Method m = ContextTvfRelationResolver.class.getDeclaredMethod(
                "buildVectorSearchBaseSql", VectorSearchExecutor.Request.class, String.class, String.class, int.class);
        m.setAccessible(true);
        return (String) m.invoke(resolver, req, "[0.1, 0.2, 0.3]", null, 40);
    }

    private static ContextMgr newMgr() {
        ContextMgr mgr = new ContextMgr();
        mgr.replayCreateContextBase(ContextOpLog.forContextBase(1L, "cb", null));
        mgr.replayCreateCollection(ContextOpLog.forCollection(2L, 1L, "cb.docs", "knowledge", ImmutableMap.of()));
        return mgr;
    }

    private static ContextTvfRelationResolver newResolver(ContextMgr mgr,
                                                           ContextReadExecutor read,
                                                           TextSearchExecutor text,
                                                           VectorSearchExecutor vector,
                                                           ReferenceExpander ref,
                                                           ContextPacker packer,
                                                           ContextSearchExecutor search) {
        return new ContextTvfRelationResolver(mgr, read, text, vector, ref, packer, search, new SnapshotResolver());
    }

    private static com.starrocks.sql.ast.expression.ArrayExpr buildEmbedding(String... values) {
        java.util.List<com.starrocks.sql.ast.expression.Expr> items = new java.util.ArrayList<>();
        for (String value : values) {
            items.add(new com.starrocks.sql.ast.expression.FloatLiteral(value));
        }
        return new com.starrocks.sql.ast.expression.ArrayExpr(null, items);
    }

    private static com.starrocks.sql.ast.expression.ArrayExpr buildLongArray(long... values) {
        java.util.List<com.starrocks.sql.ast.expression.Expr> items = new java.util.ArrayList<>();
        for (long value : values) {
            items.add(new IntLiteral(value));
        }
        return new com.starrocks.sql.ast.expression.ArrayExpr(null, items);
    }

    private static class StubReadExecutor extends ContextReadExecutor {
        private final Map<Long, EntityMeta> meta;
        private final Map<Long, Long> contextBaseIds;

        private StubReadExecutor(Map<Long, EntityMeta> meta) {
            this(meta, Collections.emptyMap());
        }

        private StubReadExecutor(Map<Long, EntityMeta> meta, Map<Long, Long> contextBaseIds) {
            this.meta = meta;
            this.contextBaseIds = contextBaseIds;
        }

        @Override
        public Map<Long, EntityMeta> loadEntityMetadata(java.util.Collection<Long> entityIds, long snapshotFence) {
            Map<Long, EntityMeta> out = new LinkedHashMap<>();
            for (Long entityId : entityIds) {
                if (meta.containsKey(entityId)) {
                    out.put(entityId, meta.get(entityId));
                }
            }
            return out;
        }

        @Override
        public long resolveContextBaseIdForEntity(long entityId) {
            return contextBaseIds.getOrDefault(entityId, -1L);
        }
    }

    private static final class StubTextSearch extends TextSearchExecutor {
        private final List<EntityHit> hits;

        private StubTextSearch(EntityHit... hits) {
            this.hits = Arrays.asList(hits);
        }

        @Override
        public List<EntityHit> search(Request request) {
            return hits;
        }
    }

    private static final class StubVectorSearch extends VectorSearchExecutor {
        private final List<EntityHit> hits;
        private Request lastRequest;

        private StubVectorSearch(EntityHit... hits) {
            this.hits = Arrays.asList(hits);
        }

        @Override
        public List<EntityHit> search(Request request) {
            this.lastRequest = request;
            return hits;
        }

        // Don't round-trip to BE for the query vector in tests — the stub returns a fixed
        // dummy vector so the resolver continues past the provider-configuration check.
        @Override
        public float[] resolveQueryEmbedding(Request request) {
            this.lastRequest = request;
            if (request.queryEmbedding != null && request.queryEmbedding.length > 0) {
                return request.queryEmbedding;
            }
            return new float[] {0.1f, 0.2f, 0.3f};
        }
    }

    // Captures the fused-search Request so tests can assert how weight literals were parsed,
    // and returns an empty result so resolve() still materializes a (zero-row) ValuesRelation.
    private static final class CapturingSearchExecutor extends ContextSearchExecutor {
        private Request lastRequest;

        private CapturingSearchExecutor(ContextMgr mgr) {
            super(mgr, new StubTextSearch(), new StubReferenceExpander(), new StubVectorSearch());
        }

        @Override
        public Result search(Request request) {
            this.lastRequest = request;
            return new Result(Collections.emptyList(), new java.util.HashMap<>());
        }
    }

    private static final class StubReferenceExpander extends ReferenceExpander {
        @Override
        public Result expand(Request request) {
            return new Result(Collections.emptyList(), false, 0);
        }
    }

    private static final class StubPacker extends ContextPacker {
        private final Result result;

        private StubPacker() {
            this(new Result("", 0, Collections.emptyList(), Collections.emptyList(),
                    Collections.emptyList(), Collections.emptyList()));
        }

        private StubPacker(Result result) {
            super(null);
            this.result = result;
        }

        @Override
        public Result pack(Request request) {
            return result;
        }
    }
}
