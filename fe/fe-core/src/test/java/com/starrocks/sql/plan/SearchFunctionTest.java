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

import com.starrocks.catalog.View;
import com.starrocks.common.Config;
import com.starrocks.sql.analyzer.AstToStringBuilder;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.expression.BinaryPredicate;
import com.starrocks.sql.ast.expression.CompoundPredicate;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.IsNullPredicate;
import com.starrocks.sql.ast.expression.MatchExpr;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.parser.ParsingException;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class SearchFunctionTest extends PlanTestBase {
    private static boolean originalEnableGin;
    private static boolean originalEnableSearchFunction;

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        originalEnableGin = Config.enable_experimental_gin;
        originalEnableSearchFunction = Config.enable_search_function;
        Config.enable_experimental_gin = true;
        Config.enable_search_function = true;

        starRocksAssert.withTable("CREATE TABLE test.search_docs ("
                + " id INT,"
                + " title STRING,"
                + " body STRING,"
                + " category STRING,"
                + " exact_value STRING,"
                + " standard_text STRING,"
                + " chinese_text STRING,"
                + " case_text STRING,"
                + " plain_text STRING,"
                + " number_value INT,"
                + " INDEX idx_title (title) USING GIN('imp_lib'='builtin', 'parser'='english'),"
                + " INDEX idx_body (body) USING GIN('imp_lib'='builtin', 'parser'='english'),"
                + " INDEX idx_category (category) USING GIN('imp_lib'='builtin', 'parser'='english'),"
                + " INDEX idx_exact (exact_value) USING GIN('imp_lib'='builtin', 'parser'='none'),"
                + " INDEX idx_standard (standard_text) USING GIN('imp_lib'='builtin', 'parser'='standard'),"
                + " INDEX idx_chinese (chinese_text) USING GIN('imp_lib'='builtin', 'parser'='chinese'),"
                + " INDEX idx_case (case_text) USING GIN('imp_lib'='builtin', 'parser'='english',"
                + " 'lower_case'='false')"
                + ") DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1"
                + " PROPERTIES ('replication_num'='1');");
    }

    @AfterAll
    public static void afterClass() {
        Config.enable_experimental_gin = originalEnableGin;
        Config.enable_search_function = originalEnableSearchFunction;
        PlanTestBase.afterClass();
    }

    @Test
    public void testTermAndDefaultOperator() throws Exception {
        Expr any = analyzedPredicate("SELECT id FROM test.search_docs "
                + "WHERE search('title:Machine-Learning')");
        assertMatch(any, "title", "Machine-Learning", MatchExpr.MatchOperator.MATCH_ANY);

        Expr all = analyzedPredicate("SELECT id FROM test.search_docs "
                + "WHERE search('title:Machine-Learning', '{\"default_operator\":\"and\"}')");
        assertMatch(all, "title", "Machine-Learning", MatchExpr.MatchOperator.MATCH_ALL);

        Expr chinese = analyzedPredicate(
                "SELECT id FROM test.search_docs WHERE search('chinese_text:机器学习')");
        assertMatch(chinese, "chinese_text", "机器学习", MatchExpr.MatchOperator.MATCH_ANY);
    }

    @Test
    public void testAnyAllInAndExact() throws Exception {
        Expr any = analyzedPredicate(
                "SELECT id FROM test.search_docs WHERE search('title:ANY(foo bar)')");
        assertMatch(any, "title", "foo bar", MatchExpr.MatchOperator.MATCH_ANY);
        Expr all = analyzedPredicate(
                "SELECT id FROM test.search_docs WHERE search('title:ALL(foo bar)')");
        assertMatch(all, "title", "foo bar", MatchExpr.MatchOperator.MATCH_ALL);

        Expr in = analyzedPredicate(
                "SELECT id FROM test.search_docs WHERE search('title:IN(Foo Bar)')");
        assertSameFieldCompound(in, CompoundPredicate.Operator.OR,
                "title", "foo", "bar", MatchExpr.MatchOperator.MATCH);

        Expr exact = analyzedPredicate(
                "SELECT id FROM test.search_docs WHERE search('exact_value:EXACT(Hello World)')");
        assertMatch(exact, "exact_value", "Hello World", MatchExpr.MatchOperator.MATCH);
    }

    @Test
    public void testBooleanPrecedence() throws Exception {
        Expr predicate = analyzedPredicate("SELECT id FROM test.search_docs "
                + "WHERE search('title:a OR body:b AND category:c')");
        assertThat(predicate).isInstanceOf(CompoundPredicate.class);
        CompoundPredicate or = (CompoundPredicate) predicate;
        assertThat(or.getOp()).isEqualTo(CompoundPredicate.Operator.OR);
        assertThat(or.getChild(1)).isInstanceOf(CompoundPredicate.class);
        assertThat(((CompoundPredicate) or.getChild(1)).getOp()).isEqualTo(CompoundPredicate.Operator.AND);
        assertThat(AstToStringBuilder.toString(predicate))
                .contains("title MATCH_ANY 'a'", "body MATCH_ANY 'b'", "category MATCH_ANY 'c'");
    }

    @Test
    public void testImplicitClausesRespectExplicitBooleanPrecedence() throws Exception {
        Expr predicate = analyzedPredicate("SELECT id FROM test.search_docs "
                + "WHERE search('a b AND c d OR e', '{\"default_field\":\"body\"}')");
        CompoundPredicate or = assertCompound(predicate, CompoundPredicate.Operator.OR);
        CompoundPredicate and = assertCompound(or.getChild(0), CompoundPredicate.Operator.AND);
        assertSameFieldCompound(and.getChild(0), CompoundPredicate.Operator.OR,
                "body", "a", "b", MatchExpr.MatchOperator.MATCH_ANY);
        assertSameFieldCompound(and.getChild(1), CompoundPredicate.Operator.OR,
                "body", "c", "d", MatchExpr.MatchOperator.MATCH_ANY);
        assertMatch(or.getChild(1), "body", "e", MatchExpr.MatchOperator.MATCH_ANY);
    }

    @Test
    public void testImplicitTermsUseDefaultCompoundOperator() throws Exception {
        Expr single = analyzedPredicate("SELECT id FROM test.search_docs "
                + "WHERE search('foo bar', "
                + "'{\"default_field\":\"body\",\"default_operator\":\"and\"}')");
        assertSameFieldCompound(single, CompoundPredicate.Operator.AND,
                "body", "foo", "bar", MatchExpr.MatchOperator.MATCH_ALL);

        Expr explicit = analyzedPredicate(
                "SELECT id FROM test.search_docs WHERE search('title:(foo bar)')");
        assertSameFieldCompound(explicit, CompoundPredicate.Operator.OR,
                "title", "foo", "bar", MatchExpr.MatchOperator.MATCH_ANY);

        Expr bestFields = analyzedPredicate("SELECT id FROM test.search_docs "
                + "WHERE search('foo bar', '{\"fields\":[\"title\",\"body\"]}')");
        CompoundPredicate alternatives = assertCompound(bestFields, CompoundPredicate.Operator.OR);
        assertSameFieldCompound(alternatives.getChild(0), CompoundPredicate.Operator.OR,
                "title", "foo", "bar", MatchExpr.MatchOperator.MATCH_ANY);
        assertSameFieldCompound(alternatives.getChild(1), CompoundPredicate.Operator.OR,
                "body", "foo", "bar", MatchExpr.MatchOperator.MATCH_ANY);
    }

    @Test
    public void testImplicitTermsExpandIndividuallyForCrossFields() throws Exception {
        Expr predicate = analyzedPredicate("SELECT id FROM test.search_docs "
                + "WHERE search('foo bar', "
                + "'{\"fields\":[\"title\",\"body\"],\"type\":\"cross_fields\","
                + "\"default_operator\":\"and\"}')");
        CompoundPredicate terms = assertCompound(predicate, CompoundPredicate.Operator.AND);
        assertCrossFieldTerm(terms.getChild(0), "foo", MatchExpr.MatchOperator.MATCH_ALL);
        assertCrossFieldTerm(terms.getChild(1), "bar", MatchExpr.MatchOperator.MATCH_ALL);
    }

    @Test
    public void testCrossFieldsUsesCompoundForExplicitSameFieldImplicitClause() throws Exception {
        Expr predicate = analyzedPredicate("SELECT id FROM test.search_docs "
                + "WHERE search('title:(foo bar) AND baz', "
                + "'{\"fields\":[\"title\",\"body\"],\"type\":\"cross_fields\","
                + "\"default_operator\":\"and\"}')");
        CompoundPredicate and = assertCompound(predicate, CompoundPredicate.Operator.AND);
        assertSameFieldCompound(and.getChild(0), CompoundPredicate.Operator.AND,
                "title", "foo", "bar", MatchExpr.MatchOperator.MATCH_ALL);
        assertCrossFieldTerm(and.getChild(1), "baz", MatchExpr.MatchOperator.MATCH_ALL);
    }

    @Test
    public void testMixedImplicitClausesAreNotPacked() throws Exception {
        Expr predicate = analyzedPredicate(
                "SELECT id FROM test.search_docs WHERE search('title:(foo bar*)')");
        CompoundPredicate alternatives = assertCompound(predicate, CompoundPredicate.Operator.OR);
        assertMatch(alternatives.getChild(0), "title", "foo", MatchExpr.MatchOperator.MATCH_ANY);
        assertMatch(alternatives.getChild(1), "title", "bar%", MatchExpr.MatchOperator.MATCH);
    }

    @Test
    public void testDefaultOperatorCombinesNotInsideImplicitClause() throws Exception {
        Expr predicate = analyzedPredicate("SELECT id FROM test.search_docs "
                + "WHERE search('title:(foo NOT bar)', '{\"default_operator\":\"and\"}')");
        CompoundPredicate and = assertCompound(predicate, CompoundPredicate.Operator.AND);
        assertMatch(and.getChild(0), "title", "foo", MatchExpr.MatchOperator.MATCH_ALL);
        CompoundPredicate not = assertCompound(and.getChild(1), CompoundPredicate.Operator.NOT);
        assertMatch(not.getChild(0), "title", "bar", MatchExpr.MatchOperator.MATCH_ALL);
    }

    @Test
    public void testMixedExplicitAndDefaultField() throws Exception {
        String plan = analyzedWhere("SELECT id FROM test.search_docs "
                + "WHERE search('category:database AND cloud', '{\"default_field\":\"body\"}')");
        assertThat(plan).contains("category MATCH_ANY 'database'", "body MATCH_ANY 'cloud'");

        String singleFieldPlan = analyzedWhere("SELECT id FROM test.search_docs "
                + "WHERE search('category:database AND cloud', '{\"fields\":[\"body\"]}')");
        assertThat(singleFieldPlan).isEqualTo(plan);

        String singleCrossFieldPlan = analyzedWhere("SELECT id FROM test.search_docs "
                + "WHERE search('category:database AND cloud', "
                + "'{\"fields\":[\"body\"],\"type\":\"cross_fields\"}')");
        assertThat(singleCrossFieldPlan).isEqualTo(plan);

        assertThat(analyzedWhere("SELECT id FROM test.search_docs d WHERE search('d.title:cloud')"))
                .contains("title MATCH_ANY 'cloud'");
        assertThatThrownBy(() -> getFragmentPlan(
                "SELECT id FROM test.search_docs d WHERE search('search_docs.title:cloud')"))
                .hasMessageContaining("does not belong to table 'd'");
    }

    @Test
    public void testCrossFieldsExpansion() throws Exception {
        Expr predicate = analyzedPredicate("SELECT id FROM test.search_docs "
                + "WHERE search('foo AND bar', "
                + "'{\"fields\":[\"title\",\"body\"],\"type\":\"cross_fields\"}')");
        CompoundPredicate and = assertCompound(predicate, CompoundPredicate.Operator.AND);
        assertCrossFieldTerm(and.getChild(0), "foo", MatchExpr.MatchOperator.MATCH_ANY);
        assertCrossFieldTerm(and.getChild(1), "bar", MatchExpr.MatchOperator.MATCH_ANY);
    }

    @Test
    public void testImplicitBeforeExplicitAndForBestAndCrossFields() throws Exception {
        Expr best = analyzedPredicate("SELECT id FROM test.search_docs "
                + "WHERE search('foo bar AND baz', "
                + "'{\"fields\":[\"title\",\"body\"],\"type\":\"best_fields\"}')");
        CompoundPredicate bestAlternatives = assertCompound(best, CompoundPredicate.Operator.OR);
        assertBestImplicitAndBranch(bestAlternatives.getChild(0), "title",
                CompoundPredicate.Operator.OR, MatchExpr.MatchOperator.MATCH_ANY);
        assertBestImplicitAndBranch(bestAlternatives.getChild(1), "body",
                CompoundPredicate.Operator.OR, MatchExpr.MatchOperator.MATCH_ANY);

        Expr cross = analyzedPredicate("SELECT id FROM test.search_docs "
                + "WHERE search('foo bar AND baz', "
                + "'{\"fields\":[\"title\",\"body\"],\"type\":\"cross_fields\"}')");
        CompoundPredicate crossAnd = assertCompound(cross, CompoundPredicate.Operator.AND);
        CompoundPredicate implicit = assertCompound(crossAnd.getChild(0), CompoundPredicate.Operator.OR);
        assertCrossFieldTerm(implicit.getChild(0), "foo", MatchExpr.MatchOperator.MATCH_ANY);
        assertCrossFieldTerm(implicit.getChild(1), "bar", MatchExpr.MatchOperator.MATCH_ANY);
        assertCrossFieldTerm(crossAnd.getChild(1), "baz", MatchExpr.MatchOperator.MATCH_ANY);

        Expr bestWithDefaultAnd = analyzedPredicate("SELECT id FROM test.search_docs "
                + "WHERE search('foo bar AND baz', "
                + "'{\"fields\":[\"title\",\"body\"],\"type\":\"best_fields\","
                + "\"default_operator\":\"and\"}')");
        CompoundPredicate bestAndAlternatives = assertCompound(
                bestWithDefaultAnd, CompoundPredicate.Operator.OR);
        assertBestImplicitAndBranch(bestAndAlternatives.getChild(0), "title",
                CompoundPredicate.Operator.AND, MatchExpr.MatchOperator.MATCH_ALL);
        assertBestImplicitAndBranch(bestAndAlternatives.getChild(1), "body",
                CompoundPredicate.Operator.AND, MatchExpr.MatchOperator.MATCH_ALL);

        Expr crossWithDefaultAnd = analyzedPredicate("SELECT id FROM test.search_docs "
                + "WHERE search('foo bar AND baz', "
                + "'{\"fields\":[\"title\",\"body\"],\"type\":\"cross_fields\","
                + "\"default_operator\":\"and\"}')");
        CompoundPredicate crossDefaultAnd = assertCompound(crossWithDefaultAnd, CompoundPredicate.Operator.AND);
        CompoundPredicate implicitAnd = assertCompound(
                crossDefaultAnd.getChild(0), CompoundPredicate.Operator.AND);
        assertCrossFieldTerm(implicitAnd.getChild(0), "foo", MatchExpr.MatchOperator.MATCH_ALL);
        assertCrossFieldTerm(implicitAnd.getChild(1), "bar", MatchExpr.MatchOperator.MATCH_ALL);
        assertCrossFieldTerm(crossDefaultAnd.getChild(1), "baz", MatchExpr.MatchOperator.MATCH_ALL);
    }

    @Test
    public void testCrossFieldsNegatesCompleteExpandedLeaf() throws Exception {
        Expr predicate = analyzedPredicate("SELECT id FROM test.search_docs "
                + "WHERE search('NOT (foo OR bar)', "
                + "'{\"fields\":[\"title\",\"body\"],\"type\":\"cross_fields\"}')");
        List<Expr> negatedMatches = flatten(predicate, CompoundPredicate.Operator.AND);
        assertThat(negatedMatches).hasSize(4);
        assertNegatedMatch(negatedMatches.get(0), "title", "foo", MatchExpr.MatchOperator.MATCH_ANY);
        assertNegatedMatch(negatedMatches.get(1), "body", "foo", MatchExpr.MatchOperator.MATCH_ANY);
        assertNegatedMatch(negatedMatches.get(2), "title", "bar", MatchExpr.MatchOperator.MATCH_ANY);
        assertNegatedMatch(negatedMatches.get(3), "body", "bar", MatchExpr.MatchOperator.MATCH_ANY);
    }

    @Test
    public void testRewriterHandlesMultipleSearchLeavesAndKeepsOtherPredicates() throws Exception {
        Expr predicate = analyzedPredicate("SELECT id FROM test.search_docs WHERE id > 10 "
                + "AND search('title:foo') AND NOT search('body:bar')");
        List<Expr> conjuncts = flatten(predicate, CompoundPredicate.Operator.AND);
        assertThat(conjuncts).hasSize(3);
        assertThat(conjuncts.get(0)).isInstanceOf(BinaryPredicate.class);
        assertMatch(conjuncts.get(1), "title", "foo", MatchExpr.MatchOperator.MATCH_ANY);
        CompoundPredicate not = assertCompound(conjuncts.get(2), CompoundPredicate.Operator.NOT);
        assertMatch(not.getChild(0), "body", "bar", MatchExpr.MatchOperator.MATCH_ANY);

        List<MatchExpr> matches = new ArrayList<>();
        predicate.collect(MatchExpr.class, matches);
        assertThat(matches).hasSize(2);
    }

    @Test
    public void testBestFieldsExpansionWithExplicitLeaf() throws Exception {
        Expr predicate = analyzedPredicate("SELECT id FROM test.search_docs "
                + "WHERE search('category:database AND foo AND bar', "
                + "'{\"fields\":[\"title\",\"body\"],\"type\":\"best_fields\"}')");
        CompoundPredicate alternatives = assertCompound(predicate, CompoundPredicate.Operator.OR);
        assertBestFieldBranch(alternatives.getChild(0), "title");
        assertBestFieldBranch(alternatives.getChild(1), "body");
    }

    @Test
    public void testAnyAndAllRemainSingleMultiTermLeavesAcrossFields() throws Exception {
        Expr any = analyzedPredicate("SELECT id FROM test.search_docs "
                + "WHERE search('ANY(foo bar)', "
                + "'{\"fields\":[\"title\",\"body\"],\"type\":\"cross_fields\"}')");
        assertCrossFieldTerm(any, "foo bar", MatchExpr.MatchOperator.MATCH_ANY);

        Expr all = analyzedPredicate("SELECT id FROM test.search_docs "
                + "WHERE search('ALL(foo bar)', "
                + "'{\"fields\":[\"title\",\"body\"],\"type\":\"cross_fields\"}')");
        assertCrossFieldTerm(all, "foo bar", MatchExpr.MatchOperator.MATCH_ALL);
    }

    @Test
    public void testBestFieldsKeepsNestedNotFieldIndependent() throws Exception {
        Expr predicate = analyzedPredicate("SELECT id FROM test.search_docs "
                + "WHERE search('foo AND (bar OR NOT baz)', "
                + "'{\"fields\":[\"title\",\"body\"],\"type\":\"best_fields\"}')");
        CompoundPredicate alternatives = assertCompound(predicate, CompoundPredicate.Operator.OR);
        assertNestedBestFieldBranch(alternatives.getChild(0), "title");
        assertNestedBestFieldBranch(alternatives.getChild(1), "body");

        Expr explicitNot = analyzedPredicate("SELECT id FROM test.search_docs "
                + "WHERE search('foo AND NOT category:bar', "
                + "'{\"fields\":[\"title\",\"body\"],\"type\":\"best_fields\"}')");
        CompoundPredicate explicitAlternatives = assertCompound(explicitNot, CompoundPredicate.Operator.OR);
        assertExplicitNotBranch(explicitAlternatives.getChild(0), "title");
        assertExplicitNotBranch(explicitAlternatives.getChild(1), "body");
    }

    @Test
    public void testBestFieldsNegatesCompleteChildResult() throws Exception {
        Expr predicate = analyzedPredicate("SELECT id FROM test.search_docs "
                + "WHERE search('NOT (foo AND bar)', "
                + "'{\"fields\":[\"title\",\"body\"],\"type\":\"best_fields\"}')");
        CompoundPredicate fields = assertCompound(predicate, CompoundPredicate.Operator.AND);
        assertSameFieldNegatedOr(fields.getChild(0), "title", "foo", "bar");
        assertSameFieldNegatedOr(fields.getChild(1), "body", "foo", "bar");
    }

    @Test
    public void testWildcardNormalizationAndExists() throws Exception {
        Expr wildcard = analyzedPredicate(
                "SELECT id FROM test.search_docs WHERE search('title:Machine*')");
        assertMatch(wildcard, "title", "machine%", MatchExpr.MatchOperator.MATCH);

        Expr caseSensitive = analyzedPredicate(
                "SELECT id FROM test.search_docs WHERE search('case_text:Machine*')");
        assertMatch(caseSensitive, "case_text", "Machine%", MatchExpr.MatchOperator.MATCH);

        Expr exists = analyzedPredicate("SELECT id FROM test.search_docs WHERE search('title:*')");
        assertThat(exists).isInstanceOf(IsNullPredicate.class);
        IsNullPredicate isNotNull = (IsNullPredicate) exists;
        assertThat(isNotNull.isNotNull()).isTrue();
        assertThat(isNotNull.getChild(0)).isInstanceOf(SlotRef.class);
        assertThat(((SlotRef) isNotNull.getChild(0)).getColName()).isEqualTo("title");
    }

    @Test
    public void testBindingFailures() {
        assertThatThrownBy(() -> getFragmentPlan(
                "SELECT id FROM test.search_docs WHERE search('foo')"))
                .hasMessageContaining("default_field");
        assertThatThrownBy(() -> getFragmentPlan(
                "SELECT id FROM test.search_docs WHERE search('missing:foo')"))
                .hasMessageContaining("unknown search() field");
        assertThatThrownBy(() -> getFragmentPlan(
                "SELECT id FROM test.search_docs WHERE search('plain_text:foo')"))
                .hasMessageContaining("requires a GIN index");
        assertThatThrownBy(() -> getFragmentPlan(
                "SELECT id FROM test.search_docs WHERE search('number_value:foo')"))
                .hasMessageContaining("must be CHAR, VARCHAR, or STRING");
        assertThatThrownBy(() -> getFragmentPlan(
                "SELECT id FROM test.search_docs d WHERE search('ignored.d.title:foo')"))
                .hasMessageContaining("must use 'column' or 'table_alias.column'");
        assertThatThrownBy(() -> getFragmentPlan("SELECT id FROM test.search_docs "
                + "WHERE search('foo', '{\"fields\":[\"title\",\"search_docs.title\"]}')"))
                .hasMessageContaining("same column more than once");
    }

    @Test
    public void testExpandedExpressionBudgetBoundary() throws Exception {
        // MATCH contributes three nodes, EXISTS contributes two, and N leaves need N - 1 OR nodes.
        String allowed = "SELECT id FROM test.search_docs WHERE search('" + mixedClauses(2498, 3) + "')";
        assertThat(UtFrameUtils.parseStmtWithNewParser(allowed, connectContext)).isNotNull();

        String rejected = "SELECT id FROM test.search_docs WHERE search('" + mixedClauses(2499, 2) + "')";
        assertThatThrownBy(() -> UtFrameUtils.parseStmtWithNewParser(rejected, connectContext))
                .hasMessageContaining("more than 10000 expression nodes");
    }

    @Test
    public void testCrossFieldsRequiresCompatibleAnalyzers() {
        assertThatThrownBy(() -> getFragmentPlan("SELECT id FROM test.search_docs "
                + "WHERE search('foo', "
                + "'{\"fields\":[\"title\",\"standard_text\"],\"type\":\"cross_fields\"}')"))
                .hasMessageContaining("compatible GIN parser");
    }

    @Test
    public void testExactMultipleWordsRemainOneDictionaryTerm() throws Exception {
        Expr exact = analyzedPredicate(
                "SELECT id FROM test.search_docs WHERE search('title:EXACT(Hello  \tWorld)')");
        assertMatch(exact, "title", "hello  \tworld", MatchExpr.MatchOperator.MATCH);
    }

    @Test
    public void testSearchMustBeDirectWherePredicate() {
        assertThatThrownBy(() -> getFragmentPlan(
                "SELECT search('title:foo') FROM test.search_docs"))
                .hasMessageContaining("WHERE predicate");
        assertThatThrownBy(() -> getFragmentPlan(
                "SELECT id FROM test.search_docs GROUP BY id HAVING search('title:foo')"))
                .hasMessageContaining("not in HAVING");
        assertThatThrownBy(() -> getFragmentPlan(
                "SELECT count(*) FROM test.search_docs GROUP BY search('title:foo')"))
                .hasMessageContaining("not in GROUP BY");
        assertThatThrownBy(() -> getFragmentPlan(
                "SELECT id FROM test.search_docs ORDER BY search('title:foo')"))
                .hasMessageContaining("not in ORDER BY");
        assertThatThrownBy(() -> getFragmentPlan("SELECT a.id FROM test.search_docs a "
                + "JOIN test.search_docs b ON search('a.title:foo')"))
                .hasMessageContaining("not in JOIN ON");
        assertThatThrownBy(() -> getFragmentPlan(
                "SELECT id FROM test.search_docs WHERE if(search('title:foo'), true, false)"))
                .hasMessageContaining("boolean leaf");
        assertThatThrownBy(() -> getFragmentPlan("SELECT a.id FROM test.search_docs a "
                + "JOIN test.search_docs b ON a.id=b.id WHERE search('a.title:foo')"))
                .hasMessageContaining("one OLAP table only");
    }

    @Test
    public void testNestedQueryBlockIsAnalyzedIndependently() throws Exception {
        String plan = getVerboseExplain("SELECT id FROM test.search_docs WHERE id IN "
                + "(SELECT id FROM test.search_docs WHERE search('title:foo'))");
        assertThat(plan).contains("title", "MATCH_ANY 'foo'");
    }

    @Test
    public void testSearchThroughPassThroughSubquery() throws Exception {
        String sql = "SELECT id FROM (SELECT id, heading AS search_text, content FROM "
                + "(SELECT id, title AS heading, body AS content "
                + "FROM test.search_docs WHERE id > 0) source_docs) docs "
                + "WHERE search('foo', "
                + "'{\"fields\":[\"search_text\",\"content\"],\"type\":\"cross_fields\"}')";
        CompoundPredicate alternatives = assertCompound(analyzedPredicate(sql), CompoundPredicate.Operator.OR);
        assertMatch(alternatives.getChild(0), "search_text", "foo", MatchExpr.MatchOperator.MATCH_ANY);
        assertMatch(alternatives.getChild(1), "content", "foo", MatchExpr.MatchOperator.MATCH_ANY);
        assertThat(getVerboseExplain(sql)).contains("title", "body", "MATCH_ANY 'foo'");
    }

    @Test
    public void testSearchRejectsNonPassThroughSubquery() {
        assertThatThrownBy(() -> getFragmentPlan("SELECT id FROM "
                + "(SELECT id, lower(title) AS heading FROM test.search_docs) docs "
                + "WHERE search('heading:foo')"))
                .hasMessageContaining("not a direct column reference");
        assertThatThrownBy(() -> getFragmentPlan("SELECT id FROM "
                + "(SELECT id, title FROM test.search_docs LIMIT 1) docs "
                + "WHERE search('title:foo')"))
                .hasMessageContaining("one OLAP table only");
    }

    @Test
    public void testKillSwitchLeavesNormalFunctionResolution() {
        boolean previous = Config.enable_search_function;
        try {
            Config.enable_search_function = false;
            assertThatThrownBy(() -> getFragmentPlan(
                    "SELECT id FROM test.search_docs WHERE search('title:foo')"))
                    .hasMessageContaining("No matching function");
        } finally {
            Config.enable_search_function = previous;
        }
    }

    @Test
    public void testEnabledSearchFunctionDoesNotFallBackByArgumentShape() {
        assertThatThrownBy(() -> getFragmentPlan(
                "SELECT id FROM test.search_docs WHERE search()"))
                .hasMessageContaining("expects one DSL string and an optional options string");
        assertThatThrownBy(() -> getFragmentPlan(
                "SELECT id FROM test.search_docs WHERE search(*)"))
                .isInstanceOf(ParsingException.class)
                .hasMessageContaining("Getting syntax error");
        assertThatThrownBy(() -> getFragmentPlan(
                "SELECT id FROM test.search_docs WHERE search(DISTINCT 'title:foo')"))
                .isInstanceOf(ParsingException.class)
                .hasMessageContaining("Getting syntax error");
        assertThatThrownBy(() -> getFragmentPlan(
                "SELECT id FROM test.search_docs WHERE search(title)"))
                .hasMessageContaining("arguments must be constant strings");
        assertThatThrownBy(() -> getFragmentPlan(
                "SELECT id FROM test.search_docs WHERE search('title:foo', title)"))
                .hasMessageContaining("arguments must be constant strings");
        assertThatThrownBy(() -> getFragmentPlan(
                "SELECT id FROM test.search_docs WHERE search('title:foo', '{}', 'extra')"))
                .hasMessageContaining("expects one DSL string and an optional options string");
    }

    @Test
    public void testDslAndOptionsErrorsAreAnalyzerErrors() {
        assertThatThrownBy(() -> getFragmentPlan(
                "SELECT id FROM test.search_docs WHERE search('title:ANY()')"))
                .isInstanceOf(SemanticException.class)
                .hasMessageContaining("unexpected");
        assertThatThrownBy(() -> getFragmentPlan("SELECT id FROM test.search_docs "
                + "WHERE search('title:foo', '{\"default_operator\":\"xor\"}')"))
                .isInstanceOf(SemanticException.class)
                .hasMessageContaining("default_operator");
    }

    @Test
    public void testViewPersistsCanonicalMatchOperators() throws Exception {
        starRocksAssert.withView("CREATE VIEW test.search_view AS SELECT id FROM test.search_docs "
                + "WHERE search('title:ALL(foo bar)')");
        try {
            View view = (View) starRocksAssert.getTable("test", "search_view");
            assertThat(view.getInlineViewDef()).contains("MATCH_ALL").doesNotContain("search(");
            assertThat(getFragmentPlan("SELECT * FROM test.search_view")).contains("MATCH_ALL");
        } finally {
            starRocksAssert.dropView("test.search_view");
        }
    }

    @Test
    public void testMaterializedViewDefinitionIsRejected() {
        assertThatThrownBy(() -> starRocksAssert.withMaterializedView(
                "CREATE MATERIALIZED VIEW test.search_mv "
                        + "DISTRIBUTED BY RANDOM AS SELECT id FROM test.search_docs "
                        + "WHERE search('title:foo')"))
                .hasMessageContaining("not supported in materialized view definitions");

        assertThatThrownBy(() -> starRocksAssert.withMaterializedView(
                "CREATE MATERIALIZED VIEW test.search_mv_nested "
                        + "DISTRIBUTED BY RANDOM AS SELECT id FROM test.search_docs "
                        + "WHERE id IN (SELECT id FROM test.search_docs WHERE search('title:foo'))"))
                .hasMessageContaining("not supported in materialized view definitions");

        assertThatThrownBy(() -> starRocksAssert.withMaterializedView(
                "CREATE MATERIALIZED VIEW test.search_sync_mv AS "
                        + "SELECT id, count(*) FROM test.search_docs "
                        + "WHERE search('title:foo') GROUP BY id"))
                .hasMessageContaining("not supported in materialized view definitions");
    }

    @Test
    public void testPreparedStatementIsRejected() {
        assertThatThrownBy(() -> UtFrameUtils.parseStmtWithNewParser(
                "PREPARE search_stmt FROM SELECT id FROM test.search_docs "
                        + "WHERE search('title:foo') AND id > ?", connectContext))
                .hasMessageContaining("search() is not supported in prepared statements");
    }

    private static String mixedClauses(int matchCount, int existsCount) {
        StringBuilder result = new StringBuilder((matchCount + existsCount) * 11);
        for (int i = 0; i < matchCount + existsCount; ++i) {
            if (result.length() > 0) {
                result.append(" OR ");
            }
            result.append(i < matchCount ? "title:a" : "title:*");
        }
        return result.toString();
    }

    private static String analyzedWhere(String sql) throws Exception {
        return AstToStringBuilder.toString(analyzedPredicate(sql));
    }

    // Full statement analysis pushes NOT through compound predicates before this helper returns.
    private static Expr analyzedPredicate(String sql) throws Exception {
        QueryStatement statement = (QueryStatement) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        return ((SelectRelation) statement.getQueryRelation()).getWhereClause();
    }

    private static void assertNestedBestFieldBranch(Expr expression, String field) {
        CompoundPredicate and = assertCompound(expression, CompoundPredicate.Operator.AND);
        assertMatch(and.getChild(0), field, "foo");
        CompoundPredicate or = assertCompound(and.getChild(1), CompoundPredicate.Operator.OR);
        assertMatch(or.getChild(0), field, "bar");

        CompoundPredicate globalBaz = assertCompound(or.getChild(1), CompoundPredicate.Operator.AND);
        assertNegatedMatch(globalBaz.getChild(0), "title", "baz", MatchExpr.MatchOperator.MATCH_ANY);
        assertNegatedMatch(globalBaz.getChild(1), "body", "baz", MatchExpr.MatchOperator.MATCH_ANY);
    }

    private static void assertBestFieldBranch(Expr expression, String field) {
        List<Expr> leaves = flatten(expression, CompoundPredicate.Operator.AND);
        assertThat(leaves).hasSize(3);
        assertMatch(leaves.get(0), "category", "database", MatchExpr.MatchOperator.MATCH_ANY);
        assertMatch(leaves.get(1), field, "foo", MatchExpr.MatchOperator.MATCH_ANY);
        assertMatch(leaves.get(2), field, "bar", MatchExpr.MatchOperator.MATCH_ANY);
    }

    private static void assertBestImplicitAndBranch(Expr expression, String field,
                                                    CompoundPredicate.Operator implicitOperator,
                                                    MatchExpr.MatchOperator matchOperator) {
        CompoundPredicate and = assertCompound(expression, CompoundPredicate.Operator.AND);
        assertSameFieldCompound(and.getChild(0), implicitOperator,
                field, "foo", "bar", matchOperator);
        assertMatch(and.getChild(1), field, "baz", matchOperator);
    }

    private static void assertExplicitNotBranch(Expr expression, String field) {
        CompoundPredicate and = assertCompound(expression, CompoundPredicate.Operator.AND);
        assertMatch(and.getChild(0), field, "foo");
        CompoundPredicate not = assertCompound(and.getChild(1), CompoundPredicate.Operator.NOT);
        assertMatch(not.getChild(0), "category", "bar");
    }

    private static void assertSameFieldNegatedOr(Expr expression, String field,
                                                 String leftQuery, String rightQuery) {
        CompoundPredicate or = assertCompound(expression, CompoundPredicate.Operator.OR);
        assertNegatedMatch(or.getChild(0), field, leftQuery, MatchExpr.MatchOperator.MATCH_ANY);
        assertNegatedMatch(or.getChild(1), field, rightQuery, MatchExpr.MatchOperator.MATCH_ANY);
    }

    private static void assertNegatedMatch(Expr expression, String field, String query,
                                           MatchExpr.MatchOperator operator) {
        CompoundPredicate not = assertCompound(expression, CompoundPredicate.Operator.NOT);
        assertMatch(not.getChild(0), field, query, operator);
    }

    private static void assertSameFieldCompound(Expr expression, CompoundPredicate.Operator compoundOperator,
                                                String field, String leftQuery, String rightQuery,
                                                MatchExpr.MatchOperator matchOperator) {
        CompoundPredicate compound = assertCompound(expression, compoundOperator);
        assertMatch(compound.getChild(0), field, leftQuery, matchOperator);
        assertMatch(compound.getChild(1), field, rightQuery, matchOperator);
    }

    private static CompoundPredicate assertCompound(Expr expression, CompoundPredicate.Operator operator) {
        assertThat(expression).isInstanceOf(CompoundPredicate.class);
        CompoundPredicate compound = (CompoundPredicate) expression;
        assertThat(compound.getOp()).isEqualTo(operator);
        return compound;
    }

    private static List<Expr> flatten(Expr expression, CompoundPredicate.Operator operator) {
        List<Expr> result = new ArrayList<>();
        flattenInto(expression, operator, result);
        return result;
    }

    private static void flattenInto(Expr expression, CompoundPredicate.Operator operator, List<Expr> result) {
        if (expression instanceof CompoundPredicate && ((CompoundPredicate) expression).getOp() == operator) {
            for (Expr child : expression.getChildren()) {
                flattenInto(child, operator, result);
            }
            return;
        }
        result.add(expression);
    }

    private static void assertMatch(Expr expression, String field, String query) {
        assertMatch(expression, field, query, null);
    }

    private static void assertMatch(Expr expression, String field, String query,
                                    MatchExpr.MatchOperator operator) {
        assertThat(expression).isInstanceOf(MatchExpr.class);
        MatchExpr match = (MatchExpr) expression;
        if (operator != null) {
            assertThat(match.getMatchOperator()).isEqualTo(operator);
        }
        assertThat(match.getChild(0)).isInstanceOf(SlotRef.class);
        assertThat(((SlotRef) match.getChild(0)).getColName()).isEqualTo(field);
        assertThat(match.getChild(1)).isInstanceOf(StringLiteral.class);
        assertThat(((StringLiteral) match.getChild(1)).getValue()).isEqualTo(query);
    }

    private static void assertCrossFieldTerm(Expr expression, String query, MatchExpr.MatchOperator operator) {
        CompoundPredicate alternatives = assertCompound(expression, CompoundPredicate.Operator.OR);
        assertMatch(alternatives.getChild(0), "title", query, operator);
        assertMatch(alternatives.getChild(1), "body", query, operator);
    }
}
