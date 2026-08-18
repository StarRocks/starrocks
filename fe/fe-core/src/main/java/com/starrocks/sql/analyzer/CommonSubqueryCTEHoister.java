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
import com.starrocks.catalog.TableName;
import com.starrocks.sql.ast.AstTraverser;
import com.starrocks.sql.ast.CTERelation;
import com.starrocks.sql.ast.GroupByClause;
import com.starrocks.sql.ast.JoinRelation;
import com.starrocks.sql.ast.PivotRelation;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.SelectListItem;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.SetOperationRelation;
import com.starrocks.sql.ast.SubqueryRelation;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.Parameter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Hoists derived tables that are textually identical into a single synthetic CTE, so the shared
 * computation is expressed once and the CTE machinery can run it once.
 *
 * <p>Motivating case is TPC-DS q65, which spells the same derived table twice - once used directly and
 * once nested inside another aggregate:
 *
 * <pre>
 * from store, item,
 *      (select ss_store_sk, avg(revenue) ave
 *       from (select ss_store_sk, ss_item_sk, sum(ss_sales_price) revenue ...) sa
 *       group by ss_store_sk) sb,
 *      (select ss_store_sk, ss_item_sk, sum(ss_sales_price) revenue ...) sc
 * </pre>
 *
 * Rewriting it by hand into {@code with sa as (...) ... from ..., sa sc} cut the query from 16.0s to
 * 10.4s on TPC-DS SF1000 (scan rows halved, CPU -58%, peak memory -50%); this pass produces that same
 * statement automatically.
 *
 * <p><b>Why the AST and not the optimizer.</b> At plan level the equivalent rewrite has to discover a
 * bijection between the {@code ColumnRefOperator}s of two independently analyzed subtrees. Nothing in
 * the optimizer does that today: every existing rule that builds a {@code LogicalCTEProduceOperator}
 * constructs both the producer and its consumers itself, so it knows the mapping by construction. Here
 * the two copies came from the same characters, so no bijection is needed at all - and none of the
 * operator-level hazards (type/nullability erasure in {@code ScalarOperator#toString}, operator fields
 * missing from {@code equals}, captured lambda columns) apply.
 *
 * <p><b>What this pass does not decide.</b> It only creates the CTE. Whether the CTE is materialized and
 * shared or inlined back is left entirely to the existing cost model: a CTE with two references reaches
 * the memo (see {@code RelationTransformer#buildCTEAnchorAndProducer}) and
 * {@code CostModel#visitPhysicalCTEAnchor} arbitrates against {@code cbo_cte_reuse_rate}.
 *
 * <p><b>Materialized views take priority.</b> MV rewrite recognizes a query by its AST, and rewriting that
 * AST makes the query stop matching an MV whose definition was never rewritten. The caller therefore skips
 * this optimization entirely for any query that touches a table with a related MV; see
 * {@code StatementPlanner#hasRelatedMaterializedView} for the mechanism and the reasoning.
 *
 * <p><b>Correlation.</b> A derived table that reads an outer column cannot be recognized before name
 * resolution, so this pass does not try. What makes that safe is the choice of destination: hoisting to
 * the <em>outermost</em> query block means the body is analyzed against an essentially empty outer scope,
 * where an outer reference can only fail to resolve - it can never quietly bind to a different column.
 * The caller therefore reverts the rewrite and re-analyzes the original statement whenever analysis
 * throws; see {@code StatementPlanner#analyzeWithCommonSubqueryCte}. (How far out such a reference may
 * legally reach is a separate question - {@code Scope#resolveField} only permits the first outer level -
 * but the fallback does not depend on the answer.)
 */
public final class CommonSubqueryCTEHoister {
    private static final Logger LOG = LogManager.getLogger(CommonSubqueryCTEHoister.class);

    private static final String CTE_NAME_PREFIX = "__sr_cse_";
    private static final int MIN_OCCURRENCES = 2;

    /** Matches every word-shaped token of the serialized subquery, including back-quoted identifiers. */
    private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_$]*");

    private CommonSubqueryCTEHoister() {
    }

    /**
     * Rewrites {@code stmt} in place. Returns a record that can undo the rewrite; the record is empty
     * when nothing was hoisted.
     */
    public static HoistRecord hoist(QueryStatement stmt) {
        HoistRecord record = new HoistRecord();
        QueryRelation top = stmt == null ? null : stmt.getQueryRelation();
        if (top == null) {
            return record;
        }

        // A prepared statement re-plans one cached AST on every EXECUTE: PrepareStmt#assignValues mutates the
        // Parameter objects in place and hands back the same inner statement. Two bodies differing only in a
        // parameter serialize identically whenever this execution happens to bind equal values, and hoisting
        // would detach one of them permanently - a later EXECUTE with different values would then feed both
        // consumers from the surviving parameter. Whether two bodies are the same must not depend on the
        // current bindings, so a parameterized statement is left alone entirely.
        if (containsParameter(stmt)) {
            return record;
        }

        Collector collector = new Collector();
        collector.collectQuery(top);
        if (collector.sites.size() < MIN_OCCURRENCES) {
            return record;
        }

        Map<String, List<Site>> groups = new LinkedHashMap<>();
        for (Site site : collector.sites) {
            String key = candidateKey(site, collector.cteNames);
            if (key != null) {
                groups.computeIfAbsent(key, k -> new ArrayList<>()).add(site);
            }
        }

        List<List<Site>> candidates = new ArrayList<>();
        for (List<Site> group : groups.values()) {
            if (group.size() >= MIN_OCCURRENCES) {
                candidates.add(group);
            }
        }
        if (candidates.isEmpty()) {
            return record;
        }
        // Outermost groups first: a shared subquery that encloses another one subsumes it, and hoisting
        // the outer one already de-duplicates whatever it contains.
        candidates.sort(Comparator.comparingInt(CommonSubqueryCTEHoister::minDepth));

        Set<SubqueryRelation> claimed = Collections.newSetFromMap(new IdentityHashMap<>());
        Set<String> reserved = collector.reservedNames;
        int sequence = 0;
        for (List<Site> group : candidates) {
            if (overlaps(group, claimed)) {
                continue;
            }
            String name = allocateName(reserved, sequence++);
            if (!apply(top, group, name, record)) {
                continue;
            }
            reserved.add(name);
            for (Site site : group) {
                claimed.add(site.subquery);
            }
        }
        return record;
    }

    private static boolean apply(QueryRelation top, List<Site> group, String name, HoistRecord record) {
        QueryStatement body = group.get(0).subquery.getQueryStatement();
        // Same allocation scheme the parser uses for a WITH clause (AstBuilder#visitCommonTableExpression):
        // the identity hash of the body relation, which is unique among the relations alive in this statement.
        CTERelation cte = new CTERelation(RelationId.of(body.getQueryRelation()).hashCode(), name,
                null /* let the analyzer derive the output names */, body, false, true);

        List<CTERelation> cteRelations = top.getCteRelations();
        // Prepend: a hoisted body never references a user CTE (guarded below), so it must not be able to
        // shadow or depend on one, and being first keeps it visible to every later definition.
        cteRelations.add(0, cte);
        record.added.add(cte);
        record.owner = top;

        for (Site site : group) {
            TableRelation reference = new TableRelation(new TableName(null, name));
            // The CTE lookup in QueryAnalyzer#resolveTableRef only fires when the db part is empty.
            TableName alias = site.subquery.getAlias();
            if (alias != null) {
                reference.setAlias(alias);
            }
            Consumer<Relation> setter = site.setter;
            SubqueryRelation original = site.subquery;
            record.undo.add(() -> setter.accept(original));
            setter.accept(reference);
        }
        return true;
    }

    private static int minDepth(List<Site> group) {
        int min = Integer.MAX_VALUE;
        for (Site site : group) {
            min = Math.min(min, site.depth);
        }
        return min;
    }

    private static boolean overlaps(List<Site> group, Set<SubqueryRelation> claimed) {
        for (Site site : group) {
            if (claimed.contains(site.subquery)) {
                return true;
            }
            for (SubqueryRelation ancestor : site.ancestors) {
                if (claimed.contains(ancestor)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * The canonical text of a hoistable subquery body, or null when the site fails a guard. The text
     * doubles as the grouping key, so anything it does not distinguish must be rejected here.
     */
    private static String candidateKey(Site site, Set<String> cteNames) {
        // LATERAL binds the right side to the left one; StarRocks only accepts it for table functions
        // anyway (QueryAnalyzer#visitJoin), but reject it rather than rely on that.
        if (site.lateral) {
            return null;
        }
        SubqueryRelation subquery = site.subquery;
        // Two occurrences of a LIMIT without ORDER BY may legitimately return different rows today; sharing
        // would force them to agree. The check has to cover the whole subtree, not just the candidate's own
        // top-level relation: a nested derived table keeps its LIMIT (SubqueryRelation's constructor only
        // drops ORDER BY when there is no LIMIT), so `(select * from (select * from t limit 1) x)` is just as
        // unsafe to share as a LIMIT written directly on the candidate.
        if (site.hasLimit) {
            return null;
        }
        // `(select ...) t(a, b)` renames the output; the CTE would have to carry the names too.
        if (subquery.getExplicitColumnNames() != null) {
            return null;
        }
        // ASSERT_ROWS asks RelationTransformer#visitSubqueryRelation for a LogicalAssertOneRowOperator.
        // Replacing the site with a plain table reference would drop it, turning a query that should fail
        // the assertion into one that quietly returns rows.
        if (subquery.isAssertRows()) {
            return null;
        }
        if (!site.hasTable) {
            return null;
        }
        // Everything the body is built from must be on the allowlist. Sharing is only safe when a body's
        // result is a function of its text, and the guards above are a blacklist over an open-ended
        // language - each round of review has found another construct belonging on it. Requiring the
        // relations to be recognized flips the failure mode: a construct nobody has considered yet is
        // simply not optimized, rather than silently made to agree across the two occurrences.
        if (!isAllowlistedRelation(subquery)) {
            return null;
        }

        String sql;
        try {
            sql = AstToSQLBuilder.toSQL(subquery.getQueryStatement());
        } catch (Exception e) {
            // toSQL is schema-dependent for a few relation kinds; those simply do not participate.
            LOG.debug("cannot serialize subquery for common-subquery hoisting", e);
            return null;
        }
        if (sql == null || sql.isEmpty()) {
            return null;
        }

        Set<String> words = words(sql);
        // Cheap pre-filter only; isUnsafeAfterAnalysis is what actually decides, because a view's body is
        // invisible here. See isUnsharableFunction for why the time functions are not in this set.
        for (String fn : FunctionSet.nonDeterministicFunctions) {
            if (words.contains(fn)) {
                return null;
            }
        }
        if (words.contains(FunctionSet.ANY_VALUE)) {
            return null;
        }
        // The same characters can bind to different objects when a WITH shadows a name at one site but
        // not at the other. Cheap over-approximation: refuse any body that mentions a CTE name at all.
        for (String cteName : cteNames) {
            if (words.contains(cteName)) {
                return null;
            }
        }
        return sql;
    }

    /**
     * Re-checks the hoisted bodies once analysis has resolved them, and reports whether any of them turned out
     * to be unsafe to share after all.
     *
     * <p>The pre-analysis guards work on SQL text, which cannot see through a view: views are only expanded
     * inside {@link QueryAnalyzer}, so a derived table reading a view whose definition calls {@code rand()} or
     * carries a {@code LIMIT} looks perfectly safe beforehand. Sharing it would make two references agree where
     * they were previously free to differ. Here the bodies are fully resolved and view bodies are reachable, so
     * this check is the authoritative one; the textual guards are only a cheap pre-filter.
     */
    public static boolean isUnsafeAfterAnalysis(HoistRecord record) {
        for (CTERelation cte : record.added) {
            boolean[] found = {false};
            new AstTraverser<Void, Void>() {
                @Override
                public Void visitFunctionCall(FunctionCallExpr expr, Void context) {
                    // A UDF's body is opaque to us and may well return something different per call, while
                    // the name check below only recognizes built-ins. Analysis has resolved the function by
                    // now, which is the only point where the two can be told apart at all.
                    Function fn = expr.getFn();
                    if ((fn != null && fn.isUdf()) || isUnsharableFunction(expr.getFunctionName())) {
                        found[0] = true;
                        return null;
                    }
                    return super.visitFunctionCall(expr, context);
                }

                @Override
                public Void visitQueryRelation(QueryRelation node, Void context) {
                    if (node.hasLimit()) {
                        found[0] = true;
                        return null;
                    }
                    return super.visitQueryRelation(node, context);
                }
            }.visit(cte.getCteQueryStatement());
            if (found[0]) {
                return true;
            }
        }
        return false;
    }

    /**
     * Functions whose result two identical occurrences may legitimately disagree on today, so that sharing them
     * would narrow behaviour the query never promised.
     *
     * <p>Deliberately narrow, and only half the story - the caller also rejects every UDF outright, since a
     * user-defined body could return anything per call and no name list can know. {@code
     * FunctionSet#nonDeterministicTimeFunctions} is <em>not</em> included: those fold to one constant per
     * query, so two occurrences already agree. {@code any_value} is, because it is defined as picking an
     * arbitrary member of its group. What remains uncovered is the under-specified built-in aggregates -
     * {@code group_concat} / {@code array_agg} without an ORDER BY, or {@code min_by} / {@code max_by} on ties -
     * because deciding that in general needs a notion of order sensitivity the FE does not have today.
     */
    private static boolean isUnsharableFunction(String name) {
        if (name == null) {
            return false;
        }
        String lower = name.toLowerCase(Locale.ROOT);
        return FunctionSet.nonDeterministicFunctions.contains(lower) || FunctionSet.ANY_VALUE.equals(lower);
    }

    private static boolean containsParameter(QueryStatement stmt) {
        boolean[] found = {false};
        new ParseTimeTraverser() {
            @Override
            public Void visitExpression(Expr node, Void context) {
                if (node instanceof Parameter) {
                    found[0] = true;
                    return null;
                }
                return super.visitExpression(node, context);
            }
        }.visit(stmt);
        return found[0];
    }

    /**
     * {@link AstTraverser#visitSelect} reaches the SELECT list and GROUP BY through {@code getOutputExpression()}
     * and {@code getGroupBy()}, which only the analyzer fills in. Before analysis those are null and both clauses
     * are silently skipped, so a pre-analysis scan built on the base traverser has a blind spot exactly where
     * expressions usually live. This walks the parse-time clauses instead.
     */
    private static class ParseTimeTraverser extends AstTraverser<Void, Void> {
        @Override
        public Void visitSelect(SelectRelation node, Void context) {
            if (node.getSelectList() != null) {
                for (SelectListItem item : node.getSelectList().getItems()) {
                    if (item.getExpr() != null) {
                        visit(item.getExpr(), context);
                    }
                }
            }
            GroupByClause groupBy = node.getGroupByClause();
            if (groupBy != null) {
                if (groupBy.getOriGroupingExprs() != null) {
                    groupBy.getOriGroupingExprs().forEach(expr -> visit(expr, context));
                }
                if (groupBy.getGroupingSetList() != null) {
                    groupBy.getGroupingSetList().forEach(set -> set.forEach(expr -> visit(expr, context)));
                }
            }
            if (node.getWhereClause() != null) {
                visit(node.getWhereClause(), context);
            }
            if (node.getHavingClause() != null) {
                visit(node.getHavingClause(), context);
            }
            return super.visitSelect(node, context);
        }
    }

    /**
     * Whether every relation making up this body is one we have reasoned about.
     *
     * <p>Only relations whose result is fully determined by the SQL that produced them qualify. The
     * exclusions are the ones that are not: {@code TABLE SAMPLE} draws an independent subset per relation,
     * so two identical clauses may legitimately disagree and sharing would force one draw on both;
     * {@code ASSERT_ROWS} attaches an assertion that a plain CTE reference would drop; a {@code LATERAL}
     * join binds its right side to the left; and a column-alias list renames the output the CTE would have
     * to carry. Any relation type not listed is rejected rather than assumed harmless.
     */
    private static boolean isAllowlistedRelation(Relation relation) {
        if (relation instanceof TableRelation) {
            // A sample is redrawn per relation, so two identical clauses are not interchangeable.
            return ((TableRelation) relation).getSampleClause() == null;
        }
        if (relation instanceof JoinRelation) {
            JoinRelation join = (JoinRelation) relation;
            return !join.isLateral()
                    && isAllowlistedRelation(join.getLeft())
                    && isAllowlistedRelation(join.getRight());
        }
        if (relation instanceof SubqueryRelation) {
            SubqueryRelation subquery = (SubqueryRelation) relation;
            return !subquery.isAssertRows()
                    && subquery.getExplicitColumnNames() == null
                    && isAllowlistedRelation(subquery.getQueryStatement().getQueryRelation());
        }
        if (relation instanceof SelectRelation) {
            Relation from = ((SelectRelation) relation).getRelation();
            // `select 1` with no FROM is fine; it has nothing to share but nothing unsafe either.
            return from == null || isAllowlistedRelation(from);
        }
        if (relation instanceof SetOperationRelation) {
            for (QueryRelation child : ((SetOperationRelation) relation).getRelations()) {
                if (!isAllowlistedRelation(child)) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    private static Set<String> words(String sql) {
        Set<String> words = new HashSet<>();
        Matcher matcher = IDENTIFIER.matcher(sql);
        while (matcher.find()) {
            words.add(matcher.group().toLowerCase(Locale.ROOT));
        }
        return words;
    }

    private static String allocateName(Set<String> reserved, int sequence) {
        int suffix = sequence;
        String name = CTE_NAME_PREFIX + suffix;
        while (reserved.contains(name)) {
            name = CTE_NAME_PREFIX + (++suffix);
        }
        return name;
    }

    /** Undo log for a rewrite, so the caller can fall back when analysis rejects the rewritten AST. */
    public static final class HoistRecord {
        private final List<Runnable> undo = new ArrayList<>();
        private final List<CTERelation> added = new ArrayList<>();
        private QueryRelation owner;

        public boolean isEmpty() {
            return added.isEmpty();
        }

        public void revert() {
            for (int i = undo.size() - 1; i >= 0; i--) {
                undo.get(i).run();
            }
            undo.clear();
            if (owner != null) {
                List<CTERelation> cteRelations = owner.getCteRelations();
                for (CTERelation cte : added) {
                    cteRelations.removeIf(existing -> existing == cte);
                }
            }
            added.clear();
            owner = null;
        }
    }

    /** One derived table in a FROM clause, together with the way to replace it in its parent. */
    private static final class Site {
        private final SubqueryRelation subquery;
        private final Consumer<Relation> setter;
        private final List<SubqueryRelation> ancestors;
        private final int depth;
        private final boolean lateral;
        private boolean hasTable;
        private boolean hasLimit;

        private Site(SubqueryRelation subquery, Consumer<Relation> setter, List<SubqueryRelation> ancestors,
                     boolean lateral) {
            this.subquery = subquery;
            this.setter = setter;
            this.ancestors = ancestors;
            this.depth = ancestors.size();
            this.lateral = lateral;
        }
    }

    /**
     * Walks the relation tree only. Subqueries that live inside expressions (IN / EXISTS / scalar) are
     * deliberately not collected: they can be correlated and they are not replaceable by a table
     * reference. Their bodies are still reached, because a derived table nested in one of them is a
     * perfectly good candidate.
     */
    private static final class Collector {
        private final List<Site> sites = new ArrayList<>();
        private final Set<String> cteNames = new HashSet<>();
        private final Set<String> reservedNames = new HashSet<>();
        private final List<SubqueryRelation> stack = new ArrayList<>();

        private void collectQuery(QueryRelation relation) {
            for (CTERelation cte : relation.getCteRelations()) {
                if (cte.getName() != null) {
                    cteNames.add(cte.getName().toLowerCase(Locale.ROOT));
                    reservedNames.add(cte.getName());
                }
                collectQuery(cte.getCteQueryStatement().getQueryRelation());
            }

            if (relation instanceof SelectRelation) {
                Relation from = ((SelectRelation) relation).getRelation();
                if (from != null) {
                    collectRelation(from, ((SelectRelation) relation)::setRelation, false);
                }
            } else if (relation instanceof SetOperationRelation) {
                // A set-operation branch must stay a QueryRelation, so it is never a replacement site.
                for (QueryRelation child : ((SetOperationRelation) relation).getRelations()) {
                    collectQuery(child);
                }
            } else if (relation instanceof SubqueryRelation) {
                collectQuery(((SubqueryRelation) relation).getQueryStatement().getQueryRelation());
            }
        }

        private void collectRelation(Relation relation, Consumer<Relation> setter, boolean lateral) {
            if (relation instanceof SubqueryRelation) {
                SubqueryRelation subquery = (SubqueryRelation) relation;
                Site site = new Site(subquery, setter, new ArrayList<>(stack), lateral);
                site.hasTable = containsTable(subquery);
                site.hasLimit = containsLimit(subquery);
                sites.add(site);

                stack.add(subquery);
                collectQuery(subquery.getQueryStatement().getQueryRelation());
                stack.remove(stack.size() - 1);
            } else if (relation instanceof JoinRelation) {
                JoinRelation join = (JoinRelation) relation;
                collectRelation(join.getLeft(), join::setLeft, false);
                collectRelation(join.getRight(), join::setRight, join.isLateral());
            } else if (relation instanceof PivotRelation) {
                PivotRelation pivot = (PivotRelation) relation;
                if (pivot.getQuery() != null) {
                    collectRelation(pivot.getQuery(), pivot::setQuery, false);
                }
            } else if (relation instanceof TableRelation) {
                TableName name = ((TableRelation) relation).getName();
                if (name != null && name.getTbl() != null) {
                    reservedNames.add(name.getTbl());
                }
            }

            TableName alias = relation.getAlias();
            if (alias != null && alias.getTbl() != null) {
                reservedNames.add(alias.getTbl());
            }
        }

        /** True when this relation, or anything nested under it, carries a LIMIT. */
        private static boolean containsLimit(Relation relation) {
            if (relation instanceof QueryRelation && ((QueryRelation) relation).hasLimit()) {
                return true;
            }
            if (relation instanceof JoinRelation) {
                JoinRelation join = (JoinRelation) relation;
                return containsLimit(join.getLeft()) || containsLimit(join.getRight());
            }
            if (relation instanceof PivotRelation) {
                PivotRelation pivot = (PivotRelation) relation;
                return pivot.getQuery() != null && containsLimit(pivot.getQuery());
            }
            if (relation instanceof SelectRelation) {
                Relation from = ((SelectRelation) relation).getRelation();
                if (from != null && containsLimit(from)) {
                    return true;
                }
            } else if (relation instanceof SetOperationRelation) {
                for (QueryRelation child : ((SetOperationRelation) relation).getRelations()) {
                    if (containsLimit(child)) {
                        return true;
                    }
                }
            } else if (relation instanceof SubqueryRelation) {
                if (containsLimit(((SubqueryRelation) relation).getQueryStatement().getQueryRelation())) {
                    return true;
                }
            }
            if (relation instanceof QueryRelation) {
                for (CTERelation cte : ((QueryRelation) relation).getCteRelations()) {
                    if (containsLimit(cte.getCteQueryStatement().getQueryRelation())) {
                        return true;
                    }
                }
            }
            return false;
        }

        private static boolean containsTable(Relation relation) {
            if (relation instanceof TableRelation) {
                return true;
            }
            if (relation instanceof JoinRelation) {
                JoinRelation join = (JoinRelation) relation;
                return containsTable(join.getLeft()) || containsTable(join.getRight());
            }
            if (relation instanceof PivotRelation) {
                PivotRelation pivot = (PivotRelation) relation;
                return pivot.getQuery() != null && containsTable(pivot.getQuery());
            }
            if (relation instanceof SelectRelation) {
                Relation from = ((SelectRelation) relation).getRelation();
                return from != null && containsTable(from);
            }
            if (relation instanceof SetOperationRelation) {
                for (QueryRelation child : ((SetOperationRelation) relation).getRelations()) {
                    if (containsTable(child)) {
                        return true;
                    }
                }
                return false;
            }
            if (relation instanceof SubqueryRelation) {
                return containsTable(((SubqueryRelation) relation).getQueryStatement().getQueryRelation());
            }
            return false;
        }
    }
}
