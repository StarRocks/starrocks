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

import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.TableName;
import com.starrocks.sql.ast.CTERelation;
import com.starrocks.sql.ast.JoinRelation;
import com.starrocks.sql.ast.PivotRelation;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.SetOperationRelation;
import com.starrocks.sql.ast.SubqueryRelation;
import com.starrocks.sql.ast.TableRelation;
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
 * <p><b>Correlation.</b> Measured, not assumed: StarRocks already rejects a correlated derived table in
 * FROM ("Column '...' cannot be resolved") whether or not this pass runs, so a body that reaches this
 * code cannot depend on an enclosing scope. Hoisting to the <em>outermost</em> query block keeps that
 * property honest anyway - the CTE body is then analyzed against an essentially empty outer scope, so
 * an outer reference could only fail to resolve, never silently rebind to a different column. The
 * caller still reverts and re-analyzes if analysis throws, as a net for cases neither of us predicted;
 * see {@code StatementPlanner#analyzeWithCommonSubqueryCte}.
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
        QueryRelation body = subquery.getQueryStatement().getQueryRelation();
        // Two occurrences of a LIMIT without ORDER BY may legitimately return different rows today;
        // sharing would force them to agree.
        if (body.hasLimit()) {
            return null;
        }
        // `(select ...) t(a, b)` renames the output; the CTE would have to carry the names too.
        if (subquery.getExplicitColumnNames() != null) {
            return null;
        }
        if (!site.hasTable) {
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
        // Only the genuinely per-call functions matter. The time functions in
        // FunctionSet#nonDeterministicTimeFunctions are folded to one constant per query, so two
        // occurrences already agree and sharing changes nothing.
        for (String fn : FunctionSet.nonDeterministicFunctions) {
            if (words.contains(fn)) {
                return null;
            }
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
