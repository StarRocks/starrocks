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

package com.starrocks.context.retrieval;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.starrocks.common.Config;
import com.starrocks.context.ContextInternalTables;
import com.starrocks.context.ContextSqlSupport;
import com.starrocks.context.error.ContextErrorCode;
import com.starrocks.context.error.ContextException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Bounded reference expansion over {@link ContextInternalTables#REFS}. The architecture doc
 * ({@code 1-agentbase-starrocks-semantic-context-architecture-design.md} §§5, 12) specifies that
 * {@code GRAPH_EXPAND} is a compatibility surface: the backing store is a plain PK table of
 * {@code (src_entity_id, src_version, ord, dst_entity_id, ref_kind, ...)} rows, not a graph engine.
 *
 * <p>This class implements the expansion as bounded breadth-first search driven by per-hop SELECT
 * queries. Each hop takes the current frontier (a set of entity ids), runs a single SELECT over
 * {@code context_entity_refs} filtered by the requested direction and ref kinds, and records the
 * successors. The loop terminates when the frontier becomes empty, the configured hop depth is
 * reached, or the frontier size exceeds {@code maxFrontier}. The standalone {@code GRAPH_EXPAND}
 * surface lets callers pass {@code require_complete=true} to convert truncation into an error;
 * fusion ({@code CONTEXT_SEARCH}) always treats truncation as a soft signal via
 * {@code explain.reference_truncated}.
 */
public class ReferenceExpander {

    private static final Logger LOG = LogManager.getLogger(ReferenceExpander.class);

    public enum Direction {
        FORWARD,
        BACKWARD,
        BOTH
    }

    /**
     * One row in the expansion output. {@code hop} is zero for seed entities, one for direct
     * neighbors, and so on. {@code refKinds} are the kinds observed on the edge that produced this
     * entry; the compatibility alias in the external contract is {@code edge_types}.
     */
    public static final class ExpansionRow {
        public final long seedId;
        public final long entityId;
        public final int hop;
        public final double pathScore;
        public final List<String> refKinds;

        public ExpansionRow(long seedId, long entityId, int hop, double pathScore, List<String> refKinds) {
            this.seedId = seedId;
            this.entityId = entityId;
            this.hop = hop;
            this.pathScore = pathScore;
            this.refKinds = refKinds;
        }
    }

    public static final class Request {
        public Collection<Long> seeds;
        public Direction direction = Direction.FORWARD;
        public int depth = 1;
        public int maxFrontier = 200;
        public Collection<String> refKinds;
        public boolean requireComplete;
        // Optional scope filters. When set, expansion only emits neighbors that live in the same
        // contextbase / collection — without these, a forward edge from a seed in collection A
        // could walk into entities in collection B and leak rows the caller has no privilege on.
        public Long contextBaseId;
        // Multi-contextbase scope. Used only when contextBaseId is null (single value wins). Edges
        // never cross contextbases, so an IN(set) scope simply confines expansion to the union of
        // the requested bases without producing cross-base neighbors.
        public List<Long> contextBaseIds;
        public Long collectionId;
        public List<Long> collectionIds;
        // Optional snapshot fence. When >= 0, the expansion uses an as-of view of heads
        // (entity_id → max version with snapshot_version <= fence) instead of current heads,
        // matching the architecture doc §10.1 "all reads pin one snapshot_version" rule.
        public long snapshotFence = -1L;
        // Optional per-seed relevance weights. When a seed has an entry, every row reached from it
        // gets pathScore = weight * 1/(1+hop) (and the seed's own row gets pathScore = weight)
        // instead of the seed-blind 1/(1+hop). Seeds without an entry default to weight 1.0, so an
        // empty/null map reproduces the legacy hop-only path score exactly.
        public Map<Long, Double> seedWeights;
    }

    public static final class Result {
        public final List<ExpansionRow> rows;
        public final boolean truncated;
        public final int maxHopReached;

        public Result(List<ExpansionRow> rows, boolean truncated, int maxHopReached) {
            this.rows = rows;
            this.truncated = truncated;
            this.maxHopReached = maxHopReached;
        }
    }

    /**
     * Single-hop adjacency probe. Issues exactly one SELECT against {@code context_entity_refs}
     * for the supplied node set, applying the same scope / snapshot-fence / ref-kind filters as
     * {@link #expand}. Returns each matching edge as a {@code long[2]} pair in the shape
     * {@code {queried_endpoint, other_endpoint}}:
     * <ul>
     *   <li>{@link Direction#FORWARD}: queried = src, other = dst</li>
     *   <li>{@link Direction#BACKWARD}: queried = dst, other = src</li>
     * </ul>
     * Unlike {@link #expand}, this method does no BFS, no visited-set bookkeeping, no truncation
     * tracking, and no hop-attenuation — it's the raw 1-hop primitive that
     * {@link com.starrocks.context.retrieval.rerank.support.AdjacencyLoader} builds undirected
     * adjacency on top of. Callers wanting iterated traversal should use {@link #expand}.
     *
     * <p>{@link Direction#BOTH} is rejected because the underlying SQL UNIONs the two projections,
     * making the pair shape ambiguous. Callers needing both directions should invoke
     * {@code scan(..., FORWARD, ...)} and {@code scan(..., BACKWARD, ...)} separately.
     */
    public List<long[]> scan(Collection<Long> nodes, Direction direction,
                             Long contextBaseId, Long collectionId, List<Long> collectionIds,
                             long snapshotFence, Collection<String> refKinds) {
        return scan(nodes, direction, contextBaseId, null, collectionId, collectionIds,
                snapshotFence, refKinds);
    }

    /** Multi-contextbase variant of {@link #scan}: {@code contextBaseIds} applies when
     *  {@code contextBaseId} is null. */
    public List<long[]> scan(Collection<Long> nodes, Direction direction,
                             Long contextBaseId, List<Long> contextBaseIds,
                             Long collectionId, List<Long> collectionIds,
                             long snapshotFence, Collection<String> refKinds) {
        if (nodes == null || nodes.isEmpty()) {
            return new ArrayList<>();
        }
        if (direction == null || direction == Direction.BOTH) {
            throw new IllegalArgumentException(
                    "ReferenceExpander.scan requires FORWARD or BACKWARD; got " + direction);
        }
        String sql = buildHopSql(nodes, direction, refKinds,
                contextBaseId, contextBaseIds, collectionId, collectionIds, snapshotFence);
        JsonArray batch = runQuery(sql);
        List<long[]> out = new ArrayList<>(batch.size());
        for (JsonElement el : batch) {
            JsonArray data = el.getAsJsonObject().getAsJsonArray("data");
            long a = data.get(0).getAsLong();
            long b = data.get(1).getAsLong();
            out.add(new long[] {a, b});
        }
        return out;
    }

    public Result expand(Request request) {
        if (request.seeds == null || request.seeds.isEmpty()) {
            return new Result(new ArrayList<>(), false, 0);
        }

        // Clamp caller-supplied depth / maxFrontier against the FE-side hard ceilings. Without
        // these, a buggy or malicious client posting graphDepth=50, maxFrontier=10000 would
        // expand to 50 SQL hops with 10k-id IN-lists each — a per-request blow-up multiplier
        // far beyond the design intent (architecture doc §§5, 12 specify bounded BFS).
        int depth = request.depth;
        int maxFrontier = request.maxFrontier;
        int depthCap = Config.context_graph_expand_max_depth;
        int frontierCap = Config.context_graph_expand_max_frontier;
        if (depthCap > 0 && depth > depthCap) {
            LOG.warn("graph expand depth clamped: requested={} cap={}", depth, depthCap);
            depth = depthCap;
        }
        if (frontierCap > 0 && maxFrontier > frontierCap) {
            LOG.warn("graph expand maxFrontier clamped: requested={} cap={}",
                    maxFrontier, frontierCap);
            maxFrontier = frontierCap;
        }

        // Enforce the frontier cap on the INITIAL frontier too. The seeds are the hop-1 frontier;
        // if the caller supplies more seed IDs than maxFrontier, sending them all to buildHopSql
        // would put an unbounded IN-list into the first SQL hop, bypassing the per-request ceiling
        // that the later nextFrontier.size() check enforces on every subsequent hop.
        boolean truncated = false;
        List<Long> seeds = new ArrayList<>(request.seeds);
        if (maxFrontier > 0 && seeds.size() > maxFrontier) {
            LOG.warn("graph expand seed frontier clamped: seeds={} cap={}", seeds.size(), maxFrontier);
            seeds = new ArrayList<>(seeds.subList(0, maxFrontier));
            truncated = true;
        }

        Set<Long> visited = new LinkedHashSet<>(seeds);
        Map<Long, Long> currentFrontier = new LinkedHashMap<>();
        for (Long seed : seeds) {
            currentFrontier.put(seed, seed);
        }

        List<ExpansionRow> rows = new ArrayList<>();
        for (Long seed : seeds) {
            rows.add(new ExpansionRow(seed, seed, 0, seedWeight(request, seed), new ArrayList<>()));
        }

        int hopReached = 0;
        for (int hop = 1; hop <= depth; hop++) {
            if (currentFrontier.isEmpty()) {
                break;
            }
            Map<Long, Long> nextFrontier = new LinkedHashMap<>();
            String sql = buildHopSql(currentFrontier.keySet(), request.direction, request.refKinds,
                    request.contextBaseId, request.contextBaseIds, request.collectionId,
                    request.collectionIds, request.snapshotFence);
            JsonArray batch = runQuery(sql);
            double hopWeight = 1.0 / (1 + hop);
            for (JsonElement el : batch) {
                JsonArray data = el.getAsJsonObject().getAsJsonArray("data");
                long src = data.get(0).getAsLong();
                long dst = data.get(1).getAsLong();
                String kind = data.get(2).getAsString();
                long seedOrigin = currentFrontier.getOrDefault(src, src);
                if (!visited.add(dst)) {
                    continue;
                }
                // Propagate the originating seed's relevance into the path score. With no seed
                // weights this is weight 1.0 → the legacy hop-only score. Seeds are enqueued in
                // descending-relevance order (deriveGraphSeeds sorts by partial fusion score), and
                // the visited-set dedup keeps the first (highest-relevance) seed to reach a node,
                // which approximates the per-node max over reaching seeds.
                double pathScore = seedWeight(request, seedOrigin) * hopWeight;
                // `max_frontier` is an inclusive cap on the next-hop frontier size (architecture
                // doc §§5, 12). Check before adding so the overflow neighbor is not admitted to
                // the row set or the next-hop frontier — a frontier of exactly `max_frontier` is
                // the largest valid result; the (max_frontier+1)th candidate trips truncation and
                // is excluded from the output.
                if (nextFrontier.size() >= maxFrontier) {
                    truncated = true;
                    break;
                }
                rows.add(new ExpansionRow(seedOrigin, dst, hop, pathScore,
                        Arrays.asList(kind)));
                nextFrontier.put(dst, seedOrigin);
            }
            hopReached = hop;
            if (truncated) {
                if (request.requireComplete) {
                    throw new ContextException(ContextErrorCode.FRONTIER_LIMIT_EXCEEDED,
                            "reference expansion exceeded max_frontier=" + maxFrontier
                                    + " with require_complete=true");
                }
                break;
            }
            currentFrontier = nextFrontier;
        }
        return new Result(rows, truncated, hopReached);
    }

    // Package-private so the unit test can pin the latest-snapshot subquery and the fence
    // predicate added when REFERENCE_RESYNC stopped overwriting history. Keep as a static-style
    // helper (no field mutation) so test calls are side-effect-free.
    String buildHopSql(Collection<Long> frontier, Direction direction,
                               Collection<String> refKinds,
                               Long contextBaseId, Long collectionId, List<Long> collectionIds,
                               long snapshotFence) {
        return buildHopSql(frontier, direction, refKinds, contextBaseId, null,
                collectionId, collectionIds, snapshotFence);
    }

    // Multi-contextbase variant. {@code contextBaseIds} applies only when {@code contextBaseId} is
    // null (single value wins), mirroring the collectionId / collectionIds precedence.
    String buildHopSql(Collection<Long> frontier, Direction direction,
                               Collection<String> refKinds,
                               Long contextBaseId, List<Long> contextBaseIds,
                               Long collectionId, List<Long> collectionIds,
                               long snapshotFence) {
        StringBuilder ids = new StringBuilder();
        boolean first = true;
        for (Long id : frontier) {
            if (!first) {
                ids.append(',');
            }
            ids.append(id);
            first = false;
        }

        String kindClause = "";
        if (refKinds != null && !refKinds.isEmpty()) {
            StringBuilder kinds = new StringBuilder();
            boolean k = true;
            for (String kind : refKinds) {
                if (!k) {
                    kinds.append(',');
                }
                kinds.append('\'').append(kind.replace("'", "''")).append('\'');
                k = false;
            }
            kindClause = " AND r.ref_kind IN (" + kinds + ")";
        }

        // Scope filter: confine expansion to the requested contextbase / collection. Apply on both
        // the source-side head row (src_entity must belong to the named scope, otherwise the seed
        // expansion silently leaks across bases) AND the destination's heads row (dst likewise).
        // Without these filters, a forward edge from collection A → collection B leaks rows the
        // caller has no privilege on.
        //
        // The alias bearing the scope columns differs between the no-fence and fence paths:
        //   no-fence: alias `h` is the heads table (has contextbase_id + collection_id)
        //   fence   : alias `h` is a derived (entity_id, av) projection — it does NOT have those
        //             columns. Use `hv` (the joined versions row) instead, which has both.
        // Same logic for dst: `hd` is heads in the no-fence path and the joined versions row in
        // the fence path; both have contextbase_id + collection_id, so `hd.<col>` works either way.
        String srcScopeAlias = snapshotFence >= 0 ? "hv" : "h";
        boolean hasContextBaseScope = contextBaseId != null
                || (contextBaseIds != null && !contextBaseIds.isEmpty());
        StringBuilder scopeSrc = new StringBuilder();
        StringBuilder scopeDst = new StringBuilder();
        if (hasContextBaseScope) {
            String srcPred = contextBaseFilter(srcScopeAlias, contextBaseId, contextBaseIds);
            String dstPred = contextBaseFilter("hd", contextBaseId, contextBaseIds);
            scopeSrc.append(" AND ").append(srcPred);
            scopeDst.append(" AND ").append(dstPred);
        }
        if (collectionId != null) {
            scopeSrc.append(" AND ").append(srcScopeAlias).append(".collection_id = ").append(collectionId);
            scopeDst.append(" AND hd.collection_id = ").append(collectionId);
        } else if (collectionIds != null && !collectionIds.isEmpty()) {
            String joinedCollectionIds = joinIds(collectionIds);
            scopeSrc.append(" AND ").append(srcScopeAlias).append(".collection_id IN (").append(joinedCollectionIds).append(")");
            scopeDst.append(" AND hd.collection_id IN (").append(joinedCollectionIds).append(")");
        }

        String refsTable = ContextInternalTables.DATABASE + "." + ContextInternalTables.REFS;
        String heads = ContextInternalTables.DATABASE + "." + ContextInternalTables.HEADS;

        // refs_view picks the latest snapshot per (src_entity_id, src_version, ord) so a
        // REFERENCE_RESYNC that appended a new snapshot layer is honored, and older layers
        // are honored at their pinned snapshot when a fence is active. Without this view the
        // expander would return rows for every historical resync of the same edge.
        //
        // The MAX(snapshot_version) subquery resolves the ACTIVE ref per (src_entity_id, src_version,
        // ord) and must be computed over the ordinal's full layer history (fence-bounded only). It is
        // deliberately NOT pre-filtered by the dst frontier: REFERENCE_RESYNC appends a new layer that
        // can move an edge's destination, so taking the MAX among only the layers that still point at
        // the requested dst would pick a stale layer and return a source whose active edge points
        // elsewhere. For FORWARD the frontier is on src (the GROUP BY key), so bounding the scan by
        // src IN (frontier) is safe (it keeps/drops whole ordinal groups) and keeps the per-hop MAX
        // from scanning the entire refs table. For BACKWARD/BOTH the frontier is (partly) on dst,
        // which cannot bound this subquery without corrupting the MAX, so it resolves fence-only and
        // the outer query applies the dst frontier filter to the already-resolved active rows.
        String fenceInner = snapshotFence >= 0 ? "snapshot_version <= " + snapshotFence : "";
        String refsLatestPredicate;
        if (direction == Direction.FORWARD) {
            String srcBound = "src_entity_id IN (" + ids + ")";
            refsLatestPredicate = " WHERE " + srcBound + (fenceInner.isEmpty() ? "" : " AND " + fenceInner);
        } else {
            refsLatestPredicate = fenceInner.isEmpty() ? "" : " WHERE " + fenceInner;
        }

        // Forward-reference-safe edges (lazy key resolution): an explicit edge whose target did not
        // exist at write time is stored with dst_entity_id=0 ("unresolved") plus its dst_entity_key.
        // Resolve such rows to the destination's current id by joining heads on the key, scoped to
        // the requested contextbase (edges never cross bases). The refs view exposes this RESOLVED
        // id under the existing `dst_entity_id` name, so all downstream FORWARD/BACKWARD/BOTH logic
        // works unchanged. Only applied when a contextbase is given (search is always scoped); with
        // no scope the key cannot be resolved unambiguously, so only pre-resolved edges traverse.
        String dstIdExpr;
        String keyFallbackJoin;
        if (hasContextBaseScope) {
            dstIdExpr = "CASE WHEN r0.dst_entity_id > 0 THEN r0.dst_entity_id ELSE kh.entity_id END";
            keyFallbackJoin = " LEFT JOIN " + heads + " kh ON r0.dst_entity_id = 0 "
                    + "AND kh.entity_key = r0.dst_entity_key AND "
                    + contextBaseFilter("kh", contextBaseId, contextBaseIds)
                    + " AND kh.current_deleted = false";
        } else {
            dstIdExpr = "r0.dst_entity_id";
            keyFallbackJoin = "";
        }
        String refs = "(SELECT r0.src_entity_id, r0.src_version, r0.ord, "
                + dstIdExpr + " AS dst_entity_id, "
                + "r0.ref_kind, r0.snapshot_version FROM " + refsTable + " r0 "
                + "JOIN (SELECT src_entity_id, src_version, ord, MAX(snapshot_version) AS sv "
                + "FROM " + refsTable + refsLatestPredicate
                + " GROUP BY src_entity_id, src_version, ord) m "
                + "ON r0.src_entity_id = m.src_entity_id AND r0.src_version = m.src_version "
                + "AND r0.ord = m.ord AND r0.snapshot_version = m.sv"
                + keyFallbackJoin + ")";

        // Snapshot fence: when set, the head-row's "current" version may not be the version the
        // caller wants to see at as_of. Use a derived as-of head (max version per entity whose
        // snapshot_version <= fence, joined back to the version row) so refs are filtered by the
        // right version. When unset, use heads.current_version directly — that's the steady-state
        // path and stays cheap on the index.
        String srcHeadJoin;
        if (snapshotFence >= 0) {
            String versions = ContextInternalTables.DATABASE + "." + ContextInternalTables.VERSIONS;
            srcHeadJoin = "JOIN ("
                    + "SELECT entity_id, MAX(version) AS av FROM " + versions
                    + " WHERE snapshot_version <= " + snapshotFence + " GROUP BY entity_id"
                    + ") h ON h.entity_id = r.src_entity_id AND h.av = r.src_version "
                    + "JOIN " + versions + " hv ON hv.entity_id = r.src_entity_id "
                    + "AND hv.version = r.src_version AND hv.deleted = false";
        } else {
            srcHeadJoin = "JOIN " + heads + " h ON h.entity_id = r.src_entity_id "
                    + "AND h.current_version = r.src_version AND h.current_deleted = false";
        }

        // Outer dst-scope filter is bolted on as a JOIN against heads aliased `hd` so we can
        // require the destination row to live in the named scope. Without a fence, dst heads
        // are evaluated at "current"; with a fence we mirror the same as-of derivation as src.
        String dstHeadJoin;
        if (snapshotFence >= 0 && (hasContextBaseScope || collectionId != null)) {
            String versions = ContextInternalTables.DATABASE + "." + ContextInternalTables.VERSIONS;
            dstHeadJoin = "JOIN ("
                    + "SELECT entity_id, MAX(version) AS av FROM " + versions
                    + " WHERE snapshot_version <= " + snapshotFence + " GROUP BY entity_id"
                    + ") hd_av ON hd_av.entity_id = r.dst_entity_id "
                    + "JOIN " + versions + " hd ON hd.entity_id = hd_av.entity_id "
                    + "AND hd.version = hd_av.av AND hd.deleted = false";
        } else if (hasContextBaseScope || collectionId != null) {
            dstHeadJoin = "JOIN " + heads + " hd ON hd.entity_id = r.dst_entity_id "
                    + "AND hd.current_deleted = false";
        } else {
            dstHeadJoin = "";
        }

        switch (direction) {
            case FORWARD:
                return String.format(
                        "SELECT r.src_entity_id, r.dst_entity_id, r.ref_kind FROM %s r "
                                + "%s %s "
                                + "WHERE r.src_entity_id IN (%s)%s%s%s",
                        refs, srcHeadJoin, dstHeadJoin, ids, kindClause, scopeSrc, scopeDst);
            case BACKWARD:
                return String.format(
                        "SELECT r.dst_entity_id, r.src_entity_id, r.ref_kind FROM %s r "
                                + "%s %s "
                                + "WHERE r.dst_entity_id IN (%s)%s%s%s",
                        refs, srcHeadJoin, dstHeadJoin, ids, kindClause, scopeSrc, scopeDst);
            case BOTH:
                return String.format(
                        "SELECT r.src_entity_id, r.dst_entity_id, r.ref_kind FROM %s r "
                                + "%s %s "
                                + "WHERE r.src_entity_id IN (%s)%s%s%s "
                                + "UNION ALL "
                                + "SELECT r.dst_entity_id, r.src_entity_id, r.ref_kind FROM %s r "
                                + "%s %s "
                                + "WHERE r.dst_entity_id IN (%s)%s%s%s",
                        refs, srcHeadJoin, dstHeadJoin, ids, kindClause, scopeSrc, scopeDst,
                        refs, srcHeadJoin, dstHeadJoin, ids, kindClause, scopeSrc, scopeDst);
            default:
                throw new IllegalArgumentException("unknown direction: " + direction);
        }
    }

    /** Relevance weight for a seed: its configured weight, or 1.0 when no weights are supplied. */
    private static double seedWeight(Request request, long seed) {
        if (request.seedWeights == null) {
            return 1.0;
        }
        Double w = request.seedWeights.get(seed);
        return w != null ? w : 1.0;
    }

    private String joinIds(List<Long> ids) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < ids.size(); i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(ids.get(i));
        }
        return sb.toString();
    }

    /** Build a {@code <alias>.contextbase_id} scope predicate: {@code = id} for a single base,
     *  {@code IN (...)} for multiple. Returns null when neither is set. */
    private String contextBaseFilter(String alias, Long contextBaseId, List<Long> contextBaseIds) {
        if (contextBaseId != null) {
            return alias + ".contextbase_id = " + contextBaseId;
        } else if (contextBaseIds != null && !contextBaseIds.isEmpty()) {
            return alias + ".contextbase_id IN (" + joinIds(contextBaseIds) + ")";
        }
        return null;
    }

    // Package-private so unit tests can stub the SQL plane (return canned edges) without a live
    // StarRocks — mirrors TextSearchExecutor.runQuery.
    JsonArray runQuery(String sql) {
        return ContextSqlSupport.executeDql(sql);
    }
}
