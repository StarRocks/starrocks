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

import com.google.common.base.Strings;
import com.starrocks.common.AuditLog;
import com.starrocks.common.Config;
import com.starrocks.common.ThreadPoolManager;
import com.starrocks.context.ContextMgr;
import com.starrocks.context.ContextReadExecutor;
import com.starrocks.context.ai.AIProvider;
import com.starrocks.context.ai.FeRerankClient;
import com.starrocks.context.policy.CollectionTypePolicy;
import com.starrocks.context.retrieval.rerank.RerankContext;
import com.starrocks.context.retrieval.rerank.RerankStrategies;
import com.starrocks.context.retrieval.rerank.RerankStrategy;
import com.starrocks.metric.MetricRepo;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Orchestrates the three retrieval paths the architecture doc §10.5 lists for {@code CONTEXT_SEARCH}:
 * text, vector, and reference expansion. Each path runs independently against the internal tables,
 * their candidate sets merge in Java with a configurable linear-combination score, and the final
 * TopN result is returned to the caller.
 *
 * <p>Vector search is delegated to {@link VectorSearchExecutor}. It can consume either a
 * provided {@code queryEmbedding} or a provider-generated embedding from {@code queryText};
 * {@code graph_mode} is the external name for the reference-expansion toggle. The implementation
 * never calls a graph engine — it only runs SELECTs over
 * {@link com.starrocks.context.ContextInternalTables#REFS}.
 *
 * <p>Graph seeds are auto-derived from the top text/vector candidates when the caller does not
 * supply explicit {@code seed_ids}. Users issuing fusion search hold a natural-language query, not
 * internal entity ids, so making them name seeds was a non-starter. The dedicated
 * {@code CONTEXT_GRAPH_EXPAND} TVF / {@code /api/context/graph-expand} REST endpoint remains for
 * programmatic callers that already know their seeds and want pure traversal.
 */
public class ContextSearchExecutor {

    private static final Logger LOG = LogManager.getLogger(ContextSearchExecutor.class);

    // Monotonic sequence for the per-request correlation id shared with the nested text_search line.
    private static final java.util.concurrent.atomic.AtomicLong REQ_SEQ =
            new java.util.concurrent.atomic.AtomicLong();

    /** Nanos → whole milliseconds for the structured timing log. */
    private static long ms(long nanos) {
        return nanos / 1_000_000L;
    }

    // Pool for running the independent text/vector channels concurrently. Cached (core=0, threads
    // die after KEEP_ALIVE) and bounded by Config#context_search_channel_pool_size; CallerRunsPolicy
    // means that when the pool is saturated the submitting thread runs the task itself, so under load
    // it degrades to serial — never drops a channel, never deadlocks (channel-level is single-level
    // parallelism; no nested submission onto this pool).
    private static final ThreadPoolExecutor CHANNEL_POOL = ThreadPoolManager.newDaemonThreadPool(
            0, Math.max(1, Config.context_search_channel_pool_size), 60L, TimeUnit.SECONDS,
            new SynchronousQueue<>(), new ThreadPoolExecutor.CallerRunsPolicy(),
            "context-search-channel", true);

    private final ContextMgr contextMgr;
    private final TextSearchExecutor textSearch;
    private final ReferenceExpander refExpander;
    private final VectorSearchExecutor vectorSearch;
    private final ContextBudgetPlanner budgetPlanner;

    public ContextSearchExecutor(ContextMgr contextMgr, TextSearchExecutor textSearch,
                                 ReferenceExpander refExpander) {
        this(contextMgr, textSearch, refExpander, new VectorSearchExecutor(), null);
    }

    public ContextSearchExecutor(ContextMgr contextMgr, TextSearchExecutor textSearch,
                                 ReferenceExpander refExpander, VectorSearchExecutor vectorSearch) {
        this(contextMgr, textSearch, refExpander, vectorSearch, null);
    }

    public ContextSearchExecutor(ContextMgr contextMgr, TextSearchExecutor textSearch,
                                 ReferenceExpander refExpander, VectorSearchExecutor vectorSearch,
                                 ContextBudgetPlanner budgetPlanner) {
        this.contextMgr = contextMgr;
        this.textSearch = textSearch;
        this.refExpander = refExpander;
        this.vectorSearch = vectorSearch;
        this.budgetPlanner = budgetPlanner;
    }

    public enum GraphMode {
        OFF,
        AUTO
    }

    /**
     * Resolves the default reference-expansion BFS direction from
     * {@link Config#context_search_default_graph_direction}. An unset or unparseable config value
     * falls back to {@link ReferenceExpander.Direction#BOTH} (the safe default that keeps
     * {@code doc1 -> entityX <- doc2} graphs mutually reachable) and logs a WARN — it never silently
     * reverts to the old FORWARD-only behavior. Used by the TVF / REST request builders when the
     * caller does not pass an explicit {@code direction}.
     */
    public static ReferenceExpander.Direction defaultGraphDirection() {
        String raw = Config.context_search_default_graph_direction;
        if (raw != null) {
            try {
                return ReferenceExpander.Direction.valueOf(raw.trim().toUpperCase(java.util.Locale.ROOT));
            } catch (IllegalArgumentException e) {
                LOG.warn("invalid context_search_default_graph_direction '{}', falling back to BOTH", raw);
            }
        }
        return ReferenceExpander.Direction.BOTH;
    }

    // Vector-dominant default weights. The text channel is deliberately low: on natural-language
    // and entity-overlapping corpora it is mostly noise, and giving it more than a small vote pulls
    // distractors above strong vector hits (validated on HotpotQA: text=0.3 regresses below pure
    // vector, text=0.1 beats it). Per-request / per-collection-profile overrides still apply.
    public static final double DEFAULT_TEXT_WEIGHT = 0.1;
    public static final double DEFAULT_VECTOR_WEIGHT = 0.6;
    public static final double DEFAULT_GRAPH_WEIGHT = 0.3;
    // Default rerank strategy when neither the request graph_strategy nor a profile fusion_mode is
    // set. RRF fuses by rank (scale-free) over the three channels and, with the vector-dominant
    // weights above, beats pure vector on real-edge corpora while still expanding along references.
    public static final String DEFAULT_AUTO_STRATEGY = "rrf";
    /** Cap on auto-derived graph seeds when {@code graphSeedTopK} is left at its sentinel 0. */
    public static final int DEFAULT_GRAPH_SEED_TOPK_CEILING = 10;
    /** Default first-stage pool size fed to the cross-encoder reranker when {@code rerankTopN} is 0. */
    public static final int DEFAULT_RERANK_TOP_N = 100;
    /**
     * Multiplicative factor applied to a synthesis entity's {@code graph_score} before it enters
     * fusion. Halves the graph contribution to break correlation with text/vector for hub
     * entities. See {@link CollectionTypePolicy#isSynthesisType(String)}.
     */
    public static final double SYNTHESIS_GRAPH_SCORE_FACTOR = 0.5;
    /**
     * Multiplicative factor applied to a synthesis entity's {@code final_score} after fusion.
     * Leaf-first tiebreak when scores are close. Stacks on top of {@link #SYNTHESIS_GRAPH_SCORE_FACTOR}.
     */
    public static final double SYNTHESIS_FINAL_SCORE_FACTOR = 0.9;

    public static final class Request {
        public String contextBase;
        public Long contextBaseIdOverride;
        // Multi-contextbase scope, set only for true multi-base search (size > 1). When present,
        // contextBase / contextBaseIdOverride are null and every retrieval path filters
        // contextbase_id IN (...). as_of_time / snapshot_version and collection-level scope are
        // rejected upstream for this mode (snapshot versions are numbered per contextbase).
        public List<Long> contextBaseIdsOverride;
        public String collection;
        public Long collectionIdOverride;
        public String queryText;
        public float[] queryEmbedding;
        public boolean allowStaleVector = true;
        public Collection<Long> seedIds;
        public List<Long> collectionIdsOverride;
        public String entityType;
        public String workspace;
        public String retrievalProfile;
        public Map<String, Object> filters;
        public String consistency;
        public boolean explicitTextWeight;
        public boolean explicitVectorWeight;
        public boolean explicitGraphWeight;
        public int maxResults = 20;
        public int maxTokens = 4000;
        public GraphMode graphMode = GraphMode.AUTO;
        public double textWeight = DEFAULT_TEXT_WEIGHT;
        public double vectorWeight = DEFAULT_VECTOR_WEIGHT;
        public double graphWeight = DEFAULT_GRAPH_WEIGHT;
        public int graphDepth = 2;
        public int maxFrontier = 200;
        // Reference-expansion BFS direction. Defaults to BOTH (config-overridable via the request
        // builders) so a doc1->entityX<-doc2 multi-hop graph is mutually reachable. Only consumed
        // when the chosen rerank strategy needs the BFS pool (additive); vector_anchor_greedy loads
        // its own undirected adjacency and ignores this field.
        public ReferenceExpander.Direction direction = ReferenceExpander.Direction.BOTH;
        public Collection<String> edgeTypes;
        // 0 means "auto: min(maxResults, DEFAULT_GRAPH_SEED_TOPK_CEILING)". Caller can override
        // for wider expansion (programmatic profiling) or set to 1 for a probe-only run.
        public int graphSeedTopK = 0;

        // Final-ranking strategy. Null/empty resolves to the default {@code additive} strategy
        // (linear combination of text/vector/graph scores). Set to {@code "vector_anchor_greedy"}
        // for the iterative FK-greedy rerank validated on AgentBase-bench. Unknown names raise
        // INVALID_ARGUMENT at registry resolution. Looked up via
        // {@link com.starrocks.context.retrieval.rerank.RerankStrategies}.
        public String graphStrategy;
        // True when the caller passed graph_strategy explicitly (REST/TVF). Explicit names resolve
        // strictly (unknown → INVALID_ARGUMENT); a name derived from a profile's fusion_mode or the
        // DEFAULT_AUTO_STRATEGY constant resolves leniently (unknown → additive + WARN).
        public boolean explicitGraphStrategy;
        // Strategy-private parameters (e.g. {@code {"beta": 0.05, "base_score": "vector_only"}}
        // for {@code vector_anchor_greedy}, or {@code {"rrf_k": 60}} for {@code rrf}). Strategies
        // read their own keys and tolerate missing values, so callers can include or omit this
        // field per-strategy without coupling.
        public Map<String, Object> strategyOptions;

        // Snapshot fence — either an explicit snapshot_version or an as_of_time string. The
        // executor pins one snapshot before any retrieval path runs (architecture doc §10.1) so
        // text/vector/reference paths agree on what the world looked like.
        public String asOfTime;
        public Long snapshotVersion;

        // ---- optional cross-encoder second-phase rerank (default OFF) ----
        // When true, after first-stage fusion the top {@code rerankTopN} candidates are re-scored by
        // an external rerank provider (cross-encoder) and reordered. A provider error degrades to the
        // fusion order (see explain.rerank_error). explicit* flags gate retrieval-profile defaults.
        public boolean rerank = false;
        public boolean explicitRerank;
        public String rerankProvider;
        public int rerankTopN = 0;            // 0 -> DEFAULT_RERANK_TOP_N
        public boolean rerankUseBody = false; // false -> rerank on preview; true -> on full body
        public boolean explicitRerankUseBody;
    }

    public static final class Candidate {
        public final long entityId;
        public final double textScore;
        public final double vectorScore;
        public final double graphScore;
        public double finalScore;
        public final int hopCount;
        public final List<String> edgeTypes;
        public final String snippet;

        public Candidate(long entityId, double textScore, double vectorScore, double graphScore,
                         int hopCount, List<String> edgeTypes, String snippet) {
            this.entityId = entityId;
            this.textScore = textScore;
            this.vectorScore = vectorScore;
            this.graphScore = graphScore;
            this.finalScore = 0.0;
            this.hopCount = hopCount;
            this.edgeTypes = edgeTypes;
            this.snippet = snippet;
        }
    }

    public static final class Result {
        public final List<Candidate> candidates;
        public final Map<String, Object> explain;

        public Result(List<Candidate> candidates, Map<String, Object> explain) {
            this.candidates = candidates;
            this.explain = explain;
        }
    }

    public Result search(Request request) {
        MetricRepo.COUNTER_CONTEXT_SEARCH_TOTAL.increase(1L);
        // ---- per-step latency instrumentation (one structured INFO line at the end) ----
        // request_id stitches this line to the nested text_search line (TextSearchExecutor logs the
        // same id). Timing never alters control flow: degraded / skipped branches are measured too.
        final String requestId = "cs-" + REQ_SEQ.incrementAndGet();
        final long tSearchStart = System.nanoTime();
        long scopeNs = 0;
        long snapshotNs = 0;
        long textNs = 0;
        long vectorNs = 0;
        long mergeNs = 0;
        long metaNs = 0;
        long graphNs = 0;
        long rerankNs = 0;
        long crossEncoderNs = 0;
        long budgetNs = 0;
        long tScope = System.nanoTime();
        Map<Long, Candidate> byEntity = new LinkedHashMap<>();

        // Resolve contextbase / collection names → ids. If the caller named a scope but it does
        // not resolve, fail fast with INVALID_SCOPE — leaving the ids null would silently search
        // the entire cluster and leak data across contextbases (a real concern flagged in review).
        Long contextBaseId = request.contextBaseIdOverride;
        List<Long> contextBaseIds = request.contextBaseIdsOverride;
        boolean multiBase = contextBaseIds != null && !contextBaseIds.isEmpty();
        Long collectionId = request.collectionIdOverride;
        List<Long> collectionIds = request.collectionIdsOverride;
        if (Strings.isNullOrEmpty(request.contextBase)) {
            if (contextBaseId == null && !multiBase) {
                // contextbase is mandatory — without it any retrieval would silently span the cluster.
                throw new com.starrocks.context.error.ContextException(
                        com.starrocks.context.error.ContextErrorCode.INVALID_SCOPE,
                        "contextbase is required for CONTEXT_SEARCH");
            }
        } else {
            ContextMgr.ContextBaseMeta cb = contextMgr.getContextBase(request.contextBase);
            if (cb == null) {
                throw new com.starrocks.context.error.ContextException(
                        com.starrocks.context.error.ContextErrorCode.INVALID_SCOPE,
                        "contextbase not found: " + request.contextBase);
            }
            contextBaseId = cb.getId();
        }
        if (!Strings.isNullOrEmpty(request.collection)) {
            // O(1) lookup via the by-qualified-name map in ContextMgr — the previous linear
            // scan over listCollections(base) was an avoidable per-request hot-path cost.
            ContextMgr.CollectionMeta col = contextMgr.getCollection(request.contextBase, request.collection);
            if (col == null) {
                throw new com.starrocks.context.error.ContextException(
                        com.starrocks.context.error.ContextErrorCode.INVALID_SCOPE,
                        "collection not found: " + request.contextBase + "." + request.collection);
            }
            collectionId = col.getId();
        }
        if (collectionIds == null && collectionId != null) {
            collectionIds = java.util.Collections.singletonList(collectionId);
        }
        applyRetrievalProfile(request, collectionId);
        scopeNs = System.nanoTime() - tScope;
        long tSnapshot = System.nanoTime();

        // Snapshot fence: pin a single snapshot for every path so text/vector/graph agree on the
        // world they're searching. The architecture doc §10.1 spells this out as the unified read
        // entrypoint. -1 means "current heads".
        //
        // Snapshot versions are numbered per contextbase, so a single fence is meaningless across a
        // multi-base scope; reject as_of_time / snapshot_version in that mode (current heads only).
        long snapshotFence = -1L;
        if (multiBase && (request.snapshotVersion != null || !Strings.isNullOrEmpty(request.asOfTime))) {
            throw new com.starrocks.context.error.ContextException(
                    com.starrocks.context.error.ContextErrorCode.INVALID_ARGUMENT,
                    "as_of_time / snapshot_version are not supported when searching multiple "
                            + "contextbases (snapshot versions are per-contextbase)");
        }
        if (request.snapshotVersion != null) {
            snapshotFence = com.starrocks.server.GlobalStateMgr.getCurrentState()
                    .getContextSnapshotResolver().resolveFromSelector(
                            contextBaseId, request.snapshotVersion.toString());
        } else if (!Strings.isNullOrEmpty(request.asOfTime)) {
            snapshotFence = com.starrocks.server.GlobalStateMgr.getCurrentState()
                    .getContextSnapshotResolver().resolveFromSelector(
                            contextBaseId, request.asOfTime);
        }
        snapshotNs = System.nanoTime() - tSnapshot;

        // First-stage retrieval depth (TopN before the *3 fusion over-fetch). The text/vector paths
        // translate this into a SQL LIMIT, so it is the hard ceiling on the candidate pool. When
        // cross-encoder rerank is on, the pool must be large enough to fill rerank_top_n; otherwise a
        // rerank_top_n above maxResults would be silently capped here, because the later
        // request.maxResults widening (just before the fusion strategy) only affects the strategy's
        // truncation, not how many candidates the first-stage SQL actually fetches.
        int firstStageTopN = request.maxResults;
        if (request.rerank) {
            int rerankN = request.rerankTopN > 0 ? request.rerankTopN : DEFAULT_RERANK_TOP_N;
            firstStageTopN = Math.max(request.maxResults, rerankN);
        }

        // ---- text + vector channels (run concurrently) ----
        // text and vector are independent retrievers; we fork vector to the channel pool and run
        // text on the caller thread, so total latency is ~max(text, vector) instead of the sum.
        // Their results merge into byEntity single-threaded afterwards (no concurrent map writes),
        // and the graph/reference-expansion path stays after the merge because it derives its seeds
        // from the merged candidates. VectorSearchExecutor owns the standalone vector contract; here
        // deepMode is left false so the fusion vector path searches BOTH preview and section
        // fragments and folds to the best per entity (long docs reachable via sections).
        boolean textEnabled = !Strings.isNullOrEmpty(request.queryText);
        boolean vectorRequested = request.vectorWeight > 0.0
                && (!Strings.isNullOrEmpty(request.queryText)
                || (request.queryEmbedding != null && request.queryEmbedding.length > 0));

        TextSearchExecutor.Request textReq = null;
        if (textEnabled) {
            textReq = new TextSearchExecutor.Request();
            textReq.requestId = requestId;
            textReq.pattern = request.queryText;
            textReq.contextBaseId = contextBaseId;
            textReq.contextBaseIds = contextBaseIds;
            textReq.collectionId = collectionId;
            textReq.collectionIds = collectionIds;
            textReq.entityType = request.entityType;
            textReq.maxResults = firstStageTopN * 3; // fuse pool is bigger than TopN
            textReq.snapshotFence = snapshotFence;
        }
        VectorSearchExecutor.Request vecReq = null;
        if (vectorRequested) {
            vecReq = new VectorSearchExecutor.Request();
            vecReq.queryText = request.queryText;
            vecReq.queryEmbedding = request.queryEmbedding;
            vecReq.allowStaleVector = request.allowStaleVector;
            vecReq.contextBaseId = contextBaseId;
            vecReq.contextBaseIds = contextBaseIds;
            vecReq.collectionId = collectionId;
            vecReq.collectionIds = collectionIds;
            vecReq.entityType = request.entityType;
            vecReq.maxResults = firstStageTopN * 3;
            vecReq.snapshotFence = snapshotFence;
        }

        // Fork vector when both channels are active and parallelism is enabled; otherwise run inline.
        final VectorSearchExecutor.Request vecReqF = vecReq;
        boolean parallel = Config.context_search_channel_parallel_enabled && textEnabled && vectorRequested;
        CompletableFuture<List<VectorSearchExecutor.EntityHit>> vecFuture = parallel
                ? CompletableFuture.supplyAsync(() -> vectorSearch.search(vecReqF), CHANNEL_POOL)
                : null;

        // Channels degrade independently: a hybrid search should still return the surviving channel's
        // results if the other errors (e.g. embedding provider down, or a text-index error on one
        // contextbase) — losing everything because one retriever failed is worse than a partial
        // answer. Only when EVERY enabled channel fails do we surface the error.
        List<TextSearchExecutor.EntityHit> textHits = Collections.emptyList();
        Throwable textErr = null;
        if (textEnabled) {
            long tText = System.nanoTime();
            try {
                textHits = textSearch.search(textReq);
            } catch (Exception e) {
                textErr = e;
                LOG.warn("context_search text channel failed; degrading to vector-only: {}",
                        e.getMessage(), e);
            }
            textNs = System.nanoTime() - tText;
        }

        List<VectorSearchExecutor.EntityHit> vecHits = Collections.emptyList();
        Throwable vecErr = null;
        long tVec = System.nanoTime();
        if (vecFuture != null || vectorRequested) {
            try {
                vecHits = vecFuture != null ? vecFuture.join() : vectorSearch.search(vecReqF);
            } catch (Exception e) {
                vecErr = (e instanceof CompletionException && e.getCause() != null) ? e.getCause() : e;
                LOG.warn("context_search vector channel failed; degrading to text-only: {}",
                        vecErr.getMessage(), vecErr);
            }
        }
        vectorNs = System.nanoTime() - tVec;

        // Surface an error only if no enabled channel produced a result.
        boolean textSucceeded = textEnabled && textErr == null;
        boolean vectorSucceeded = vectorRequested && vecErr == null;
        if ((textEnabled || vectorRequested) && !textSucceeded && !vectorSucceeded) {
            Throwable cause = textErr != null ? textErr : vecErr;
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            if (cause instanceof Error) {
                throw (Error) cause;
            }
            throw new RuntimeException(cause);
        }

        // ---- merge text hits (single-threaded) ----
        long tMerge = System.nanoTime();
        for (TextSearchExecutor.EntityHit hit : textHits) {
            if (!byEntity.containsKey(hit.entityId)) {
                // text emits each entity once; if it somehow recurs, keep the first.
                byEntity.put(hit.entityId, new Candidate(hit.entityId, hit.textScore, 0.0, 0.0, 0,
                        new ArrayList<>(), hit.topSnippet == null ? null : hit.topSnippet.snippet));
            }
        }

        // ---- merge vector hits (single-threaded) ----
        boolean vectorEnabled = false;
        String vectorPathStatus;
        if (vecErr != null) {
            vectorPathStatus = "channel error: " + vecErr.getMessage();
        } else if (!vectorRequested) {
            vectorPathStatus = "skipped: no query_text or query_embedding";
        } else {
            vectorPathStatus = "no provider configured";
        }
        if (vectorRequested && vecErr == null && !vecHits.isEmpty()) {
            vectorEnabled = true;
            vectorPathStatus = request.queryEmbedding != null && request.queryEmbedding.length > 0
                    ? "provided query_embedding; hits=" + vecHits.size()
                    : "embedding-provider query_text search; hits=" + vecHits.size();
            for (VectorSearchExecutor.EntityHit hit : vecHits) {
                Candidate c = byEntity.get(hit.entityId);
                if (c == null) {
                    byEntity.put(hit.entityId, new Candidate(hit.entityId, 0.0, hit.score, 0.0, 0,
                            new ArrayList<>(), null));
                } else {
                    byEntity.put(hit.entityId, new Candidate(
                            c.entityId, c.textScore, Math.max(c.vectorScore, hit.score),
                            c.graphScore, c.hopCount, c.edgeTypes, c.snippet));
                }
            }
        }
        mergeNs = System.nanoTime() - tMerge;

        // ---- pre-load metadata for the text/vector pool ----
        // We need entity_type at seed-derivation time to filter out synthesis entities
        // (per CollectionTypePolicy.isSynthesisType): they are aggregation targets, not
        // navigation entry points, so seeding from them just walks back to leaves we
        // already discovered while inflating the synthesis's own score from converging
        // inbound paths. The same map is reused for graph_score demotion, fusion-time
        // demotion, and budget planning — so loading it once here is a net win.
        Map<Long, ContextReadExecutor.EntityMeta> metaByEntity = new HashMap<>();
        if (!byEntity.isEmpty()) {
            long tMeta = System.nanoTime();
            metaByEntity.putAll(loadMetadataSafe(byEntity.keySet(), snapshotFence));
            metaNs += System.nanoTime() - tMeta;
        }

        // ---- final-ranking strategy ----
        // Resolved up-front so the reference-expansion path can be skipped when the chosen
        // strategy doesn't need graphScore populated on candidates (e.g. vector_anchor_greedy
        // loads its own 1-hop adjacency restricted to the pool and ignores BFS graphScore).
        RerankStrategy strategy = resolveStrategy(request);
        // Reflect the resolved strategy back so explain / downstream see the effective name.
        request.graphStrategy = strategy.name();

        // ---- reference expansion path ----
        // Seeds = union(explicit seed_ids, top-K text/vector candidates by partial score). Users
        // issuing fusion search only have natural-language queries, so deriving seeds from text and
        // vector hits is what makes the graph weight actually contribute. Explicit seeds (rare,
        // power-user only) compose with derived ones. Synthesis entities are filtered out of the
        // derived seed set (architecture doc §10.5).
        long tGraph = System.nanoTime();
        boolean hasExplicitSeeds = request.seedIds != null && !request.seedIds.isEmpty();
        java.util.LinkedHashSet<Long> finalSeeds = new java.util.LinkedHashSet<>();
        if (hasExplicitSeeds) {
            finalSeeds.addAll(request.seedIds);
        }
        int resolvedSeedTopK = resolveGraphSeedTopK(request);
        List<Long> derivedSeeds = java.util.Collections.emptyList();
        Map<Long, Double> derivedSeedScores = java.util.Collections.emptyMap();
        int synthesisFilteredSeeds = 0;
        boolean strategyWantsBfs = strategy.needsGraphExpandedPool();
        if (request.graphMode != GraphMode.OFF && resolvedSeedTopK > 0 && strategyWantsBfs) {
            DerivedSeedResult derivedResult = deriveGraphSeeds(
                    byEntity.values(), metaByEntity, request, resolvedSeedTopK);
            derivedSeeds = derivedResult.seedIds;
            derivedSeedScores = derivedResult.seedScores;
            synthesisFilteredSeeds = derivedResult.synthesisSkipped;
            finalSeeds.addAll(derivedSeeds);
        }
        boolean refEnabled = request.graphMode != GraphMode.OFF && !finalSeeds.isEmpty()
                && strategyWantsBfs;
        ReferenceExpander.Result refResult = null;
        if (refEnabled) {
            ReferenceExpander.Request expandReq = new ReferenceExpander.Request();
            expandReq.seeds = finalSeeds;
            expandReq.direction = request.direction != null
                    ? request.direction : ReferenceExpander.Direction.BOTH;
            expandReq.depth = request.graphDepth;
            expandReq.maxFrontier = request.maxFrontier;
            expandReq.refKinds = request.edgeTypes;
            // Fusion treats truncation as a soft signal — surface it via explain.reference_truncated
            // and let the caller decide. Strict callers should use the standalone CONTEXT_GRAPH_EXPAND
            // TVF, which has its own require_complete flag.
            expandReq.requireComplete = false;
            expandReq.contextBaseId = contextBaseId;
            expandReq.contextBaseIds = contextBaseIds;
            expandReq.collectionId = collectionId;
            expandReq.collectionIds = collectionIds;
            expandReq.snapshotFence = snapshotFence;
            // Relevance-propagating path score: weight each derived seed's expansion by its partial
            // fusion relevance so a doc reached from a strong seed outranks one reached from a
            // marginal seed. Explicit seeds (no partial score) keep the default weight 1.0.
            if (!derivedSeedScores.isEmpty()) {
                expandReq.seedWeights = derivedSeedScores;
            }
            refResult = refExpander.expand(expandReq);
            for (ReferenceExpander.ExpansionRow row : refResult.rows) {
                Candidate c = byEntity.get(row.entityId);
                if (c == null) {
                    c = new Candidate(row.entityId, 0.0, 0.0, row.pathScore, row.hop,
                            row.refKinds, null);
                    byEntity.put(row.entityId, c);
                } else {
                    // Re-hydrate the candidate with the ref-path score + hop info without losing
                    // text/vector scores we already captured.
                    Candidate merged = new Candidate(
                            c.entityId, c.textScore, c.vectorScore,
                            Math.max(c.graphScore, row.pathScore),
                            row.hop,
                            mergeEdgeTypes(c.edgeTypes, row.refKinds),
                            c.snippet);
                    byEntity.put(row.entityId, merged);
                }
            }
            // Top up metadata for entities only reachable via graph expansion. Fusion / budget
            // planning need their entity_type to apply synthesis demotion.
            Set<Long> newcomers = new HashSet<>(byEntity.keySet());
            newcomers.removeAll(metaByEntity.keySet());
            if (!newcomers.isEmpty()) {
                metaByEntity.putAll(loadMetadataSafe(newcomers, snapshotFence));
            }
        }
        graphNs = System.nanoTime() - tGraph;
        String graphSeedsSource;
        if (hasExplicitSeeds && !derivedSeeds.isEmpty()) {
            graphSeedsSource = "mixed";
        } else if (hasExplicitSeeds) {
            graphSeedsSource = "explicit";
        } else if (!derivedSeeds.isEmpty()) {
            graphSeedsSource = "derived";
        } else {
            graphSeedsSource = "none";
        }
        String graphStatus;
        if (request.graphMode == GraphMode.OFF) {
            graphStatus = "skipped_off";
        } else if (refEnabled) {
            graphStatus = "ran";
        } else {
            graphStatus = "skipped_no_seeds";
        }

        // ---- final ranking via pluggable strategy ----
        // Strategy was resolved up-front (line ~313). Pool is everything text+vector(+optional
        // BFS-merged graph) produced; the strategy decides how to score and order. The two-layer
        // synthesis demotion documented in architecture doc §10.5 is encapsulated inside
        // AdditiveRerankStrategy — strategies that don't model synthesis (e.g. greedy) skip it.
        List<Candidate> pool = new ArrayList<>(byEntity.values());
        Map<String, Object> explain = new LinkedHashMap<>();
        RerankContext rerankCtx = RerankContext.builder()
                .pool(pool)
                .metaByEntity(metaByEntity)
                .request(request)
                .contextBaseId(contextBaseId == null ? 0L : contextBaseId)
                .contextBaseIds(contextBaseIds)
                .collectionIds(collectionIds)
                .snapshotFence(snapshotFence)
                .refExpander(this.refExpander)
                .readExecutor(GlobalStateMgr.getCurrentState().getContextReadExecutor())
                .explain(explain)
                .build();
        // When cross-encoder rerank is on, let first-stage fusion return a larger pool (rerankTopN)
        // so the reranker has candidates to reorder; we truncate back to the caller's maxResults after.
        int originalMaxResults = request.maxResults;
        int rerankN = request.rerank
                ? (request.rerankTopN > 0 ? request.rerankTopN : DEFAULT_RERANK_TOP_N) : 0;
        if (request.rerank && rerankN > originalMaxResults) {
            request.maxResults = rerankN;
        }
        long tRerank = System.nanoTime();
        List<Candidate> top = strategy.rerank(rerankCtx);
        request.maxResults = originalMaxResults;
        rerankNs = System.nanoTime() - tRerank;
        if (request.rerank) {
            long tCross = System.nanoTime();
            top = applyCrossEncoderRerank(top, request, metaByEntity, explain);
            crossEncoderNs = System.nanoTime() - tCross;
        }

        long tBudget = System.nanoTime();
        ContextBudgetPlanner.Result budgetResult = budgetPlanner == null
                ? null : budgetPlanner.plan(top, snapshotFence, request.maxTokens, metaByEntity);
        budgetNs = System.nanoTime() - tBudget;

        // ---- explain (per arch doc §15) ----
        explain.put("contextbase", request.contextBase);
        explain.put("contextbases", contextBaseIds);
        explain.put("collection", request.collection);
        explain.put("collections", collectionIds);
        explain.put("workspace", request.workspace);
        explain.put("retrieval_profile", request.retrievalProfile);
        explain.put("consistency", request.consistency);
        explain.put("snapshot_fence", snapshotFence);
        explain.put("text_enabled", textEnabled);
        explain.put("vector_enabled", vectorEnabled);
        explain.put("vector_path_status", vectorPathStatus);
        explain.put("reference_enabled", refEnabled);
        explain.put("reference_direction",
                (request.direction != null ? request.direction : ReferenceExpander.Direction.BOTH).name());
        explain.put("candidate_pool_size", pool.size());
        explain.put("top_k", top.size());
        explain.put("max_results", request.maxResults);
        explain.put("max_tokens", request.maxTokens);
        explain.put("graph_mode", request.graphMode.name());
        explain.put("graph_status", graphStatus);
        explain.put("graph_seeds_source", graphSeedsSource);
        explain.put("graph_seed_count", finalSeeds.size());
        explain.put("graph_seed_topk_used", resolvedSeedTopK);
        explain.put("synthesis_filtered_seeds", synthesisFilteredSeeds);
        explain.put("graph_depth", request.graphDepth);
        explain.put("max_frontier", request.maxFrontier);
        // Weights actually used for this request.
        Map<String, Object> weights = new LinkedHashMap<>();
        weights.put("text", request.textWeight);
        weights.put("vector", request.vectorWeight);
        weights.put("graph", request.graphWeight);
        explain.put("weights", weights);
        explain.put("text_score_model", "bm25_lite");
        if (refResult != null) {
            explain.put("reference_truncated", refResult.truncated);
            explain.put("reference_max_hop", refResult.maxHopReached);
            if (refResult.truncated) {
                explain.put("degrade_reason", "max_frontier=" + request.maxFrontier
                        + " exceeded; consider raising it or narrowing seed_ids");
            }
        }
        if (request.graphMode == GraphMode.OFF && hasExplicitSeeds) {
            // OFF + explicit seeds is unusual — caller probably forgot to set graph_mode. Surface a
            // hint rather than silently dropping the seeds.
            explain.put("hint", "seed_ids supplied but graph_mode=OFF — reference path skipped");
        }
        if (budgetResult != null) {
            explain.put("packed_text", budgetResult.packedText);
            explain.put("used_tokens_estimate", budgetResult.usedTokensEstimate);
            explain.put("included_entities", budgetResult.includedEntities);
            explain.put("truncated_entities", budgetResult.truncatedEntities);
            // Stringify the Long keys: ContextBudgetPlanner.Result.disclosureLevels is
            // Map<Long, String>, but Jackson's default MapSerializer assumes String keys and
            // throws ClassCastException("Long cannot be cast to String") on serialization,
            // which RestBaseAction.sendResultByJson then silently swallows — leaving callers
            // with HTTP 200 + empty body. JSON keys are always strings anyway, so the API
            // contract is unchanged.
            Map<String, String> stringKeyed = new java.util.LinkedHashMap<>();
            for (Map.Entry<Long, String> entry : budgetResult.disclosureLevels.entrySet()) {
                stringKeyed.put(String.valueOf(entry.getKey()), entry.getValue());
            }
            explain.put("disclosure_levels", stringKeyed);
        }
        AuditLog.getInternalAudit().info(
                "context_search | contextbase={} collection={} text_enabled={} ref_enabled={} "
                        + "candidates={} top_k={} graph_mode={}",
                request.contextBase, request.collection, textEnabled, refEnabled,
                pool.size(), top.size(), request.graphMode.name());
        if (LOG.isInfoEnabled()) {
            long embedNs = vecReqF != null ? vecReqF.embedNanos : 0L;
            long annNs = vecReqF != null ? vecReqF.annNanos : 0L;
            LOG.info("context_search done request_id={} total={}ms strategy={} candidates={} top_k={} "
                            + "scope={}ms snapshot={}ms text={}ms vector={}ms(embed={}ms,ann={}ms) merge={}ms "
                            + "meta={}ms graph={}ms rerank={}ms cross_encoder={}ms budget={}ms",
                    requestId, ms(System.nanoTime() - tSearchStart), request.graphStrategy, pool.size(),
                    top.size(), ms(scopeNs), ms(snapshotNs), ms(textNs), ms(vectorNs), ms(embedNs), ms(annNs),
                    ms(mergeNs), ms(metaNs), ms(graphNs), ms(rerankNs), ms(crossEncoderNs), ms(budgetNs));
        }
        return new Result(top, explain);
    }

    /**
     * Optional second-phase cross-encoder rerank. Re-scores the fusion-ordered {@code top} pool with
     * an external rerank provider and reorders it, then truncates to {@code request.maxResults}. Any
     * failure (no/invalid provider, HTTP/parse error) degrades to the fusion order and records
     * {@code explain.rerank_error} — rerank must never break search.
     */
    private List<Candidate> applyCrossEncoderRerank(List<Candidate> top, Request request,
            Map<Long, ContextReadExecutor.EntityMeta> metaByEntity, Map<String, Object> explain) {
        if (top.isEmpty()) {
            return top;
        }
        try {
            AIProvider provider = FeRerankClient.resolveProvider(request.rerankProvider);
            Map<Long, String> bodyById = request.rerankUseBody
                    ? loadRerankBodies(top, metaByEntity) : java.util.Collections.emptyMap();
            List<String> docs = new ArrayList<>(top.size());
            for (Candidate c : top) {
                String text = request.rerankUseBody ? bodyById.get(c.entityId) : null;
                if (Strings.isNullOrEmpty(text)) {
                    ContextReadExecutor.EntityMeta meta = metaByEntity.get(c.entityId);
                    text = meta != null ? meta.preview : null;
                }
                if (Strings.isNullOrEmpty(text)) {
                    text = c.snippet != null ? c.snippet : "";
                }
                docs.add(text);
            }
            long t0 = System.nanoTime();
            List<FeRerankClient.ScoredIndex> ranked = FeRerankClient.rerank(provider, request.queryText, docs);
            long ms = (System.nanoTime() - t0) / 1_000_000;

            List<Candidate> reordered = new ArrayList<>(top.size());
            boolean[] used = new boolean[top.size()];
            for (FeRerankClient.ScoredIndex s : ranked) {
                if (s.index >= 0 && s.index < top.size() && !used[s.index]) {
                    Candidate c = top.get(s.index);
                    c.finalScore = s.score;
                    reordered.add(c);
                    used[s.index] = true;
                }
            }
            // Any candidates the provider omitted (or were beyond its max_documents cap) keep their
            // fusion order after the reranked ones.
            for (int i = 0; i < top.size(); i++) {
                if (!used[i]) {
                    reordered.add(top.get(i));
                }
            }
            List<Candidate> out = reordered.size() > request.maxResults
                    ? new ArrayList<>(reordered.subList(0, request.maxResults)) : reordered;
            explain.put("rerank_provider", provider.getName());
            explain.put("rerank_model", provider.getModel());
            explain.put("rerank_pool", top.size());
            explain.put("rerank_input", request.rerankUseBody ? "body" : "preview");
            explain.put("rerank_latency_ms", ms);
            return out;
        } catch (Exception e) {
            LOG.warn("cross-encoder rerank failed; falling back to fusion order: {}", e.toString());
            explain.put("rerank_error", e.getMessage());
            return top.size() > request.maxResults
                    ? new ArrayList<>(top.subList(0, request.maxResults)) : top;
        }
    }

    private Map<Long, String> loadRerankBodies(List<Candidate> pool,
            Map<Long, ContextReadExecutor.EntityMeta> metaByEntity) {
        List<ContextReadExecutor.EntityVersionKey> keys = new ArrayList<>();
        for (Candidate c : pool) {
            ContextReadExecutor.EntityMeta meta = metaByEntity.get(c.entityId);
            if (meta != null) {
                keys.add(new ContextReadExecutor.EntityVersionKey(c.entityId, meta.version));
            }
        }
        Map<Long, String> out = new HashMap<>();
        if (keys.isEmpty()) {
            return out;
        }
        Map<ContextReadExecutor.EntityVersionKey, ContextReadExecutor.VersionRow> rows =
                GlobalStateMgr.getCurrentState().getContextReadExecutor().loadVersionRows(keys);
        for (ContextReadExecutor.VersionRow r : rows.values()) {
            if (r != null) {
                out.put(r.entityId, Strings.isNullOrEmpty(r.body) ? r.preview : r.body);
            }
        }
        return out;
    }

    private void applyRetrievalProfile(Request request, Long collectionId) {
        String profileName = request.retrievalProfile;
        if (Strings.isNullOrEmpty(profileName) && collectionId != null && !Strings.isNullOrEmpty(request.contextBase)) {
            ContextMgr.CollectionMeta meta = contextMgr.getCollection(request.contextBase, request.collection);
            if (meta != null) {
                profileName = meta.getProperties().get("retrieval_profile");
            }
        }
        if (Strings.isNullOrEmpty(profileName)) {
            return;
        }
        ContextMgr.RetrievalProfileMeta profile = contextMgr.getRetrievalProfile(profileName);
        if (profile == null) {
            return;
        }
        request.retrievalProfile = profileName;
        if (!request.explicitTextWeight && profile.getProperties().containsKey("text_weight")) {
            request.textWeight = Double.parseDouble(profile.getProperties().get("text_weight"));
        }
        if (!request.explicitVectorWeight && profile.getProperties().containsKey("vector_weight")) {
            request.vectorWeight = Double.parseDouble(profile.getProperties().get("vector_weight"));
        }
        if (!request.explicitGraphWeight && profile.getProperties().containsKey("graph_weight")) {
            request.graphWeight = Double.parseDouble(profile.getProperties().get("graph_weight"));
        }
        // fusion_mode picks the rerank strategy when the request didn't set graph_strategy
        // explicitly. RRF → the scale-free reciprocal-rank strategy; LINEAR → legacy additive.
        if (!request.explicitGraphStrategy && Strings.isNullOrEmpty(request.graphStrategy)
                && profile.getProperties().containsKey("fusion_mode")) {
            request.graphStrategy = mapFusionMode(profile.getProperties().get("fusion_mode"));
        }
        // Cross-encoder rerank defaults from the profile (applied only when the request did not set
        // them explicitly), so an operator can turn rerank on per-collection without changing callers.
        if (!request.explicitRerank && profile.getProperties().containsKey("rerank_enabled")) {
            request.rerank = Boolean.parseBoolean(profile.getProperties().get("rerank_enabled"));
        }
        if (Strings.isNullOrEmpty(request.rerankProvider)
                && profile.getProperties().containsKey("rerank_provider")) {
            request.rerankProvider = profile.getProperties().get("rerank_provider");
        }
        if (request.rerankTopN == 0 && profile.getProperties().containsKey("rerank_top_n")) {
            try {
                request.rerankTopN = Integer.parseInt(profile.getProperties().get("rerank_top_n"));
            } catch (NumberFormatException ignore) {
                // leave at 0 -> DEFAULT_RERANK_TOP_N
            }
        }
        if (!request.explicitRerankUseBody && profile.getProperties().containsKey("rerank_use_body")) {
            request.rerankUseBody = Boolean.parseBoolean(profile.getProperties().get("rerank_use_body"));
        }
    }

    /** Map a retrieval-profile {@code fusion_mode} property to a registered rerank strategy name. */
    private static String mapFusionMode(String fusionMode) {
        if (Strings.isNullOrEmpty(fusionMode)) {
            return null;
        }
        String normalized = fusionMode.trim().toLowerCase(java.util.Locale.ROOT);
        switch (normalized) {
            case "rrf":
                return com.starrocks.context.retrieval.rerank.RrfRerankStrategy.NAME;
            case "linear":
            case "additive":
                return com.starrocks.context.retrieval.rerank.AdditiveRerankStrategy.NAME;
            default:
                // Any other value is treated as a (possibly custom) strategy name; resolution is
                // lenient for profile-derived names, so an unknown value falls back to additive.
                return normalized;
        }
    }

    /**
     * Resolve the rerank strategy. An explicit request {@code graph_strategy} resolves strictly
     * (unknown → INVALID_ARGUMENT). A name derived from a profile {@code fusion_mode} or, when none
     * is set, from the {@link #DEFAULT_AUTO_STRATEGY} constant, resolves leniently: an unknown name
     * falls back to additive with a WARN so a misconfigured profile never fails live search traffic.
     */
    private RerankStrategy resolveStrategy(Request request) {
        if (request.explicitGraphStrategy && !Strings.isNullOrEmpty(request.graphStrategy)) {
            return RerankStrategies.resolve(request.graphStrategy);
        }
        String name = request.graphStrategy;
        if (Strings.isNullOrEmpty(name)) {
            name = DEFAULT_AUTO_STRATEGY;
        }
        try {
            return RerankStrategies.resolve(name);
        } catch (com.starrocks.context.error.ContextException e) {
            LOG.warn("unknown context_search rerank strategy '{}' (from profile default); "
                    + "falling back to additive", name);
            return RerankStrategies.resolve(
                    com.starrocks.context.retrieval.rerank.AdditiveRerankStrategy.NAME);
        }
    }

    private int resolveGraphSeedTopK(Request request) {
        if (request.graphSeedTopK > 0) {
            return request.graphSeedTopK;
        }
        return Math.max(1, Math.min(request.maxResults, DEFAULT_GRAPH_SEED_TOPK_CEILING));
    }

    /**
     * Bulk-fetch entity metadata, swallowing test-context failures. Returning an empty map (rather
     * than propagating) keeps the seed/fusion pipeline resilient when {@link GlobalStateMgr} isn't
     * available — e.g. unit-test stubs that bypass the SQL plane entirely. Marked
     * {@code protected} so tests can inject deterministic metadata without needing a live SQL plane.
     */
    protected Map<Long, ContextReadExecutor.EntityMeta> loadMetadataSafe(
            Collection<Long> ids, long snapshotFence) {
        if (ids == null || ids.isEmpty()) {
            return java.util.Collections.emptyMap();
        }
        try {
            ContextReadExecutor reader = GlobalStateMgr.getCurrentState().getContextReadExecutor();
            if (reader == null) {
                return java.util.Collections.emptyMap();
            }
            return reader.loadEntityMetadata(new ArrayList<>(ids), snapshotFence);
        } catch (Exception e) {
            return java.util.Collections.emptyMap();
        }
    }

    /** Result of {@link #deriveGraphSeeds} — the picked seed ids plus a count of synthesis
     *  candidates that were skipped (for explain-side observability). */
    private static final class DerivedSeedResult {
        final List<Long> seedIds;
        final int synthesisSkipped;
        /** Per-seed partial fusion score (text·w + vector·w), used to weight graph path scores. */
        final Map<Long, Double> seedScores;

        DerivedSeedResult(List<Long> seedIds, int synthesisSkipped, Map<Long, Double> seedScores) {
            this.seedIds = seedIds;
            this.synthesisSkipped = synthesisSkipped;
            this.seedScores = seedScores;
        }
    }

    /**
     * Pick the Top-K most-promising entities to seed the reference-expansion path. Ranking uses the
     * partial fusion score from text and vector (graph hasn't run yet, hence "partial"); this is
     * the same metric we trust for the final ranking, so it's the same metric we trust for picking
     * graph entry points.
     *
     * <p>Returns at most {@code topK} ids. Skips:
     * <ul>
     *   <li>Candidates with non-positive partial score — seeding from those would expand
     *       neighborhoods of effectively-irrelevant candidates and bloat the frontier.</li>
     *   <li>Synthesis entities (per {@link CollectionTypePolicy#isSynthesisType}) — they are
     *       aggregation targets, not navigation entry points; expanding from them just walks
     *       back to leaves we already discovered while inflating the synthesis's own score.</li>
     * </ul>
     */
    private DerivedSeedResult deriveGraphSeeds(Collection<Candidate> candidates,
                                               Map<Long, ContextReadExecutor.EntityMeta> metaByEntity,
                                               Request request, int topK) {
        if (candidates.isEmpty() || topK <= 0) {
            return new DerivedSeedResult(java.util.Collections.emptyList(), 0,
                    java.util.Collections.emptyMap());
        }
        List<Candidate> ranked = new ArrayList<>(candidates);
        ranked.sort((a, b) -> Double.compare(
                request.textWeight * b.textScore + request.vectorWeight * b.vectorScore,
                request.textWeight * a.textScore + request.vectorWeight * a.vectorScore));
        List<Long> seeds = new ArrayList<>(Math.min(topK, ranked.size()));
        Map<Long, Double> seedScores = new HashMap<>();
        int synthesisSkipped = 0;
        for (Candidate c : ranked) {
            if (seeds.size() >= topK) {
                break;
            }
            double partial = request.textWeight * c.textScore + request.vectorWeight * c.vectorScore;
            if (partial <= 0.0) {
                continue;
            }
            ContextReadExecutor.EntityMeta meta = metaByEntity.get(c.entityId);
            if (meta != null && CollectionTypePolicy.isSynthesisType(meta.entityType)) {
                synthesisSkipped++;
                continue;
            }
            seeds.add(c.entityId);
            seedScores.put(c.entityId, partial);
        }
        return new DerivedSeedResult(seeds, synthesisSkipped, seedScores);
    }

    private List<String> mergeEdgeTypes(List<String> existing, List<String> incoming) {
        if (existing == null || existing.isEmpty()) {
            return incoming;
        }
        if (incoming == null || incoming.isEmpty()) {
            return existing;
        }
        List<String> merged = new ArrayList<>(existing);
        for (String t : incoming) {
            if (!merged.contains(t)) {
                merged.add(t);
            }
        }
        // Keep a stable small list; de-dup without sorting preserves provenance order.
        return Arrays.asList(merged.toArray(new String[0]));
    }

}
