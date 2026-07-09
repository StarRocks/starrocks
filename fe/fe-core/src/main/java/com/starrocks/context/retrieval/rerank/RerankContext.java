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

package com.starrocks.context.retrieval.rerank;

import com.starrocks.context.ContextReadExecutor;
import com.starrocks.context.retrieval.ContextSearchExecutor;
import com.starrocks.context.retrieval.ReferenceExpander;

import java.util.List;
import java.util.Map;

/**
 * Inputs passed to {@link RerankStrategy#rerank}. Bundles everything a strategy needs to compute
 * final ranking — the merged candidate pool, request parameters, the search-time snapshot fence,
 * and helpers ({@link ReferenceExpander}) that strategies use to issue their own graph reads.
 *
 * <p>Construct via {@link #builder()}. Fields are final and exposed directly for terse access
 * inside strategy implementations.
 */
public final class RerankContext {

    /** Candidate pool merged from text + vector paths (and graph if the strategy requested it). */
    public final List<ContextSearchExecutor.Candidate> pool;

    /** Pre-loaded entity metadata for everyone in {@link #pool}; needed for synthesis demotion. */
    public final Map<Long, ContextReadExecutor.EntityMeta> metaByEntity;

    /** Original request — strategies read weights, beta, max_results, edge_types, etc. */
    public final ContextSearchExecutor.Request request;

    /** Resolved contextbase id; needed when the strategy issues its own graph SQL. For a
     *  multi-contextbase search this carries 0 and {@link #contextBaseIds} holds the set. */
    public final long contextBaseId;

    /** Resolved contextbase ids for multi-contextbase search. Applies when {@link #contextBaseId}
     *  is 0. May be null/empty for the single-base path. */
    public final List<Long> contextBaseIds;

    /** Resolved collection ids; same use as {@link #contextBaseId}. May be null/empty. */
    public final List<Long> collectionIds;

    /** Snapshot fence pinned at search start; -1L means "current heads". */
    public final long snapshotFence;

    /** Shared expander instance — strategies call {@code refExpander.scan(...)} for 1-hop reads. */
    public final ReferenceExpander refExpander;

    /** Shared reader for any metadata top-ups a strategy might need. */
    public final ContextReadExecutor readExecutor;

    /**
     * Mutable explain map. Strategies append their breadcrumbs here ({@code rerank_strategy},
     * {@code rerank_beta}, etc.) so the response carries provenance for debugging / tuning.
     */
    public final Map<String, Object> explain;

    private RerankContext(Builder b) {
        this.pool = b.pool;
        this.metaByEntity = b.metaByEntity;
        this.request = b.request;
        this.contextBaseId = b.contextBaseId;
        this.contextBaseIds = b.contextBaseIds;
        this.collectionIds = b.collectionIds;
        this.snapshotFence = b.snapshotFence;
        this.refExpander = b.refExpander;
        this.readExecutor = b.readExecutor;
        this.explain = b.explain;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private List<ContextSearchExecutor.Candidate> pool;
        private Map<Long, ContextReadExecutor.EntityMeta> metaByEntity;
        private ContextSearchExecutor.Request request;
        private long contextBaseId;
        private List<Long> contextBaseIds;
        private List<Long> collectionIds;
        private long snapshotFence = -1L;
        private ReferenceExpander refExpander;
        private ContextReadExecutor readExecutor;
        private Map<String, Object> explain;

        public Builder pool(List<ContextSearchExecutor.Candidate> pool) {
            this.pool = pool;
            return this;
        }

        public Builder metaByEntity(Map<Long, ContextReadExecutor.EntityMeta> metaByEntity) {
            this.metaByEntity = metaByEntity;
            return this;
        }

        public Builder request(ContextSearchExecutor.Request request) {
            this.request = request;
            return this;
        }

        public Builder contextBaseId(long contextBaseId) {
            this.contextBaseId = contextBaseId;
            return this;
        }

        public Builder contextBaseIds(List<Long> contextBaseIds) {
            this.contextBaseIds = contextBaseIds;
            return this;
        }

        public Builder collectionIds(List<Long> collectionIds) {
            this.collectionIds = collectionIds;
            return this;
        }

        public Builder snapshotFence(long snapshotFence) {
            this.snapshotFence = snapshotFence;
            return this;
        }

        public Builder refExpander(ReferenceExpander refExpander) {
            this.refExpander = refExpander;
            return this;
        }

        public Builder readExecutor(ContextReadExecutor readExecutor) {
            this.readExecutor = readExecutor;
            return this;
        }

        public Builder explain(Map<String, Object> explain) {
            this.explain = explain;
            return this;
        }

        public RerankContext build() {
            return new RerankContext(this);
        }
    }
}
