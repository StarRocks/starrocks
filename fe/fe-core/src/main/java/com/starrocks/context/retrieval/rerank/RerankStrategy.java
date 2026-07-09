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

import com.starrocks.context.retrieval.ContextSearchExecutor;

import java.util.List;

/**
 * Pluggable final-ranking step for {@code CONTEXT_SEARCH}. Implementations consume the merged
 * text+vector(+graph) candidate pool produced by {@link ContextSearchExecutor} and emit the final
 * Top-N ordering.
 *
 * <p>Strategies are looked up by {@link #name()} (case-insensitive) via {@link RerankStrategies}.
 * The {@code graph_strategy} field on the search request payload selects which strategy to use;
 * unknown names trip {@code INVALID_ARGUMENT} at registry resolution time. Adding a new ranking
 * algorithm means creating one class implementing this interface and registering it once — no
 * touches to {@link ContextSearchExecutor#search}.
 */
public interface RerankStrategy {

    /**
     * Stable, lowercase name used in the {@code graph_strategy} request field. Registry lookup is
     * case-insensitive, but the canonical form returned here should be lowercase + snake_case.
     */
    String name();

    /**
     * Whether {@link ContextSearchExecutor#search} should pre-run the {@code ReferenceExpander} BFS
     * and merge graph hits into the candidate pool before invoking this strategy. Strategies whose
     * scoring formula reads {@code graphScore} (e.g. the legacy additive fusion) must return
     * {@code true}. Strategies that load their own adjacency directly (e.g. the FK greedy rerank)
     * return {@code false} to skip the redundant BFS work.
     */
    boolean needsGraphExpandedPool();

    /**
     * Produce the final ranked top-K candidates. The returned list is taken as-is — implementations
     * must respect {@code ctx.request.maxResults}, assign {@code finalScore} on each returned
     * {@link ContextSearchExecutor.Candidate}, and optionally append explain breadcrumbs to
     * {@code ctx.explain}.
     */
    List<ContextSearchExecutor.Candidate> rerank(RerankContext ctx);
}
