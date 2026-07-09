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

import com.google.common.base.Strings;
import com.starrocks.context.error.ContextErrorCode;
import com.starrocks.context.error.ContextException;

import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Static registry for {@link RerankStrategy} implementations. The {@code graph_strategy} field on
 * the search request payload selects which strategy runs; lookup is case-insensitive against
 * {@link RerankStrategy#name()}.
 *
 * <p>Adding a new strategy means writing one class and calling {@link #register} once — either in
 * this class's static block (for first-party strategies) or from a plugin's initialization path.
 * No touches to {@link com.starrocks.context.retrieval.ContextSearchExecutor#search} are needed.
 */
public final class RerankStrategies {

    private static final Map<String, RerankStrategy> REGISTRY = new ConcurrentHashMap<>();

    static {
        register(new AdditiveRerankStrategy());
        register(new RrfRerankStrategy());
        register(new VectorAnchorGreedyRerankStrategy());
    }

    private RerankStrategies() {
    }

    /**
     * Register a strategy under its canonical {@link RerankStrategy#name() name}. Subsequent
     * registrations with the same name override earlier ones — useful in tests, but production
     * code should pick unique names.
     */
    public static void register(RerankStrategy strategy) {
        REGISTRY.put(strategy.name().toLowerCase(Locale.ROOT), strategy);
    }

    /**
     * Resolve a strategy by request-supplied name. A null or empty name maps to the default
     * additive strategy (keeps existing callers' behavior unchanged). Unknown names throw
     * {@code INVALID_ARGUMENT} so callers see a clear error instead of silently falling back.
     */
    public static RerankStrategy resolve(String name) {
        if (Strings.isNullOrEmpty(name)) {
            return REGISTRY.get(AdditiveRerankStrategy.NAME);
        }
        RerankStrategy s = REGISTRY.get(name.toLowerCase(Locale.ROOT));
        if (s == null) {
            throw new ContextException(ContextErrorCode.INVALID_ARGUMENT,
                    "unknown graph_strategy '" + name + "'; known: " + REGISTRY.keySet());
        }
        return s;
    }

    /** Returns the set of registered strategy names (canonical lowercase form). */
    public static Set<String> available() {
        return Collections.unmodifiableSet(REGISTRY.keySet());
    }
}
