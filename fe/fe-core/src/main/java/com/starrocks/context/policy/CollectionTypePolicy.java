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

package com.starrocks.context.policy;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Encodes the {@code collection_type → allowed entity_type} matrix from
 * {@code 1-agentbase-starrocks-semantic-context-architecture-design.md §4.2}.
 *
 * <p>The matrix is conservative by design: {@code channel} only allows {@code page} entities,
 * {@code memory} rejects raw {@code object}/{@code doc} entities, and so on. This class is the
 * single place where those rules are enforced, so changing the matrix in one place updates both
 * the analyzer path and any future planner path without semantic drift.
 */
public final class CollectionTypePolicy {

    public static final String TYPE_KNOWLEDGE = "knowledge";
    public static final String TYPE_SKILL = "skill";
    public static final String TYPE_MEMORY = "memory";
    public static final String TYPE_TASK_SUMMARY = "task_summary";
    public static final String TYPE_CHANNEL = "channel";

    private static final Map<String, Set<String>> ALLOWED_ENTITY_TYPES = ImmutableMap.of(
            TYPE_KNOWLEDGE, ImmutableSet.of(
                    "object", "doc", "page", "homepage", "derived_page", "derived_homepage"),
            TYPE_SKILL, ImmutableSet.of(
                    "page", "homepage", "derived_page", "derived_homepage"),
            TYPE_MEMORY, ImmutableSet.of(
                    "page", "derived_page", "derived_homepage"),
            TYPE_TASK_SUMMARY, ImmutableSet.of(
                    "page", "derived_page", "derived_homepage"),
            TYPE_CHANNEL, ImmutableSet.of(
                    "page"));

    private static final Set<String> ALL_COLLECTION_TYPES = ALLOWED_ENTITY_TYPES.keySet();

    // Entity types whose body is itself a synthesis of underlying leaf entities. Fusion
    // retrieval treats these specially: they don't seed graph expansion (they would just
    // walk back to the leaves we already have via text/vector), their graph_score is
    // discounted to avoid double-counting against the same leaves they aggregate, and the
    // budget planner upgrades leaves to STANDARD/DEEP before lifting synthesis above
    // PREVIEW. See architecture doc §10.5 / §12.
    private static final Set<String> SYNTHESIS_ENTITY_TYPES = ImmutableSet.of("derived_page");

    private CollectionTypePolicy() {
    }

    public static boolean isValidCollectionType(String collectionType) {
        if (collectionType == null) {
            return false;
        }
        return ALL_COLLECTION_TYPES.contains(collectionType.toLowerCase(Locale.ROOT));
    }

    /**
     * Whether the given {@code entity_type} is a "synthesis" type — a body that aggregates
     * multiple leaf entities. Fusion retrieval discounts these so they don't crowd out the
     * leaf evidence agents actually need to ground their answers.
     */
    public static boolean isSynthesisType(String entityType) {
        if (entityType == null) {
            return false;
        }
        return SYNTHESIS_ENTITY_TYPES.contains(entityType.toLowerCase(Locale.ROOT));
    }

    /**
     * Returns the canonical lowercased entity-type list permitted under the given collection type.
     * The output is unmodifiable and safe to cache.
     */
    public static Set<String> allowedEntityTypes(String collectionType) {
        if (collectionType == null) {
            return ImmutableSet.of();
        }
        Set<String> set = ALLOWED_ENTITY_TYPES.get(collectionType.toLowerCase(Locale.ROOT));
        return set == null ? ImmutableSet.of() : set;
    }

    /**
     * Validate a combination. Throws {@link IllegalArgumentException} with an actionable message.
     */
    public static void check(String collectionType, String entityType) {
        if (collectionType == null || collectionType.isEmpty()) {
            throw new IllegalArgumentException("collection_type is required");
        }
        String col = collectionType.toLowerCase(Locale.ROOT);
        if (!ALL_COLLECTION_TYPES.contains(col)) {
            throw new IllegalArgumentException("unknown collection_type: " + collectionType
                    + "; expected one of " + ALL_COLLECTION_TYPES);
        }
        if (entityType == null || entityType.isEmpty()) {
            throw new IllegalArgumentException("entity_type is required");
        }
        Set<String> allowed = ALLOWED_ENTITY_TYPES.get(col);
        if (!allowed.contains(entityType.toLowerCase(Locale.ROOT))) {
            throw new IllegalArgumentException(String.format(
                    "entity_type '%s' is not allowed in a %s collection; allowed types: %s",
                    entityType, col, allowed));
        }
    }
}
