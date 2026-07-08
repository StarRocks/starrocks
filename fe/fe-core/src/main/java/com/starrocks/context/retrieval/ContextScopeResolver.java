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
import com.google.common.collect.ImmutableList;
import com.starrocks.context.ContextMgr;
import com.starrocks.context.error.ContextErrorCode;
import com.starrocks.context.error.ContextException;
import com.starrocks.context.policy.CollectionTypePolicy;

import java.util.ArrayList;
import java.util.List;

/**
 * Canonical scope resolver shared by the retrieval surfaces.
 *
 * <p>The external contract uses either an explicit {@code (contextbase, collection)} pair or a
 * single {@code scope} string of the form {@code contextbase.collection} / {@code contextbase.*}.
 * This helper normalizes both to the concrete numeric ids the executors need.
 */
public final class ContextScopeResolver {

    private ContextScopeResolver() {
    }

    public static final class ResolvedScope {
        // Single-base name; null when the scope spans multiple contextbases. Prefer
        // {@link #contextBases} / {@link #contextBaseIds}, which are always populated (1+ entries).
        public final String contextBase;
        public final List<String> contextBases;
        public final List<Long> contextBaseIds;
        public final String collection;
        public final List<String> collections;
        public final String collectionType;
        // Single-base id; for a multi-base scope this carries the first id only (callers searching
        // multiple bases must consume {@link #contextBaseIds}).
        public final long contextBaseId;
        public final Long collectionId;
        public final List<Long> collectionIds;

        public ResolvedScope(String contextBase, String collection, List<String> collections,
                             String collectionType, long contextBaseId, Long collectionId,
                             List<Long> collectionIds) {
            this.contextBase = contextBase;
            this.contextBases = ImmutableList.of(contextBase);
            this.contextBaseIds = ImmutableList.of(contextBaseId);
            this.collection = collection;
            this.collections = collections == null ? ImmutableList.of() : ImmutableList.copyOf(collections);
            this.collectionType = collectionType;
            this.contextBaseId = contextBaseId;
            this.collectionId = collectionId;
            this.collectionIds = collectionIds == null ? ImmutableList.of() : ImmutableList.copyOf(collectionIds);
        }

        /** Multi-base scope: no collection-level filtering, no single {@code contextBase} name. */
        private ResolvedScope(List<String> contextBases, List<Long> contextBaseIds) {
            this.contextBase = null;
            this.contextBases = ImmutableList.copyOf(contextBases);
            this.contextBaseIds = ImmutableList.copyOf(contextBaseIds);
            this.collection = null;
            this.collections = ImmutableList.of();
            this.collectionType = null;
            this.contextBaseId = contextBaseIds.isEmpty() ? 0L : contextBaseIds.get(0);
            this.collectionId = null;
            this.collectionIds = ImmutableList.of();
        }

        /** True when more than one contextbase is in scope. */
        public boolean isMultiContextBase() {
            return contextBaseIds.size() > 1;
        }

        public boolean isWildcardCollection() {
            return collection == null && collectionIds.isEmpty() && Strings.isNullOrEmpty(collectionType);
        }
    }

    public static ResolvedScope resolve(ContextMgr mgr, String scope, String contextBase, String collection) {
        return resolve(mgr, scope, contextBase, collection, null, null);
    }

    public static ResolvedScope resolve(ContextMgr mgr, String scope, String contextBase, String collection,
                                        List<String> collections, String collectionType) {
        if (!Strings.isNullOrEmpty(scope)) {
            // scope is a self-contained selector; combining it with the explicit
            // contextbase/collection/collections/collection_type selectors is ambiguous and would
            // silently run under the scope value while ignoring the rest. Reject the conflict.
            if (!Strings.isNullOrEmpty(contextBase)
                    || !Strings.isNullOrEmpty(collection)
                    || (collections != null && !collections.isEmpty())
                    || !Strings.isNullOrEmpty(collectionType)) {
                throw new ContextException(ContextErrorCode.INVALID_SCOPE,
                        "scope cannot be combined with contextbase/collection/collections/collection_type; "
                                + "pick one");
            }
            String[] parts = scope.split("\\.", 2);
            if (parts.length != 2 || Strings.isNullOrEmpty(parts[0]) || Strings.isNullOrEmpty(parts[1])) {
                throw new ContextException(ContextErrorCode.INVALID_SCOPE,
                        "scope must be contextbase.collection or contextbase.*: " + scope);
            }
            return resolveNames(mgr, parts[0], "*".equals(parts[1]) ? null : parts[1], null, null);
        }
        return resolveNames(mgr, contextBase, collection, collections, collectionType);
    }

    public static ResolvedScope resolveNames(ContextMgr mgr, String contextBase, String collection) {
        return resolveNames(mgr, contextBase, collection, null, null);
    }

    /**
     * Resolve a list of contextbase names for multi-contextbase search. A single-element list
     * delegates to {@link #resolveNames} so collection-level filtering still works exactly as the
     * single-base path. With more than one base, collection-level scope ({@code collection} /
     * {@code collections} / {@code collection_type}) is rejected — collection names are not unique
     * across contextbases, so the filter would be ambiguous. Duplicate names are de-duplicated
     * while preserving first-seen order.
     */
    public static ResolvedScope resolveContextBases(ContextMgr mgr, List<String> contextBases,
                                                    String collection, List<String> collections,
                                                    String collectionType) {
        if (contextBases == null || contextBases.isEmpty()) {
            throw new ContextException(ContextErrorCode.INVALID_SCOPE,
                    "contextbases is required");
        }
        List<String> names = new ArrayList<>();
        for (String name : contextBases) {
            if (Strings.isNullOrEmpty(name)) {
                throw new ContextException(ContextErrorCode.INVALID_SCOPE,
                        "contextbases must not contain empty names");
            }
            if (!names.contains(name)) {
                names.add(name);
            }
        }
        if (names.size() == 1) {
            return resolveNames(mgr, names.get(0), collection, collections, collectionType);
        }
        if (!Strings.isNullOrEmpty(collection)
                || (collections != null && !collections.isEmpty())
                || !Strings.isNullOrEmpty(collectionType)) {
            throw new ContextException(ContextErrorCode.INVALID_SCOPE,
                    "collection-level scope (collection/collections/collection_type) cannot be "
                            + "combined with multiple contextbases");
        }
        List<Long> ids = new ArrayList<>(names.size());
        for (String name : names) {
            ContextMgr.ContextBaseMeta cb = mgr.getContextBase(name);
            if (cb == null) {
                throw new ContextException(ContextErrorCode.INVALID_SCOPE,
                        "contextbase not found: " + name);
            }
            ids.add(cb.getId());
        }
        return new ResolvedScope(names, ids);
    }

    public static ResolvedScope resolveNames(ContextMgr mgr, String contextBase, String collection,
                                             List<String> collections, String collectionType) {
        if (Strings.isNullOrEmpty(contextBase)) {
            throw new ContextException(ContextErrorCode.INVALID_SCOPE,
                    "contextbase is required");
        }
        ContextMgr.ContextBaseMeta cb = mgr.getContextBase(contextBase);
        if (cb == null) {
            throw new ContextException(ContextErrorCode.INVALID_SCOPE,
                    "contextbase not found: " + contextBase);
        }
        if (!Strings.isNullOrEmpty(collection)
                && collections != null && !collections.isEmpty()) {
            throw new ContextException(ContextErrorCode.INVALID_SCOPE,
                    "use either collection or collections, not both");
        }
        if (!Strings.isNullOrEmpty(collectionType)
                && ((collections != null && !collections.isEmpty()) || !Strings.isNullOrEmpty(collection))) {
            throw new ContextException(ContextErrorCode.INVALID_SCOPE,
                    "collection_type cannot be combined with collection/collections");
        }
        if (!Strings.isNullOrEmpty(collection)) {
            for (ContextMgr.CollectionMeta col : mgr.listCollections(contextBase)) {
                if (col.getName().equals(collection)) {
                    return new ResolvedScope(contextBase, collection, ImmutableList.of(collection), null,
                            cb.getId(), col.getId(), ImmutableList.of(col.getId()));
                }
            }
            throw new ContextException(ContextErrorCode.INVALID_SCOPE,
                    "collection not found: " + contextBase + "." + collection);
        }
        if (collections != null && !collections.isEmpty()) {
            List<Long> ids = new ArrayList<>();
            for (String name : collections) {
                boolean matched = false;
                for (ContextMgr.CollectionMeta col : mgr.listCollections(contextBase)) {
                    if (col.getName().equals(name)) {
                        ids.add(col.getId());
                        matched = true;
                        break;
                    }
                }
                if (!matched) {
                    throw new ContextException(ContextErrorCode.INVALID_SCOPE,
                            "collection not found: " + contextBase + "." + name);
                }
            }
            return new ResolvedScope(contextBase, null, collections, null,
                    cb.getId(), ids.size() == 1 ? ids.get(0) : null, ids);
        }
        if (!Strings.isNullOrEmpty(collectionType)) {
            // Reject an unknown collection_type up front so a typo surfaces as a non-retryable
            // parameter error instead of silently resolving to an empty (zero-result) scope.
            if (!CollectionTypePolicy.isValidCollectionType(collectionType)) {
                throw new ContextException(ContextErrorCode.INVALID_COLLECTION_TYPE,
                        "unknown collection_type: " + collectionType);
            }
            List<String> names = new ArrayList<>();
            List<Long> ids = new ArrayList<>();
            for (ContextMgr.CollectionMeta col : mgr.listCollections(contextBase)) {
                if (collectionType.equalsIgnoreCase(col.getCollectionType())) {
                    names.add(col.getName());
                    ids.add(col.getId());
                }
            }
            return new ResolvedScope(contextBase, null, names, collectionType,
                    cb.getId(), ids.size() == 1 ? ids.get(0) : null, ids);
        }
        return new ResolvedScope(contextBase, null, ImmutableList.of(), null, cb.getId(), null, ImmutableList.of());
    }
}
