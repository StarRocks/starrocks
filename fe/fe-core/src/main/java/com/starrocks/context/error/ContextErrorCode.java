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

package com.starrocks.context.error;

/**
 * Stable error codes for the semantic-context module per arch doc §16.2 / API doc §12.
 *
 * <p>The codes group into four classes (parameter / semantic / resource / consistency) so callers
 * can build retry policies without parsing free-form text. Each code carries default values for
 * {@code retryable} (whether a blind retry is sane) and a {@code degradeSuggestion} (the canonical
 * mitigation hint shown to the operator). REST actions can override either when the situation
 * warrants context-specific advice.
 */
public enum ContextErrorCode {

    // -------- Parameter errors --------
    INVALID_ARGUMENT("parameter", false, "verify argument names and types"),
    INVALID_SCOPE("parameter", false, "supply contextbase / collection / collection_type / contextbase.* — pick one"),
    INVALID_COLLECTION_TYPE("parameter", false,
            "use one of knowledge / skill / memory / task_summary / channel"),
    INVALID_ENTITY_TYPE("parameter", false,
            "verify the entity_type is allowed by the collection's collection_type matrix"),
    INVALID_ENTITY_KEY("parameter", false,
            "entity_key must contain at least one non-digit character so [[e:<key>]] does not collide with [[e:<id>]]"),

    // -------- Semantic / not-found --------
    ENTITY_NOT_FOUND("semantic", false, "verify id / entity_key against SHOW COLLECTIONS"),
    WORKSPACE_EXPIRED("semantic", false, "start a fresh workspace via POST /api/workspaces/start"),

    // -------- Resource errors --------
    TOKEN_BUDGET_EXCEEDED("resource", false,
            "raise max_tokens, narrow entity_ids, or switch to disclosure level=preview"),
    FRONTIER_LIMIT_EXCEEDED("resource", true,
            "raise max_frontier, narrow seed_ids, or unset require_complete on graph_expand"),

    // -------- System / consistency errors --------
    REFERENCE_INDEX_NOT_READY("consistency", true,
            "wait for context_entity_refs to settle, or set graph_mode=OFF"),
    VECTOR_NOT_READY("consistency", true,
            "configure a DEFAULT EMBEDDING PROVIDER, or pass query_embedding directly"),

    // -------- Internal / opaque errors --------
    // Generic bucket for unexpected server-side failures. The error response carries only this
    // code and a generic message so internal stack-trace text and SQL fragments are not leaked
    // to unauthenticated callers; the real failure is logged server-side at WARN.
    INTERNAL_ERROR("internal", true,
            "retry the request; if it persists, check the FE log for the matching warning"),

    // -------- Authorization errors --------
    // Raised when a caller lacks USAGE on a semantic-context base they tried to read through a
    // TVF (context_get, text_search, etc.) or REST endpoint. Distinct from INVALID_ARGUMENT so
    // clients can distinguish "you spelled the contextbase wrong" from "the contextbase exists
    // but you are not allowed to see it".
    ACCESS_DENIED("authorization", false,
            "grant USAGE on the target contextbase or run as an OPERATE/SECURITY admin");

    private final String errorClass;
    private final boolean retryable;
    private final String degradeSuggestion;

    ContextErrorCode(String errorClass, boolean retryable, String degradeSuggestion) {
        this.errorClass = errorClass;
        this.retryable = retryable;
        this.degradeSuggestion = degradeSuggestion;
    }

    public String errorClass() {
        return errorClass;
    }

    public boolean retryable() {
        return retryable;
    }

    public String degradeSuggestion() {
        return degradeSuggestion;
    }
}
