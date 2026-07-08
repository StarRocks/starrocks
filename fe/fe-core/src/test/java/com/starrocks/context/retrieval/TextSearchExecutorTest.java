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
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Locks the SQL the MATCH/LIKE text-search path produces. Three routing modes:
 *   (a) single case-sensitive token → GIN {@code MATCH} push-down;
 *   (b) multi-token case-sensitive → per-token {@code LIKE} OR (MATCH can't be OR'd);
 *   (c) case-insensitive → {@code LOWER(...) LIKE}.
 * No query may reference {@code gin_term_postings}: the exact-BM25 postings path is deferred until
 * that BE table function exists.
 */
public class TextSearchExecutorTest {

    /** Captures the SQL sent to the SQL plane and returns no rows, so no live cluster is needed. */
    private static final class CapturingExecutor extends TextSearchExecutor {
        String lastSql;

        @Override
        JsonArray runQuery(String sql) {
            lastSql = sql;
            return new JsonArray();
        }
    }

    private static TextSearchExecutor.Request request(String pattern) {
        TextSearchExecutor.Request r = new TextSearchExecutor.Request();
        r.pattern = pattern;
        r.contextBaseId = 42L;
        return r;
    }

    @Test
    public void testSingleTokenUsesMatchPushdown() {
        CapturingExecutor exec = new CapturingExecutor();
        exec.search(request("baseline"));
        assertTrue(exec.lastSql.contains("fragment_text MATCH 'baseline'"),
                "single token should push down to the GIN index via MATCH: " + exec.lastSql);
        assertFalse(exec.lastSql.contains("gin_term_postings"),
                "the deferred postings TVF must not be referenced: " + exec.lastSql);
    }

    @Test
    public void testMultiTokenUsesLikeOr() {
        CapturingExecutor exec = new CapturingExecutor();
        exec.search(request("smb baseline"));
        assertTrue(exec.lastSql.contains("LOWER(fragment_text) LIKE '%smb%'"), "per-token LIKE: " + exec.lastSql);
        assertTrue(exec.lastSql.contains("LOWER(fragment_text) LIKE '%baseline%'"), "per-token LIKE: " + exec.lastSql);
        assertTrue(exec.lastSql.contains(" OR "), "tokens OR'd: " + exec.lastSql);
        assertFalse(exec.lastSql.contains("gin_term_postings"), "no deferred TVF: " + exec.lastSql);
    }

    @Test
    public void testCaseInsensitiveUsesLowerLike() {
        CapturingExecutor exec = new CapturingExecutor();
        TextSearchExecutor.Request req = request("Baseline");
        req.caseInsensitive = true;
        exec.search(req);
        assertTrue(exec.lastSql.contains("LOWER(fragment_text) LIKE '%baseline%'"),
                "case-insensitive should LIKE on LOWER(fragment_text): " + exec.lastSql);
        assertFalse(exec.lastSql.contains(" MATCH "), "case-insensitive must not MATCH: " + exec.lastSql);
    }

    @Test
    public void testBlankPatternReturnsEmptyWithoutQuery() {
        CapturingExecutor exec = new CapturingExecutor();
        assertTrue(exec.search(request("   ")).isEmpty(), "whitespace-only pattern returns no hits");
        // A blank query must short-circuit before building any SQL (never a broad LIKE '%%' scan).
        assertNull(exec.lastSql, "blank pattern must not issue a query");
    }

    @Test
    public void testLikeLiteralEscaping() {
        CapturingExecutor exec = new CapturingExecutor();
        TextSearchExecutor.Request req = request("o'brien 50%");
        req.caseInsensitive = true;
        exec.search(req);
        // Single quote doubled (no early literal termination).
        assertTrue(exec.lastSql.contains("o''brien"), "quote must be doubled: " + exec.lastSql);
        // A user '%' is escaped so it matches literally instead of acting as a LIKE wildcard: the
        // LIKE-level "\%" then survives ContextSqlEscape.body's backslash doubling as "\\%".
        assertTrue(exec.lastSql.contains("50\\\\%"), "user %% must be LIKE-escaped: " + exec.lastSql);
    }
}
