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

import com.google.common.collect.Maps;
import com.starrocks.catalog.View;
import com.starrocks.sql.ast.QueryStatement;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import java.util.Objects;

/**
 * View bodies parsed before the meta lock was taken, with the external tables inside them already resolved.
 *
 * <p><b>Why this exists.</b> The unlocked pre-pass that keeps connector metadata off the lock critical path
 * ({@code QueryAnalyzer#analyzeExternalTablesOnly}) only sees the relations written in the statement. A view
 * is one opaque name there; the tables it reads only appear once the locked analyzer expands it, so a view
 * defined over an external catalog used to fetch its base tables from the connector with the lock held --
 * and any statement mixing an internal table with such a view hits that.
 *
 * <p>Handing the body over rather than just warming a cache is deliberate: {@link View#getQueryStatement()}
 * re-parses {@code inlineViewDef} on every call, so a body resolved in the pre-pass is a different AST from
 * the one expansion would build, and nothing about the pre-pass would reach it.
 *
 * <p><b>The "still current" check.</b> A body captured without the lock can go stale -- ALTER VIEW rewrites
 * {@code inlineViewDef} in place. {@link #take} therefore hands a body back only when the view's definition
 * and SQL mode still match the ones captured with it, and expansion re-parses otherwise. Bodies are handed
 * out at most once each, because the analyzer rewrites the AST it expands and two references to one view
 * must not share it; the pre-pass records one entry per reference it walked, so a view named twice in a
 * statement has two.
 */
public class PreResolvedViewBodies {
    private record Entry(String inlineViewDef, long sqlMode, QueryStatement body) {
    }

    private final Map<Long, Deque<Entry>> byViewId = Maps.newHashMap();

    public void put(View view, QueryStatement body) {
        byViewId.computeIfAbsent(view.getId(), id -> new ArrayDeque<>())
                .addLast(new Entry(view.getInlineViewDef(), view.getSqlMode(), body));
    }

    /**
     * @return a body pre-resolved for this view, or null when there is none left or the view changed since
     */
    public QueryStatement take(View view) {
        Deque<Entry> entries = byViewId.get(view.getId());
        if (entries == null || entries.isEmpty()) {
            return null;
        }
        Entry entry = entries.peekFirst();
        if (entry.sqlMode() != view.getSqlMode()
                || !Objects.equals(entry.inlineViewDef(), view.getInlineViewDef())) {
            // Redefined between the pre-pass and now. Drop everything captured for this view: every entry
            // carries the same stale definition.
            byViewId.remove(view.getId());
            return null;
        }
        entries.pollFirst();
        return entry.body();
    }

    public boolean isEmpty() {
        return byViewId.isEmpty();
    }

    public void clear() {
        byViewId.clear();
    }
}
