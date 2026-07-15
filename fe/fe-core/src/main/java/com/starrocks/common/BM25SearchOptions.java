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

package com.starrocks.common;

import com.starrocks.thrift.TBM25ColumnQuery;
import com.starrocks.thrift.TBM25SearchOptions;

import java.util.ArrayList;
import java.util.List;

/**
 * Per-query state for a builtin GIN BM25 full-text top-N ranking scan, produced by
 * {@code RewriteToBM25PlanRule} and threaded down to {@code OlapScanNode.toThrift}.
 *
 * <p>Lives only on the scan operator/plan node and is serialized to {@link TBM25SearchOptions}. v1 holds
 * exactly one column-query (a single MATCH column); the list shape leaves room for multi-column MATCH.
 */
public class BM25SearchOptions {
    /**
     * Name of the synthetic column carrying the per-row BM25 relevance score. {@code RewriteToBM25PlanRule}
     * injects it into the scan operator's colRef maps (never {@code Table.addColumn}) in place of score(),
     * and the BE produces it. Kept here as the single source of truth for the sentinel name.
     */
    public static final String SCORE_COLUMN_NAME = "__bm25_score";

    /** A single (indexed column, query string) unit -- the natural unit for multi-column MATCH. */
    public static class Bm25ColumnQuery {
        // Stable column identity (Column.columnId), unchanged by rename; the BE resolves it to the GIN
        // index exactly as it resolves a MATCH predicate's column. Never the FE unique id or display name.
        private final String columnId;
        private final String query;

        public Bm25ColumnQuery(String columnId, String query) {
            this.columnId = columnId;
            this.query = query;
        }

        public String getColumnId() {
            return columnId;
        }

        public String getQuery() {
            return query;
        }
    }

    private boolean enable = false;
    private List<Bm25ColumnQuery> columns = new ArrayList<>();
    private String scoreColumnName = "";
    private int scoreSlotId = 0;
    private double k1 = 1.2;
    private double b = 0.75;
    // Top-k pushed into the scored GIN scan (= LIMIT + OFFSET), so the BE keeps only the top-k rows by
    // score instead of scoring/returning every matched row. 0 means no pushdown (score all matched rows;
    // the TopN operator above applies the limit). Only set when it is safe -- see RewriteToBM25PlanRule.
    private long topk = 0;

    public boolean isEnable() {
        return enable;
    }

    public void setEnable(boolean enable) {
        this.enable = enable;
    }

    public void setColumns(List<Bm25ColumnQuery> columns) {
        this.columns = columns;
    }

    public void setScoreColumnName(String scoreColumnName) {
        this.scoreColumnName = scoreColumnName;
    }

    public void setScoreSlotId(int scoreSlotId) {
        this.scoreSlotId = scoreSlotId;
    }

    public void setK1(double k1) {
        this.k1 = k1;
    }

    public void setB(double b) {
        this.b = b;
    }

    public void setTopk(long topk) {
        this.topk = topk;
    }

    public TBM25SearchOptions toThrift() {
        TBM25SearchOptions opts = new TBM25SearchOptions();
        opts.setEnable(enable);
        List<TBM25ColumnQuery> tColumns = new ArrayList<>();
        for (Bm25ColumnQuery column : columns) {
            TBM25ColumnQuery tColumn = new TBM25ColumnQuery();
            tColumn.setColumn_id(column.getColumnId());
            tColumn.setQuery(column.getQuery());
            tColumns.add(tColumn);
        }
        opts.setColumns(tColumns);
        opts.setScore_column_name(scoreColumnName);
        opts.setScore_slot_id(scoreSlotId);
        opts.setK1(k1);
        opts.setB(b);
        opts.setTopk(topk);
        return opts;
    }

    public String getExplainString(String prefix) {
        StringBuilder columnsStr = new StringBuilder();
        for (Bm25ColumnQuery column : columns) {
            if (columnsStr.length() > 0) {
                columnsStr.append(", ");
            }
            columnsStr.append("<").append(column.getColumnId()).append(":")
                    .append(column.getQuery()).append(">");
        }
        return prefix + "BM25 SCORE: " +
                "Query: [" + columnsStr + "], " +
                "Score Column: <" + scoreSlotId + ":" + scoreColumnName + ">, " +
                "K1: " + k1 + ", " +
                "B: " + b + ", " +
                "TopK: " + topk +
                "\n";
    }
}
