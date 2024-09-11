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

package com.starrocks.sql.automv.generator;

import com.starrocks.common.Pair;
import com.starrocks.sql.automv.column.GenericColumn;
import com.starrocks.sql.optimizer.base.ColumnRefSet;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public final class QueryGenerateContext {
    private final boolean trace;
    private final boolean reserveConjuncts;
    private final boolean newTableAliasForTopPiece;
    private final List<Pair<Integer, GenericColumn>> outputColumns;
    private final ColumnRefSet inputColumnIds;
    private List<QueryGenerateResult> inputResults;

    private QueryGenerateContext(boolean trace, boolean reserveConjuncts, boolean newTableAliasForTopPiece,
                                 List<Pair<Integer, GenericColumn>> outputColumns, ColumnRefSet inputColumnIds) {
        this.trace = trace;
        this.reserveConjuncts = reserveConjuncts;
        this.newTableAliasForTopPiece = newTableAliasForTopPiece;
        this.outputColumns = outputColumns;
        this.inputColumnIds = inputColumnIds;
    }

    public static QueryGenerateContext of(
            boolean trace,
            boolean reserveConjuncts) {
        return new QueryGenerateContext(trace, reserveConjuncts, true, Collections.emptyList(), ColumnRefSet.of());
    }

    public static QueryGenerateContext ofNoTopAlias() {
        return new QueryGenerateContext(false, false, false, Collections.emptyList(), ColumnRefSet.of());
    }

    public static QueryGenerateContext of(
            boolean trace,
            boolean reserveConjuncts,
            List<Pair<Integer, GenericColumn>> outputColumns,
            ColumnRefSet inputColumnIds) {
        return new QueryGenerateContext(trace, reserveConjuncts, true, outputColumns, inputColumnIds);
    }

    public boolean isNewTableAliasForTopPiece() {
        return newTableAliasForTopPiece;
    }

    public QueryGenerateContext derive(List<Pair<Integer, GenericColumn>> outputColumns, ColumnRefSet inputColumnIds) {
        return new QueryGenerateContext(trace, reserveConjuncts, newTableAliasForTopPiece, outputColumns,
                inputColumnIds);
    }

    public boolean isTrace() {
        return trace;
    }

    public boolean isReserveConjuncts() {
        return reserveConjuncts;
    }

    public ColumnRefSet getInputColumnIds() {
        return Objects.requireNonNull(inputColumnIds);
    }

    public List<Pair<Integer, GenericColumn>> getOutputColumns() {
        return Objects.requireNonNull(outputColumns);
    }

    public List<QueryGenerateResult> getInputResults() {
        return Objects.requireNonNull(inputResults);
    }

    public void setInputResults(List<QueryGenerateResult> inputResults) {
        this.inputResults = Objects.requireNonNull(inputResults);
    }
}
