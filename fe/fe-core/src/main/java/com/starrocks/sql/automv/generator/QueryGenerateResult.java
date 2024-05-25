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
import com.starrocks.sql.automv.column.ColumnAlias;
import com.starrocks.sql.automv.column.GenericColumn;
import com.starrocks.sql.automv.util.PrettyPrinter;
import com.starrocks.sql.automv.util.TieredMap;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nullable;

public class QueryGenerateResult {
    private final PrettyPrinter subquery;
    @Nullable
    private final String tableAlias;
    private final TieredMap<Integer, ColumnAlias> columnAliases;
    private transient String mvName;
    private transient List<Pair<Integer, GenericColumn>> orderedDimensions;
    private transient List<Pair<Integer, GenericColumn>> orderedColumns;

    private transient PrettyPrinter traceLog;

    private QueryGenerateResult(PrettyPrinter subQuery, @Nullable String tableAlias,
                                TieredMap<Integer, ColumnAlias> columnAliases) {
        this.subquery = Objects.requireNonNull(subQuery);
        this.tableAlias = tableAlias;
        this.columnAliases = Objects.requireNonNull(columnAliases);
    }

    public static QueryGenerateResult of(PrettyPrinter subQuery, String tableAlias,
                                         TieredMap<Integer, ColumnAlias> columnAliases) {
        return new QueryGenerateResult(subQuery, tableAlias, columnAliases);
    }

    public PrettyPrinter getSubquery() {
        return subquery;
    }

    @Nullable
    public String getTableAlias() {
        return tableAlias;
    }

    public TieredMap<Integer, ColumnAlias> getColumnAliases() {
        return columnAliases;
    }

    public QueryGenerateResult updateColumnAliases(TieredMap<Integer, ColumnAlias> newColumnAliases) {
        return QueryGenerateResult.of(subquery, tableAlias, newColumnAliases);
    }

    public QueryGenerateResult updateSubquery(PrettyPrinter subquery) {
        return QueryGenerateResult.of(subquery, tableAlias, columnAliases);
    }

    public String getMvName() {
        return mvName;
    }

    public QueryGenerateResult setMvName(String mvName) {
        this.mvName = mvName;
        return this;
    }

    public List<Pair<Integer, GenericColumn>> getOrderedDimensions() {
        return Objects.requireNonNull(orderedDimensions);
    }

    public QueryGenerateResult setOrderedDimensions(
            List<Pair<Integer, GenericColumn>> orderedColumns) {
        this.orderedDimensions = Objects.requireNonNull(orderedColumns);
        return this;
    }

    public List<Pair<Integer, GenericColumn>> getOrderedColumns() {
        return Objects.requireNonNull(orderedColumns);
    }

    public QueryGenerateResult setOrderedColumns(
            List<Pair<Integer, GenericColumn>> orderedColumns) {
        this.orderedColumns = Objects.requireNonNull(orderedColumns);
        return this;
    }

    public PrettyPrinter toSql() {
        if (tableAlias != null) {
            PrettyPrinter newSubquery = new PrettyPrinter();
            newSubquery.add("(").newLine();
            newSubquery.indentEnclose(() -> {
                newSubquery.addSuperStepWithIndent(subquery);
            });
            newSubquery.newLine().add(")").spaces(1).add(tableAlias);
            return newSubquery;
        } else {
            return subquery;
        }
    }

    public Optional<PrettyPrinter> getTraceLog() {
        return Optional.ofNullable(traceLog);
    }

    public QueryGenerateResult setTraceLog(PrettyPrinter traceLog) {
        this.traceLog = traceLog;
        return this;
    }
}