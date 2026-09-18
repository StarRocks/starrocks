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

package com.starrocks.sql.optimizer.rewrite;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.JDBCTable;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.type.PrimitiveType;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Which PostgreSQL columns an ordering or comparison may be pushed down for, and under which
 * collation.
 *
 * <p>StarRocks compares strings byte by byte while PostgreSQL compares them under the column's
 * collation, so a database created with e.g. en_US.UTF-8 orders 'B' before 'a'. A pushed comparison
 * therefore has to name {@code COLLATE "C"} -- PostgreSQL's byte order -- or the remote answer
 * differs from the one StarRocks would have produced locally.
 *
 * <p>Only the types PostgreSQL actually collates may carry it: {@code COLLATE "C"} on a numeric or
 * a date is an error rather than a differently ordered result, and several PostgreSQL types map to
 * StarRocks VARCHAR without being collatable remotely (an unconstrained {@code numeric}, an enum,
 * {@code json}). The source type name is therefore required, and a column without that metadata
 * fails closed.
 *
 * <p>{@code bpchar} is deliberately absent even though PostgreSQL collates it: it maps to StarRocks
 * CHAR, and the two disagree on trailing spaces ({@code 'ab'} and {@code 'ab '} compare equal on
 * PostgreSQL and unequal on StarRocks) — a difference no collation can reconcile.
 */
public final class PostgresCollation {

    /** Rendered after a collatable operand; the leading space keeps call sites free of spacing. */
    public static final String COLLATE_C = " COLLATE \"C\"";

    private PostgresCollation() {
    }

    /**
     * The subset of {@code colRefToColumnMetaMap}'s columns whose remote comparison order can be
     * made to match StarRocks' by sorting them under {@code COLLATE "C"}. Empty for every dialect
     * other than PostgreSQL, so callers can pass the result unconditionally.
     */
    public static Set<ColumnRefOperator> collatableColumns(JDBCTable table,
                                                           Map<ColumnRefOperator, Column> colRefToColumnMetaMap) {
        if (table == null || table.getProtocolType() != JDBCTable.ProtocolType.POSTGRES
                || colRefToColumnMetaMap == null) {
            return Collections.emptySet();
        }
        Set<ColumnRefOperator> collatable = new HashSet<>();
        for (Map.Entry<ColumnRefOperator, Column> entry : colRefToColumnMetaMap.entrySet()) {
            if (isCollatable(table, entry.getValue())) {
                collatable.add(entry.getKey());
            }
        }
        return collatable;
    }

    /**
     * Whether a pushed comparison over {@code column} can be rendered under {@code COLLATE "C"}:
     * StarRocks maps it to VARCHAR and PostgreSQL reports it as one of its collatable string types.
     */
    public static boolean isCollatable(JDBCTable table, Column column) {
        if (column == null || column.getType().getPrimitiveType() != PrimitiveType.VARCHAR) {
            return false;
        }
        return isSourceType(table.getOriginalJdbcColumnTypeNames().get(column.getName()),
                "text", "varchar", "character varying");
    }

    /**
     * Whether the remote type name recorded for a column is one of {@code accepted}. Missing
     * metadata (a table whose schema predates it, or a derived column such as a pushed aggregate
     * result) is not one of them, so every caller fails closed.
     */
    public static boolean isSourceType(String typeName, String... accepted) {
        if (typeName == null) {
            return false;
        }
        for (String candidate : accepted) {
            if (candidate.equalsIgnoreCase(typeName)) {
                return true;
            }
        }
        return false;
    }
}
