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
import java.util.function.BiPredicate;

/**
 * Which PostgreSQL columns a comparison may be pushed down for, and which of them need
 * {@code COLLATE "C"} to say so.
 *
 * <p>StarRocks compares strings byte by byte while PostgreSQL compares them under the column's
 * collation, so a database created with e.g. en_US.UTF-8 orders 'B' before 'a'. A pushed ordering
 * comparison therefore has to agree with StarRocks about the order, or the remote answer differs
 * from the one StarRocks would have produced locally.
 *
 * <p><b>Three questions, not one.</b> Each caller asks a different one, and the answers are
 * deliberately different sets:
 *
 * <ul>
 *   <li>{@link #collatableColumns} -- the renderer's. A column here gets {@code COLLATE "C"}
 *       appended. Only the types PostgreSQL actually collates may be in it: {@code COLLATE "C"} on
 *       a type that has no collation is an outright error, not a differently ordered result.</li>
 *   <li>{@link #orderSafeColumns} -- the push-down gates'. A superset: a column here compares in
 *       the same order on both sides, whether that takes an explicit collation or not.</li>
 *   <li>{@link #minMaxPushableColumns} -- the aggregate rule's, for a MIN/MAX argument. Ordering
 *       safety is necessary but not sufficient here, because PostgreSQL has to have the aggregate
 *       for the type at all.</li>
 * </ul>
 *
 * <p>Until PostgreSQL {@code uuid} started mapping to VARCHAR all three coincided, which is why one
 * set served every caller. {@code uuid} is the first type that is order-safe without being
 * collatable: its canonical text is fixed-position hyphens plus lowercase hex, and in ASCII
 * {@code '0'-'9'} sorts below {@code 'a'-'f'}, so comparing that text byte by byte is the same
 * order PostgreSQL compares the 16 stored bytes in. Measured on PostgreSQL 16.15 over 200,003 rows:
 * ordering by the uuid column and ordering by its canonical text under C collation put every row in
 * the same position, and a range comparison and an {@code ORDER BY} over it both use the column's
 * btree index. Naming a collation for it is not merely unnecessary but fatal -- PostgreSQL answers
 * {@code collations are not supported by type uuid} and the whole query fails.
 *
 * <p>The same measurement is also why {@code uuid} is the first type the third set has to exclude
 * while the second admits it: PostgreSQL has a btree opclass for {@code uuid} but no {@code min} or
 * {@code max} aggregate over it, so the ordering that makes a comparison safe does not make an
 * aggregate possible.
 *
 * <p>Every other type StarRocks maps to VARCHAR still has to prove itself by source type name, and
 * a column without that metadata fails closed.
 *
 * <p>{@code bpchar} is deliberately absent from all three even though PostgreSQL both collates and
 * aggregates it: it maps to StarRocks CHAR, and the two disagree on trailing spaces ({@code 'ab'}
 * and {@code 'ab '} compare equal on PostgreSQL and unequal on StarRocks) — a difference no
 * collation can reconcile.
 */
public final class PostgresCollation {

    /** Rendered after a collatable operand; the leading space keeps call sites free of spacing. */
    public static final String COLLATE_C = " COLLATE \"C\"";

    private PostgresCollation() {
    }

    /**
     * The columns the <em>renderer</em> may append {@code COLLATE "C"} to. Empty for every dialect
     * other than PostgreSQL, so callers can pass the result unconditionally.
     *
     * <p>Pass this to a renderer, never to a push-down gate: a gate fed this set turns away the
     * order-safe-but-uncollatable columns {@link #orderSafeColumns} exists for.
     */
    public static Set<ColumnRefOperator> collatableColumns(JDBCTable table,
                                                           Map<ColumnRefOperator, Column> colRefToColumnMetaMap) {
        return select(table, colRefToColumnMetaMap, PostgresCollation::isCollatable);
    }

    /**
     * The columns a pushed ordering comparison ({@code < <= > >=}, {@code BETWEEN}) is allowed to
     * name at all -- a superset of {@link #collatableColumns}, adding the columns whose remote
     * order already matches StarRocks' without a collation.
     *
     * <p>Pass this to a push-down gate, never to a renderer.
     */
    public static Set<ColumnRefOperator> orderSafeColumns(JDBCTable table,
                                                          Map<ColumnRefOperator, Column> colRefToColumnMetaMap) {
        return select(table, colRefToColumnMetaMap, PostgresCollation::isOrderSafe);
    }

    /**
     * The columns a pushed {@code MIN}/{@code MAX} may take as its argument.
     *
     * <p>This is a third question, not a rephrasing of the other two, and it is the narrower one.
     * PostgreSQL catalogues {@code min}/{@code max} per type rather than deriving them from the
     * type's btree ordering, and {@code uuid} is a type it never got them for: {@code SELECT
     * min(uuid_col)} fails with {@code function min(uuid) does not exist} even though {@code ORDER
     * BY uuid_col} and {@code uuid_col < '...'} both work and both use the column's index. So a
     * uuid column is in {@link #orderSafeColumns} and must stay out of this set -- passing the
     * wider set here renders a statement the database rejects outright.
     *
     * <p>That leaves exactly the collatable types, which PostgreSQL does aggregate and whose
     * extreme it takes under the argument's collation -- the collation the renderer pins with
     * {@code COLLATE "C"}, which is why the two sets coincide today. They coincide by arithmetic,
     * not by definition: a future type that PostgreSQL aggregates without collating would belong
     * here and not in {@link #collatableColumns}.
     */
    public static Set<ColumnRefOperator> minMaxPushableColumns(JDBCTable table,
                                                               Map<ColumnRefOperator, Column> colRefToColumnMetaMap) {
        return select(table, colRefToColumnMetaMap, PostgresCollation::isCollatable);
    }

    private static Set<ColumnRefOperator> select(JDBCTable table,
                                                 Map<ColumnRefOperator, Column> colRefToColumnMetaMap,
                                                 BiPredicate<JDBCTable, Column> admits) {
        if (table == null || table.getProtocolType() != JDBCTable.ProtocolType.POSTGRES
                || colRefToColumnMetaMap == null) {
            return Collections.emptySet();
        }
        Set<ColumnRefOperator> selected = new HashSet<>();
        for (Map.Entry<ColumnRefOperator, Column> entry : colRefToColumnMetaMap.entrySet()) {
            if (admits.test(table, entry.getValue())) {
                selected.add(entry.getKey());
            }
        }
        return selected;
    }

    /**
     * Whether a pushed comparison over {@code column} can be rendered under {@code COLLATE "C"}:
     * StarRocks maps it to VARCHAR and PostgreSQL reports it as one of its collatable string types.
     */
    public static boolean isCollatable(JDBCTable table, Column column) {
        return isVarchar(column) && isSourceType(remoteTypeName(table, column),
                "text", "varchar", "character varying");
    }

    /**
     * Whether a pushed ordering comparison over {@code column} returns what StarRocks would have
     * returned locally -- either because the renderer will name {@code COLLATE "C"}
     * ({@link #isCollatable}), or because the remote type's own order already agrees.
     *
     * <p>{@code uuid} is the second case, and currently the only one. See the class comment for the
     * measurement; the short form is that the canonical text and the 16 stored bytes sort alike, and
     * {@code COLLATE "C"} on a uuid column is an error rather than a no-op, so it must be admitted
     * here and refused by {@link #isCollatable}.
     */
    public static boolean isOrderSafe(JDBCTable table, Column column) {
        return isCollatable(table, column)
                || (isVarchar(column) && isSourceType(remoteTypeName(table, column), "uuid"));
    }

    private static boolean isVarchar(Column column) {
        return column != null && column.getType().getPrimitiveType() == PrimitiveType.VARCHAR;
    }

    private static String remoteTypeName(JDBCTable table, Column column) {
        return table.getOriginalJdbcColumnTypeNames().get(column.getName());
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
