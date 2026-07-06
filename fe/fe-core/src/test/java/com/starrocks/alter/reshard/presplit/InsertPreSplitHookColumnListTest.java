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

package com.starrocks.alter.reshard.presplit;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.OlapTable;
import com.starrocks.sql.ast.InsertStmt;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the two target-column-list gates in {@link InsertPreSplitHook}:
 * <ul>
 *   <li>{@code targetColumnListIsPreSplitSafe} — the source-agnostic gate: an
 *       explicit list must be a valid INSERT permutation (no unknown/duplicate/
 *       generated names, every required column present) and contain every sort
 *       key, otherwise pre-split is skipped.</li>
 *   <li>{@code targetColumnListIsFullIdentity} — the stricter gate used by the
 *       INSERT-from-table source: the list must be the full base schema in order.</li>
 * </ul>
 */
public class InsertPreSplitHookColumnListTest {

    private static List<Column> requiredColumns(String... names) {
        // PresplitTestSupport.bigintColumn is NOT NULL with no default => required.
        return Arrays.stream(names).map(PresplitTestSupport::bigintColumn).collect(Collectors.toList());
    }

    private static OlapTable tableWithBaseColumns(String... names) {
        return tableWithSchema(requiredColumns(names));
    }

    private static OlapTable tableWithSchema(List<Column> baseColumns) {
        OlapTable table = mock(OlapTable.class);
        when(table.getBaseSchema()).thenReturn(baseColumns);
        when(table.getBaseSchemaWithoutGeneratedColumn()).thenReturn(baseColumns);
        return table;
    }

    private static InsertStmt insertWithTargetColumns(List<String> targetColumnNames) {
        InsertStmt stmt = mock(InsertStmt.class);
        when(stmt.getTargetColumnNames()).thenReturn(targetColumnNames);
        return stmt;
    }

    // ---- targetColumnListIsPreSplitSafe (source-agnostic gate) ----

    @Test
    public void noColumnListIsSafe() {
        Assertions.assertTrue(InsertPreSplitHook.targetColumnListIsPreSplitSafe(
                insertWithTargetColumns(null), tableWithBaseColumns("k", "v"), requiredColumns("k")));
    }

    @Test
    public void emptyColumnListIsSafe() {
        // A mocked InsertStmt yields an empty (not null) list; treat it like a bare INSERT.
        Assertions.assertTrue(InsertPreSplitHook.targetColumnListIsPreSplitSafe(
                insertWithTargetColumns(List.of()), tableWithBaseColumns("k", "v"), requiredColumns("k")));
    }

    @Test
    public void fullValidListWithAllSortKeysIsSafe() {
        Assertions.assertTrue(InsertPreSplitHook.targetColumnListIsPreSplitSafe(
                insertWithTargetColumns(List.of("k", "v")), tableWithBaseColumns("k", "v"), requiredColumns("k")));
    }

    @Test
    public void partialListOmittingOnlyNullableNonKeyIsSafe() {
        // base: k (required, sort key), v (nullable). Omitting v is allowed.
        List<Column> base = List.of(PresplitTestSupport.bigintColumn("k"),
                PresplitTestSupport.nullableBigintColumn("v"));
        Assertions.assertTrue(InsertPreSplitHook.targetColumnListIsPreSplitSafe(
                insertWithTargetColumns(List.of("k")), tableWithSchema(base), requiredColumns("k")));
    }

    @Test
    public void partialListMissingRequiredColumnIsUnsafe() {
        // base: k (sort key), v (required, no default, not null). Omitting v would be
        // rejected by InsertAnalyzer, so pre-split must skip rather than reshard.
        Assertions.assertFalse(InsertPreSplitHook.targetColumnListIsPreSplitSafe(
                insertWithTargetColumns(List.of("k")), tableWithBaseColumns("k", "v"), requiredColumns("k")));
    }

    @Test
    public void listMissingASortKeyIsUnsafe() {
        // base: k (nullable, sort key), v (nullable). The list covers required columns
        // (none) but omits the sort key k -> degenerate split.
        List<Column> base = List.of(PresplitTestSupport.nullableBigintColumn("k"),
                PresplitTestSupport.nullableBigintColumn("v"));
        Assertions.assertFalse(InsertPreSplitHook.targetColumnListIsPreSplitSafe(
                insertWithTargetColumns(List.of("v")), tableWithSchema(base),
                List.of(PresplitTestSupport.nullableBigintColumn("k"))));
    }

    @Test
    public void unknownColumnInListIsUnsafe() {
        Assertions.assertFalse(InsertPreSplitHook.targetColumnListIsPreSplitSafe(
                insertWithTargetColumns(List.of("k", "typo")), tableWithBaseColumns("k"), requiredColumns("k")));
    }

    @Test
    public void duplicateColumnInListIsUnsafe() {
        Assertions.assertFalse(InsertPreSplitHook.targetColumnListIsPreSplitSafe(
                insertWithTargetColumns(List.of("k", "k")), tableWithBaseColumns("k"), requiredColumns("k")));
    }

    @Test
    public void sortKeyMatchIsCaseInsensitive() {
        Assertions.assertTrue(InsertPreSplitHook.targetColumnListIsPreSplitSafe(
                insertWithTargetColumns(List.of("K", "V")), tableWithBaseColumns("k", "v"), requiredColumns("k")));
    }

    // ---- targetColumnListIsFullIdentity (INSERT-from-table gate) ----

    @Test
    public void noColumnListIsFullIdentity() {
        Assertions.assertTrue(InsertPreSplitHook.targetColumnListIsFullIdentity(
                insertWithTargetColumns(null), tableWithBaseColumns("a", "b", "c")));
    }

    @Test
    public void fullInOrderListIsIdentity() {
        Assertions.assertTrue(InsertPreSplitHook.targetColumnListIsFullIdentity(
                insertWithTargetColumns(List.of("a", "b", "c")), tableWithBaseColumns("a", "b", "c")));
    }

    @Test
    public void identityIsCaseInsensitive() {
        Assertions.assertTrue(InsertPreSplitHook.targetColumnListIsFullIdentity(
                insertWithTargetColumns(List.of("A", "B", "C")), tableWithBaseColumns("a", "b", "c")));
    }

    @Test
    public void reorderedListIsNotIdentity() {
        Assertions.assertFalse(InsertPreSplitHook.targetColumnListIsFullIdentity(
                insertWithTargetColumns(List.of("a", "c", "b")), tableWithBaseColumns("a", "b", "c")));
    }

    @Test
    public void partialListIsNotIdentity() {
        Assertions.assertFalse(InsertPreSplitHook.targetColumnListIsFullIdentity(
                insertWithTargetColumns(List.of("a", "b")), tableWithBaseColumns("a", "b", "c")));
    }

    @Test
    public void supersetListIsNotIdentity() {
        Assertions.assertFalse(InsertPreSplitHook.targetColumnListIsFullIdentity(
                insertWithTargetColumns(List.of("a", "b", "c", "d")), tableWithBaseColumns("a", "b", "c")));
    }
}
