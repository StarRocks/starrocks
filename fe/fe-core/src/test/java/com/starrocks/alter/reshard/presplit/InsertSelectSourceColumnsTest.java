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
import com.starrocks.catalog.TableName;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.SelectList;
import com.starrocks.sql.ast.SelectListItem;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class InsertSelectSourceColumnsTest {

    // --- helpers ---

    private static Column col(String name) {
        return new Column(name, IntegerType.INT);
    }

    /** Build a mock InsertStmt that returns the given byName setting. */
    private static InsertStmt insertStmt(boolean byName) {
        InsertStmt stmt = mock(InsertStmt.class);
        when(stmt.isColumnMatchByName()).thenReturn(byName);
        return stmt;
    }

    /** Build a mock OlapTable stubbing getBaseSchemaWithoutGeneratedColumn and
     * getVisibleColumnsWithoutGeneratedColumn to return the given column lists,
     * and hasGeneratedColumn() to the given flag.  We intentionally do NOT stub
     * getColumn(String) to verify no code path falls through to it.
     */
    private static OlapTable olapTable(List<Column> baseCols, List<Column> visibleCols, boolean hasGenCol) {
        OlapTable t = mock(OlapTable.class);
        when(t.getBaseSchemaWithoutGeneratedColumn()).thenReturn(baseCols);
        when(t.getVisibleColumnsWithoutGeneratedColumn()).thenReturn(visibleCols);
        when(t.hasGeneratedColumn()).thenReturn(hasGenCol);
        return t;
    }

    /** Build a SELECT * relation. */
    private static SelectRelation starRelation() {
        SelectListItem starItem = mock(SelectListItem.class);
        when(starItem.isStar()).thenReturn(true);
        SelectList list = mock(SelectList.class);
        when(list.getItems()).thenReturn(Collections.singletonList(starItem));
        SelectRelation rel = mock(SelectRelation.class);
        when(rel.getSelectList()).thenReturn(list);
        return rel;
    }

    /** Build a bare-column select item (SlotRef, optional table qualifier, optional alias). */
    private static SelectListItem bareItem(String colName, TableName tblName, String alias) {
        SlotRef slot = mock(SlotRef.class);
        when(slot.getColName()).thenReturn(colName);
        when(slot.getTblName()).thenReturn(tblName);
        SelectListItem item = mock(SelectListItem.class);
        when(item.isStar()).thenReturn(false);
        when(item.getExpr()).thenReturn(slot);
        when(item.getAlias()).thenReturn(alias);
        return item;
    }

    private static SelectListItem bareItem(String colName) {
        return bareItem(colName, null, null);
    }

    private static SelectRelation bareRelation(SelectListItem... items) {
        SelectList list = mock(SelectList.class);
        when(list.getItems()).thenReturn(Arrays.asList(items));
        SelectRelation rel = mock(SelectRelation.class);
        when(rel.getSelectList()).thenReturn(list);
        return rel;
    }

    private static final TableName SRC_NAME = new TableName("cat", "db1", "src");

    // --- tests ---

    @Test
    public void starByPositionAlignedSchemaMapsIdentity() {
        // source [k, v]; target [k, v]; sortKey [k]; SELECT * by position -> ["k"], partition []
        List<Column> cols = Arrays.asList(col("k"), col("v"));
        OlapTable target = olapTable(cols, cols, false);
        OlapTable source = olapTable(cols, cols, false);

        InsertSelectSourceColumns result = InsertSelectSourceColumns.resolve(
                insertStmt(false), starRelation(),
                target, source, SRC_NAME, null,
                Collections.singletonList(col("k")),
                Collections.emptyList());

        Assertions.assertNotNull(result);
        Assertions.assertEquals(Collections.singletonList("k"), result.sortKeySourceColumnNames());
        Assertions.assertEquals(Collections.emptyList(), result.partitionSourceColumnNames());
    }

    @Test
    public void starByPositionMisalignedNameReturnsNull() {
        // source [x, v]; target [k, v]; position-0 name mismatch -> null
        List<Column> targetCols = Arrays.asList(col("k"), col("v"));
        List<Column> sourceCols = Arrays.asList(col("x"), col("v"));
        OlapTable target = olapTable(targetCols, targetCols, false);
        OlapTable source = olapTable(sourceCols, sourceCols, false);

        InsertSelectSourceColumns result = InsertSelectSourceColumns.resolve(
                insertStmt(false), starRelation(),
                target, source, SRC_NAME, null,
                Collections.singletonList(col("k")),
                Collections.emptyList());

        Assertions.assertNull(result);
    }

    @Test
    public void starByPositionDifferentArityReturnsNull() {
        // source [k, v, w]; target [k, v] -> size mismatch -> null
        List<Column> targetCols = Arrays.asList(col("k"), col("v"));
        List<Column> sourceCols = Arrays.asList(col("k"), col("v"), col("w"));
        OlapTable target = olapTable(targetCols, targetCols, false);
        OlapTable source = olapTable(sourceCols, sourceCols, false);

        InsertSelectSourceColumns result = InsertSelectSourceColumns.resolve(
                insertStmt(false), starRelation(),
                target, source, SRC_NAME, null,
                Collections.singletonList(col("k")),
                Collections.emptyList());

        Assertions.assertNull(result);
    }

    @Test
    public void starByNameMapsBySameName() {
        // INSERT BY NAME; SELECT *; target [k, v] present in source (any order) -> sortKey k -> "k"
        List<Column> targetCols = Arrays.asList(col("k"), col("v"));
        List<Column> sourceCols = Arrays.asList(col("v"), col("k")); // different order
        OlapTable target = olapTable(targetCols, targetCols, false);
        OlapTable source = olapTable(sourceCols, sourceCols, false);

        InsertSelectSourceColumns result = InsertSelectSourceColumns.resolve(
                insertStmt(true), starRelation(),
                target, source, SRC_NAME, null,
                Collections.singletonList(col("k")),
                Collections.emptyList());

        Assertions.assertNotNull(result);
        Assertions.assertEquals(Collections.singletonList("k"), result.sortKeySourceColumnNames());
    }

    @Test
    public void starByNameMissingSourceColumnReturnsNull() {
        // target "k" absent from source -> null
        List<Column> targetCols = Arrays.asList(col("k"), col("v"));
        List<Column> sourceCols = Collections.singletonList(col("v"));
        OlapTable target = olapTable(targetCols, targetCols, false);
        OlapTable source = olapTable(sourceCols, sourceCols, false);

        InsertSelectSourceColumns result = InsertSelectSourceColumns.resolve(
                insertStmt(true), starRelation(),
                target, source, SRC_NAME, null,
                Collections.singletonList(col("k")),
                Collections.emptyList());

        Assertions.assertNull(result);
    }

    @Test
    public void starByNameExtraSourceColumnReturnsNull() {
        // source [k, v, extra]; target [k, v]; INSERT BY NAME SELECT * -> set mismatch -> null
        List<Column> targetCols = Arrays.asList(col("k"), col("v"));
        List<Column> sourceCols = Arrays.asList(col("k"), col("v"), col("extra"));
        OlapTable target = olapTable(targetCols, targetCols, false);
        OlapTable source = olapTable(sourceCols, sourceCols, false);

        InsertSelectSourceColumns result = InsertSelectSourceColumns.resolve(
                insertStmt(true), starRelation(),
                target, source, SRC_NAME, null,
                Collections.singletonList(col("k")),
                Collections.emptyList());

        Assertions.assertNull(result);
    }

    @Test
    public void starWithSourceGeneratedColumnReturnsNull() {
        // sourceTable.hasGeneratedColumn()==true -> null
        List<Column> cols = Arrays.asList(col("k"), col("v"));
        OlapTable target = olapTable(cols, cols, false);
        OlapTable source = olapTable(cols, cols, true); // has generated column

        InsertSelectSourceColumns result = InsertSelectSourceColumns.resolve(
                insertStmt(false), starRelation(),
                target, source, SRC_NAME, null,
                Collections.singletonList(col("k")),
                Collections.emptyList());

        Assertions.assertNull(result);
    }

    @Test
    public void bareColumnsByPositionMapThroughTargetPosition() {
        // target [a, b]; sortKey [b]; SELECT b, a -> target[0]=a<-"b", target[1]=b<-"a"; sortKey b -> "a"
        List<Column> targetCols = Arrays.asList(col("a"), col("b"));
        List<Column> sourceCols = Arrays.asList(col("a"), col("b"));
        OlapTable target = olapTable(targetCols, targetCols, false);
        OlapTable source = olapTable(sourceCols, sourceCols, false);

        SelectRelation rel = bareRelation(bareItem("b"), bareItem("a"));

        InsertSelectSourceColumns result = InsertSelectSourceColumns.resolve(
                insertStmt(false), rel,
                target, source, SRC_NAME, null,
                Collections.singletonList(col("b")),
                Collections.emptyList());

        Assertions.assertNotNull(result);
        // target[1]=b <- output[1]="a"; sortKey b -> source "a"
        Assertions.assertEquals(Collections.singletonList("a"), result.sortKeySourceColumnNames());
    }

    @Test
    public void bareColumnsByNameMapBySelectOutputName() {
        // INSERT BY NAME; SELECT v AS k, k AS v; sortKey [k] -> output "k" -> source "v"
        List<Column> targetCols = Arrays.asList(col("k"), col("v"));
        List<Column> sourceCols = Arrays.asList(col("k"), col("v"));
        OlapTable target = olapTable(targetCols, targetCols, false);
        OlapTable source = olapTable(sourceCols, sourceCols, false);

        SelectRelation rel = bareRelation(
                bareItem("v", null, "k"),   // SELECT v AS k
                bareItem("k", null, "v"));  // SELECT k AS v

        InsertSelectSourceColumns result = InsertSelectSourceColumns.resolve(
                insertStmt(true), rel,
                target, source, SRC_NAME, null,
                Collections.singletonList(col("k")),
                Collections.emptyList());

        Assertions.assertNotNull(result);
        Assertions.assertEquals(Collections.singletonList("v"), result.sortKeySourceColumnNames());
    }

    @Test
    public void bareColumnForeignTableQualifierReturnsNull() {
        // SELECT other.k -> tbl != source -> null
        List<Column> cols = Arrays.asList(col("k"), col("v"));
        OlapTable target = olapTable(cols, cols, false);
        OlapTable source = olapTable(cols, cols, false);

        TableName otherTbl = new TableName(null, null, "other");
        SelectRelation rel = bareRelation(bareItem("k", otherTbl, null), bareItem("v", null, null));

        InsertSelectSourceColumns result = InsertSelectSourceColumns.resolve(
                insertStmt(false), rel,
                target, source, SRC_NAME, null,
                Collections.singletonList(col("k")),
                Collections.emptyList());

        Assertions.assertNull(result);
    }

    @Test
    public void bareColumnForeignDbQualifierReturnsNull() {
        // SELECT db2.src.k while source is db1.src -> db mismatch -> null
        List<Column> cols = Arrays.asList(col("k"), col("v"));
        OlapTable target = olapTable(cols, cols, false);
        OlapTable source = olapTable(cols, cols, false);

        TableName wrongDb = new TableName(null, "db2", "src");
        SelectRelation rel = bareRelation(bareItem("k", wrongDb, null), bareItem("v", null, null));

        InsertSelectSourceColumns result = InsertSelectSourceColumns.resolve(
                insertStmt(false), rel,
                target, source, SRC_NAME, null,
                Collections.singletonList(col("k")),
                Collections.emptyList());

        Assertions.assertNull(result);
    }

    @Test
    public void bareColumnSourceQualifierMatches() {
        // SELECT src.k -> tbl == source name -> ok
        List<Column> cols = Arrays.asList(col("k"), col("v"));
        OlapTable target = olapTable(cols, cols, false);
        OlapTable source = olapTable(cols, cols, false);

        TableName srcTbl = new TableName(null, null, "src");
        SelectRelation rel = bareRelation(bareItem("k", srcTbl, null), bareItem("v", null, null));

        InsertSelectSourceColumns result = InsertSelectSourceColumns.resolve(
                insertStmt(false), rel,
                target, source, SRC_NAME, null,
                Collections.singletonList(col("k")),
                Collections.emptyList());

        Assertions.assertNotNull(result);
        Assertions.assertEquals(Collections.singletonList("k"), result.sortKeySourceColumnNames());
    }

    @Test
    public void bareColumnVirtualNameReturnsNull() {
        // colName absent from physical map (virtual-only) -> null
        List<Column> targetCols = Arrays.asList(col("k"), col("v"));
        List<Column> sourceCols = Arrays.asList(col("k"), col("v")); // "ghost" not present
        OlapTable target = olapTable(targetCols, targetCols, false);
        OlapTable source = olapTable(sourceCols, sourceCols, false);

        SelectRelation rel = bareRelation(bareItem("ghost"), bareItem("v"));

        InsertSelectSourceColumns result = InsertSelectSourceColumns.resolve(
                insertStmt(false), rel,
                target, source, SRC_NAME, null,
                Collections.singletonList(col("k")),
                Collections.emptyList());

        Assertions.assertNull(result);
    }

    @Test
    public void bareColumnByPositionLengthMismatchReturnsNull() {
        // outputs.size != targetCols.size -> null
        List<Column> targetCols = Arrays.asList(col("k"), col("v"));
        List<Column> sourceCols = Arrays.asList(col("k"), col("v"));
        OlapTable target = olapTable(targetCols, targetCols, false);
        OlapTable source = olapTable(sourceCols, sourceCols, false);

        SelectRelation rel = bareRelation(bareItem("k")); // only 1 item, target has 2

        InsertSelectSourceColumns result = InsertSelectSourceColumns.resolve(
                insertStmt(false), rel,
                target, source, SRC_NAME, null,
                Collections.singletonList(col("k")),
                Collections.emptyList());

        Assertions.assertNull(result);
    }

    @Test
    public void byNameDuplicateOutputNameReturnsNull() {
        // SELECT k, x AS k -> duplicate output name -> null
        List<Column> targetCols = Arrays.asList(col("k"), col("v"));
        List<Column> sourceCols = Arrays.asList(col("k"), col("v"), col("x"));
        OlapTable target = olapTable(targetCols, targetCols, false);
        OlapTable source = olapTable(sourceCols, sourceCols, false);

        SelectRelation rel = bareRelation(bareItem("k", null, null), bareItem("x", null, "k"));

        InsertSelectSourceColumns result = InsertSelectSourceColumns.resolve(
                insertStmt(true), rel,
                target, source, SRC_NAME, null,
                Collections.singletonList(col("k")),
                Collections.emptyList());

        Assertions.assertNull(result);
    }

    @Test
    public void bareColumnsByNameExtraOrMissingOutputReturnsNull() {
        // by-name output-name set != target non-generated set (extra column) -> null
        List<Column> targetCols = Arrays.asList(col("k"), col("v"));
        List<Column> sourceCols = Arrays.asList(col("k"), col("v"), col("extra"));
        OlapTable target = olapTable(targetCols, targetCols, false);
        OlapTable source = olapTable(sourceCols, sourceCols, false);

        // SELECT k, v, extra — output has "extra" not in target
        SelectRelation rel = bareRelation(bareItem("k"), bareItem("v"), bareItem("extra"));

        InsertSelectSourceColumns result = InsertSelectSourceColumns.resolve(
                insertStmt(true), rel,
                target, source, SRC_NAME, null,
                Collections.singletonList(col("k")),
                Collections.emptyList());

        Assertions.assertNull(result);
    }

    @Test
    public void sortKeyNotInProjectionReturnsNull() {
        // by-name; sortKey col absent from output -> null
        List<Column> targetCols = Arrays.asList(col("k"), col("v"));
        List<Column> sourceCols = Arrays.asList(col("k"), col("v"));
        OlapTable target = olapTable(targetCols, targetCols, false);
        OlapTable source = olapTable(sourceCols, sourceCols, false);

        // SELECT k, v by-name aligned but sortKey is col("z") which is absent
        SelectRelation rel = bareRelation(bareItem("k"), bareItem("v"));

        InsertSelectSourceColumns result = InsertSelectSourceColumns.resolve(
                insertStmt(true), rel,
                target, source, SRC_NAME, null,
                Collections.singletonList(col("z")),  // z not in target
                Collections.emptyList());

        Assertions.assertNull(result);
    }

    @Test
    public void partitionColumnsMappedAlongsideSortKey() {
        // partitioned: sortKey [k], partition [dt]; source aligned [k, dt, v]
        List<Column> cols = Arrays.asList(col("k"), col("dt"), col("v"));
        OlapTable target = olapTable(cols, cols, false);
        OlapTable source = olapTable(cols, cols, false);

        InsertSelectSourceColumns result = InsertSelectSourceColumns.resolve(
                insertStmt(false), starRelation(),
                target, source, SRC_NAME, null,
                Collections.singletonList(col("k")),
                Collections.singletonList(col("dt")));

        Assertions.assertNotNull(result);
        Assertions.assertEquals(Collections.singletonList("k"), result.sortKeySourceColumnNames());
        Assertions.assertEquals(Collections.singletonList("dt"), result.partitionSourceColumnNames());
    }

    @Test
    public void partitionColumnMissingReturnsNull() {
        // partition col unmapped -> null
        List<Column> targetCols = Arrays.asList(col("k"), col("v"));
        List<Column> sourceCols = Arrays.asList(col("k"), col("v"));
        OlapTable target = olapTable(targetCols, targetCols, false);
        OlapTable source = olapTable(sourceCols, sourceCols, false);

        InsertSelectSourceColumns result = InsertSelectSourceColumns.resolve(
                insertStmt(false), starRelation(),
                target, source, SRC_NAME, null,
                Collections.singletonList(col("k")),
                Collections.singletonList(col("dt"))); // dt not in target schema

        Assertions.assertNull(result);
    }

    @Test
    public void expressionItemReturnsNull() {
        // SELECT k+1 -> getExpr() not SlotRef -> null
        List<Column> cols = Arrays.asList(col("k"), col("v"));
        OlapTable target = olapTable(cols, cols, false);
        OlapTable source = olapTable(cols, cols, false);

        // Create an item whose getExpr() returns a non-SlotRef expression
        SelectListItem item = mock(SelectListItem.class);
        when(item.isStar()).thenReturn(false);
        when(item.getExpr()).thenReturn(mock(com.starrocks.sql.ast.expression.FunctionCallExpr.class));

        SelectList list = mock(SelectList.class);
        when(list.getItems()).thenReturn(Collections.singletonList(item));
        SelectRelation rel = mock(SelectRelation.class);
        when(rel.getSelectList()).thenReturn(list);

        InsertSelectSourceColumns result = InsertSelectSourceColumns.resolve(
                insertStmt(false), rel,
                target, source, SRC_NAME, null,
                Collections.singletonList(col("k")),
                Collections.emptyList());

        Assertions.assertNull(result);
    }

    // --- matchesSource direct tests (alias branch; reused by Task 2) ---

    @Test
    public void matchesSourceAliasMatches() {
        // slot tbl == alias, no db/catalog -> true
        TableName slotTbl = new TableName(null, null, "s");
        Assertions.assertTrue(
                InsertSelectSourceColumns.matchesSource(slotTbl, SRC_NAME, "s"));
    }

    @Test
    public void matchesSourceAliasWithDbQualifierReturnsFalse() {
        // alias in scope, but slot carries a spurious db qualifier -> false
        TableName slotTbl = new TableName(null, "db1", "s");
        Assertions.assertFalse(
                InsertSelectSourceColumns.matchesSource(slotTbl, SRC_NAME, "s"));
    }

    @Test
    public void matchesSourceAliasNameMismatchReturnsFalse() {
        // alias in scope, slot tbl != alias -> false
        TableName slotTbl = new TableName(null, null, "other");
        Assertions.assertFalse(
                InsertSelectSourceColumns.matchesSource(slotTbl, SRC_NAME, "s"));
    }
}
