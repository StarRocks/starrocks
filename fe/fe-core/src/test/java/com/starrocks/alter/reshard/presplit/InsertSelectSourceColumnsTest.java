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
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableFunctionTable;
import com.starrocks.catalog.TableName;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectList;
import com.starrocks.sql.ast.SelectListItem;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.ast.expression.Subquery;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

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

    /**
     * Build a mock InsertStmt carrying an explicit target column list. BY NAME and a written
     * column list cannot be combined by the parser, but {@code InsertAnalyzer} sets the list to
     * the SELECT output names for a BY NAME statement, so the analyzed INSERT OVERWRITE ... BY
     * NAME that the overwrite hooks see does carry both.
     */
    private static InsertStmt insertStmt(boolean byName, List<String> targetColumnNames) {
        InsertStmt stmt = insertStmt(byName);
        when(stmt.getTargetColumnNames()).thenReturn(targetColumnNames);
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

    /**
     * A {@code parse_json(v)} projection over the source column {@code v}. The expression is a real
     * AST node, not a mock, because the reference gate walks the tree with {@code collectAll} --
     * a mock would silently answer "nothing rejected" for every input.
     */
    private static SelectListItem expressionItem(String alias) {
        return expressionItem(alias, sourceExpr(new SlotRef((TableName) null, "v")));
    }

    private static SelectListItem expressionItem(String alias, Expr expr) {
        SelectListItem item = mock(SelectListItem.class);
        when(item.isStar()).thenReturn(false);
        when(item.getExpr()).thenReturn(expr);
        when(item.getAlias()).thenReturn(alias);
        return item;
    }

    private static FunctionCallExpr sourceExpr(Expr argument) {
        return new FunctionCallExpr("parse_json", Collections.singletonList(argument));
    }

    private static SelectRelation bareRelation(SelectListItem... items) {
        SelectList list = mock(SelectList.class);
        when(list.getItems()).thenReturn(Arrays.asList(items));
        SelectRelation rel = mock(SelectRelation.class);
        when(rel.getSelectList()).thenReturn(list);
        return rel;
    }

    private static final TableName SRC_NAME = new TableName("cat", "db1", "src");

    /**
     * The table-source call: the effective target columns are the explicit target column list, or
     * the full non-generated base schema without one, and the source schema must be an exact image
     * of the target's.
     */
    private static Map<String, String> resolveExact(
            InsertStmt stmt, SelectRelation rel, OlapTable target, Table source,
            TableName normalizedSourceName, String sourceAlias,
            List<Column> sortKeyColumns, List<Column> partitionColumns) {
        List<Column> targetCols = InsertSelectSourceColumns.effectiveTargetColumns(stmt, target);
        if (targetCols == null) {
            return null;
        }
        return InsertSelectSourceColumns.resolve(
                stmt, rel, targetCols, source,
                normalizedSourceName, sourceAlias, sortKeyColumns, partitionColumns,
                InsertSelectSourceColumns.SchemaPairing.EXACT);
    }

    /** The FILES call: whatever columns correspond are paired, extras on either side are fine. */
    private static Map<String, String> resolvePerColumn(
            InsertStmt stmt, SelectRelation rel, List<Column> targetCols, Table source,
            List<Column> sortKeyColumns, List<Column> partitionColumns) {
        return InsertSelectSourceColumns.resolve(
                stmt, rel, targetCols, source, SRC_NAME, null, sortKeyColumns, partitionColumns,
                InsertSelectSourceColumns.SchemaPairing.PER_COLUMN);
    }

    // --- tests ---

    @Test
    public void starByPositionAlignedSchemaMapsIdentity() {
        // source [k, v]; target [k, v]; sortKey [k]; SELECT * by position -> ["k"], partition []
        List<Column> cols = Arrays.asList(col("k"), col("v"));
        OlapTable target = olapTable(cols, cols, false);
        OlapTable source = olapTable(cols, cols, false);

        Map<String, String> result = resolveExact(
                insertStmt(false), starRelation(),
                target, source, SRC_NAME, null,
                Collections.singletonList(col("k")),
                Collections.emptyList());

        Assertions.assertNotNull(result);
        Assertions.assertEquals("k", result.get("k"));
    }

    @Test
    public void resolveExposesFullTargetToSourceMap() {
        // source [k, v]; target [k, v]; SELECT * by position -> full map covers EVERY
        // non-generated base column (not just the sort key), lower-cased target -> source.
        List<Column> cols = Arrays.asList(col("k"), col("v"));
        OlapTable target = olapTable(cols, cols, false);
        OlapTable source = olapTable(cols, cols, false);

        Map<String, String> result = resolveExact(
                insertStmt(false), starRelation(),
                target, source, SRC_NAME, null,
                Collections.singletonList(col("k")),
                Collections.emptyList());

        Assertions.assertNotNull(result);
        Assertions.assertEquals(Map.of("k", "k", "v", "v"), result);
    }

    @Test
    public void starByPositionMisalignedNameReturnsNull() {
        // source [x, v]; target [k, v]; position-0 name mismatch -> null
        List<Column> targetCols = Arrays.asList(col("k"), col("v"));
        List<Column> sourceCols = Arrays.asList(col("x"), col("v"));
        OlapTable target = olapTable(targetCols, targetCols, false);
        OlapTable source = olapTable(sourceCols, sourceCols, false);

        Map<String, String> result = resolveExact(
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

        Map<String, String> result = resolveExact(
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

        Map<String, String> result = resolveExact(
                insertStmt(true), starRelation(),
                target, source, SRC_NAME, null,
                Collections.singletonList(col("k")),
                Collections.emptyList());

        Assertions.assertNotNull(result);
        Assertions.assertEquals("k", result.get("k"));
    }

    @Test
    public void starByNameMissingSourceColumnReturnsNull() {
        // target "k" absent from source -> null
        List<Column> targetCols = Arrays.asList(col("k"), col("v"));
        List<Column> sourceCols = Collections.singletonList(col("v"));
        OlapTable target = olapTable(targetCols, targetCols, false);
        OlapTable source = olapTable(sourceCols, sourceCols, false);

        Map<String, String> result = resolveExact(
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

        Map<String, String> result = resolveExact(
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

        Map<String, String> result = resolveExact(
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

        Map<String, String> result = resolveExact(
                insertStmt(false), rel,
                target, source, SRC_NAME, null,
                Collections.singletonList(col("b")),
                Collections.emptyList());

        Assertions.assertNotNull(result);
        // target[1]=b <- output[1]="a"; sortKey b -> source "a"
        Assertions.assertEquals("a", result.get("b"));
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

        Map<String, String> result = resolveExact(
                insertStmt(true), rel,
                target, source, SRC_NAME, null,
                Collections.singletonList(col("k")),
                Collections.emptyList());

        Assertions.assertNotNull(result);
        Assertions.assertEquals("v", result.get("k"));
    }

    @Test
    public void bareColumnForeignTableQualifierReturnsNull() {
        // SELECT other.k -> tbl != source -> null
        List<Column> cols = Arrays.asList(col("k"), col("v"));
        OlapTable target = olapTable(cols, cols, false);
        OlapTable source = olapTable(cols, cols, false);

        TableName otherTbl = new TableName(null, null, "other");
        SelectRelation rel = bareRelation(bareItem("k", otherTbl, null), bareItem("v", null, null));

        Map<String, String> result = resolveExact(
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

        Map<String, String> result = resolveExact(
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

        Map<String, String> result = resolveExact(
                insertStmt(false), rel,
                target, source, SRC_NAME, null,
                Collections.singletonList(col("k")),
                Collections.emptyList());

        Assertions.assertNotNull(result);
        Assertions.assertEquals("k", result.get("k"));
    }

    @Test
    public void bareColumnVirtualNameReturnsNull() {
        // colName absent from physical map (virtual-only) -> null
        List<Column> targetCols = Arrays.asList(col("k"), col("v"));
        List<Column> sourceCols = Arrays.asList(col("k"), col("v")); // "ghost" not present
        OlapTable target = olapTable(targetCols, targetCols, false);
        OlapTable source = olapTable(sourceCols, sourceCols, false);

        SelectRelation rel = bareRelation(bareItem("ghost"), bareItem("v"));

        Map<String, String> result = resolveExact(
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

        Map<String, String> result = resolveExact(
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

        Map<String, String> result = resolveExact(
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

        Map<String, String> result = resolveExact(
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

        Map<String, String> result = resolveExact(
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

        Map<String, String> result = resolveExact(
                insertStmt(false), starRelation(),
                target, source, SRC_NAME, null,
                Collections.singletonList(col("k")),
                Collections.singletonList(col("dt")));

        Assertions.assertNotNull(result);
        Assertions.assertEquals("k", result.get("k"));
        Assertions.assertEquals("dt", result.get("dt"));
    }

    @Test
    public void partitionColumnMissingReturnsNull() {
        // partition col unmapped -> null
        List<Column> targetCols = Arrays.asList(col("k"), col("v"));
        List<Column> sourceCols = Arrays.asList(col("k"), col("v"));
        OlapTable target = olapTable(targetCols, targetCols, false);
        OlapTable source = olapTable(sourceCols, sourceCols, false);

        Map<String, String> result = resolveExact(
                insertStmt(false), starRelation(),
                target, source, SRC_NAME, null,
                Collections.singletonList(col("k")),
                Collections.singletonList(col("dt"))); // dt not in target schema

        Assertions.assertNull(result);
    }

    @Test
    public void expressionOnNonKeyByPositionMapsRequiredColumns() {
        // target [k, v]; SELECT k, parse_json(v) -> only k is needed by the sampler.
        List<Column> cols = Arrays.asList(col("k"), col("v"));
        OlapTable target = olapTable(cols, cols, false);
        OlapTable source = olapTable(cols, cols, false);

        SelectRelation rel = bareRelation(bareItem("k"), expressionItem(null));

        Map<String, String> result = resolveExact(
                insertStmt(false), rel,
                target, source, SRC_NAME, null,
                Collections.singletonList(col("k")),
                Collections.emptyList());

        Assertions.assertEquals(Map.of("k", "k"), result);
    }

    @Test
    public void expressionOnSortKeyByPositionReturnsNull() {
        List<Column> cols = Arrays.asList(col("k"), col("v"));
        OlapTable target = olapTable(cols, cols, false);
        OlapTable source = olapTable(cols, cols, false);

        SelectRelation rel = bareRelation(expressionItem(null), bareItem("v"));

        Map<String, String> result = resolveExact(
                insertStmt(false), rel,
                target, source, SRC_NAME, null,
                Collections.singletonList(col("k")),
                Collections.emptyList());

        Assertions.assertNull(result);
    }

    @Test
    public void expressionOnPartitionColumnByPositionReturnsNull() {
        List<Column> cols = Arrays.asList(col("k"), col("dt"), col("v"));
        OlapTable target = olapTable(cols, cols, false);
        OlapTable source = olapTable(cols, cols, false);

        SelectRelation rel = bareRelation(bareItem("k"), expressionItem(null), bareItem("v"));

        Map<String, String> result = resolveExact(
                insertStmt(false), rel,
                target, source, SRC_NAME, null,
                Collections.singletonList(col("k")),
                Collections.singletonList(col("dt")));

        Assertions.assertNull(result);
    }

    @Test
    public void expressionOnNonKeyByNameUsesAlias() {
        List<Column> cols = Arrays.asList(col("k"), col("v"));
        OlapTable target = olapTable(cols, cols, false);
        OlapTable source = olapTable(cols, cols, false);

        SelectRelation rel = bareRelation(bareItem("k"), expressionItem("v"));

        Map<String, String> result = resolveExact(
                insertStmt(true), rel,
                target, source, SRC_NAME, null,
                Collections.singletonList(col("k")),
                Collections.emptyList());

        Assertions.assertEquals(Map.of("k", "k"), result);
    }

    @Test
    public void expressionByNameWithoutAliasReturnsNull() {
        List<Column> cols = Arrays.asList(col("k"), col("v"));
        OlapTable target = olapTable(cols, cols, false);
        OlapTable source = olapTable(cols, cols, false);

        SelectRelation rel = bareRelation(bareItem("k"), expressionItem(null));

        Map<String, String> result = resolveExact(
                insertStmt(true), rel,
                target, source, SRC_NAME, null,
                Collections.singletonList(col("k")),
                Collections.emptyList());

        Assertions.assertNull(result);
    }

    @Test
    public void expressionQualifiedWithSourceMapsRequiredColumns() {
        // SELECT k, parse_json(src.v) -> the qualifier names the source, so the expression is fine.
        List<Column> cols = Arrays.asList(col("k"), col("v"));
        OlapTable target = olapTable(cols, cols, false);
        OlapTable source = olapTable(cols, cols, false);

        SelectListItem expr = expressionItem(
                null, sourceExpr(new SlotRef(new TableName(null, null, "src"), "v")));
        SelectRelation rel = bareRelation(bareItem("k"), expr);

        Map<String, String> result = resolveExact(
                insertStmt(false), rel,
                target, source, SRC_NAME, null,
                Collections.singletonList(col("k")),
                Collections.emptyList());

        Assertions.assertEquals(Map.of("k", "k"), result);
    }

    @Test
    public void expressionOverForeignTableReturnsNull() {
        // SELECT k, parse_json(other.v) -> the hook never resolved or authorized `other`.
        List<Column> cols = Arrays.asList(col("k"), col("v"));
        OlapTable target = olapTable(cols, cols, false);
        OlapTable source = olapTable(cols, cols, false);

        SelectListItem expr = expressionItem(
                null, sourceExpr(new SlotRef(new TableName(null, null, "other"), "v")));
        SelectRelation rel = bareRelation(bareItem("k"), expr);

        Map<String, String> result = resolveExact(
                insertStmt(false), rel,
                target, source, SRC_NAME, null,
                Collections.singletonList(col("k")),
                Collections.emptyList());

        Assertions.assertNull(result);
    }

    @Test
    public void expressionOverUnknownColumnReturnsNull() {
        // SELECT k, parse_json(nosuch) -> analysis would reject this, so do not reshard for it.
        List<Column> cols = Arrays.asList(col("k"), col("v"));
        OlapTable target = olapTable(cols, cols, false);
        OlapTable source = olapTable(cols, cols, false);

        SelectListItem expr = expressionItem(null, sourceExpr(new SlotRef((TableName) null, "nosuch")));
        SelectRelation rel = bareRelation(bareItem("k"), expr);

        Map<String, String> result = resolveExact(
                insertStmt(false), rel,
                target, source, SRC_NAME, null,
                Collections.singletonList(col("k")),
                Collections.emptyList());

        Assertions.assertNull(result);
    }

    @Test
    public void scalarSubqueryProjectionReturnsNull() {
        // SELECT k, (SELECT ...) -> reads a relation outside the authorized source.
        List<Column> cols = Arrays.asList(col("k"), col("v"));
        OlapTable target = olapTable(cols, cols, false);
        OlapTable source = olapTable(cols, cols, false);

        Subquery subquery = new Subquery(mock(QueryStatement.class), NodePosition.ZERO);
        SelectRelation rel = bareRelation(bareItem("k"), expressionItem(null, subquery));

        Map<String, String> result = resolveExact(
                insertStmt(false), rel,
                target, source, SRC_NAME, null,
                Collections.singletonList(col("k")),
                Collections.emptyList());

        Assertions.assertNull(result);
    }

    @Test
    public void nestedSubqueryInsideExpressionReturnsNull() {
        // SELECT k, parse_json((SELECT ...)) -> the walk must reach nested nodes, not just the root.
        List<Column> cols = Arrays.asList(col("k"), col("v"));
        OlapTable target = olapTable(cols, cols, false);
        OlapTable source = olapTable(cols, cols, false);

        Subquery subquery = new Subquery(mock(QueryStatement.class), NodePosition.ZERO);
        SelectRelation rel = bareRelation(bareItem("k"), expressionItem(null, sourceExpr(subquery)));

        Map<String, String> result = resolveExact(
                insertStmt(false), rel,
                target, source, SRC_NAME, null,
                Collections.singletonList(col("k")),
                Collections.emptyList());

        Assertions.assertNull(result);
    }

    // --- explicit target column list: outputs pair against the list, not the base schema ---

    @Test
    public void partialColumnListByPositionMapsOnlyListedColumns() {
        // target base [k, v, extra]; INSERT INTO t (k, v) SELECT k, v -- the omitted "extra"
        // is defaulted by the load and simply never enters the map.
        List<Column> targetCols = Arrays.asList(col("k"), col("v"), col("extra"));
        List<Column> sourceCols = Arrays.asList(col("k"), col("v"));
        OlapTable target = olapTable(targetCols, targetCols, false);
        OlapTable source = olapTable(sourceCols, sourceCols, false);

        Map<String, String> result = resolveExact(
                insertStmt(false, List.of("k", "v")), bareRelation(bareItem("k"), bareItem("v")),
                target, source, SRC_NAME, null,
                Collections.singletonList(col("k")),
                Collections.emptyList());

        Assertions.assertEquals(Map.of("k", "k", "v", "v"), result);
    }

    @Test
    public void reorderedColumnListByPositionPairsInListOrder() {
        // target base [k, v]; INSERT INTO t (v, k) SELECT k, v -> v<-"k", k<-"v".
        // Pairing against the base schema instead would give the identity map.
        List<Column> cols = Arrays.asList(col("k"), col("v"));
        OlapTable target = olapTable(cols, cols, false);
        OlapTable source = olapTable(cols, cols, false);

        Map<String, String> result = resolveExact(
                insertStmt(false, List.of("v", "k")), bareRelation(bareItem("k"), bareItem("v")),
                target, source, SRC_NAME, null,
                Collections.singletonList(col("k")),
                Collections.emptyList());

        Assertions.assertEquals(Map.of("k", "v", "v", "k"), result);
    }

    @Test
    public void starWithPartialColumnListMapsListedColumns() {
        // target base [k, v, extra]; INSERT INTO t (k, v) SELECT * FROM src(k, v).
        List<Column> targetCols = Arrays.asList(col("k"), col("v"), col("extra"));
        List<Column> sourceCols = Arrays.asList(col("k"), col("v"));
        OlapTable target = olapTable(targetCols, targetCols, false);
        OlapTable source = olapTable(sourceCols, sourceCols, false);

        Map<String, String> result = resolveExact(
                insertStmt(false, List.of("k", "v")), starRelation(),
                target, source, SRC_NAME, null,
                Collections.singletonList(col("k")),
                Collections.emptyList());

        Assertions.assertEquals(Map.of("k", "k", "v", "v"), result);
    }

    @Test
    public void partialColumnListByPositionLengthMismatchReturnsNull() {
        // INSERT INTO t (k, v) SELECT k -- two listed targets, one output.
        List<Column> targetCols = Arrays.asList(col("k"), col("v"), col("extra"));
        List<Column> sourceCols = Arrays.asList(col("k"), col("v"));
        OlapTable target = olapTable(targetCols, targetCols, false);
        OlapTable source = olapTable(sourceCols, sourceCols, false);

        Map<String, String> result = resolveExact(
                insertStmt(false, List.of("k", "v")), bareRelation(bareItem("k")),
                target, source, SRC_NAME, null,
                Collections.singletonList(col("k")),
                Collections.emptyList());

        Assertions.assertNull(result);
    }

    @Test
    public void partialColumnListByNameMapsListedColumns() {
        // target base [k, v, extra]; BY NAME with the analyzed list (k, v) and outputs k, v.
        List<Column> targetCols = Arrays.asList(col("k"), col("v"), col("extra"));
        List<Column> sourceCols = Arrays.asList(col("k"), col("v"));
        OlapTable target = olapTable(targetCols, targetCols, false);
        OlapTable source = olapTable(sourceCols, sourceCols, false);

        Map<String, String> result = resolveExact(
                insertStmt(true, List.of("k", "v")), bareRelation(bareItem("k"), bareItem("v")),
                target, source, SRC_NAME, null,
                Collections.singletonList(col("k")),
                Collections.emptyList());

        Assertions.assertEquals(Map.of("k", "k", "v", "v"), result);
    }

    @Test
    public void reorderedColumnListByNameMapsBySelectOutputName() {
        // target base [k, v, extra]; BY NAME with the analyzed list (v, k) -- partial AND in an
        // order the schema does not have. Matching stays by output name, so SELECT v AS k,
        // k AS v gives k<-"v" and v<-"k", and the unlisted "extra" stays out of the map.
        List<Column> targetCols = Arrays.asList(col("k"), col("v"), col("extra"));
        List<Column> sourceCols = Arrays.asList(col("k"), col("v"));
        OlapTable target = olapTable(targetCols, targetCols, false);
        OlapTable source = olapTable(sourceCols, sourceCols, false);

        SelectRelation rel = bareRelation(
                bareItem("v", null, "k"),   // SELECT v AS k
                bareItem("k", null, "v"));  // SELECT k AS v

        Map<String, String> result = resolveExact(
                insertStmt(true, List.of("v", "k")), rel,
                target, source, SRC_NAME, null,
                Collections.singletonList(col("k")),
                Collections.emptyList());

        Assertions.assertEquals(Map.of("k", "v", "v", "k"), result);
    }

    @Test
    public void columnListByNameOutputSetMismatchReturnsNull() {
        // BY NAME: the outputs must be exactly the listed columns, not a subset of them.
        List<Column> targetCols = Arrays.asList(col("k"), col("v"), col("extra"));
        List<Column> sourceCols = Arrays.asList(col("k"), col("v"));
        OlapTable target = olapTable(targetCols, targetCols, false);
        OlapTable source = olapTable(sourceCols, sourceCols, false);

        Map<String, String> result = resolveExact(
                insertStmt(true, List.of("k", "v")), bareRelation(bareItem("k")),
                target, source, SRC_NAME, null,
                Collections.singletonList(col("k")),
                Collections.emptyList());

        Assertions.assertNull(result);
    }

    @Test
    public void columnListOmittingSortKeyReturnsNull() {
        // INSERT INTO t (v) SELECT v, with k the sort key. targetColumnListIsPreSplitSafe rejects
        // this upstream; the sort-key lookup here is the second line of defence.
        List<Column> targetCols = Arrays.asList(col("k"), col("v"));
        List<Column> sourceCols = Arrays.asList(col("k"), col("v"));
        OlapTable target = olapTable(targetCols, targetCols, false);
        OlapTable source = olapTable(sourceCols, sourceCols, false);

        Map<String, String> result = resolveExact(
                insertStmt(false, List.of("v")), bareRelation(bareItem("v")),
                target, source, SRC_NAME, null,
                Collections.singletonList(col("k")),
                Collections.emptyList());

        Assertions.assertNull(result);
    }

    @Test
    public void columnListNamingUnresolvableColumnReturnsNull() {
        // A name that is not a base non-generated column cannot be paired with an output.
        List<Column> cols = Arrays.asList(col("k"), col("v"));
        OlapTable target = olapTable(cols, cols, false);
        OlapTable source = olapTable(cols, cols, false);

        Map<String, String> result = resolveExact(
                insertStmt(false, List.of("k", "typo")), bareRelation(bareItem("k"), bareItem("v")),
                target, source, SRC_NAME, null,
                Collections.singletonList(col("k")),
                Collections.emptyList());

        Assertions.assertNull(result);
    }

    // --- SchemaPairing.PER_COLUMN (a FILES(...) source) ---

    /** A mock FILES table: the inferred file schema, no generated columns. */
    private static Table filesTable(Column... columns) {
        TableFunctionTable table = mock(TableFunctionTable.class);
        when(table.getFullVisibleSchema()).thenReturn(Arrays.asList(columns));
        return table;
    }

    @Test
    public void filesStarByPositionMapsByOrdinalEvenWhenNamesDiffer() {
        // INSERT INTO t(k, v) SELECT * FROM FILES(a, b): by position the load writes file column a
        // into target k, so the sampler must read a -- not the file's own "k", which does not exist.
        // EXACT rejects this shape outright (starByPositionMisalignedNameReturnsNull above).
        List<Column> targetCols = Arrays.asList(col("k"), col("v"));

        Map<String, String> result = resolvePerColumn(
                insertStmt(false), starRelation(), targetCols, filesTable(col("a"), col("b")),
                Collections.singletonList(col("k")), Collections.emptyList());

        Assertions.assertEquals(Map.of("k", "a", "v", "b"), result);
    }

    @Test
    public void filesStarByPositionStillRequiresEqualArity() {
        // A file wider than the target makes the by-position pairing of the tail ambiguous
        // (the load trims), so the whole projection is declined rather than half-mapped.
        List<Column> targetCols = Arrays.asList(col("k"), col("v"));

        Map<String, String> result = resolvePerColumn(
                insertStmt(false), starRelation(), targetCols,
                filesTable(col("k"), col("v"), col("w")),
                Collections.singletonList(col("k")), Collections.emptyList());

        Assertions.assertNull(result);
    }

    @Test
    public void filesStarByNameToleratesExtraFileColumns() {
        // INSERT INTO t BY NAME SELECT * FROM FILES(...): a file carrying fields the target does not
        // have is the ordinary case -- BY NAME simply does not read them. EXACT rejects it
        // (starByNameExtraSourceColumnReturnsNull above).
        List<Column> targetCols = Arrays.asList(col("k"), col("v"));

        Map<String, String> result = resolvePerColumn(
                insertStmt(true), starRelation(), targetCols,
                filesTable(col("v"), col("k"), col("extra")),
                Collections.singletonList(col("k")), Collections.emptyList());

        Assertions.assertEquals(Map.of("k", "k", "v", "v"), result);
    }

    @Test
    public void filesStarByNameLeavesASortKeyTheFileLacksUnmapped() {
        // The file has no "k", so the load defaults that column for every row and no FILES column
        // can be sampled for it -> the final presence gate declines.
        List<Column> targetCols = Arrays.asList(col("k"), col("v"));

        Map<String, String> result = resolvePerColumn(
                insertStmt(true), starRelation(), targetCols, filesTable(col("v")),
                Collections.singletonList(col("k")), Collections.emptyList());

        Assertions.assertNull(result);
    }

    @Test
    public void filesStarByNameMapsANonKeyColumnTheFileLacks() {
        // Same shape, but the column the file lacks is not a key: the load defaults it and pre-split
        // is unaffected, so the projection is admitted with that column simply absent from the map.
        List<Column> targetCols = Arrays.asList(col("k"), col("v"));

        Map<String, String> result = resolvePerColumn(
                insertStmt(true), starRelation(), targetCols, filesTable(col("k")),
                Collections.singletonList(col("k")), Collections.emptyList());

        Assertions.assertEquals(Map.of("k", "k"), result);
    }

    @Test
    public void filesExplicitListByPositionMapsEachOutputToItsTargetOrdinal() {
        // INSERT INTO t(k, v) SELECT b, a FROM FILES(a, b) -- the load writes b into k and a into v.
        List<Column> targetCols = Arrays.asList(col("k"), col("v"));

        Map<String, String> result = resolvePerColumn(
                insertStmt(false), bareRelation(bareItem("b"), bareItem("a")),
                targetCols, filesTable(col("a"), col("b")),
                Collections.singletonList(col("k")), Collections.emptyList());

        Assertions.assertEquals(Map.of("k", "b", "v", "a"), result);
    }

    @Test
    public void filesExplicitListAdmitsAnExpressionOverANonKeyColumn() {
        // INSERT INTO t(k, v) SELECT k, parse_json(v) FROM FILES(...): the sampler never evaluates a
        // non-key projection, so only the sort key has to be a direct file-column reference.
        List<Column> targetCols = Arrays.asList(col("k"), col("v"));

        Map<String, String> result = resolvePerColumn(
                insertStmt(false), bareRelation(bareItem("k"), expressionItem(null)),
                targetCols, filesTable(col("k"), col("v")),
                Collections.singletonList(col("k")), Collections.emptyList());

        Assertions.assertEquals(Map.of("k", "k"), result);
    }

    @Test
    public void filesExplicitListByNameNeedNotCoverEveryTargetColumn() {
        // INSERT INTO t BY NAME SELECT k, v FROM FILES(...) against a target that also has w: the
        // load defaults w. EXACT requires the outputs to be exactly the target's columns.
        List<Column> targetCols = Arrays.asList(col("k"), col("v"), col("w"));

        Map<String, String> result = resolvePerColumn(
                insertStmt(true), bareRelation(bareItem("k"), bareItem("v")),
                targetCols, filesTable(col("k"), col("v"), col("other")),
                Collections.singletonList(col("k")), Collections.emptyList());

        Assertions.assertEquals(Map.of("k", "k", "v", "v"), result);
    }

    @Test
    public void filesExplicitListByNameStillRejectsDuplicateOutputNames() {
        // Two outputs named v leave the target position of each ambiguous, whatever the pairing mode.
        List<Column> targetCols = Arrays.asList(col("k"), col("v"));

        Map<String, String> result = resolvePerColumn(
                insertStmt(true), bareRelation(bareItem("k"), bareItem("v"), bareItem("v", null, "v")),
                targetCols, filesTable(col("k"), col("v")),
                Collections.singletonList(col("k")), Collections.emptyList());

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
