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
import com.starrocks.catalog.Function;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableFunctionTable;
import com.starrocks.catalog.TableName;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectList;
import com.starrocks.sql.ast.SelectListItem;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.NullLiteral;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.ast.expression.Subquery;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.thrift.TFunctionBinaryType;
import com.starrocks.type.BooleanType;
import com.starrocks.type.DateType;
import com.starrocks.type.FloatType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.StructField;
import com.starrocks.type.StructType;
import com.starrocks.type.Type;
import com.starrocks.type.TypeFactory;
import com.starrocks.type.VarcharType;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.starrocks.alter.reshard.presplit.PresplitTestSupport.ACTIVITY_DATE;
import static com.starrocks.alter.reshard.presplit.PresplitTestSupport.activityMonth;
import static com.starrocks.alter.reshard.presplit.PresplitTestSupport.generatedColumn;
import static com.starrocks.alter.reshard.presplit.PresplitTestSupport.stubGeneratedSchema;
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
     * The table-source call: the effective target columns are the full non-generated base schema,
     * and the source schema must be an exact image of the target's.
     */
    private static Map<String, String> resolveExact(
            InsertStmt stmt, SelectRelation rel, OlapTable target, Table source,
            TableName normalizedSourceName, String sourceAlias,
            List<Column> sortKeyColumns, List<Column> partitionColumns) {
        return sourceMapOf(resolveExactFully(
                stmt, rel, target, source, normalizedSourceName, sourceAlias, sortKeyColumns, partitionColumns));
    }

    private static InsertSelectSourceColumns.Resolved resolveExactFully(
            InsertStmt stmt, SelectRelation rel, OlapTable target, Table source,
            TableName normalizedSourceName, String sourceAlias,
            List<Column> sortKeyColumns, List<Column> partitionColumns) {
        return gated(InsertSelectSourceColumns.resolveUngated(
                stmt, rel, target,
                new InsertSelectSourceColumns.ResolutionContext(source, normalizedSourceName, sourceAlias,
                        InsertSelectSourceColumns.SchemaPairing.EXACT, /*computedProjectionContext*/ null)),
                sortKeyColumns, partitionColumns);
    }

    /** The FILES call: whatever columns correspond are paired, extras on either side are fine. */
    private static Map<String, String> resolvePerColumn(
            InsertStmt stmt, SelectRelation rel, OlapTable target, Table source,
            List<Column> sortKeyColumns, List<Column> partitionColumns) {
        return sourceMapOf(resolvePerColumnFully(stmt, rel, target, source, sortKeyColumns, partitionColumns));
    }

    private static InsertSelectSourceColumns.Resolved resolvePerColumnFully(
            InsertStmt stmt, SelectRelation rel, List<Column> targetCols, Table source,
            List<Column> sortKeyColumns, List<Column> partitionColumns) {
        return resolvePerColumnFully(
                stmt, rel, olapTable(targetCols, targetCols, false), source, sortKeyColumns, partitionColumns);
    }

    private static InsertSelectSourceColumns.Resolved resolvePerColumnFully(
            InsertStmt stmt, SelectRelation rel, OlapTable target, Table source,
            List<Column> sortKeyColumns, List<Column> partitionColumns) {
        return gated(InsertSelectSourceColumns.resolveUngated(
                stmt, rel, target,
                new InsertSelectSourceColumns.ResolutionContext(source, SRC_NAME, null,
                        InsertSelectSourceColumns.SchemaPairing.PER_COLUMN, /*computedProjectionContext*/ null)),
                sortKeyColumns, partitionColumns);
    }

    /** The presence gates the sources apply after resolveUngated: a fed, non-degenerate key and fed partition columns. */
    private static InsertSelectSourceColumns.Resolved gated(
            InsertSelectSourceColumns.Resolved resolved, List<Column> sortKeyColumns, List<Column> partitionColumns) {
        if (resolved == null || !InsertSelectSourceColumns.sortKeySampleable(sortKeyColumns, resolved)
                || InsertSelectSourceColumns.firstUnsampleable(partitionColumns, resolved,
                        InsertSelectSourceColumns.InputReading.AS_SELECTED) != null) {
            return null;
        }
        return resolved;
    }

    private static Map<String, String> sourceMapOf(InsertSelectSourceColumns.Resolved resolved) {
        return resolved == null ? null : resolved.targetToSource();
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
                insertStmt(false), starRelation(), olapTable(targetCols, targetCols, false),
                filesTable(col("a"), col("b")),
                Collections.singletonList(col("k")), Collections.emptyList());

        Assertions.assertEquals(Map.of("k", "a", "v", "b"), result);
    }

    @Test
    public void filesStarByPositionStillRequiresEqualArity() {
        // A file wider than the target makes the by-position pairing of the tail ambiguous
        // (the load trims), so the whole projection is declined rather than half-mapped.
        List<Column> targetCols = Arrays.asList(col("k"), col("v"));

        Map<String, String> result = resolvePerColumn(
                insertStmt(false), starRelation(), olapTable(targetCols, targetCols, false),
                filesTable(col("k"), col("v"), col("w")),
                Collections.singletonList(col("k")), Collections.emptyList());

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

    @Test
    public void filesStarByNameDeclinesAFileWiderThanTheTarget() {
        // INSERT INTO t BY NAME SELECT * FROM FILES(...) where the file also carries "extra".
        // BY NAME makes the query's OUTPUT names the target column names, and `SELECT *` over FILES
        // outputs every file column, so InsertAnalyzer fails this statement with
        // "Unknown column 'extra' in 't'". Pre-split runs BEFORE analysis, so admitting it would
        // reshard the target for a load that never runs.
        List<Column> targetCols = Arrays.asList(col("k"), col("v"));

        Map<String, String> result = resolvePerColumn(
                insertStmt(true), starRelation(), olapTable(targetCols, targetCols, false),
                filesTable(col("v"), col("k"), col("extra")),
                Collections.singletonList(col("k")), Collections.emptyList());

        Assertions.assertNull(result);
    }

    @Test
    public void filesStarByNameAdmitsAFileNarrowerThanTheTarget() {
        // The other direction stays admitted: every file column IS a target column, and the target
        // columns the file omits are simply defaulted by the load.
        List<Column> targetCols = Arrays.asList(col("k"), col("v"), col("w"));

        Map<String, String> result = resolvePerColumn(
                insertStmt(true), starRelation(), olapTable(targetCols, targetCols, false),
                filesTable(col("v"), col("k")),
                Collections.singletonList(col("k")), Collections.emptyList());

        Assertions.assertEquals(Map.of("k", "k", "v", "v"), result);
    }

    @Test
    public void filesStarByNameLeavesASortKeyTheFileLacksUnmapped() {
        // The file has no "k", so the load defaults that column for every row and no FILES column
        // can be sampled for it -> the final presence gate declines.
        List<Column> targetCols = Arrays.asList(col("k"), col("v"));

        Map<String, String> result = resolvePerColumn(
                insertStmt(true), starRelation(), olapTable(targetCols, targetCols, false), filesTable(col("v")),
                Collections.singletonList(col("k")), Collections.emptyList());

        Assertions.assertNull(result);
    }

    @Test
    public void filesStarByNameMapsANonKeyColumnTheFileLacks() {
        // Same shape, but the column the file lacks is not a key: the load defaults it and pre-split
        // is unaffected, so the projection is admitted with that column simply absent from the map.
        List<Column> targetCols = Arrays.asList(col("k"), col("v"));

        Map<String, String> result = resolvePerColumn(
                insertStmt(true), starRelation(), olapTable(targetCols, targetCols, false), filesTable(col("k")),
                Collections.singletonList(col("k")), Collections.emptyList());

        Assertions.assertEquals(Map.of("k", "k"), result);
    }

    @Test
    public void filesExplicitListByPositionMapsEachOutputToItsTargetOrdinal() {
        // INSERT INTO t(k, v) SELECT b, a FROM FILES(a, b) -- the load writes b into k and a into v.
        List<Column> targetCols = Arrays.asList(col("k"), col("v"));

        Map<String, String> result = resolvePerColumn(
                insertStmt(false), bareRelation(bareItem("b"), bareItem("a")),
                olapTable(targetCols, targetCols, false), filesTable(col("a"), col("b")),
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
                olapTable(targetCols, targetCols, false), filesTable(col("k"), col("v")),
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
                olapTable(targetCols, targetCols, false), filesTable(col("k"), col("v"), col("other")),
                Collections.singletonList(col("k")), Collections.emptyList());

        Assertions.assertEquals(Map.of("k", "k", "v", "v"), result);
    }

    @Test
    public void filesExplicitListByNameDeclinesAnOutputThatIsNoTargetColumn() {
        // INSERT INTO t BY NAME SELECT k, v AS not_a_target_col FROM FILES(...): only k needs to map
        // for the sort key, but BY NAME turns "not_a_target_col" into a target column name and
        // InsertAnalyzer rejects it with "Unknown column". A partial output set is legitimate here;
        // an output naming nothing on the target is not.
        List<Column> targetCols = Arrays.asList(col("k"), col("v"));

        Map<String, String> result = resolvePerColumn(
                insertStmt(true), bareRelation(bareItem("k"), bareItem("v", null, "not_a_target_col")),
                olapTable(targetCols, targetCols, false), filesTable(col("k"), col("v")),
                Collections.singletonList(col("k")), Collections.emptyList());

        Assertions.assertNull(result);
    }

    @Test
    public void filesExplicitListByNameStillRejectsDuplicateOutputNames() {
        // Two outputs named v leave the target position of each ambiguous, whatever the pairing mode.
        List<Column> targetCols = Arrays.asList(col("k"), col("v"));

        Map<String, String> result = resolvePerColumn(
                insertStmt(true), bareRelation(bareItem("k"), bareItem("v"), bareItem("v", null, "v")),
                olapTable(targetCols, targetCols, false), filesTable(col("k"), col("v")),
                Collections.singletonList(col("k")), Collections.emptyList());

        Assertions.assertNull(result);
    }

    // --- a partition column fed by a literal ---

    private static Column dateCol(String name) {
        return new Column(name, DateType.DATE);
    }

    @Test
    public void filesByNameRecordsALiteralFedPartitionColumnAsAConstant() {
        // The customer shape, parsed for real: dt lives in the directory name, not the file, so the
        // user writes it as a literal. Before, dt never entered the map and the presence gate
        // silently skipped the whole statement.
        InsertStmt stmt = (InsertStmt) SqlParser.parseSingleStatement(
                "INSERT INTO t BY NAME SELECT exp_id, grp_id, bucket_id, '20260917' AS dt "
                        + "FROM FILES(\"path\" = \"s3://b/dt=20260917/*\", \"format\" = \"parquet\")",
                SqlModeHelper.MODE_DEFAULT);
        SelectRelation rel = (SelectRelation) stmt.getQueryStatement().getQueryRelation();
        List<Column> targetCols = Arrays.asList(col("exp_id"), col("grp_id"), col("bucket_id"), dateCol("dt"));

        InsertSelectSourceColumns.Resolved resolved = resolvePerColumnFully(
                stmt, rel, targetCols, filesTable(col("exp_id"), col("grp_id"), col("bucket_id")),
                Collections.singletonList(col("exp_id")), Collections.singletonList(dateCol("dt")));

        Assertions.assertNotNull(resolved);
        Assertions.assertEquals(
                Map.of("exp_id", "exp_id", "grp_id", "grp_id", "bucket_id", "bucket_id"), resolved.targetToSource());
        Assertions.assertEquals(Map.of("dt", "'20260917'"), resolved.targetToConstantSql());
    }

    @Test
    public void tableByPositionRecordsALiteralFedPartitionColumnAsAConstant() {
        // INSERT INTO t SELECT k, v, '2026-09-17' FROM src -- the table path had the identical skip.
        // By position the literal needs no alias: its ordinal names the target column.
        List<Column> targetCols = Arrays.asList(col("k"), col("v"), dateCol("dt"));
        OlapTable target = olapTable(targetCols, targetCols, false);
        List<Column> sourceCols = Arrays.asList(col("k"), col("v"));
        OlapTable source = olapTable(sourceCols, sourceCols, false);
        SelectRelation rel = bareRelation(
                bareItem("k"), bareItem("v"), expressionItem(null, new StringLiteral("2026-09-17")));

        InsertSelectSourceColumns.Resolved resolved = resolveExactFully(
                insertStmt(false), rel, target, source, SRC_NAME, null,
                Collections.singletonList(col("k")), Collections.singletonList(dateCol("dt")));

        Assertions.assertNotNull(resolved);
        Assertions.assertEquals(Map.of("k", "k", "v", "v"), resolved.targetToSource());
        Assertions.assertEquals(Map.of("dt", "'2026-09-17'"), resolved.targetToConstantSql());
    }

    @Test
    public void literalLeadingACompoundSortKeyIsAdmitted() {
        // ORDER BY (dt, exp_id, grp_id, bucket_id) with dt = '20260917', the natural layout of a daily
        // table: every boundary is (2026-09-17, exp_id_k, ...), and the cuts come from the columns
        // that vary.
        InsertStmt stmt = (InsertStmt) SqlParser.parseSingleStatement(
                "INSERT INTO t BY NAME SELECT exp_id, grp_id, bucket_id, '20260917' AS dt "
                        + "FROM FILES(\"path\" = \"s3://b/dt=20260917/*\", \"format\" = \"parquet\")",
                SqlModeHelper.MODE_DEFAULT);
        SelectRelation rel = (SelectRelation) stmt.getQueryStatement().getQueryRelation();
        List<Column> targetCols = Arrays.asList(dateCol("dt"), col("exp_id"), col("grp_id"), col("bucket_id"));

        InsertSelectSourceColumns.Resolved resolved = resolvePerColumnFully(
                stmt, rel, targetCols, filesTable(col("exp_id"), col("grp_id"), col("bucket_id")),
                targetCols, Collections.singletonList(dateCol("dt")));

        Assertions.assertNotNull(resolved);
        Assertions.assertEquals(Map.of("dt", "'20260917'"), resolved.targetToConstantSql());
        Assertions.assertEquals(
                List.of("CAST('20260917' AS date)", "`exp_id`", "`grp_id`", "`bucket_id`"),
                InsertSelectSourceColumns.projections(
                        targetCols, resolved.targetToSource(), resolved.targetToConstantSql(), Map.of()));
    }

    @Test
    public void literalInTheMiddleOfASortKeyIsAdmitted() {
        List<Column> targetCols = Arrays.asList(col("k"), dateCol("dt"), col("v"));
        SelectRelation rel = bareRelation(
                bareItem("k"), expressionItem("dt", new StringLiteral("20260917")), bareItem("v"));

        Assertions.assertNotNull(resolvePerColumnFully(
                insertStmt(true), rel, targetCols, filesTable(col("k"), col("v")),
                targetCols, Collections.emptyList()));
    }

    @Test
    public void sortKeyMadeOnlyOfLiteralsIsDeclined() {
        // Every row carries the same key value, so no cut can separate them -- even when the column
        // is also the partition column.
        List<Column> targetCols = Arrays.asList(dateCol("dt"), col("v"));
        SelectRelation rel = bareRelation(expressionItem("dt", new StringLiteral("20260917")), bareItem("v"));

        Assertions.assertNull(resolvePerColumnFully(
                insertStmt(true), rel, targetCols, filesTable(col("v")),
                Collections.singletonList(dateCol("dt")), Collections.singletonList(dateCol("dt"))));
    }

    @Test
    public void partitionColumnComputedFromSourceColumnsIsStillDeclined() {
        // date_trunc('day', ts) AS dt is not a plan-time constant: the partition value varies per
        // row, and the sampler does not evaluate projections.
        List<Column> targetCols = Arrays.asList(col("k"), dateCol("dt"));
        FunctionCallExpr computed = new FunctionCallExpr("date_trunc",
                Arrays.asList(new StringLiteral("day"), new SlotRef((TableName) null, "ts")));
        SelectRelation rel = bareRelation(bareItem("k"), expressionItem("dt", computed));

        Assertions.assertNull(resolvePerColumnFully(
                insertStmt(true), rel, targetCols, filesTable(col("k"), col("ts")),
                Collections.singletonList(col("k")), Collections.singletonList(dateCol("dt"))));
    }

    @Test
    public void nonDeterministicPartitionValueIsDeclined() {
        // now() AS dt references no source column, but its value is not the one the load will see.
        List<Column> targetCols = Arrays.asList(col("k"), dateCol("dt"));
        SelectRelation rel = bareRelation(bareItem("k"),
                expressionItem("dt", new FunctionCallExpr("now", Collections.emptyList())));

        Assertions.assertNull(resolvePerColumnFully(
                insertStmt(true), rel, targetCols, filesTable(col("k")),
                Collections.singletonList(col("k")), Collections.singletonList(dateCol("dt"))));
    }

    @Test
    public void nullFedPartitionColumnIsDeclined() {
        List<Column> targetCols = Arrays.asList(col("k"), dateCol("dt"));
        SelectRelation rel = bareRelation(bareItem("k"), expressionItem("dt", new NullLiteral()));

        Assertions.assertNull(resolvePerColumnFully(
                insertStmt(true), rel, targetCols, filesTable(col("k")),
                Collections.singletonList(col("k")), Collections.singletonList(dateCol("dt"))));
    }

    @Test
    public void literalFedValueColumnIsAdmittedAsBefore() {
        // A literal feeding an ordinary value column is admitted as before. It is recorded with the
        // other constants, but no projection ever asks for it.
        List<Column> targetCols = Arrays.asList(col("k"), col("v"));
        SelectRelation rel = bareRelation(bareItem("k"), expressionItem("v", new StringLiteral("x")));

        InsertSelectSourceColumns.Resolved resolved = resolvePerColumnFully(
                insertStmt(true), rel, targetCols, filesTable(col("k")),
                Collections.singletonList(col("k")), Collections.emptyList());

        Assertions.assertNotNull(resolved);
        Assertions.assertEquals(Map.of("k", "k"), resolved.targetToSource());
    }

    @Test
    public void rollupSortKeyIsJudgedOnItsOwn() {
        // Each index's key is judged by the same rule, the base key (k) and every rollup's alike.
        // A rollup (dt, k) is sampleable, a rollup (dt) alone is not.
        InsertSelectSourceColumns.Resolved resolved =
                new InsertSelectSourceColumns.Resolved(Map.of("k", "k"), Map.of("dt", "'20260917'"), Map.of(), Set.of());

        Assertions.assertTrue(InsertSelectSourceColumns.sortKeySampleable(
                Arrays.asList(dateCol("dt"), col("k")), resolved));
        Assertions.assertFalse(InsertSelectSourceColumns.sortKeySampleable(
                Collections.singletonList(dateCol("dt")), resolved));
        Assertions.assertFalse(InsertSelectSourceColumns.sortKeySampleable(
                Arrays.asList(col("k"), col("unmapped")), resolved));
    }

    @Test
    public void projectionsCastALiteralToTheTargetColumnType() {
        // The load casts '20260917' to the DATE column before routing the row, so the grouper has to
        // see that DATE -- not the string -- or it pre-creates a partition the load never uses.
        List<String> projections = InsertSelectSourceColumns.projections(
                Arrays.asList(col("region"), dateCol("dt")),
                Map.of("region", "src_region"), Map.of("dt", "'20260917'"), Map.of());

        Assertions.assertEquals(List.of("`src_region`", "CAST('20260917' AS date)"), projections);
        Assertions.assertNull(InsertSelectSourceColumns.projections(
                Collections.singletonList(dateCol("dt")), Map.of(), Map.of(), Map.of()));
    }

    @Test
    public void partitionProjectionCastParsesForEveryPartitionColumnType() {
        // The cast is spliced into the sampling sub-query as SQL text, so the rendered type name must
        // parse back for each type a partition column can have.
        List<Type> partitionTypes = List.of(DateType.DATE, DateType.DATETIME, IntegerType.TINYINT,
                IntegerType.SMALLINT, IntegerType.INT, IntegerType.BIGINT, IntegerType.LARGEINT,
                TypeFactory.createVarcharType(64), TypeFactory.createCharType(8));
        for (Type type : partitionTypes) {
            String projection = InsertSelectSourceColumns.projections(
                    Collections.singletonList(new Column("p", type)), Map.of(), Map.of("p", "'1'"), Map.of()).get(0);
            Assertions.assertDoesNotThrow(() -> SqlParser.parseSingleStatement(
                    "SELECT " + projection + " FROM t", SqlModeHelper.MODE_DEFAULT), projection);
        }
    }

    // --- computed sampled projections (INSERT-from-FILES) ---

    private static final String FILES_CALL =
            "FILES(\"path\" = \"s3://b/d/*\", \"format\" = \"parquet\")";

    /** The FILES call with computed projections admitted, folded in a context pinned to 2026-09-23 UTC. */
    private static InsertSelectSourceColumns.Resolved resolveComputed(
            String selectList, boolean byName, List<Column> targetCols, Column... sourceCols) {
        return resolveComputed(selectList, byName, targetCols, computedProjectionContext(), sourceCols);
    }

    private static InsertSelectSourceColumns.Resolved resolveComputed(
            String selectList, boolean byName, List<Column> targetCols, ConnectContext context,
            Column... sourceCols) {
        return resolveComputed(SqlModeHelper.MODE_DEFAULT, selectList, byName, targetCols, context, sourceCols);
    }

    /**
     * {@link #resolveComputed} for a statement parsed in {@code parseSqlMode}. A SET_VAR hint that drops a mode from
     * the session leaves the parse in the session's mode, while {@code context} carries the hint's.
     */
    private static InsertSelectSourceColumns.Resolved resolveComputed(
            long parseSqlMode, String selectList, boolean byName, List<Column> targetCols, ConnectContext context,
            Column... sourceCols) {
        InsertStmt stmt = (InsertStmt) SqlParser.parseSingleStatement(
                "INSERT INTO t " + (byName ? "BY NAME " : "") + "SELECT " + selectList + " FROM " + FILES_CALL,
                parseSqlMode);
        SelectRelation rel = (SelectRelation) stmt.getQueryStatement().getQueryRelation();
        return InsertSelectSourceColumns.resolveUngated(
                stmt, rel, olapTable(targetCols, targetCols, false),
                new InsertSelectSourceColumns.ResolutionContext(filesTable(sourceCols), SRC_NAME, null,
                        InsertSelectSourceColumns.SchemaPairing.PER_COLUMN, context));
    }

    private static ConnectContext computedProjectionContext() {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        ctx.getSessionVariable().setTimeZone("UTC");
        ctx.setStartTime(Instant.parse("2026-09-23T10:00:00Z"));
        return ctx;
    }

    private static Column datetimeCol(String name) {
        return new Column(name, DateType.DATETIME);
    }

    @Test
    public void filesByNameAdmitsADateTruncSortKeyAndPartitionColumn() {
        // ORDER BY (dt, k) PARTITION BY (dt), dt = date_trunc('day', ts): the sampler evaluates the
        // same expression over the same FILES rows and casts it to DATE, as the load does.
        List<Column> targetCols = Arrays.asList(dateCol("dt"), col("k"));
        InsertSelectSourceColumns.Resolved resolved = resolveComputed(
                "k, date_trunc('day', ts) AS dt", /*byName*/ true, targetCols, col("k"), datetimeCol("ts"));

        Assertions.assertNotNull(resolved);
        Assertions.assertEquals(Map.of("k", "k"), resolved.targetToSource());
        Assertions.assertEquals(Map.of("dt", "date_trunc('day', `ts`)"), resolved.targetToExpressionSql());
        Assertions.assertTrue(resolved.targetToConstantSql().isEmpty());
        Assertions.assertTrue(resolved.unsupportedProjectionTargets().isEmpty());
        Assertions.assertNull(InsertSelectSourceColumns.firstUnsampleable(targetCols, resolved,
                InsertSelectSourceColumns.InputReading.AS_SELECTED));
        Assertions.assertNull(InsertSelectSourceColumns.firstUnsampleable(List.of(dateCol("dt")), resolved,
                InsertSelectSourceColumns.InputReading.AS_SELECTED));
        Assertions.assertEquals(List.of("CAST(date_trunc('day', `ts`) AS date)", "`k`"),
                InsertSelectSourceColumns.projections(targetCols, resolved.targetToSource(),
                        resolved.targetToConstantSql(), resolved.targetToExpressionSql()));
    }

    @Test
    public void filesByPositionAdmitsAnUnaliasedComputedColumn() {
        // By position the ordinal names the target column, so the expression needs no alias.
        List<Column> targetCols = Arrays.asList(col("k"), dateCol("dt"));
        InsertSelectSourceColumns.Resolved resolved = resolveComputed(
                "k, date_trunc('day', ts)", /*byName*/ false, targetCols, col("k"), datetimeCol("ts"));

        Assertions.assertNotNull(resolved);
        Assertions.assertEquals(Map.of("dt", "date_trunc('day', `ts`)"), resolved.targetToExpressionSql());
        Assertions.assertTrue(InsertSelectSourceColumns.sortKeySampleable(List.of(dateCol("dt")), resolved));
    }

    @Test
    public void sortKeyOfOnlyComputedColumnsVaries() {
        // A computed column reads a source column, so unlike a literal it is not a degenerate key --
        // even when no other output maps a source column directly.
        List<Column> targetCols = Collections.singletonList(dateCol("dt"));
        InsertSelectSourceColumns.Resolved resolved = resolveComputed(
                "date_trunc('day', ts) AS dt", /*byName*/ true, targetCols, datetimeCol("ts"));

        Assertions.assertNotNull(resolved);
        Assertions.assertTrue(resolved.targetToSource().isEmpty());
        Assertions.assertTrue(InsertSelectSourceColumns.sortKeySampleable(targetCols, resolved));
    }

    @Test
    public void computedProjectionKeepsTheUsersCastUnderTheTargetCast() {
        List<Column> targetCols = Arrays.asList(col("k"), new Column("v", TypeFactory.createVarcharType(16)));
        InsertSelectSourceColumns.Resolved resolved = resolveComputed(
                "k, CAST(k * 10 AS BIGINT) AS v", /*byName*/ true, targetCols, col("k"));

        Assertions.assertNotNull(resolved);
        String projection = InsertSelectSourceColumns.projections(
                Collections.singletonList(targetCols.get(1)), resolved.targetToSource(),
                resolved.targetToConstantSql(), resolved.targetToExpressionSql()).get(0);
        Assertions.assertEquals("CAST(CAST((`k` * 10) AS BIGINT) AS varchar(16))", projection);
        // The projection is spliced into the sampling sub-query as SQL text, so it must re-parse.
        Assertions.assertDoesNotThrow(() -> SqlParser.parseSingleStatement(
                "SELECT " + projection + " FROM t", SqlModeHelper.MODE_DEFAULT), projection);
    }

    @Test
    public void computedProjectionFoldsItsPlanTimeConstantsInTheUsersContext() {
        // The ROOT sampling context has its own clock and time zone, so current_date() must reach it
        // as the literal the INSERT folds, not as the call.
        List<Column> targetCols = Arrays.asList(col("k"), new Column("region", TypeFactory.createVarcharType(8)));
        InsertSelectSourceColumns.Resolved resolved = resolveComputed(
                "k, if(ts >= date_sub(current_date(), 1), 'recent', 'old') AS region", /*byName*/ true,
                targetCols, col("k"), datetimeCol("ts"));

        Assertions.assertNotNull(resolved);
        Assertions.assertEquals(
                "if(`ts` >= (CAST('2026-09-22 00:00:00' AS DATETIME)), 'recent', 'old')",
                resolved.targetToExpressionSql().get("region"));
    }

    @Test
    public void columnFreeComputedProjectionFoldsToAConstant() {
        // current_date() AS dt reads no column: folded in the user's context it is one value for every
        // row, i.e. a constant, and a key made only of it stays degenerate.
        List<Column> targetCols = Arrays.asList(col("k"), dateCol("dt"));
        InsertSelectSourceColumns.Resolved resolved = resolveComputed(
                "k, current_date() AS dt", /*byName*/ true, targetCols, col("k"));

        Assertions.assertNotNull(resolved);
        Assertions.assertEquals(Map.of("dt", "CAST('2026-09-23' AS DATE)"), resolved.targetToConstantSql());
        Assertions.assertTrue(resolved.targetToExpressionSql().isEmpty());
        Assertions.assertFalse(InsertSelectSourceColumns.sortKeySampleable(List.of(dateCol("dt")), resolved));
    }

    @Test
    public void unsafeComputedProjectionsStayUnsupported() {
        // Each depends on something the ROOT sampling context does not share with the INSERT (session
        // time zone, user, variables, a per-row random value), is not a vetted built-in, binds no built-in
        // (no built-in upper takes two arguments), or yields NULL.
        List<String> unsafe = List.of(
                "from_unixtime(k)",
                "unix_timestamp(ts)",
                "convert_tz(ts, 'UTC', 'Asia/Shanghai')",
                "concat(k, current_user())",
                "concat(k, @suffix)",
                "concat(k, @@time_zone)",
                "date_add(ts, INTERVAL rand() DAY)",
                "md5(ts)",
                "db1.my_udf(ts)",
                "CAST(NULL AS DATE)",
                "uuid()",
                "upper(k, k)");
        List<Column> targetCols = Arrays.asList(col("k"), dateCol("dt"));
        for (String expression : unsafe) {
            InsertSelectSourceColumns.Resolved resolved = resolveComputed(
                    "k, " + expression + " AS dt", /*byName*/ true, targetCols, col("k"), datetimeCol("ts"));

            Assertions.assertNotNull(resolved, expression);
            Assertions.assertEquals(Set.of("dt"), resolved.unsupportedProjectionTargets(), expression);
            Assertions.assertTrue(resolved.targetToExpressionSql().isEmpty(), expression);
            Assertions.assertTrue(resolved.targetToConstantSql().isEmpty(), expression);
            Assertions.assertEquals("dt",
                    InsertSelectSourceColumns.firstUnsampleable(targetCols, resolved,
                            InsertSelectSourceColumns.InputReading.AS_SELECTED).column().getName(), expression);
        }
    }

    @Test
    public void subqueryInAComputedProjectionRejectsTheStatement() {
        // A subquery reads a relation the hook never resolved or authorized: not an unsupported key,
        // but a statement the hook must not act on at all.
        Assertions.assertNull(resolveComputed("k, date_trunc('day', (SELECT max(ts) FROM other)) AS dt",
                /*byName*/ true, Arrays.asList(col("k"), dateCol("dt")), col("k"), datetimeCol("ts")));
    }

    @Test
    public void withoutAContextNoComputedProjectionIsAdmitted() {
        // A caller that passes no context admits no computed projection.
        List<Column> targetCols = Arrays.asList(col("k"), dateCol("dt"));
        InsertSelectSourceColumns.Resolved resolved = resolveComputed(
                "k, date_trunc('day', ts) AS dt", /*byName*/ true, targetCols, (ConnectContext) null,
                col("k"), datetimeCol("ts"));

        Assertions.assertNotNull(resolved);
        Assertions.assertEquals(Set.of("dt"), resolved.unsupportedProjectionTargets());
        Assertions.assertTrue(resolved.targetToExpressionSql().isEmpty());
    }

    // --- generated sampled columns ---

    /** A target whose non-generated base schema is {@code baseCols} and that also carries {@code generatedCols}. */
    private static OlapTable generatedTarget(List<Column> baseCols, Column... generatedCols) {
        OlapTable target = olapTable(baseCols, baseCols, true);
        stubGeneratedSchema(target, baseCols, generatedCols);
        return target;
    }

    /**
     * {@code resolveUngated} plus {@code withGeneratedColumns} over a FILES-shaped statement, with the inputs
     * handed over as {@code reading} says.
     */
    private static InsertSelectSourceColumns.Resolved resolveWithGenerated(
            String selectList, OlapTable target, List<Column> sampledColumns,
            InsertSelectSourceColumns.InputReading reading, Column... sourceCols) {
        return resolveWithGenerated(computedProjectionContext(), selectList, target, sampledColumns, reading,
                sourceCols);
    }

    /** {@link #resolveWithGenerated} for a user whose session is {@code context}; the statement is parsed in its sql_mode. */
    private static InsertSelectSourceColumns.Resolved resolveWithGenerated(
            ConnectContext context, String selectList, OlapTable target, List<Column> sampledColumns,
            InsertSelectSourceColumns.InputReading reading, Column... sourceCols) {
        InsertStmt stmt = (InsertStmt) SqlParser.parseSingleStatement("INSERT INTO t BY NAME SELECT " + selectList
                + " FROM " + FILES_CALL, context.getSessionVariable().getSqlMode());
        SelectRelation rel = (SelectRelation) stmt.getQueryStatement().getQueryRelation();
        InsertSelectSourceColumns.Resolved resolved = InsertSelectSourceColumns.resolveUngated(
                stmt, rel, target,
                new InsertSelectSourceColumns.ResolutionContext(filesTable(sourceCols), SRC_NAME, null,
                        InsertSelectSourceColumns.SchemaPairing.PER_COLUMN, context));
        return InsertSelectSourceColumns.withGeneratedColumns(
                resolved, target, sampledColumns, reading, context, SRC_NAME, null);
    }

    /** {@link #resolveWithGenerated} with the inputs bound as selected, as INSERT from a table binds them. */
    private static InsertSelectSourceColumns.Resolved resolveWithGenerated(
            String selectList, OlapTable target, List<Column> sampledColumns, Column... sourceCols) {
        return resolveWithGenerated(selectList, target, sampledColumns,
                InsertSelectSourceColumns.InputReading.AS_SELECTED, sourceCols);
    }

    private static final Column ACCOUNT_ID = col("account_id");
    private static final Column ACTIVITY_MONTH = activityMonth();

    @Test
    public void generatedPartitionColumnIsComputedFromTheSourceColumnItReads() {
        OlapTable target = generatedTarget(List.of(ACCOUNT_ID, ACTIVITY_DATE), ACTIVITY_MONTH);

        InsertSelectSourceColumns.Resolved resolved = resolveWithGenerated("account_id, activity_date", target,
                List.of(ACCOUNT_ID, ACTIVITY_MONTH), col("account_id"), datetimeCol("activity_date"));

        // The reference reads the value the load stores in activity_date: its source column cast to
        // the target column's type.
        Assertions.assertEquals(
                Map.of("activity_date_month", "date_trunc('month', CAST(`activity_date` AS DATETIME))"),
                resolved.targetToExpressionSql());
        Assertions.assertTrue(resolved.generatedColumnFailures().isEmpty());
        Assertions.assertNull(InsertSelectSourceColumns.firstUnsampleable(List.of(ACCOUNT_ID, ACTIVITY_MONTH), resolved,
                InsertSelectSourceColumns.InputReading.AS_SELECTED));
        String projection = InsertSelectSourceColumns.projections(List.of(ACTIVITY_MONTH), resolved.targetToSource(),
                resolved.targetToConstantSql(), resolved.targetToExpressionSql()).get(0);
        Assertions.assertEquals(
                "CAST(date_trunc('month', CAST(`activity_date` AS DATETIME)) AS datetime)", projection);
        Assertions.assertDoesNotThrow(() -> SqlParser.parseSingleStatement(
                "SELECT " + projection + " FROM t", SqlModeHelper.MODE_DEFAULT), projection);
    }

    @Test
    public void generatedColumnOverALiteralFoldsToAConstant() {
        OlapTable target = generatedTarget(List.of(ACCOUNT_ID, ACTIVITY_DATE), ACTIVITY_MONTH);

        InsertSelectSourceColumns.Resolved resolved = resolveWithGenerated(
                "account_id, '2025-11-15 10:00:00' AS activity_date", target,
                List.of(ACCOUNT_ID, ACTIVITY_MONTH), col("account_id"));

        Assertions.assertEquals("CAST('2025-11-01 00:00:00' AS DATETIME)",
                resolved.targetToConstantSql().get("activity_date_month"));
        Assertions.assertFalse(resolved.targetToExpressionSql().containsKey("activity_date_month"));
    }

    @Test
    public void generatedColumnOverALiteralThatFoldsToNullIsDeclined() {
        // A literal that does not convert to the input's DATETIME makes the definition NULL for every row.
        OlapTable target = generatedTarget(List.of(ACCOUNT_ID, ACTIVITY_DATE), ACTIVITY_MONTH);

        InsertSelectSourceColumns.Resolved resolved = resolveWithGenerated(
                "account_id, 'not a date' AS activity_date", target,
                List.of(ACCOUNT_ID, ACTIVITY_MONTH), col("account_id"));

        InsertSelectSourceColumns.Unsampleable unsampleable = InsertSelectSourceColumns.firstUnsampleable(
                List.of(ACTIVITY_MONTH), resolved, InsertSelectSourceColumns.InputReading.AS_SELECTED);
        Assertions.assertEquals(SkipReason.UNSUPPORTED_GENERATED_COLUMN, unsampleable.reason());
        Assertions.assertTrue(unsampleable.detail().contains("does not fold to a non-NULL constant"),
                unsampleable.detail());
    }

    @Test
    public void generatedColumnOverAComputedProjectionNestsThatExpression() {
        OlapTable target = generatedTarget(List.of(ACCOUNT_ID, ACTIVITY_DATE), ACTIVITY_MONTH);

        InsertSelectSourceColumns.Resolved resolved = resolveWithGenerated(
                "account_id, CAST(ts AS DATETIME) AS activity_date", target,
                List.of(ACCOUNT_ID, ACTIVITY_MONTH), col("account_id"), datetimeCol("ts"));

        Assertions.assertEquals("date_trunc('month', CAST((CAST(`ts` AS DATETIME)) AS DATETIME))",
                resolved.targetToExpressionSql().get("activity_date_month"));
    }

    @Test
    public void generatedColumnWhoseInputTheSourceLacksIsAttributedToTheGeneratedColumn() {
        OlapTable target = generatedTarget(List.of(ACCOUNT_ID, ACTIVITY_DATE), ACTIVITY_MONTH);

        InsertSelectSourceColumns.Resolved resolved = resolveWithGenerated("account_id", target,
                List.of(ACCOUNT_ID, ACTIVITY_MONTH), col("account_id"));

        InsertSelectSourceColumns.Unsampleable unsampleable =
                InsertSelectSourceColumns.firstUnsampleable(List.of(ACCOUNT_ID, ACTIVITY_MONTH), resolved,
                        InsertSelectSourceColumns.InputReading.AS_SELECTED);
        Assertions.assertNotNull(unsampleable);
        Assertions.assertEquals("activity_date_month", unsampleable.column().getName());
        Assertions.assertEquals(SkipReason.UNSUPPORTED_GENERATED_COLUMN, unsampleable.reason());
        Assertions.assertTrue(unsampleable.detail().contains("\"activity_date\""), unsampleable.detail());
        Assertions.assertTrue(unsampleable.detail().contains("not take from the source"), unsampleable.detail());
    }

    @Test
    public void generatedColumnWhoseInputIsAnUnsupportedProjectionNamesThatProjection() {
        OlapTable target = generatedTarget(List.of(ACCOUNT_ID, ACTIVITY_DATE), ACTIVITY_MONTH);

        InsertSelectSourceColumns.Resolved resolved = resolveWithGenerated(
                "account_id, from_unixtime(account_id) AS activity_date", target,
                List.of(ACCOUNT_ID, ACTIVITY_MONTH), col("account_id"));

        InsertSelectSourceColumns.Unsampleable unsampleable =
                InsertSelectSourceColumns.firstUnsampleable(List.of(ACTIVITY_MONTH), resolved,
                        InsertSelectSourceColumns.InputReading.AS_SELECTED);
        Assertions.assertEquals(SkipReason.UNSUPPORTED_GENERATED_COLUMN, unsampleable.reason());
        Assertions.assertTrue(unsampleable.detail().contains("cannot reproduce"), unsampleable.detail());
    }

    @Test
    public void generatedColumnTheSamplerCannotEvaluateIsDeclined() {
        Column payload = new Column("payload", TypeFactory.createVarcharType(64), true);
        Column payloadKey = generatedColumn("payload_key", TypeFactory.createVarcharType(64),
                "get_json_string(payload, '$.k')", List.of(ACCOUNT_ID, payload));
        OlapTable target = generatedTarget(List.of(ACCOUNT_ID, payload), payloadKey);

        InsertSelectSourceColumns.Resolved resolved = resolveWithGenerated("account_id, payload", target,
                List.of(payloadKey), col("account_id"), new Column("payload", TypeFactory.createVarcharType(64)));

        InsertSelectSourceColumns.Unsampleable unsampleable =
                InsertSelectSourceColumns.firstUnsampleable(List.of(payloadKey), resolved,
                        InsertSelectSourceColumns.InputReading.AS_SELECTED);
        Assertions.assertEquals(SkipReason.UNSUPPORTED_GENERATED_COLUMN, unsampleable.reason());
        Assertions.assertTrue(unsampleable.detail().contains("cannot evaluate"), unsampleable.detail());
    }

    @Test
    public void unsampledGeneratedColumnIsIgnored() {
        // A generated column outside the sort key, partition columns and rollup keys never reaches
        // the sampler, so an expression the sampler could not evaluate must not decline the load.
        Column payload = new Column("payload", TypeFactory.createVarcharType(64), true);
        Column payloadKey = generatedColumn("payload_key", TypeFactory.createVarcharType(64),
                "get_json_string(payload, '$.k')", List.of(ACCOUNT_ID, payload));
        OlapTable target = generatedTarget(List.of(ACCOUNT_ID, payload), payloadKey);

        InsertSelectSourceColumns.Resolved resolved = resolveWithGenerated("account_id, payload", target,
                List.of(ACCOUNT_ID), col("account_id"), new Column("payload", TypeFactory.createVarcharType(64)));

        Assertions.assertTrue(resolved.generatedColumnFailures().isEmpty());
        Assertions.assertFalse(resolved.targetToExpressionSql().containsKey("payload_key"));
        Assertions.assertNull(InsertSelectSourceColumns.firstUnsampleable(List.of(ACCOUNT_ID), resolved,
                InsertSelectSourceColumns.InputReading.AS_SELECTED));
    }

    @Test
    public void definitionReadingAnInputTwiceReplacesEveryOccurrence() {
        // Stored with a table qualifier, as a DDL may write it: the sampler SQL must carry no
        // qualifier, and both references must read the source column.
        Column month = generatedColumn("activity_date_month", DateType.DATETIME,
                "if(t.activity_date IS NULL, NULL, date_trunc('month', t.activity_date))",
                List.of(ACCOUNT_ID, ACTIVITY_DATE));
        OlapTable target = generatedTarget(List.of(ACCOUNT_ID, ACTIVITY_DATE), month);

        InsertSelectSourceColumns.Resolved resolved = resolveWithGenerated("account_id, activity_date", target,
                List.of(month), col("account_id"), datetimeCol("activity_date"));

        String sql = resolved.targetToExpressionSql().get("activity_date_month");
        Assertions.assertNotNull(sql, resolved.generatedColumnFailures().toString());
        Assertions.assertFalse(sql.contains("`t`"), sql);
        Assertions.assertEquals(2, sql.split("CAST\\(`activity_date` AS DATETIME\\)", -1).length - 1, sql);
    }

    @Test
    public void substitutionUsesTheSourceColumnsOwnSpelling() {
        OlapTable target = generatedTarget(List.of(ACCOUNT_ID, ACTIVITY_DATE), ACTIVITY_MONTH);

        InsertSelectSourceColumns.Resolved resolved = resolveWithGenerated("account_id, Activity_Date", target,
                List.of(ACTIVITY_MONTH), col("account_id"), datetimeCol("Activity_Date"));

        Assertions.assertEquals("date_trunc('month', CAST(`Activity_Date` AS DATETIME))",
                resolved.targetToExpressionSql().get("activity_date_month"));
    }

    // INSERT binds a generated column's inputs to the SELECT output as selected and converts each only to the
    // parameter type of the function that reads it (InsertPlanner#fillGeneratedColumns + ImplicitCastRule).
    // The sampler converts every input to its column type, so a type mismatch is admitted only where both
    // conversions provably give the same argument.

    @Test
    public void dateInputOfADatetimeFunctionParameterIsAdmitted() {
        // source d DATE, column activity_date DATETIME: date_trunc's parameter is DATETIME, the column's own
        // type, so INSERT converts the DATE straight to DATETIME -- the same value the sampler's cast yields.
        OlapTable target = generatedTarget(List.of(ACCOUNT_ID, ACTIVITY_DATE), ACTIVITY_MONTH);

        InsertSelectSourceColumns.Resolved resolved = resolveWithGenerated("account_id, activity_date", target,
                List.of(ACTIVITY_MONTH), col("account_id"), dateCol("activity_date"));

        Assertions.assertEquals("date_trunc('month', CAST(`activity_date` AS DATETIME))",
                resolved.targetToExpressionSql().get("activity_date_month"), resolved.generatedColumnFailures().toString());
    }

    @Test
    public void widenedInputThatAFunctionStringifiesIsDeclined() {
        // source n INT, column n DECIMAL(12,2), g AS concat(n, ''): INSERT converts the INT straight to concat's
        // VARCHAR parameter and writes '1'; the sampler would cast to DECIMAL first and compute '1.00'.
        Column n = new Column("n", TypeFactory.createUnifiedDecimalType(12, 2), true);
        Column g = generatedColumn("g", TypeFactory.createVarcharType(64), "concat(n, '')", List.of(ACCOUNT_ID, n));
        OlapTable target = generatedTarget(List.of(ACCOUNT_ID, n), g);

        InsertSelectSourceColumns.Resolved resolved = resolveWithGenerated("account_id, n", target,
                List.of(g), col("account_id"), col("n"));

        InsertSelectSourceColumns.Unsampleable unsampleable =
                InsertSelectSourceColumns.firstUnsampleable(List.of(g), resolved,
                        InsertSelectSourceColumns.InputReading.AS_SELECTED);
        Assertions.assertNotNull(unsampleable);
        Assertions.assertEquals(SkipReason.UNSUPPORTED_GENERATED_COLUMN, unsampleable.reason());
        Assertions.assertTrue(unsampleable.detail().contains("\"n\""), unsampleable.detail());
        Assertions.assertTrue(unsampleable.detail().contains("cannot reproduce that conversion"), unsampleable.detail());
    }

    @Test
    public void widenedInputIsAdmittedWhenTheLoadConvertsItToTheColumnTypeFirst() {
        // Broker Load converts the input to the column type before the definition reads it, which is exactly what
        // the sampler does.
        Column n = new Column("n", TypeFactory.createUnifiedDecimalType(12, 2), true);
        Column g = generatedColumn("g", TypeFactory.createVarcharType(64), "concat(n, '')", List.of(ACCOUNT_ID, n));
        OlapTable target = generatedTarget(List.of(ACCOUNT_ID, n), g);

        InsertSelectSourceColumns.Resolved resolved = resolveWithGenerated("account_id, n", target,
                List.of(g), InsertSelectSourceColumns.InputReading.AS_COLUMN_TYPE, col("account_id"), col("n"));

        Assertions.assertTrue(resolved.generatedColumnFailures().isEmpty(), resolved.generatedColumnFailures().toString());
        Assertions.assertTrue(resolved.targetToExpressionSql().containsKey("g"));
    }

    @Test
    public void integerWideningIsAdmittedWhereverTheInputIsRead() {
        // INT to BIGINT keeps both the value and its text, so even a stringifying definition reads the same.
        Column n = new Column("n", IntegerType.BIGINT, true);
        Column g = generatedColumn("g", TypeFactory.createVarcharType(64), "concat(n, '')", List.of(ACCOUNT_ID, n));
        OlapTable target = generatedTarget(List.of(ACCOUNT_ID, n), g);

        InsertSelectSourceColumns.Resolved resolved = resolveWithGenerated("account_id, n", target,
                List.of(g), col("account_id"), col("n"));

        Assertions.assertTrue(resolved.generatedColumnFailures().isEmpty(), resolved.generatedColumnFailures().toString());
    }

    @Test
    public void inputOfAnOperatorTakingTheColumnTypeIsAdmitted() {
        // source x INT, column x DOUBLE, g AS x * 2: the multiply resolves to DOUBLE, the column's own type, so
        // INSERT converts the INT straight to DOUBLE -- the value the sampler's cast yields.
        Column x = new Column("x", FloatType.DOUBLE, true);
        Column g = generatedColumn("g", FloatType.DOUBLE, "x * 2", List.of(ACCOUNT_ID, x));
        OlapTable target = generatedTarget(List.of(ACCOUNT_ID, x), g);

        InsertSelectSourceColumns.Resolved resolved = resolveWithGenerated("account_id, x", target,
                List.of(g), col("account_id"), col("x"));

        Assertions.assertEquals("(CAST(`x` AS DOUBLE)) * 2", resolved.targetToExpressionSql().get("g"),
                resolved.generatedColumnFailures().toString());
    }

    @Test
    public void inputOfAnOperatorTakingAnotherTypeIsDeclined() {
        // source x DOUBLE, column x INT, g AS x + 1: the add resolves to BIGINT, not the column's INT, so INSERT
        // converts the DOUBLE straight to BIGINT while the sampler would cast it to INT first.
        Column x = new Column("x", IntegerType.INT, true);
        Column g = generatedColumn("g", IntegerType.BIGINT, "x + 1", List.of(ACCOUNT_ID, x));
        OlapTable target = generatedTarget(List.of(ACCOUNT_ID, x), g);

        InsertSelectSourceColumns.Resolved resolved = resolveWithGenerated("account_id, x", target,
                List.of(g), col("account_id"), new Column("x", FloatType.DOUBLE));

        InsertSelectSourceColumns.Unsampleable unsampleable = InsertSelectSourceColumns.firstUnsampleable(
                List.of(g), resolved, InsertSelectSourceColumns.InputReading.AS_SELECTED);
        Assertions.assertEquals(SkipReason.UNSUPPORTED_GENERATED_COLUMN, unsampleable.reason());
        Assertions.assertTrue(unsampleable.detail().contains("\"x\""), unsampleable.detail());
        Assertions.assertTrue(unsampleable.detail().contains("only where a function needs it"), unsampleable.detail());
    }

    @Test
    public void definitionThatDoesNotAnalyzeOnItsOwnIsDeclined() {
        // The conversion check analyzes the definition against the target's own columns; here the target's schema
        // does not list x, so the definition does not analyze and the conversion cannot be checked.
        Column x = new Column("x", FloatType.DOUBLE, true);
        Column g = generatedColumn("g", FloatType.DOUBLE, "x * 2", List.of(ACCOUNT_ID, x));
        OlapTable target = generatedTarget(List.of(ACCOUNT_ID, x), g);
        when(target.getBaseSchema()).thenReturn(List.of(ACCOUNT_ID, g));

        InsertSelectSourceColumns.Resolved resolved = resolveWithGenerated("account_id, x", target,
                List.of(g), col("account_id"), col("x"));

        InsertSelectSourceColumns.Unsampleable unsampleable = InsertSelectSourceColumns.firstUnsampleable(
                List.of(g), resolved, InsertSelectSourceColumns.InputReading.AS_SELECTED);
        Assertions.assertEquals(SkipReason.UNSUPPORTED_GENERATED_COLUMN, unsampleable.reason());
        Assertions.assertTrue(unsampleable.detail().contains("does not analyze on its own"), unsampleable.detail());
    }

    @Test
    public void definitionComparingAStringWithANumberIsAdmitted() {
        // s = 1 converts both sides to the type cbo_eq_base_type names. The sample session carries the load's value
        // of that variable, so the sampler compares as the load does.
        Column k = col("k");
        Column s = new Column("s", VarcharType.VARCHAR, true);
        Column g = generatedColumn("g", IntegerType.BIGINT, "if(s = 1, k, k + 1000000)", List.of(ACCOUNT_ID, k, s));
        OlapTable target = generatedTarget(List.of(ACCOUNT_ID, k, s), g);

        InsertSelectSourceColumns.Resolved resolved = resolveWithGenerated("account_id, k, s", target,
                List.of(g), col("account_id"), col("k"), new Column("s", VarcharType.VARCHAR));

        Assertions.assertNull(InsertSelectSourceColumns.firstUnsampleable(
                List.of(g), resolved, InsertSelectSourceColumns.InputReading.AS_SELECTED),
                resolved.generatedColumnFailures().toString());
        Assertions.assertTrue(resolved.targetToExpressionSql().containsKey("g"), resolved.toString());
    }

    @Test
    public void callBoundToAnythingButABuiltinIsDeclined() {
        // The first call, nested or not, that does not bind to a built-in is named. Which bindings count is
        // SamplingPredicateGateTest's.
        Function builtin = mock(Function.class);
        when(builtin.getBinaryType()).thenReturn(TFunctionBinaryType.BUILTIN);
        Function javaUdf = mock(Function.class);
        when(javaUdf.getBinaryType()).thenReturn(TFunctionBinaryType.SRJAR);
        FunctionCallExpr inner = new FunctionCallExpr("upper", List.of(new SlotRef((TableName) null, "k")));
        inner.setFn(javaUdf);
        FunctionCallExpr outer = new FunctionCallExpr("concat", List.of(inner, new StringLiteral("-")));
        outer.setFn(builtin);
        Assertions.assertEquals("calls upper, which does not bind to a built-in function, so the sample session may "
                + "resolve it differently", InsertSelectSourceColumns.sessionDependentPart(outer));
        outer.setFn(javaUdf);
        Assertions.assertEquals("calls concat, which does not bind to a built-in function, so the sample session may "
                + "resolve it differently", InsertSelectSourceColumns.sessionDependentPart(outer));

        outer.setFn(builtin);
        inner.setFn(builtin);
        Assertions.assertNull(InsertSelectSourceColumns.sessionDependentPart(outer));
    }

    /** A user session whose sql_mode reads a decimal literal as a DOUBLE. */
    private static ConnectContext doubleLiteralContext() {
        ConnectContext context = computedProjectionContext();
        context.getSessionVariable().setSqlMode(SqlModeHelper.MODE_DEFAULT | SqlModeHelper.MODE_DOUBLE_LITERAL);
        return context;
    }

    /** The first unsampleable of {@code g} when {@code target} is loaded from (account_id, k) in {@code context}. */
    private static InsertSelectSourceColumns.Unsampleable unsampleableIn(ConnectContext context, OlapTable target,
                                                                         Column g) {
        return InsertSelectSourceColumns.firstUnsampleable(List.of(g),
                resolveWithGenerated(context, "account_id, k", target, List.of(g),
                        InsertSelectSourceColumns.InputReading.AS_SELECTED, col("account_id"), col("k")),
                InsertSelectSourceColumns.InputReading.AS_SELECTED);
    }

    @Test
    public void definitionWithADecimalLiteralIsDeclinedUnderTheLoadsDoubleLiteral() {
        // A stored definition was parsed in the default sql_mode: its 1.5 is a DECIMAL32(2,1). The sampler parses the
        // rendered SQL in the load's sql_mode, where DOUBLE_LITERAL reads it as a DOUBLE. Without DOUBLE_LITERAL the
        // rendered expression is what it has always been.
        Column k = col("k");
        Column g = generatedColumn("g", IntegerType.BIGINT, "if(k > 1.5, k, 0)", List.of(ACCOUNT_ID, k));
        OlapTable target = generatedTarget(List.of(ACCOUNT_ID, k), g);

        InsertSelectSourceColumns.Unsampleable unsampleable = unsampleableIn(doubleLiteralContext(), target, g);
        Assertions.assertNotNull(unsampleable);
        Assertions.assertEquals(SkipReason.UNSUPPORTED_GENERATED_COLUMN, unsampleable.reason());
        Assertions.assertTrue(unsampleable.detail().contains("DOUBLE_LITERAL"), unsampleable.detail());

        InsertSelectSourceColumns.Resolved admitted = resolveWithGenerated("account_id, k", target, List.of(g),
                col("account_id"), col("k"));
        Assertions.assertEquals("if((CAST(`k` AS INT)) > 1.5, CAST(`k` AS INT), 0)",
                admitted.targetToExpressionSql().get("g"), admitted.generatedColumnFailures().toString());
    }

    @Test
    public void computedProjectionWithADecimalLiteralReadBackAsADoubleIsNotAdmitted() {
        // CAST('2.5' AS DECIMAL(10, 1)) folds to CAST(2.5 AS DECIMAL64(10,1)): without DOUBLE_LITERAL its 2.5 reads back
        // as a decimal of the same value, which the CAST converts as before; under DOUBLE_LITERAL it reads back as a
        // DOUBLE. An integer wider than LARGEINT renders as 100...0E0, which reads back as a DOUBLE in every sql_mode.
        List<Column> targetCols = Arrays.asList(col("k"), new Column("dt", FloatType.DOUBLE),
                new Column("v", FloatType.DOUBLE), new Column("w", FloatType.DOUBLE));
        String selectList = "k, CAST('2.5' AS DECIMAL(10, 1)) AS dt, k * CAST(concat('2', '.5') AS DECIMAL(10, 1)) AS v, "
                + "100000000000000000000000000000000000000000 AS w";

        InsertSelectSourceColumns.Resolved declined = resolveComputed(selectList, /*byName*/ true, targetCols,
                doubleLiteralContext(), col("k"));
        Assertions.assertEquals(Set.of("dt", "v", "w"), declined.unsupportedProjectionTargets());

        InsertSelectSourceColumns.Resolved admitted = resolveComputed(selectList, /*byName*/ true, targetCols, col("k"));
        Assertions.assertEquals(Map.of("dt", "CAST(2.5 AS DECIMAL64(10,1))"), admitted.targetToConstantSql());
        Assertions.assertEquals(Map.of("v", "`k` * (CAST(2.5 AS DECIMAL64(10,1)))"), admitted.targetToExpressionSql());
        Assertions.assertEquals(Set.of("w"), admitted.unsupportedProjectionTargets());
    }

    @Test
    public void inputFedByADecimalLiteralIsReadAsTheLoadParsedIt() {
        // Under DOUBLE_LITERAL the SELECT's 3.14159265358979 is a DOUBLE, d's own type; re-read in the default sql_mode
        // it would be a DECIMAL that concat reads unconverted, and g would be declined.
        Column d = new Column("d", FloatType.DOUBLE, true);
        Column g = generatedColumn("g", VarcharType.VARCHAR, "concat(d, '')", List.of(ACCOUNT_ID, d));
        OlapTable target = generatedTarget(List.of(ACCOUNT_ID, d), g);

        InsertSelectSourceColumns.Resolved resolved = resolveWithGenerated(doubleLiteralContext(),
                "account_id, 3.14159265358979 AS d", target, List.of(g),
                InsertSelectSourceColumns.InputReading.AS_SELECTED, col("account_id"));

        Assertions.assertNull(InsertSelectSourceColumns.firstUnsampleable(
                List.of(g), resolved, InsertSelectSourceColumns.InputReading.AS_SELECTED),
                resolved.generatedColumnFailures().toString());
    }

    @Test
    public void literalTheParserReadAsADoubleIsAdmittedUnderItsCastOnceAHintDropsDoubleLiteral() {
        // A SET_VAR hint drops the session's DOUBLE_LITERAL: 0.5 was parsed as a FLOAT the sampler would read back as a
        // DECIMAL, so it goes through the computed-projection branch, where its CAST restores the same value.
        long sessionSqlMode = SqlModeHelper.MODE_DEFAULT | SqlModeHelper.MODE_DOUBLE_LITERAL;
        List<Column> targetCols = Arrays.asList(col("k"), new Column("v", FloatType.DOUBLE));
        String selectList = "k, 0.5 AS v";

        InsertSelectSourceColumns.Resolved sameSqlMode = resolveComputed(sessionSqlMode, selectList, /*byName*/ true,
                targetCols, doubleLiteralContext(), col("k"));
        Assertions.assertEquals(Map.of("v", "0.5"), sameSqlMode.targetToConstantSql());

        InsertSelectSourceColumns.Resolved hintDropsDoubleLiteral = resolveComputed(sessionSqlMode, selectList,
                /*byName*/ true, targetCols, computedProjectionContext(), col("k"));
        Assertions.assertEquals(Map.of("v", "CAST(0.5 AS FLOAT)"), hintDropsDoubleLiteral.targetToConstantSql());
        Assertions.assertEquals(Set.of(), hintDropsDoubleLiteral.unsupportedProjectionTargets());
    }

    @Test
    public void filesColumnPushedDownToAnotherColumnsTypeIsDeclined() {
        // FILES x feeds n DATETIME and m DATE, g AS hour(n). Push-down reads x at the last mapping's type, DATE,
        // so the load computes g = 0 for a 10:00 row while the sampler, reading x as DATETIME, would compute 10.
        Column n = new Column("n", DateType.DATETIME, true);
        Column m = new Column("m", DateType.DATE, true);
        Column g = generatedColumn("g", IntegerType.TINYINT, "hour(n)", List.of(ACCOUNT_ID, n, m));
        OlapTable target = generatedTarget(List.of(ACCOUNT_ID, n, m), g);

        InsertSelectSourceColumns.Resolved resolved = resolveWithGenerated("account_id, x AS n, x AS m", target,
                List.of(g), InsertSelectSourceColumns.InputReading.AS_SELECTED_AFTER_PUSH_DOWN, col("account_id"),
                datetimeCol("x"));

        Assertions.assertTrue(resolved.targetsReadingRetypedSource().contains("n"));
        InsertSelectSourceColumns.Unsampleable unsampleable =
                InsertSelectSourceColumns.firstUnsampleable(List.of(g), resolved,
                        InsertSelectSourceColumns.InputReading.AS_SELECTED_AFTER_PUSH_DOWN);
        Assertions.assertEquals(SkipReason.UNSUPPORTED_GENERATED_COLUMN, unsampleable.reason());
        Assertions.assertTrue(unsampleable.detail().contains("push-down"), unsampleable.detail());
    }

    @Test
    public void sameShapeIsAdmittedWhereNothingRetypesTheSourceColumn() {
        // From a table (or FILES with an explicit schema) x is read at its own type for both columns, so n reads
        // the DATETIME the sampler reads.
        Column n = new Column("n", DateType.DATETIME, true);
        Column m = new Column("m", DateType.DATE, true);
        Column g = generatedColumn("g", IntegerType.TINYINT, "hour(n)", List.of(ACCOUNT_ID, n, m));
        OlapTable target = generatedTarget(List.of(ACCOUNT_ID, n, m), g);

        InsertSelectSourceColumns.Resolved resolved = resolveWithGenerated("account_id, x AS n, x AS m", target,
                List.of(g), col("account_id"), datetimeCol("x"));

        Assertions.assertTrue(resolved.generatedColumnFailures().isEmpty(), resolved.generatedColumnFailures().toString());
        Assertions.assertEquals("hour(CAST(`x` AS DATETIME))", resolved.targetToExpressionSql().get("g"));
    }

    @Test
    public void structInputOfADifferentSchemaIsDeclined() {
        // source s STRUCT<INT>, column s STRUCT<BIGINT>: complex-typed inputs are not sampled.
        Column s = new Column("s", new StructType(List.of(IntegerType.BIGINT)), true);
        Column g = generatedColumn("g", BooleanType.BOOLEAN, "s IS NULL", List.of(ACCOUNT_ID, s));
        OlapTable target = generatedTarget(List.of(ACCOUNT_ID, s), g);

        InsertSelectSourceColumns.Resolved resolved = Assertions.assertDoesNotThrow(() -> resolveWithGenerated(
                "account_id, s", target, List.of(g), col("account_id"),
                new Column("s", new StructType(List.of(IntegerType.INT)))));

        Assertions.assertEquals(SkipReason.UNSUPPORTED_GENERATED_COLUMN,
                InsertSelectSourceColumns.firstUnsampleable(List.of(g), resolved,
                        InsertSelectSourceColumns.InputReading.AS_SELECTED).reason());
    }

    @Test
    public void filesKeyReadingAColumnPushedDownElsewhereIsUnsampleable() {
        // A plain (non-generated) sampled key n = CAST(x AS DATETIME) while x also feeds m DATE: push-down reads
        // x as DATE, so the load's n loses its time of day and the sampler's would not.
        Column n = new Column("n", DateType.DATETIME, true);
        Column m = new Column("m", DateType.DATE, true);
        OlapTable target = generatedTarget(List.of(ACCOUNT_ID, n, m));

        InsertSelectSourceColumns.Resolved resolved = resolveWithGenerated(
                "account_id, CAST(x AS DATETIME) AS n, x AS m", target, List.of(ACCOUNT_ID, n),
                InsertSelectSourceColumns.InputReading.AS_SELECTED_AFTER_PUSH_DOWN, col("account_id"),
                datetimeCol("x"));

        InsertSelectSourceColumns.Unsampleable unsampleable = InsertSelectSourceColumns.firstUnsampleable(
                List.of(ACCOUNT_ID, n), resolved, InsertSelectSourceColumns.InputReading.AS_SELECTED_AFTER_PUSH_DOWN);
        Assertions.assertNotNull(unsampleable);
        Assertions.assertEquals("n", unsampleable.column().getName());
        Assertions.assertEquals(SkipReason.UNSUPPORTED_SAMPLED_PROJECTION, unsampleable.reason());
        Assertions.assertNull(InsertSelectSourceColumns.firstUnsampleable(
                List.of(ACCOUNT_ID, n), resolved, InsertSelectSourceColumns.InputReading.AS_SELECTED));
    }

    @Test
    public void structInputIsDeclinedEvenWhenItsTypeMatches() {
        // Same STRUCT on both sides, so the input itself needs no conversion; complex-typed inputs are still not
        // sampled, whatever the definition does with them (here an explicit cast).
        StructType bThenA = new StructType(List.of(new StructField("b", 0, IntegerType.INT, null),
                new StructField("a", 1, IntegerType.INT, null)), true);
        Column s = new Column("s", bThenA, true);
        Column g = generatedColumn("g", BooleanType.BOOLEAN, "CAST(s AS STRUCT<a INT, b INT>) IS NULL",
                List.of(ACCOUNT_ID, s));
        OlapTable target = generatedTarget(List.of(ACCOUNT_ID, s), g);

        InsertSelectSourceColumns.Resolved resolved = resolveWithGenerated("account_id, s", target, List.of(g),
                col("account_id"), new Column("s", bThenA));

        Assertions.assertEquals(SkipReason.UNSUPPORTED_GENERATED_COLUMN, InsertSelectSourceColumns.firstUnsampleable(
                List.of(g), resolved, InsertSelectSourceColumns.InputReading.AS_SELECTED).reason());
    }

    @Test
    public void computedProjectionReadingAComplexColumnIsNotAdmitted() {
        // A scalar projection can still coerce a STRUCT inside it (ifnull(s, t).a); whether that goes by field
        // name or by position is the session's SQL mode, so no projection reading a complex column is admitted.
        Column n = new Column("n", IntegerType.INT, true);
        OlapTable target = generatedTarget(List.of(ACCOUNT_ID, n));

        InsertSelectSourceColumns.Resolved resolved = resolveWithGenerated("account_id, if(s IS NULL, 0, 1) AS n",
                target, List.of(ACCOUNT_ID, n), col("account_id"),
                new Column("s", new StructType(List.of(IntegerType.INT))));

        Assertions.assertEquals(Set.of("n"), resolved.unsupportedProjectionTargets());
        Assertions.assertFalse(resolved.targetToExpressionSql().containsKey("n"));
    }

    @Test
    public void definitionThatDoesNotAnalyzeIsDeclinedWithNoConversionToCheck() {
        // account_id needs no conversion, so only the binding check analyzes the definition, and the dictionary it
        // reads does not exist. Declining dictionary_get itself is SamplingPredicateGateTest.dictionaryLookupRejected.
        Column g = generatedColumn("g", TypeFactory.createVarcharType(64), "dictionary_get('dict', account_id)",
                List.of(ACCOUNT_ID));
        OlapTable target = generatedTarget(List.of(ACCOUNT_ID), g);

        InsertSelectSourceColumns.Resolved resolved = resolveWithGenerated("account_id", target,
                List.of(g), col("account_id"));

        InsertSelectSourceColumns.Unsampleable unsampleable = InsertSelectSourceColumns.firstUnsampleable(List.of(g),
                resolved, InsertSelectSourceColumns.InputReading.AS_SELECTED);
        Assertions.assertEquals(SkipReason.UNSUPPORTED_GENERATED_COLUMN, unsampleable.reason());
        Assertions.assertTrue(unsampleable.detail().endsWith(
                "does not analyze on its own, so the sampler cannot check how the load evaluates it"),
                unsampleable.detail());
    }

    @Test
    public void firstUnsampleableNamesTheReasonOfEachUnfedShape() {
        InsertSelectSourceColumns.Resolved resolved = new InsertSelectSourceColumns.Resolved(
                Map.of("k", "k"), Map.of(), Map.of(), Set.of("v"), Map.of(), Set.of(),
                Map.of("g", "is generated as x and fails"));

        Assertions.assertNull(InsertSelectSourceColumns.firstUnsampleable(List.of(col("k")), resolved,
                InsertSelectSourceColumns.InputReading.AS_SELECTED));
        Assertions.assertEquals(SkipReason.UNSUPPORTED_SAMPLED_PROJECTION,
                InsertSelectSourceColumns.firstUnsampleable(List.of(col("k"), col("v")), resolved,
                        InsertSelectSourceColumns.InputReading.AS_SELECTED).reason());
        Assertions.assertEquals(SkipReason.UNSUPPORTED_GENERATED_COLUMN,
                InsertSelectSourceColumns.firstUnsampleable(List.of(col("g")), resolved,
                        InsertSelectSourceColumns.InputReading.AS_SELECTED).reason());
        Assertions.assertEquals(SkipReason.SOURCE_MISSING_SAMPLED_COLUMN,
                InsertSelectSourceColumns.firstUnsampleable(List.of(col("omitted")), resolved,
                        InsertSelectSourceColumns.InputReading.AS_SELECTED).reason());
    }

    @Test
    public void sampledColumnsListsBaseKeyThenPartitionThenRollupKeys() {
        Assertions.assertEquals(List.of("k", "p", "r1", "r2"),
                InsertSelectSourceColumns.sampledColumns(List.of(col("k")), List.of(col("p")),
                                List.of(new SecondaryIndexSpec(7L, List.of(col("r1"), col("r2")))))
                        .stream().map(Column::getName).toList());
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
