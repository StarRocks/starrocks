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

package com.starrocks.common.proc;

import com.starrocks.common.AnalysisException;
import com.starrocks.common.util.DateUtils;
import com.starrocks.sql.ast.expression.BinaryPredicate;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.ast.expression.DateLiteral;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.type.DateType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

public class ProcUtilsTest {

    @Test
    public void testAnalyzeColumnResolvesAgainstTheListItIsGiven() throws AnalysisException {
        // The layouts disagree on where a column sits, which is the whole reason the list is a
        // parameter: State is index 9 for schema changes and 8 for rollups.
        Assertions.assertEquals(9, ProcUtils.analyzeColumn(SchemaChangeProcDir.TITLE_NAMES, "State"));
        Assertions.assertEquals(8, ProcUtils.analyzeColumn(RollupProcDir.TITLE_NAMES, "State"));
        Assertions.assertEquals(6, ProcUtils.analyzeColumn(OptimizeProcDir.TITLE_NAMES, "State"));
    }

    @Test
    public void testAnalyzeColumnIgnoresCase() throws AnalysisException {
        List<String> titleNames = List.of("JobId", "TableName", "CreateTime");
        Assertions.assertEquals(1, ProcUtils.analyzeColumn(titleNames, "TableName"));
        Assertions.assertEquals(1, ProcUtils.analyzeColumn(titleNames, "tablename"));
        Assertions.assertEquals(1, ProcUtils.analyzeColumn(titleNames, "TABLENAME"));
    }

    @Test
    public void testAnalyzeColumnRejectsAnUnknownName() {
        List<String> titleNames = List.of("JobId", "TableName");
        AnalysisException e = Assertions.assertThrows(AnalysisException.class,
                () -> ProcUtils.analyzeColumn(titleNames, "NoSuchColumn"));
        Assertions.assertTrue(e.getMessage().contains("Title name[NoSuchColumn] does not exist"),
                e.getMessage());
    }

    @Test
    public void testAnalyzeColumnOnAnEmptyList() {
        Assertions.assertThrows(AnalysisException.class, () -> ProcUtils.analyzeColumn(List.of(), "JobId"));
    }

    @Test
    public void testToProcResultBuildsUnderTheGivenTitles() {
        List<String> titleNames = List.of("JobId", "TableName", "State");
        List<List<Comparable>> rows = List.of(List.of(1L, "t1", "FINISHED"), List.of(2L, "t2", "RUNNING"));

        BaseProcResult result = ProcUtils.toProcResult(titleNames, rows);
        Assertions.assertEquals(titleNames, result.getColumnNames());
        // Cells are stringified on the way in, which is what both roads did before.
        Assertions.assertEquals(List.of("1", "t1", "FINISHED"), result.getRows().get(0));
        Assertions.assertEquals(List.of("2", "t2", "RUNNING"), result.getRows().get(1));
    }

    @Test
    public void testToProcResultOnNoRows() {
        BaseProcResult result = ProcUtils.toProcResult(List.of("JobId"), List.of());
        Assertions.assertEquals(List.of("JobId"), result.getColumnNames());
        Assertions.assertTrue(result.getRows().isEmpty());
    }

    @Test
    public void testFilterResultPassesWhatItDoesNotFilter() throws AnalysisException {
        // Filtering is opt-in per column: no filter at all, or a filter that does not mention this
        // column, keeps the row. Every column of every row goes through here, so this is the path
        // almost all of them take.
        Assertions.assertTrue(ProcUtils.filterResult("TableName", "t1", null));
        Assertions.assertTrue(ProcUtils.filterResult("TableName", "t1", Map.of()));
        Assertions.assertTrue(ProcUtils.filterResult("TableName", "t1", Map.of("state", eq("FINISHED"))));
    }

    @Test
    public void testFilterResultOnAStringColumn() throws AnalysisException {
        Map<String, Expr> filter = Map.of("tablename", eq("t1"));
        Assertions.assertTrue(ProcUtils.filterResult("TableName", "t1", filter));
        Assertions.assertFalse(ProcUtils.filterResult("TableName", "t2", filter));
        // The key is lower-cased by the analyzer, and the lookup lower-cases the title to match.
        Assertions.assertTrue(ProcUtils.filterResult("TABLENAME", "t1", filter));
    }

    @Test
    public void testFilterResultOnADateColumn() throws AnalysisException {
        // The analyzer casts a date predicate's right-hand side to DATETIME; that cast is what
        // opens up the comparison operators here.
        String cell = "2020-06-15 12:00:00";
        Assertions.assertTrue(ProcUtils.filterResult("CreateTime", cell, dateFilter(BinaryType.GT, "2020-01-01 00:00:00")));
        Assertions.assertFalse(ProcUtils.filterResult("CreateTime", cell, dateFilter(BinaryType.GT, "2099-01-01 00:00:00")));
        Assertions.assertTrue(ProcUtils.filterResult("CreateTime", cell, dateFilter(BinaryType.LT, "2099-01-01 00:00:00")));
        Assertions.assertTrue(ProcUtils.filterResult("CreateTime", cell, dateFilter(BinaryType.EQ, cell)));
        Assertions.assertFalse(ProcUtils.filterResult("CreateTime", cell, dateFilter(BinaryType.NE, cell)));
    }

    @Test
    public void testFilterResultLeavesAnUnrecognisedShapeAlone() throws AnalysisException {
        // A string right-hand side with an operator other than = matches neither branch. It passes
        // rather than throwing: the analyzer is what rejects predicates it does not support.
        Map<String, Expr> filter = Map.of("tablename",
                new BinaryPredicate(BinaryType.GT, new SlotRef(null, "TableName"), new StringLiteral("t1")));
        Assertions.assertTrue(ProcUtils.filterResult("TableName", "t2", filter));
    }

    private static Expr eq(String value) {
        return new BinaryPredicate(BinaryType.EQ, new SlotRef(null, "col"), new StringLiteral(value));
    }

    private static Map<String, Expr> dateFilter(BinaryType op, String value) {
        DateLiteral right = new DateLiteral(DateUtils.parseStrictDateTime(value), DateType.DATETIME);
        return Map.of("createtime", new BinaryPredicate(op, new SlotRef(null, "CreateTime"), right));
    }
}
