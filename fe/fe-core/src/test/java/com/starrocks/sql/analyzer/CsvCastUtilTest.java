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

package com.starrocks.sql.analyzer;

import com.starrocks.catalog.ArrayType;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.sql.ast.expression.CastExpr;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.ast.expression.StringLiteral;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.wildfly.common.Assert.assertTrue;

public class CsvCastUtilTest {

    private List<Field> testFields;

    @BeforeAll
    public void setUp() {
        // Prepare test data: delimiters, quotation marks, and three data fields
        testFields = new ArrayList<>();
        testFields.add(new Field(",", Type.VARCHAR, null, null));        // separator
        testFields.add(new Field("\"", Type.VARCHAR, null, null));       // Quotation marks
        testFields.add(new Field("col1", Type.INT, null, null));         // data column1
        testFields.add(new Field("col2", Type.VARCHAR, null, null));     // data column2
        testFields.add(new Field("col3", Type.BOOLEAN, null, null));     // data column3
    }

    @Test
    public void testBuildCsvFormatExpressionFromFields_UseBackendOperator() {
        // Test the situation of using the BE operator
        FunctionCallExpr expr = CsvCastUtil.buildCsvFormatExpressionFromFields(testFields, true);

        assertNotNull(expr);
        assertEquals(FunctionSet.CSV_FORMAT, expr.getFnName().getFunction());
        // Delimiter + quotation marks + 3 data fields
        assertEquals(5, expr.getChildren().size());

        // Verify the parameter sequence
        assertEquals(",", ((StringLiteral) expr.getChild(0)).getStringValue());
        assertEquals("\"", ((StringLiteral) expr.getChild(1)).getStringValue());
        assertTrue(expr.getChild(2) instanceof SlotRef);
        assertTrue(expr.getChild(3) instanceof SlotRef);
        assertTrue(expr.getChild(4) instanceof SlotRef);
    }

    @Test
    public void testBuildCsvFormatExpressionFromFields_UseFrontendOperator() {
        // Test the situation of using the FE operator
        FunctionCallExpr expr = CsvCastUtil.buildCsvFormatExpressionFromFields(testFields, false);

        assertNotNull(expr);
        assertEquals(FunctionSet.CONCAT_WS, expr.getFnName().getFunction());
        // Delimiter + multiple CONCAT expressions
        assertTrue(expr.getChildren().size() > 3);
    }

    @Test
    public void testBuildBeCsvFormatExpression() {
        // Test the construction of BE CSV format expressions
        List<Expr> columns = new ArrayList<>();
        columns.add(new StringLiteral(","));     // separator
        columns.add(new StringLiteral("\""));    // Quotation marks
        columns.add(new SlotRef(null, "col1"));  // data column1
        columns.add(new SlotRef(null, "col2"));  // data column2

        FunctionCallExpr expr = CsvCastUtil.buildBeCsvFormatExpression(columns);

        assertNotNull(expr);
        assertEquals(FunctionSet.CSV_FORMAT, expr.getFnName().getFunction());
        assertEquals(4, expr.getChildren().size());
    }

    @Test
    public void testBuildFeCsvFormatExpression() {
        // Test the construction of FE CSV format expressions
        List<Expr> columns = new ArrayList<>();
        columns.add(new StringLiteral(","));     // separator
        columns.add(new StringLiteral("\""));    // Quotation marks
        columns.add(new SlotRef(null, "col1"));  // data column1
        columns.add(new SlotRef(null, "col2"));  // data column2

        FunctionCallExpr expr = CsvCastUtil.buildFeCsvFormatExpression(columns);

        assertNotNull(expr);
        assertEquals(FunctionSet.CONCAT_WS, expr.getFnName().getFunction());
        // should contain delimiters and multiple CONCAT expressions
        // delimiter + 2 CONCAT expressions
        assertEquals(3, expr.getChildren().size());
    }

    @Test
    public void testPreprocessComplexTypes_WithComplexType() {
        // Test preprocessing of complex types
        ArrayType arrayType = new ArrayType(Type.INT);
        SlotRef arrayColumn = new SlotRef(null, "array_col");
        arrayColumn.setType(arrayType);

        Expr processed = CsvCastUtil.preprocessComplexTypes(arrayColumn);

        assertTrue(processed instanceof FunctionCallExpr);
        FunctionCallExpr funcCall = (FunctionCallExpr) processed;
        assertEquals(FunctionSet.TO_JSON, funcCall.getFnName().getFunction());
        assertEquals(1, funcCall.getChildren().size());
    }

    @Test
    public void testPreprocessComplexTypes_WithStringType() {
        // Preprocessing of the test string type (which should remain unchanged)
        SlotRef stringColumn = new SlotRef(null, "string_col");
        stringColumn.setType(Type.VARCHAR);

        Expr processed = CsvCastUtil.preprocessComplexTypes(stringColumn);
        // The original object should be returned
        assertEquals(stringColumn, processed);
    }

    @Test
    public void testPreprocessComplexTypes_WithNumericType() {
        // Preprocessing of test numeric types (should be converted to VARCHAR)
        SlotRef intColumn = new SlotRef(null, "int_col");
        intColumn.setType(Type.INT);

        Expr processed = CsvCastUtil.preprocessComplexTypes(intColumn);

        assertTrue(processed instanceof CastExpr);
        CastExpr castExpr = (CastExpr) processed;
        assertEquals(Type.VARCHAR, castExpr.getTargetTypeDef().getType());
        assertEquals(intColumn, castExpr.getChild(0));
    }

    @Test
    public void testBuildCoalesce() {
        // Test the construction of COALESCE expressions
        SlotRef column = new SlotRef(null, "test_col");
        StringLiteral defaultValue = new StringLiteral("");

        FunctionCallExpr coalesceExpr = CsvCastUtil.buildCoalesce(column, defaultValue);

        assertEquals(FunctionSet.COALESCE, coalesceExpr.getFnName().getFunction());
        assertEquals(2, coalesceExpr.getChildren().size());
        assertEquals(column, coalesceExpr.getChild(0));
        assertEquals(defaultValue, coalesceExpr.getChild(1));
    }

    @Test
    public void testBuildQuoteEscape() {
        // Test the construction of quote escape expressions
        SlotRef column = new SlotRef(null, "test_col");
        String enclose = "\"";

        FunctionCallExpr escapeExpr = CsvCastUtil.buildQuoteEscape(column, enclose);

        assertEquals(FunctionSet.REPLACE, escapeExpr.getFnName().getFunction());
        assertEquals(3, escapeExpr.getChildren().size());
        assertEquals(column, escapeExpr.getChild(0));
        assertEquals(enclose, ((StringLiteral) escapeExpr.getChild(1)).getStringValue());
        assertEquals(enclose + enclose, ((StringLiteral) escapeExpr.getChild(2)).getStringValue());
    }

    @Test
    public void testBuildCsvFormattedColumn() {
        // Test the construction of a complete CSV column formatting expression
        SlotRef column = new SlotRef(null, "test_col");
        String enclose = "\"";

        Expr formattedColumn = CsvCastUtil.buildCsvFormattedColumn(column, enclose);

        assertTrue(formattedColumn instanceof FunctionCallExpr);
        FunctionCallExpr concatExpr = (FunctionCallExpr) formattedColumn;
        assertEquals(FunctionSet.CONCAT, concatExpr.getFnName().getFunction());
        assertEquals(3, concatExpr.getChildren().size());

        // Verify three parts: the start quote + the escaped content + the end quote
        assertEquals(enclose, ((StringLiteral) concatExpr.getChild(0)).getStringValue());
        assertEquals(enclose, ((StringLiteral) concatExpr.getChild(2)).getStringValue());
    }

    @Test
    public void testConvertFieldListToExprList() {
        // Conversion from the test field list to the expression list
        List<Expr> exprList = CsvCastUtil.convertFieldListToExprList(testFields);

        assertEquals(5, exprList.size());
        assertTrue(exprList.get(0) instanceof StringLiteral); // separator
        assertTrue(exprList.get(1) instanceof StringLiteral); // Quotation marks
        assertTrue(exprList.get(2) instanceof SlotRef);       // data column1
        assertTrue(exprList.get(3) instanceof SlotRef);       // data column2
        assertTrue(exprList.get(4) instanceof SlotRef);       // data column3

        assertEquals(",", ((StringLiteral) exprList.get(0)).getStringValue());
        assertEquals("\"", ((StringLiteral) exprList.get(1)).getStringValue());
    }

    @Test
    public void testEmptyFields() {
        // Test the situation of the empty field list
        List<Field> emptyFields = new ArrayList<>();
        emptyFields.add(new Field(",", Type.VARCHAR, null, null));
        emptyFields.add(new Field("\"", Type.VARCHAR, null, null));

        assertThrows(SemanticException.class,
                () -> CsvCastUtil.buildCsvFormatExpressionFromFields(emptyFields, true));
    }

    @Test
    public void testDifferentEncloseCharacters() {
        // Test different quotation mark characters
        List<Field> fields = new ArrayList<>();
        // separator
        fields.add(new Field("|", Type.VARCHAR, null, null));
        // Single quotes are used as quotation marks
        fields.add(new Field("'", Type.VARCHAR, null, null));
        // data column
        fields.add(new Field("col1", Type.VARCHAR, null, null));

        FunctionCallExpr expr = CsvCastUtil.buildCsvFormatExpressionFromFields(fields, false);

        assertNotNull(expr);
        assertEquals(FunctionSet.CONCAT_WS, expr.getFnName().getFunction());
        assertEquals("|", ((StringLiteral) expr.getChild(0)).getStringValue());
    }
}
