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

import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.ast.SelectListItem;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.FunctionName;
import com.starrocks.sql.ast.expression.FunctionParams;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.ast.expression.TableName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AnalyzeCsvFormatTest {

    @Mock
    private ConnectContext session;

    @Mock
    private SessionVariable sessionVariable;

    @Mock
    private AnalyzeState analyzeState;

    @Mock
    private Scope scope;

    @Mock
    private RelationFields relationFields;

    private SelectAnalyzer analyzer;

    @BeforeEach
    public void setUp() {
        when(session.getSessionVariable()).thenReturn(sessionVariable);
        when(sessionVariable.isEnableCsvFormatBackendOperator()).thenReturn(false);
        when(sessionVariable.getCsvFormatSeparator()).thenReturn(",");
        when(sessionVariable.getCsvFormatEnclose()).thenReturn("\"");
        analyzer = new SelectAnalyzer(session);
    }

    @Test
    public void testAnalyzeCsvFormatWithStar() {
        // Prepare the test data
        SelectListItem item = mock(SelectListItem.class);
        FunctionCallExpr functionCallExpr = mock(FunctionCallExpr.class);
        FunctionParams functionParams = mock(FunctionParams.class);

        when(item.getExpr()).thenReturn(functionCallExpr);
        when(functionCallExpr.getFnName()).thenReturn(new FunctionName(FunctionSet.CSV_FORMAT));
        when(functionCallExpr.getParams()).thenReturn(functionParams);
        when(functionParams.isStar()).thenReturn(true);
        when(item.getTblName()).thenReturn(null);

        // Prepare the field list
        List<Field> fields = new ArrayList<>();
        Field field1 = new Field("sum(1)", ScalarType.createType(PrimitiveType.BIGINT),
                new TableName("test_db", "test_tbl"), null, true);
        Field field2 = new Field("sum(is_low_income_flag)", ScalarType.createType(PrimitiveType.BIGINT),
                new TableName("test_db", "test_tbl"), null, true);
        fields.add(field1);
        fields.add(field2);

        when(scope.getRelationFields()).thenReturn(relationFields);
        when(relationFields.getAllFields()).thenReturn(fields);
        when(relationFields.indexOf(any(Field.class))).thenAnswer(invocation -> {
            Field f = invocation.getArgument(0);
            return fields.indexOf(f);
        });

        analyzer.analyzeCsvFormat(item, analyzeState, scope);

        verify(item).setExpr(any(FunctionCallExpr.class));
    }

    @Test
    public void testAnalyzeCsvFormatWithStarAndTableName() {
        // Prepare the test data
        SelectListItem item = mock(SelectListItem.class);
        FunctionCallExpr functionCallExpr = mock(FunctionCallExpr.class);
        FunctionParams functionParams = mock(FunctionParams.class);
        TableName tableName = new TableName("test_db", "test_tbl");

        when(item.getExpr()).thenReturn(functionCallExpr);
        when(functionCallExpr.getFnName()).thenReturn(new FunctionName(FunctionSet.CSV_FORMAT));
        when(functionCallExpr.getParams()).thenReturn(functionParams);
        when(functionParams.isStar()).thenReturn(true);
        when(item.getTblName()).thenReturn(tableName);

        // Prepare the field list
        List<Field> fields = new ArrayList<>();
        Field field1 = new Field("col1", ScalarType.createType(PrimitiveType.VARCHAR), tableName, null, true);
        Field field2 = new Field("col2", ScalarType.createType(PrimitiveType.INT), tableName, null, true);
        fields.add(field1);
        fields.add(field2);

        when(scope.getRelationFields()).thenReturn(relationFields);
        when(relationFields.resolveFieldsWithPrefix(tableName)).thenReturn(fields);
        when(relationFields.indexOf(any(Field.class))).thenAnswer(invocation -> {
            Field f = invocation.getArgument(0);
            return fields.indexOf(f);
        });

        analyzer.analyzeCsvFormat(item, analyzeState, scope);

        // Verification result
        verify(item).setExpr(any(FunctionCallExpr.class));
    }

    @Test
    public void testAnalyzeCsvFormatWithSpecificParameters() {
        // Prepare the test data
        SelectListItem item = mock(SelectListItem.class);
        FunctionCallExpr functionCallExpr = mock(FunctionCallExpr.class);
        FunctionParams functionParams = mock(FunctionParams.class);

        when(item.getExpr()).thenReturn(functionCallExpr);
        when(functionCallExpr.getFnName()).thenReturn(new FunctionName(FunctionSet.CSV_FORMAT));
        when(functionCallExpr.getParams()).thenReturn(functionParams);
        when(functionParams.isStar()).thenReturn(false);

        // Prepare the parameter list
        ArrayList<Expr> children = new ArrayList<>();
        children.add(new StringLiteral(","));
        children.add(new StringLiteral("\""));
        children.add(new SlotRef(new TableName("test_db", "test_tbl"), "col1"));

        when(functionCallExpr.getChildren()).thenReturn(children);

        analyzer.analyzeCsvFormat(item, analyzeState, scope);

        // Verification result
        verify(item).setExpr(any(FunctionCallExpr.class));
    }

    @Test
    public void testAnalyzeCsvFormatWithInsufficientParameters() {
        // Prepare the test data
        SelectListItem item = mock(SelectListItem.class);
        FunctionCallExpr functionCallExpr = mock(FunctionCallExpr.class);
        FunctionParams functionParams = mock(FunctionParams.class);

        when(item.getExpr()).thenReturn(functionCallExpr);
        when(functionCallExpr.getFnName()).thenReturn(new FunctionName(FunctionSet.CSV_FORMAT));
        when(functionCallExpr.getParams()).thenReturn(functionParams);
        when(functionParams.isStar()).thenReturn(false);

        // List of inadequately prepared parameters
        ArrayList<Expr> children = new ArrayList<>();
        // There are only 2 parameters, less than 3
        children.add(new StringLiteral(","));
        children.add(new StringLiteral("\""));

        when(functionCallExpr.getChildren()).thenReturn(children);

        // Verification exception
        try {
            analyzer.analyzeCsvFormat(item, analyzeState, scope);
            fail("Expected SemanticException");
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof SemanticException);
        }
    }

    @Test
    public void testAnalyzeCsvFormatWithNonCsvFormatFunction() {
        // Prepare the test data - not csv_format function
        SelectListItem item = mock(SelectListItem.class);
        FunctionCallExpr functionCallExpr = mock(FunctionCallExpr.class);

        when(item.getExpr()).thenReturn(functionCallExpr);
        when(functionCallExpr.getFnName()).thenReturn(new FunctionName("other_function"));

        analyzer.analyzeCsvFormat(item, analyzeState, scope);

        // Verify that no expression is set (should be returned in advance)
        verify(item, never()).setExpr(any());
    }

    @Test
    public void testAnalyzeCsvFormatWithNonFunctionCallExpr() {
        // Prepare the test data - Non-function call expression
        SelectListItem item = mock(SelectListItem.class);
        SlotRef slotRef = mock(SlotRef.class);

        when(item.getExpr()).thenReturn(slotRef);

        analyzer.analyzeCsvFormat(item, analyzeState, scope);

        // Verify that no expression is set (should be returned in advance)
        verify(item, never()).setExpr(any());
    }
}
