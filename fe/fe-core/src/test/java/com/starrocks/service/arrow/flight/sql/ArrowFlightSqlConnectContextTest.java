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

package com.starrocks.service.arrow.flight.sql;

import com.starrocks.catalog.Column;
import com.starrocks.common.util.ArrowUtil;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.type.TypeFactory;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class ArrowFlightSqlConnectContextTest {

    // Non-ASCII text: Polish and Japanese
    private static final String NON_ASCII = "zażółć 日本語";

    private ArrowFlightSqlConnectContext context;
    private final String token = "test-token";

    @BeforeEach
    public void setUp() {
        context = new ArrowFlightSqlConnectContext(token);
    }

    @Test
    public void testReset() {
        context.resetForStatement();
        assertNotNull(context.getQueryId());
    }

    @Test
    public void testSetEmptyResultIfNotExist() {
        String queryId = "query-1";
        assertNull(context.getResult(queryId));

        try (var mocked = mockStatic(ArrowUtil.class)) {
            VectorSchemaRoot mockRoot = mock(VectorSchemaRoot.class);
            mocked.when(() -> ArrowUtil.createSingleSchemaRoot(anyString(), anyString()))
                    .thenReturn(mockRoot);

            context.setEmptyResultIfNotExist(queryId);
            assertNotNull(context.getResult(queryId));
        }
    }

    @Test
    public void testAddShowResultAndGetResult() {
        String queryId = "query-2";

        Column column = new Column("col1", TypeFactory.createVarcharType(20));
        ShowResultSetMetaData metaData = new ShowResultSetMetaData(Collections.singletonList(column));
        ShowResultSet showResultSet = new ShowResultSet(metaData, List.of(Collections.singletonList("value1")));

        try (MockedStatic<ArrowUtil> mocked = mockStatic(ArrowUtil.class)) {
            // mock VarCharVector
            VarCharVector mockVector = mock(VarCharVector.class);
            doNothing().when(mockVector).setSafe(anyInt(), any(byte[].class));
            doNothing().when(mockVector).setNull(anyInt());

            // mock ArrowUtil.createVarCharVector
            mocked.when(() -> ArrowUtil.createVarCharVector(anyString(), any(), anyInt()))
                    .thenReturn(mockVector);

            context.addShowResult(queryId, showResultSet);
            VectorSchemaRoot result = context.getResult(queryId);

            assertNotNull(result);
            assertEquals(1, result.getRowCount());
        }
    }

    // Arrow's Utf8 vectors hold UTF-8, but the bytes used to come from String.getBytes() in the JVM's default charset,
    // which is ASCII in a container without a locale and turned non-ASCII text into '?'.
    @Test
    public void testAddShowResultEncodesValuesAsUtf8() {
        Column column = new Column("col1", TypeFactory.createVarcharType(20));
        ShowResultSetMetaData metaData = new ShowResultSetMetaData(Collections.singletonList(column));
        ShowResultSet showResultSet = new ShowResultSet(metaData, List.of(Collections.singletonList(NON_ASCII)));

        try (MockedStatic<ArrowUtil> mocked = mockStatic(ArrowUtil.class)) {
            VarCharVector mockVector = mock(VarCharVector.class);
            mocked.when(() -> ArrowUtil.createVarCharVector(anyString(), any(), anyInt()))
                    .thenReturn(mockVector);

            context.addShowResult("query-utf8-show", showResultSet);

            verify(mockVector).setSafe(0, NON_ASCII.getBytes(StandardCharsets.UTF_8));
        }
    }

    @Test
    public void testAddExplainResultEncodesLinesAsUtf8() {
        try (MockedStatic<ArrowUtil> mocked = mockStatic(ArrowUtil.class)) {
            VarCharVector mockVector = mock(VarCharVector.class);
            mocked.when(() -> ArrowUtil.createVarCharVector(anyString(), any(), anyInt()))
                    .thenReturn(mockVector);

            context.addExplainResult("query-utf8-explain", NON_ASCII + "\nPLAN FRAGMENT 0");

            verify(mockVector).setSafe(0, NON_ASCII.getBytes(StandardCharsets.UTF_8));
            verify(mockVector).setSafe(1, "PLAN FRAGMENT 0".getBytes(StandardCharsets.UTF_8));
        }
    }

    @Test
    public void testRemoveResult() {
        String queryId = "query-3";
        try (var mocked = mockStatic(ArrowUtil.class)) {
            VectorSchemaRoot mockRoot = mock(VectorSchemaRoot.class);
            mocked.when(() -> ArrowUtil.createSingleSchemaRoot(anyString(), anyString()))
                    .thenReturn(mockRoot);

            context.setEmptyResultIfNotExist(queryId);
            assertNotNull(context.getResult(queryId));
            context.removeResult(queryId);
            assertNull(context.getResult(queryId));
        }
    }

    @Test
    public void testRemoveAllResults() {
        try (var mocked = mockStatic(ArrowUtil.class)) {
            VectorSchemaRoot mockRoot = mock(VectorSchemaRoot.class);
            mocked.when(() -> ArrowUtil.createSingleSchemaRoot(anyString(), anyString()))
                    .thenReturn(mockRoot);

            context.setEmptyResultIfNotExist("query-1");
            context.setEmptyResultIfNotExist("query-2");
            context.removeAllResults();
            assertNull(context.getResult("query-1"));
            assertNull(context.getResult("query-2"));
        }
    }

    @Test
    public void testCancelQuery() {
        var mockExecutor = mock(com.starrocks.qe.StmtExecutor.class);
        context.setStmtExecutor(mockExecutor);
        context.cancelQuery();
        verify(mockExecutor, times(1)).cancel("Arrow Flight SQL client disconnected");
    }
}
