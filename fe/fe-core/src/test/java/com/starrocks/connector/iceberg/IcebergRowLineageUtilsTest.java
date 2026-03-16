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

package com.starrocks.connector.iceberg;

import com.starrocks.catalog.IcebergTable;
import com.starrocks.sql.ast.IcebergRewriteStmt;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.OriginStatement;
import com.starrocks.sql.ast.QualifiedName;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.TableRef;
import com.starrocks.sql.ast.ValuesRelation;
import com.starrocks.sql.parser.NodePosition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;

public class IcebergRowLineageUtilsTest {

    @Test
    public void shouldNotWriteRowLineageColumnsForRegularInsert() {
        IcebergTable icebergTable = Mockito.mock(IcebergTable.class);
        Mockito.when(icebergTable.getFormatVersion()).thenReturn(3);

        InsertStmt insertStmt = newInsertStmt();

        Assertions.assertFalse(
                IcebergRowLineageUtils.shouldWriteRowLineageColumns(insertStmt, icebergTable));
    }

    @Test
    public void shouldWriteRowLineageColumnsForRewriteInsertOnV3Table() {
        IcebergTable icebergTable = Mockito.mock(IcebergTable.class);
        Mockito.when(icebergTable.getFormatVersion()).thenReturn(3);

        IcebergRewriteStmt rewriteStmt = new IcebergRewriteStmt(newInsertStmt(), true, true);

        Assertions.assertTrue(
                IcebergRowLineageUtils.shouldWriteRowLineageColumns(rewriteStmt, icebergTable));
    }

    @Test
    public void shouldNotWriteRowLineageColumnsWhenRewriteStmtDoesNotWriteRowLineage() {
        IcebergTable icebergTable = Mockito.mock(IcebergTable.class);
        Mockito.when(icebergTable.getFormatVersion()).thenReturn(3);

        IcebergRewriteStmt rewriteStmt = new IcebergRewriteStmt(newInsertStmt(), true, false);

        Assertions.assertFalse(
                IcebergRowLineageUtils.shouldWriteRowLineageColumns(rewriteStmt, icebergTable));
    }

    @Test
    public void shouldNotWriteRowLineageColumnsForV2Table() {
        IcebergTable icebergTable = Mockito.mock(IcebergTable.class);
        Mockito.when(icebergTable.getFormatVersion()).thenReturn(2);

        IcebergRewriteStmt rewriteStmt = new IcebergRewriteStmt(newInsertStmt(), true, true);

        Assertions.assertFalse(
                IcebergRowLineageUtils.shouldWriteRowLineageColumns(rewriteStmt, icebergTable));
    }

    private InsertStmt newInsertStmt() {
        ValuesRelation valuesRelation = new ValuesRelation(Collections.emptyList(), Collections.emptyList());
        QueryStatement queryStatement = new QueryStatement(valuesRelation);
        TableRef tableRef = new TableRef(QualifiedName.of("catalog", "db", "tbl"), null, NodePosition.ZERO);
        InsertStmt insertStmt = new InsertStmt(tableRef, queryStatement);
        insertStmt.setOrigStmt(new OriginStatement("insert into tbl values ()", 0));
        return insertStmt;
    }
}
