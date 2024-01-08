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

package com.starrocks.connector.iceberg.alter;

import com.starrocks.analysis.ColumnPosition;
import com.starrocks.catalog.Column;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.sql.ast.AlterClause;
import com.starrocks.sql.ast.ModifyColumnClause;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

import static com.starrocks.connector.iceberg.IcebergApiConverter.toIcebergColumnType;

public class ModifyColumnAction implements AlterTableAction {

    @Override
    public void execute(Transaction transaction, AlterClause clause) {
        UpdateSchema updateSchema = transaction.updateSchema();

        Preconditions.checkArgument(clause instanceof ModifyColumnClause, "alter clause instance is not correct.");

        ModifyColumnClause modifyColumnClause = (ModifyColumnClause) clause;
        ColumnPosition colPos = modifyColumnClause.getColPos();
        Column column = modifyColumnClause.getColumnDef().toColumn();
        org.apache.iceberg.types.Type colType = toIcebergColumnType(column.getType());

        // UPDATE column type
        Preconditions.checkArgument(colType.isPrimitiveType(), "Cannot modify " + column.getName() + ", not a primitive type");
        updateSchema.updateColumn(column.getName(), colType.asPrimitiveType());

        // UPDATE comment
        if (column.getComment() != null) {
            updateSchema.updateColumnDoc(column.getName(), column.getComment());
        }

        // NOT NULL / NULL
        Preconditions.checkArgument(column.isAllowNull(), "column in iceberg table must be nullable.");
        updateSchema.makeColumnOptional(column.getName());

        // AFTER column / FIRST
        if (colPos != null) {
            if (colPos.isFirst()) {
                updateSchema.moveFirst(column.getName());
            } else if (colPos.getLastCol() != null) {
                updateSchema.moveAfter(column.getName(), colPos.getLastCol());
            } else {
                throw new StarRocksConnectorException("Unsupported position: " + colPos);
            }
        }

        updateSchema.commit();
    }
}
