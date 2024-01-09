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

import com.starrocks.analysis.ColumnPosition;
import com.starrocks.catalog.Column;
import com.starrocks.common.DdlException;
import com.starrocks.connector.ConnectorAlterTableExecutor;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AddColumnClause;
import com.starrocks.sql.ast.AddColumnsClause;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.ColumnDef;
import com.starrocks.sql.ast.ColumnRenameClause;
import com.starrocks.sql.ast.DropColumnClause;
import com.starrocks.sql.ast.ModifyColumnClause;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdateSchema;

import java.util.List;
import java.util.stream.Collectors;

import static com.starrocks.connector.iceberg.IcebergApiConverter.toIcebergColumnType;

public class IcebergAlterTableExecutor extends ConnectorAlterTableExecutor {
    private org.apache.iceberg.Table table;
    private UpdateSchema updateSchema;
    private Transaction transaction;

    public IcebergAlterTableExecutor(AlterTableStmt stmt, org.apache.iceberg.Table table) {
        super(stmt);
        this.table = table;

    }

    @Override
    public void checkConflict() throws DdlException {
    }

    @Override
    public void applyClauses() throws DdlException {
        transaction = table.newTransaction();
        updateSchema = this.transaction.updateSchema();
        super.applyClauses();
        updateSchema.commit();
        transaction.commitTransaction();
    }

    @Override
    public Void visitAddColumnClause(AddColumnClause clause, ConnectContext context) {
        actions.add(() -> {
            AddColumnClause addColumnClause = (AddColumnClause) clause;
            ColumnPosition pos = addColumnClause.getColPos();
            Column column = addColumnClause.getColumnDef().toColumn();

            // All non-partition columns must use NULL as the default value.
            if (!column.isAllowNull()) {
                throw new StarRocksConnectorException("column in iceberg table must be nullable.");
            }
            updateSchema.addColumn(
                    column.getName(),
                    toIcebergColumnType(column.getType()),
                    column.getComment());

            // AFTER column / FIRST
            if (pos != null) {
                if (pos.isFirst()) {
                    updateSchema.moveFirst(column.getName());
                } else if (pos.getLastCol() != null) {
                    updateSchema.moveAfter(column.getName(), pos.getLastCol());
                } else {
                    throw new StarRocksConnectorException("Unsupported position: " + pos);
                }
            }
        });
        return null;
    }

    @Override
    public Void visitAddColumnsClause(AddColumnsClause clause, ConnectContext context) {
        actions.add(() -> {
            AddColumnsClause addColumnsClause = (AddColumnsClause) clause;
            List<Column> columns = addColumnsClause
                    .getColumnDefs()
                    .stream()
                    .map(ColumnDef::toColumn)
                    .collect(Collectors.toList());

            for (Column column : columns) {
                if (!column.isAllowNull()) {
                    throw new StarRocksConnectorException("column in iceberg table must be nullable.");
                }
                updateSchema.addColumn(
                        column.getName(),
                        toIcebergColumnType(column.getType()),
                        column.getComment());
            }
        });
        return null;
    }

    @Override
    public Void visitDropColumnClause(DropColumnClause clause, ConnectContext context) {
        actions.add(() -> {
            DropColumnClause dropColumnClause = (DropColumnClause) clause;
            String columnName = dropColumnClause.getColName();
            updateSchema.deleteColumn(columnName);
        });
        return null;
    }

    @Override
    public Void visitColumnRenameClause(ColumnRenameClause clause, ConnectContext context) {
        actions.add(() -> {
            ColumnRenameClause columnRenameClause = (ColumnRenameClause) clause;
            updateSchema.renameColumn(columnRenameClause.getColName(), columnRenameClause.getNewColName());
        });
        return null;
    }

    @Override
    public Void visitModifyColumnClause(ModifyColumnClause clause, ConnectContext context) {
        actions.add(() -> {
            ModifyColumnClause modifyColumnClause = (ModifyColumnClause) clause;
            ColumnPosition colPos = modifyColumnClause.getColPos();
            Column column = modifyColumnClause.getColumnDef().toColumn();
            org.apache.iceberg.types.Type colType = toIcebergColumnType(column.getType());

            // UPDATE column type
            if (!colType.isPrimitiveType()) {
                throw new StarRocksConnectorException(
                        "Cannot modify " + column.getName() + ", not a primitive type");
            }
            updateSchema.updateColumn(column.getName(), colType.asPrimitiveType());

            // UPDATE comment
            if (column.getComment() != null) {
                updateSchema.updateColumnDoc(column.getName(), column.getComment());
            }

            // NOT NULL / NULL
            if (column.isAllowNull()) {
                updateSchema.makeColumnOptional(column.getName());
            } else {
                throw new StarRocksConnectorException(
                        "column in iceberg table must be nullable.");
            }

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
        });
        return null;
    }
}
