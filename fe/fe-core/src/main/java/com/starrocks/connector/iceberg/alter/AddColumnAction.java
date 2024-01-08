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
import com.starrocks.sql.ast.AddColumnClause;
import com.starrocks.sql.ast.AlterClause;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

import static com.starrocks.connector.iceberg.IcebergApiConverter.toIcebergColumnType;

public class AddColumnAction implements AlterTableAction {

    @Override
    public void execute(Transaction transaction, AlterClause clause) {
        UpdateSchema updateSchema = transaction.updateSchema();

        Preconditions.checkArgument(clause instanceof AddColumnClause, "alter clause instance is not correct.");
        AddColumnClause addColumnClause = (AddColumnClause) clause;

        validateParam(addColumnClause);

        ColumnPosition pos = addColumnClause.getColPos();
        Column column = addColumnClause.getColumnDef().toColumn();

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

        updateSchema.commit();
    }

    public void validateParam(AddColumnClause clause) throws StarRocksConnectorException {
        Column column = clause.getColumnDef().toColumn();
        // All non-partition columns must use NULL as the default value.
        if (!column.isAllowNull()) {
            throw new StarRocksConnectorException("column in iceberg table must be nullable.");
        }
    }
}
