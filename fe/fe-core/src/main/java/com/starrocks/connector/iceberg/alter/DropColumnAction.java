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

import com.starrocks.sql.ast.AlterClause;
import com.starrocks.sql.ast.DropColumnClause;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public class DropColumnAction implements AlterTableAction {

    @Override
    public void execute(Transaction transaction, AlterClause clause) {
        UpdateSchema updateSchema = transaction.updateSchema();

        Preconditions.checkArgument(clause instanceof DropColumnClause, "alter clause instance is not correct.");
        DropColumnClause dropColumnClause = (DropColumnClause) clause;
        String columnName = dropColumnClause.getColName();
        updateSchema.deleteColumn(columnName);
        updateSchema.commit();
    }
}
