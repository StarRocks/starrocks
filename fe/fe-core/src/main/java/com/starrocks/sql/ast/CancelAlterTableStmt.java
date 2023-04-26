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


package com.starrocks.sql.ast;

import com.starrocks.analysis.TableName;
import com.starrocks.sql.ast.ShowAlterStmt.AlterType;
import com.starrocks.sql.parser.NodePosition;

import java.util.List;

/*
 * CANCEL ALTER COLUMN|ROLLUP FROM db_name.table_name
 */
public class CancelAlterTableStmt extends CancelStmt {

    private final AlterType alterType;

    private final TableName dbTableName;

    public AlterType getAlterType() {
        return alterType;
    }

    public String getDbName() {
        return dbTableName.getDb();
    }

    public void setDbName(String dbName) {
        this.dbTableName.setDb(dbName);
    }

    public TableName getDbTableName() {
        return this.dbTableName;
    }

    public String getTableName() {
        return dbTableName.getTbl();
    }

    private final List<Long> alterJobIdList;

    public CancelAlterTableStmt(AlterType alterType, TableName dbTableName) {
        this(alterType, dbTableName, null);
    }

    public CancelAlterTableStmt(AlterType alterType, TableName dbTableName, List<Long> alterJobIdList) {
        this(alterType, dbTableName, alterJobIdList, NodePosition.ZERO);
    }

    public CancelAlterTableStmt(AlterType alterType, TableName dbTableName, List<Long> alterJobIdList, NodePosition pos) {
        super(pos);
        this.alterType = alterType;
        this.dbTableName = dbTableName;
        this.alterJobIdList = alterJobIdList;
    }

    public List<Long> getAlterJobIdList() {
        return alterJobIdList;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCancelAlterTableStatement(this, context);
    }

    @Override
    public String toString() {
        return toSql();
    }

}
