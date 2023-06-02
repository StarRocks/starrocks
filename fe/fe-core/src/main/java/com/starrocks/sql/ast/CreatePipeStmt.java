// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.sql.ast;

import com.starrocks.catalog.Table;
import com.starrocks.sql.parser.NodePosition;

public class CreatePipeStmt extends DdlStmt {

    private final boolean ifNotExists;
    private final String pipeName;
    private final InsertStmt insertStmt;
    private Table targetTable;
    private TableFunctionRelation tableFunctionRelation;

    public CreatePipeStmt(boolean ifNotExists, String pipeName, InsertStmt insertStmt, NodePosition pos) {
        super(pos);
        this.ifNotExists = ifNotExists;
        this.pipeName = pipeName;
        this.insertStmt = insertStmt;
    }

    public boolean isIfNotExists() {
        return ifNotExists;
    }

    public String getPipeName() {
        return pipeName;
    }

    public InsertStmt getInsertStmt() {
        return insertStmt;
    }

    public Table getTargetTable() {
        return targetTable;
    }

    public void setTargetTable(Table targetTable) {
        this.targetTable = targetTable;
    }

    public TableFunctionRelation getTableFunctionRelation() {
        return tableFunctionRelation;
    }

    public void setTableFunctionRelation(TableFunctionRelation tableFunctionRelation) {
        this.tableFunctionRelation = tableFunctionRelation;
    }

    @Override
    public String toSql() {
        return "CREATE PIPE " + pipeName + " AS " + insertStmt.toSql();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCreatePipeStatement(this, context);
    }
}
