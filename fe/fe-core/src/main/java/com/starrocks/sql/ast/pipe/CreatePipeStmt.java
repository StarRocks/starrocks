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

package com.starrocks.sql.ast.pipe;

import com.starrocks.analysis.TableName;
import com.starrocks.load.pipe.FilePipeSource;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.DdlStmt;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.parser.NodePosition;

import java.util.Map;

public class CreatePipeStmt extends DdlStmt {

    private final boolean orReplace;
    private final boolean ifNotExists;
    private final PipeName pipeName;
    private final int insertSqlStartIndex;
    private final Map<String, String> properties;
    private final InsertStmt insertStmt;
    private String insertSql;
    private TableName targetTable;
    private FilePipeSource pipeSource;

    public CreatePipeStmt(boolean ifNotExists, boolean orReplace,
                          PipeName pipeName, int insertSqlStartIndex, InsertStmt insertStmt,
                          Map<String, String> properties, NodePosition pos) {
        super(pos);
        this.orReplace = orReplace;
        this.ifNotExists = ifNotExists;
        this.pipeName = pipeName;
        this.insertSqlStartIndex = insertSqlStartIndex;
        this.insertStmt = insertStmt;
        this.properties = properties;
    }

    public boolean isReplace() {
        return orReplace;
    }

    public boolean isIfNotExists() {
        return ifNotExists;
    }

    public PipeName getPipeName() {
        return pipeName;
    }

    public int getInsertSqlStartIndex() {
        return insertSqlStartIndex;
    }

    public void setInsertSql(String insertSql) {
        this.insertSql = insertSql;
    }

    public String getInsertSql() {
        return insertSql;
    }

    public InsertStmt getInsertStmt() {
        return insertStmt;
    }

    public TableName getTargetTable() {
        return targetTable;
    }

    public void setTargetTable(TableName targetTable) {
        this.targetTable = targetTable;
    }

    public void setDataSource(FilePipeSource source) {
        this.pipeSource = source;
    }

    public FilePipeSource getDataSource() {
        return pipeSource;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public String toSql() {
        return "CREATE PIPE " + pipeName + " AS " + insertSql;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCreatePipeStatement(this, context);
    }

}
