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

import com.starrocks.sql.parser.NodePosition;

import java.util.Map;

public class SubmitTaskStmt extends DdlStmt {

    private String dbName;

    private String taskName;

    private Map<String, String> properties;

    private int sqlBeginIndex;

    private String sqlText;

    private CreateTableAsSelectStmt createTableAsSelectStmt;

    public SubmitTaskStmt(String dbName, String taskName, Map<String, String> properties, int sqlBeginIndex,
                          CreateTableAsSelectStmt createTableAsSelectStmt) {
        this(dbName, taskName, properties, sqlBeginIndex, createTableAsSelectStmt, NodePosition.ZERO);
    }

    public SubmitTaskStmt(String dbName, String taskName, Map<String, String> properties, int sqlBeginIndex,
                          CreateTableAsSelectStmt createTableAsSelectStmt, NodePosition pos) {
        super(pos);
        this.dbName = dbName;
        this.taskName = taskName;
        this.properties = properties;
        this.sqlBeginIndex = sqlBeginIndex;
        this.createTableAsSelectStmt = createTableAsSelectStmt;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getTaskName() {
        return taskName;
    }

    public void setTaskName(String taskName) {
        this.taskName = taskName;
    }

    public int getSqlBeginIndex() {
        return sqlBeginIndex;
    }

    public void setSqlBeginIndex(int sqlBeginIndex) {
        this.sqlBeginIndex = sqlBeginIndex;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public String getSqlText() {
        return sqlText;
    }

    public void setSqlText(String sqlText) {
        this.sqlText = sqlText;
    }

    public CreateTableAsSelectStmt getCreateTableAsSelectStmt() {
        return createTableAsSelectStmt;
    }

    public void setCreateTableAsSelectStmt(CreateTableAsSelectStmt createTableAsSelectStmt) {
        this.createTableAsSelectStmt = createTableAsSelectStmt;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitSubmitTaskStatement(this, context);
    }
}
