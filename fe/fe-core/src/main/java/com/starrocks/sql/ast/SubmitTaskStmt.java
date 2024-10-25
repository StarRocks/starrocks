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

import com.google.common.collect.Maps;
import com.starrocks.analysis.TaskName;
import com.starrocks.scheduler.persist.TaskSchedule;
import com.starrocks.sql.parser.NodePosition;

import java.util.Map;

public class SubmitTaskStmt extends DdlStmt {
    private String catalogName;

    private String dbName;

    private String taskName;

    private Map<String, String> properties = Maps.newHashMap();

    private int sqlBeginIndex;

    private String sqlText;
    private TaskSchedule schedule;

    private CreateTableAsSelectStmt createTableAsSelectStmt;
    private DataCacheSelectStatement dataCacheSelectStmt;
    private InsertStmt insertStmt;

    public SubmitTaskStmt(TaskName taskName, int sqlBeginIndex, CreateTableAsSelectStmt createTableAsSelectStmt,
                          NodePosition pos) {
        super(pos);
        this.dbName = taskName.getDbName();
        this.taskName = taskName.getName();
        this.sqlBeginIndex = sqlBeginIndex;
        this.createTableAsSelectStmt = createTableAsSelectStmt;
    }

    public SubmitTaskStmt(TaskName taskName, int sqlBeginIndex, DataCacheSelectStatement dataCacheSelectStatement,
                          NodePosition pos) {
        super(pos);
        this.dbName = taskName.getDbName();
        this.taskName = taskName.getName();
        this.sqlBeginIndex = sqlBeginIndex;
        this.dataCacheSelectStmt = dataCacheSelectStatement;
        if (this.dataCacheSelectStmt.getProperties() != null) {
            this.properties.putAll(this.dataCacheSelectStmt.getProperties());
        }
    }

    public SubmitTaskStmt(TaskName taskName, int sqlBeginIndex, InsertStmt insertStmt, NodePosition pos) {
        super(pos);
        this.dbName = taskName.getDbName();
        this.taskName = taskName.getName();
        this.sqlBeginIndex = sqlBeginIndex;
        this.insertStmt = insertStmt;
    }

    public String getCatalogName() {
        return catalogName;
    }

    public void setCatalogName(String catalogName) {
        this.catalogName = catalogName;
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

    public void setSchedule(TaskSchedule schedule) {
        this.schedule = schedule;
    }

    public TaskSchedule getSchedule() {
        return schedule;
    }

    public CreateTableAsSelectStmt getCreateTableAsSelectStmt() {
        return createTableAsSelectStmt;
    }

    public void setCreateTableAsSelectStmt(CreateTableAsSelectStmt createTableAsSelectStmt) {
        this.createTableAsSelectStmt = createTableAsSelectStmt;
    }

    public InsertStmt getInsertStmt() {
        return insertStmt;
    }

    public DataCacheSelectStatement getDataCacheSelectStmt() {
        return dataCacheSelectStmt;
    }

    public void setInsertStmt(InsertStmt insertStmt) {
        this.insertStmt = insertStmt;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitSubmitTaskStatement(this, context);
    }
}
