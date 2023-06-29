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

package com.starrocks.sql.ast.pipe;

import com.starrocks.analysis.Expr;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.load.pipe.Pipe;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.ShowStmt;
import com.starrocks.sql.parser.NodePosition;

import java.util.List;
import java.util.Optional;

public class ShowPipeStmt extends ShowStmt {

    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("DATABASE_ID", ScalarType.BIGINT))
                    .addColumn(new Column("ID", ScalarType.BIGINT))
                    .addColumn(new Column("NAME", ScalarType.createVarchar(64)))
                    .addColumn(new Column("TABLE_NAME", ScalarType.createVarchar(64)))
                    .addColumn(new Column("STATE", ScalarType.createVarcharType(8)))
                    .addColumn(new Column("LOADED_FILES", ScalarType.BIGINT))
                    .addColumn(new Column("LOADED_ROWS", ScalarType.BIGINT))
                    .addColumn(new Column("LOADED_BYTES", ScalarType.BIGINT))
                    .addColumn(new Column("CREATED_TIME", ScalarType.DATETIME))
                    .build();

    private String dbName;
    private final String like;
    private final Expr where;

    public ShowPipeStmt(String dbName, String like, Expr where, NodePosition pos) {
        super(pos);
        this.dbName = dbName;
        this.like = like;
        this.where = where;
    }

    /**
     * NOTE: Must be consistent with the META_DATA
     */
    public static void handleShow(List<String> row, Pipe pipe) {
        Pipe.LoadStatus loadStatus = pipe.getLoadStatus();
        row.add(String.valueOf(pipe.getPipeId().getDbId()));
        row.add(String.valueOf(pipe.getPipeId().getId()));
        row.add(pipe.getName());
        row.add(Optional.ofNullable(pipe.getTargetTable()).map(TableName::toString).orElse(""));
        row.add(String.valueOf(pipe.getState()));
        row.add(String.valueOf(loadStatus.loadFiles));
        row.add(String.valueOf(loadStatus.loadRows));
        row.add(String.valueOf(loadStatus.loadBytes));
        if (pipe.getCreatedTime() == -1) {
            row.add(null);
        } else {
            row.add(TimeUtils.longToTimeString(pipe.getCreatedTime()));
        }
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getDbName() {
        return dbName;
    }

    public String getLike() {
        return like;
    }

    public Expr getWhere() {
        return where;
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowPipeStatement(this, context);
    }
}
