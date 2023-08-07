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

import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.load.pipe.Pipe;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.ShowStmt;
import com.starrocks.sql.parser.NodePosition;

import java.util.List;
import java.util.Optional;

public class DescPipeStmt extends ShowStmt {

    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("DATABASE_ID", ScalarType.BIGINT))
                    .addColumn(new Column("ID", ScalarType.BIGINT))
                    .addColumn(new Column("NAME", ScalarType.createVarchar(64)))
                    .addColumn(new Column("TYPE", ScalarType.createVarchar(8)))
                    .addColumn(new Column("TABLE_NAME", ScalarType.createVarchar(64)))
                    .addColumn(new Column("SOURCE", ScalarType.createVarcharType(128)))
                    .addColumn(new Column("SQL", ScalarType.createVarcharType(128)))
                    .build();

    private final PipeName name;

    public DescPipeStmt(NodePosition pos, PipeName name) {
        super(pos);
        this.name = name;
    }

    public static void handleDesc(List<String> row, Pipe pipe) {
        row.add(String.valueOf(pipe.getPipeId().getDbId()));
        row.add(String.valueOf(pipe.getPipeId().getId()));
        row.add(pipe.getName());
        row.add(String.valueOf(pipe.getType()));
        row.add(Optional.ofNullable(pipe.getTargetTable()).map(TableName::toString).orElse(""));
        row.add(pipe.getPipeSource().toString());
        row.add(pipe.getOriginSql());
    }

    public PipeName getName() {
        return name;
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitDescPipeStatement(this, context);
    }
}
