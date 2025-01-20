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

import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.sql.parser.NodePosition;

import java.util.LinkedList;
import java.util.List;

public class DescribeOutputStmt extends ShowStmt {

    private static final ShowResultSetMetaData DESC_OUTPUT_META_DATA =
            ShowResultSetMetaData.builder().addColumn(new Column("Column Name", ScalarType.createVarcharType()))
                    .addColumn(new Column("Catalog", ScalarType.createVarcharType()))
                    .addColumn(new Column("Schema", ScalarType.createVarcharType()))
                    .addColumn(new Column("Table", ScalarType.createVarcharType()))
                    .addColumn(new Column("Type", ScalarType.createVarcharType()))
                    .addColumn(new Column("Type Size", ScalarType.createVarcharType()))
                    .addColumn(new Column("Aliased", ScalarType.createVarcharType())).build();

    private final String name;

    List<List<String>> totalRows;

    public DescribeOutputStmt(String name) {
        this(name, NodePosition.ZERO);
    }

    public DescribeOutputStmt(String name, NodePosition pos) {
        super(pos);
        this.name = name;
        this.totalRows = new LinkedList<>();
    }

    public String getName() {
        return name;
    }

    public List<List<String>> getTotalRows() {
        return totalRows;
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return DESC_OUTPUT_META_DATA;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitDescOutputStmt(this, context);
    }
}
