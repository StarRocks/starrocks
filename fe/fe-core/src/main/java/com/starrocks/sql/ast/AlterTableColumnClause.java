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

import com.starrocks.alter.AlterOpType;
import com.starrocks.catalog.Column;
import com.starrocks.sql.parser.NodePosition;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class AlterTableColumnClause extends AlterTableClause {
    // if rollupName is null, add to column to base index.
    protected String rollupName;
    protected Map<String, String> properties;

    // set in analyze
    // for AddColumnClause and ModifyColumnClause
    private Column column;
    // for AddColumnsClause
    private final List<Column> columns = new ArrayList<>();

    protected AlterTableColumnClause(AlterOpType opType, String rollupName, Map<String, String> properties,
                                  NodePosition pos) {
        super(opType, pos);
        this.rollupName = rollupName;
        this.properties = properties;
    }

    public String getRollupName() {
        return rollupName;
    }

    public void setRollupName(String rollupName) {
        this.rollupName = rollupName;
    }

    @Override
    public Map<String, String> getProperties() {
        return this.properties;
    }

    public Column getColumn() {
        return column;
    }

    public void setColumn(Column column) {
        this.column = column;
    }

    public List<Column> getColumns() {
        return columns;
    }

    public void addColumn(Column column) {
        this.columns.add(column);
    }
}
