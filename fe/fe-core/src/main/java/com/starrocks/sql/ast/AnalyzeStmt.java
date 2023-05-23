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

import com.starrocks.analysis.RedirectStatus;
import com.starrocks.analysis.TableName;
import com.starrocks.sql.parser.NodePosition;

import java.util.List;
import java.util.Map;

public class AnalyzeStmt extends StatementBase {
    private final TableName tbl;
    private List<String> columnNames;
    private final boolean isSample;
    private final boolean isAsync;
    private boolean isExternal = false;
    private Map<String, String> properties;
    private final AnalyzeTypeDesc analyzeTypeDesc;

    public AnalyzeStmt(TableName tbl, List<String> columns, Map<String, String> properties,
                       boolean isSample, boolean isAsync,
                       AnalyzeTypeDesc analyzeTypeDesc) {
        this(tbl, columns, properties, isSample, isAsync, analyzeTypeDesc, NodePosition.ZERO);
    }

    public AnalyzeStmt(TableName tbl, List<String> columns, Map<String, String> properties,
                       boolean isSample, boolean isAsync,
                       AnalyzeTypeDesc analyzeTypeDesc, NodePosition pos) {
        super(pos);
        this.tbl = tbl;
        this.columnNames = columns;
        this.isSample = isSample;
        this.isAsync = isAsync;
        this.properties = properties;
        this.analyzeTypeDesc = analyzeTypeDesc;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public void setColumnNames(List<String> columnNames) {
        this.columnNames = columnNames;
    }

    public TableName getTableName() {
        return tbl;
    }

    public boolean isSample() {
        return isSample;
    }

    public boolean isAsync() {
        return isAsync;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public AnalyzeTypeDesc getAnalyzeTypeDesc() {
        return analyzeTypeDesc;
    }

    public boolean isExternal() {
        return isExternal;
    }

    public void setExternal(boolean isExternal) {
        this.isExternal = isExternal;
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.FORWARD_WITH_SYNC;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAnalyzeStatement(this, context);
    }
}
