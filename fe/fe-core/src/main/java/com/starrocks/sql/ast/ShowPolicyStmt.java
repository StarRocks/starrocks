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

public class ShowPolicyStmt extends ShowStmt {
    private static final ShowResultSetMetaData META_DATA;
    private String catalog;
    private String dbName;
    private final PolicyType policyType;

    static {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        builder.addColumn(new Column("Name", ScalarType.createVarchar(100)));
        builder.addColumn(new Column("Type", ScalarType.createVarchar(100)));
        builder.addColumn(new Column("Catalog", ScalarType.createVarchar(100)));
        builder.addColumn(new Column("Database", ScalarType.createVarchar(100)));
        META_DATA = builder.build();
    }

    public ShowPolicyStmt(String catalog, String dbName, PolicyType policyType, NodePosition pos) {
        super(pos);
        this.catalog = catalog;
        this.dbName = dbName;
        this.policyType = policyType;
    }

    public String getCatalog() {
        return catalog;
    }

    public void setCatalog(String catalog) {
        this.catalog = catalog;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public PolicyType getPolicyType() {
        return policyType;
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowPolicyStatement(this, context);
    }
}
