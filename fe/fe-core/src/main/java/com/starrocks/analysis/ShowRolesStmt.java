// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/ShowRolesStmt.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.analysis;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.sql.ast.AstVisitor;

public class ShowRolesStmt extends ShowStmt {
    private static final ShowResultSetMetaData META_DATA;

    static {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();

        builder.addColumn(new Column("Name", ScalarType.createVarchar(100)));
        builder.addColumn(new Column("Users", ScalarType.createVarchar(100)));
        builder.addColumn(new Column("GlobalPrivs", ScalarType.createVarchar(300)));
        builder.addColumn(new Column("DatabasePrivs", ScalarType.createVarchar(300)));
        builder.addColumn(new Column("TablePrivs", ScalarType.createVarchar(300)));
        builder.addColumn(new Column("ResourcePrivs", ScalarType.createVarchar(300)));

        META_DATA = builder.build();
    }

    public ShowRolesStmt() {

    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowRolesStatement(this, context);
    }

    @Override
    public boolean isSupportNewPlanner() {
        return true;
    }

}
