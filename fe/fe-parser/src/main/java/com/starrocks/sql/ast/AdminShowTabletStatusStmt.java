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

import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.parser.NodePosition;

import java.util.Map;

// ADMIN SHOW TABLET STATUS FROM db.table PARTITION (p1) PROPERTIES ('key'='value') where status != "NORMAL";
public class AdminShowTabletStatusStmt extends ShowStmt {
    private final TableRef tblRef;
    private final Expr where;
    private final Map<String, String> properties;

    // default value is 5, means show at most 5 missing data files per tablet
    private int maxMissingDataFilesToShow = 5;

    public AdminShowTabletStatusStmt(TableRef tblRef, Expr where, Map<String, String> properties, NodePosition pos) {
        super(pos);
        this.tblRef = tblRef;
        this.where = where;
        this.properties = properties;
    }

    public String getDbName() {
        return tblRef.getDbName();
    }

    public String getTblName() {
        return tblRef.getTableName();
    }

    public PartitionRef getPartitionRef() {
        return tblRef.getPartitionDef();
    }

    public Expr getWhere() {
        return where;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public int getMaxMissingDataFilesToShow() {
        return maxMissingDataFilesToShow;
    }

    public void setMaxMissingDataFilesToShow(int maxMissingDataFilesToShow) {
        this.maxMissingDataFilesToShow = maxMissingDataFilesToShow;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAdminShowTabletStatusStatement(this, context);
    }
}
