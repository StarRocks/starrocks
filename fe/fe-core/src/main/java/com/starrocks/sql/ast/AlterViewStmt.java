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
import com.starrocks.analysis.TableName;
import com.starrocks.sql.parser.NodePosition;

import java.util.Map;

// Alter view statement
public class AlterViewStmt extends DdlStmt {
    private final TableName tableName;
    private final boolean security;
    private final AlterDialectType alterDialect;
    private final Map<String, String> properties;
    private final AlterViewClause alterClause;

    public enum AlterDialectType {
        NONE,
        ADD,
        MODIFY
    }

    public AlterViewStmt(TableName tableName, boolean security, AlterDialectType alterDialect, Map<String, String> properties,
                         AlterViewClause alterClause, NodePosition pos) {
        super(pos);
        this.tableName = tableName;
        this.security = security;
        this.alterDialect = alterDialect;
        this.properties = properties;
        this.alterClause = alterClause;
    }

    public static AlterViewStmt fromReplaceStmt(CreateViewStmt stmt) {
        AlterViewClause alterViewClause = new AlterViewClause(
                stmt.getColWithComments(), stmt.getQueryStatement(), NodePosition.ZERO);
        alterViewClause.setInlineViewDef(stmt.getInlineViewDef());
        alterViewClause.setColumns(stmt.getColumns());
        alterViewClause.setComment(stmt.getComment());
        return new AlterViewStmt(stmt.getTableName(), stmt.isSecurity(), AlterDialectType.NONE, Maps.newHashMap(),
                alterViewClause, NodePosition.ZERO);
    }

    public String getCatalog() {
        return tableName.getCatalog();
    }

    public String getDbName() {
        return tableName.getDb();
    }

    public String getTable() {
        return tableName.getTbl();
    }

    public TableName getTableName() {
        return tableName;
    }

    public boolean isSecurity() {
        return security;
    }

    public boolean isAlterDialect() {
        return alterDialect == AlterDialectType.ADD || alterDialect == AlterDialectType.MODIFY;
    }

    public AlterDialectType getAlterDialectType() {
        return alterDialect;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public AlterViewClause getAlterClause() {
        return alterClause;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAlterViewStatement(this, context);
    }
}
