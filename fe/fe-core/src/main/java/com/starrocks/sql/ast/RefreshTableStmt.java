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

/**
 * This command used to refresh connector table of external catalog.
 * For example:
 * 'REFRESH EXTERNAL TABLE catalog1.db1.table1'
 * This sql will refresh table1 of db1 in catalog1.
 */
public class RefreshTableStmt extends DdlStmt {
    private final TableName tableName;
    private final List<String> partitionNames;

    public RefreshTableStmt(TableName tableName, List<String> partitionNames) {
        this(tableName, partitionNames, NodePosition.ZERO);
    }

    public RefreshTableStmt(TableName tableName, List<String> partitionNames, NodePosition pos) {
        super(pos);
        this.tableName = tableName;
        this.partitionNames = partitionNames;
    }

    public TableName getTableName() {
        return tableName;
    }

    public List<String> getPartitions() {
        return partitionNames;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitRefreshTableStatement(this, context);
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.NO_FORWARD;
    }
}
