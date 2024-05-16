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

public class AdminSetPartitionVersionStmt extends DdlStmt {
    private final TableName tableName;
    private String partitionName;
    private long partitionId = -1L;
    private final long version;

    public AdminSetPartitionVersionStmt(TableName tableName, String partitionName, long version, NodePosition pos) {
        super(pos);
        this.tableName = tableName;
        this.partitionName = partitionName;
        this.version = version;
    }

    public AdminSetPartitionVersionStmt(TableName tableName, long partitionId, long version, NodePosition pos) {
        super(pos);
        this.tableName = tableName;
        this.partitionId = partitionId;
        this.version = version;
    }

    public TableName getTableName() {
        return tableName;
    }

    public String getPartitionName() {
        return partitionName;
    }

    public long getPartitionId() {
        return partitionId;
    }

    public long getVersion() {
        return version;
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.FORWARD_NO_SYNC;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAdminSetPartitionVersionStmt(this, context);
    }
}
