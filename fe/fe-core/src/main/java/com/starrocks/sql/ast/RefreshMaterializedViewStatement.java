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

import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.sql.parser.NodePosition;

public class RefreshMaterializedViewStatement extends DdlStmt {

    private final TableName mvName;
    private final PartitionRangeDesc partitionRangeDesc;
    private final boolean forceRefresh;
    private final boolean isSync;

    public static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("QUERY_ID", ScalarType.createVarchar(60)))
                    .build();

    public RefreshMaterializedViewStatement(TableName mvName,
                                            PartitionRangeDesc partitionRangeDesc,
                                            boolean forceRefresh, boolean isSync) {
        this(mvName, partitionRangeDesc, forceRefresh, isSync, NodePosition.ZERO);
    }

    public RefreshMaterializedViewStatement(TableName mvName,
                                            PartitionRangeDesc partitionRangeDesc,
                                            boolean forceRefresh, boolean isSync, NodePosition pos) {
        super(pos);
        this.mvName = mvName;
        this.partitionRangeDesc = partitionRangeDesc;
        this.forceRefresh = forceRefresh;
        this.isSync = isSync;
    }

    public TableName getMvName() {
        return mvName;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitRefreshMaterializedViewStatement(this, context);
    }

    public PartitionRangeDesc getPartitionRangeDesc() {
        return partitionRangeDesc;
    }

    public boolean isForceRefresh() {
        return forceRefresh;
    }

    public boolean isSync() {
        return isSync;
    }
}
