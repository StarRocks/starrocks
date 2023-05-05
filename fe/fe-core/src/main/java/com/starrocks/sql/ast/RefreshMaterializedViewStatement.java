// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.ast;

import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.qe.ShowResultSetMetaData;

public class RefreshMaterializedViewStatement extends DdlStmt {
    private final TableName mvName;
    private final PartitionRangeDesc partitionRangeDesc;
    private final boolean forceRefresh;

    public static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("QUERY_ID", ScalarType.createVarchar(60)))
                    .build();

    public RefreshMaterializedViewStatement(TableName mvName,
                                            PartitionRangeDesc partitionRangeDesc,
                                            boolean forceRefresh) {
        this.mvName = mvName;
        this.partitionRangeDesc = partitionRangeDesc;
        this.forceRefresh = forceRefresh;
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
}
