// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.google.common.collect.Lists;
import com.starrocks.analysis.DdlStmt;
import com.starrocks.analysis.TableRef;

import java.util.List;

//  ADMIN REPAIR TABLE table_name partitions;
public class AdminRepairTableStmt extends DdlStmt {

    private final TableRef tblRef;
    private final List<String> partitions = Lists.newArrayList();

    private long timeoutS = 0;

    public AdminRepairTableStmt(TableRef tblRef) {
        this.tblRef = tblRef;
    }

    public void setDbName(String dbName) {
        this.tblRef.getName().setDb(dbName);
    }

    public void setPartitions(PartitionNames partitionNames) {
        this.partitions.addAll(partitionNames.getPartitionNames());
    }

    public void setTimeoutSec(long timeoutS) {
        this.timeoutS = timeoutS;
    }

    public String getDbName() {
        return tblRef.getName().getDb();
    }

    public String getTblName() {
        return tblRef.getName().getTbl();
    }

    public PartitionNames getPartitionNames() {
        return tblRef.getPartitionNames();
    }

    public List<String> getPartitions() {
        return partitions;
    }

    public long getTimeoutS() {
        return timeoutS;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAdminRepairTableStatement(this, context);
    }

    @Override
    public boolean isSupportNewPlanner() {
        return true;
    }
}
