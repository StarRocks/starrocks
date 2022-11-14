// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.google.common.collect.Lists;
import com.starrocks.analysis.TableRef;

import java.util.List;

public class AdminCancelRepairTableStmt extends DdlStmt {

    private final TableRef tblRef;
    private final List<String> partitions = Lists.newArrayList();

    public AdminCancelRepairTableStmt(TableRef tblRef) {
        this.tblRef = tblRef;
    }

    public void setDbName(String dbName) {
        this.tblRef.getName().setDb(dbName);
    }

    public void setPartitions(PartitionNames partitionNames) {
        this.partitions.addAll(partitionNames.getPartitionNames());
    }

    public String getDbName() {
        return tblRef.getName().getDb();
    }

    public String getTblName() {
        return tblRef.getName().getTbl();
    }

    public List<String> getPartitions() {
        return partitions;
    }

    public PartitionNames getPartitionNames() {
        return tblRef.getPartitionNames();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAdminCancelRepairTableStatement(this, context);
    }
}