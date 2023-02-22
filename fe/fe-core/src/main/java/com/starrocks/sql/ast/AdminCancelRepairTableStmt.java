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

import com.google.common.collect.Lists;
import com.starrocks.analysis.TableRef;
import com.starrocks.sql.parser.NodePosition;

import java.util.List;

// ADMIN CANCEL REPAIR TABLE example_db.example_table PARTITION(p1);
public class AdminCancelRepairTableStmt extends DdlStmt {

    private final TableRef tblRef;
    private final List<String> partitions = Lists.newArrayList();

    public AdminCancelRepairTableStmt(TableRef tblRef) {
        this(tblRef, NodePosition.ZERO);
    }

    public AdminCancelRepairTableStmt(TableRef tblRef, NodePosition pos) {
        super(pos);
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