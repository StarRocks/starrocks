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
import com.google.common.collect.Maps;
import com.starrocks.analysis.TableRef;
import com.starrocks.sql.parser.NodePosition;

import java.util.List;
import java.util.Map;

//  ADMIN REPAIR TABLE table_name partitions properties("key" = "value");
public class AdminRepairTableStmt extends DdlStmt {

    private final TableRef tblRef;
    private final List<String> partitions = Lists.newArrayList();
    private final Map<String, String> properties;

    // Enforces consistent version across all tablets in a physical partition
    private boolean enforceConsistentVersion = true;
    // Allows empty tablet recovery of tablets with no valid metadata
    private boolean allowEmptyTabletRecovery = false;
    // If true, just return the repair plan without executing it
    private boolean dryRun = false;

    private long timeoutS = 0;

    public AdminRepairTableStmt(TableRef tblRef) {
        this(tblRef, Maps.newHashMap(), NodePosition.ZERO);
    }

    public AdminRepairTableStmt(TableRef tblRef, Map<String, String> properties, NodePosition pos) {
        super(pos);
        this.tblRef = tblRef;
        this.properties = properties;
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

    public Map<String, String> getProperties() {
        return properties;
    }

    public boolean isEnforceConsistentVersion() {
        return enforceConsistentVersion;
    }

    public void setEnforceConsistentVersion(boolean enforceConsistentVersion) {
        this.enforceConsistentVersion = enforceConsistentVersion;
    }

    public boolean isAllowEmptyTabletRecovery() {
        return allowEmptyTabletRecovery;
    }

    public void setAllowEmptyTabletRecovery(boolean allowEmptyTabletRecovery) {
        this.allowEmptyTabletRecovery = allowEmptyTabletRecovery;
    }

    public boolean isDryRun() {
        return dryRun;
    }

    public void setDryRun(boolean dryRun) {
        this.dryRun = dryRun;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAdminRepairTableStatement(this, context);
    }
}
