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
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.RedirectStatus;
import com.starrocks.analysis.TableRef;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.parser.NodePosition;

import java.util.List;
import java.util.Map;

// ADMIN SHOW TABLET STATUS FROM db.table PARTITION (p1) PROPERTIES ('key'='value') where status != "NORMAL";
public class AdminShowTabletStatusStmt extends ShowStmt {
    private final TableRef tblRef;
    private final Expr where;
    private final Map<String, String> properties;

    private List<String> partitions = Lists.newArrayList();

    // default value is 5, means show at most 5 missing data files per tablet
    private int maxMissingDataFilesToShow = 5;

    public AdminShowTabletStatusStmt(TableRef tblRef, Expr where, Map<String, String> properties, NodePosition pos) {
        super(pos);
        this.tblRef = tblRef;
        this.where = where;
        this.properties = properties;
    }

    public String getDbName() {
        return tblRef.getName().getDb();
    }

    public void setDbName(String dbName) {
        this.tblRef.getName().setDb(dbName);
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

    public void setPartitions(List<String> partitions) {
        this.partitions = partitions;
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
    public RedirectStatus getRedirectStatus() {
        if (ConnectContext.get().getSessionVariable().getForwardToLeader()) {
            return RedirectStatus.FORWARD_NO_SYNC;
        } else {
            return RedirectStatus.NO_FORWARD;
        }
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAdminShowTabletStatusStatement(this, context);
    }
}
