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

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.LimitElement;
import com.starrocks.analysis.OrderByElement;
import com.starrocks.analysis.RedirectStatus;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Table;
import com.starrocks.common.proc.LakeTabletsProcNode;
import com.starrocks.common.proc.LocalTabletsProcDir;
import com.starrocks.common.util.OrderByPair;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.parser.NodePosition;

import java.util.ArrayList;
import java.util.List;

public class ShowTabletStmt extends ShowStmt {
    private static final ImmutableList<String> SINGLE_TABLET_TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("DbName").add("TableName").add("PartitionName").add("IndexName")
            .add("DbId").add("TableId").add("PartitionId").add("IndexId")
            .add("IsSync").add("DetailCmd")
            .build();

    private String dbName;
    private String tableName;
    private long tabletId;
    private PartitionNames partitionNames;
    private Expr whereClause;
    private List<OrderByElement> orderByElements;
    private LimitElement limitElement;

    private long version;
    private long backendId;
    private String indexName;
    private Replica.ReplicaState replicaState;
    private ArrayList<OrderByPair> orderByPairs;

    private boolean isShowSingleTablet;

    public ShowTabletStmt(TableName dbTableName, long tabletId, NodePosition pos) {
        this(dbTableName, tabletId, null, null, null, null, pos);
    }

    public ShowTabletStmt(TableName dbTableName, long tabletId, PartitionNames partitionNames,
                          Expr whereClause, List<OrderByElement> orderByElements, LimitElement limitElement) {
        this(dbTableName, tabletId, partitionNames, whereClause, orderByElements, limitElement, NodePosition.ZERO);
    }

    public ShowTabletStmt(TableName dbTableName, long tabletId, PartitionNames partitionNames,
                          Expr whereClause, List<OrderByElement> orderByElements, LimitElement limitElement,
                          NodePosition pos) {
        super(pos);
        if (dbTableName == null) {
            this.dbName = null;
            this.tableName = null;
            this.isShowSingleTablet = true;
            this.indexName = null;
        } else {
            this.dbName = dbTableName.getDb();
            this.tableName = dbTableName.getTbl();
            this.isShowSingleTablet = false;
            this.indexName = Strings.emptyToNull(indexName);
        }
        this.tabletId = tabletId;
        this.partitionNames = partitionNames;
        this.whereClause = whereClause;
        this.orderByElements = orderByElements;
        this.limitElement = limitElement;

        this.version = -1;
        this.backendId = -1;
        this.indexName = null;
        this.replicaState = null;
        this.orderByPairs = null;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String db) {
        this.dbName = db;
    }

    public String getTableName() {
        return tableName;
    }

    public long getTabletId() {
        return tabletId;
    }

    public boolean isShowSingleTablet() {
        return isShowSingleTablet;
    }

    public boolean hasOffset() {
        return limitElement != null && limitElement.hasOffset();
    }

    public long getOffset() {
        return limitElement.getOffset();
    }

    public boolean hasPartition() {
        return partitionNames != null;
    }

    public PartitionNames getPartitionNames() {
        return partitionNames;
    }

    public boolean hasLimit() {
        return limitElement != null && limitElement.hasLimit();
    }

    public long getLimit() {
        return limitElement.getLimit();
    }

    public long getVersion() {
        return version;
    }

    public long getBackendId() {
        return backendId;
    }

    public String getIndexName() {
        return indexName;
    }

    public List<OrderByPair> getOrderByPairs() {
        return orderByPairs;
    }

    public Replica.ReplicaState getReplicaState() {
        return replicaState;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public void setBackendId(long backendId) {
        this.backendId = backendId;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public void setReplicaState(Replica.ReplicaState replicaState) {
        this.replicaState = replicaState;
    }

    public void setOrderByPairs(ArrayList<OrderByPair> orderByPairs) {
        this.orderByPairs = orderByPairs;
    }

    public Expr getWhereClause() {
        return whereClause;
    }

    public List<OrderByElement> getOrderByElements() {
        return orderByElements;
    }

    public LimitElement getLimitElement() {
        return limitElement;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowTabletStatement(this, context);
    }

    private ImmutableList<String> getTitleNames() {
        if (isShowSingleTablet) {
            return SINGLE_TABLET_TITLE_NAMES;
        }

        Database db = GlobalStateMgr.getCurrentState().getDb(dbName);
        if (db == null) {
            return ImmutableList.of();
        }

        db.readLock();
        try {
            Table table = db.getTable(tableName);
            if (table == null || !table.isNativeTable()) {
                return ImmutableList.of();
            }

            if (table.isCloudNativeTable()) {
                return LakeTabletsProcNode.TITLE_NAMES;
            } else {
                return LocalTabletsProcDir.TITLE_NAMES;
            }
        } finally {
            db.readUnlock();
        }
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String title : getTitleNames()) {
            builder.addColumn(new Column(title, ScalarType.createVarchar(30)));
        }
        return builder.build();
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        if (ConnectContext.get().getSessionVariable().getForwardToLeader()) {
            return RedirectStatus.FORWARD_NO_SYNC;
        } else {
            return RedirectStatus.NO_FORWARD;
        }
    }
}
