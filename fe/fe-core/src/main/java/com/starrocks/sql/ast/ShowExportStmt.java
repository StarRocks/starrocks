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
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.LimitElement;
import com.starrocks.analysis.OrderByElement;
import com.starrocks.analysis.RedirectStatus;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.common.proc.ExportProcNode;
import com.starrocks.common.util.OrderByPair;
import com.starrocks.load.ExportJob.JobState;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.sql.parser.NodePosition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

// SHOW EXPORT STATUS statement used to get status of load job.
//
// syntax:
//      SHOW EXPORT [FROM db] [WHERE queryid = "921d8f80-7c9d-11eb-9342-acde48001122"]
public class ShowExportStmt extends ShowStmt {
    private static final Logger LOG = LogManager.getLogger(ShowExportStmt.class);

    private String dbName;
    private Expr whereClause;
    private LimitElement limitElement;
    private List<OrderByElement> orderByElements;

    private long jobId = 0;
    private String stateValue = null;
    private UUID queryId = null;

    private JobState jobState;

    private ArrayList<OrderByPair> orderByPairs;

    public ShowExportStmt(String db, Expr whereExpr, List<OrderByElement> orderByElements,
                          LimitElement limitElement) {
        this(db, whereExpr, orderByElements, limitElement, NodePosition.ZERO);
    }

    public ShowExportStmt(String db, Expr whereExpr, List<OrderByElement> orderByElements,
                          LimitElement limitElement, NodePosition pos) {
        super(pos);
        this.dbName = db;
        this.whereClause = whereExpr;
        this.orderByElements = orderByElements;
        this.limitElement = limitElement;
    }

    public void setStateValue(String stateValue) {
        this.stateValue = stateValue;
    }

    public void setQueryId(UUID queryId) {
        this.queryId = queryId;
    }

    public void setJobId(long jobId) {
        this.jobId = jobId;
    }

    public void setJobState(JobState jobState) {
        this.jobState = jobState;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public void setOrderByPairs(ArrayList<OrderByPair> orderByPairs) {
        this.orderByPairs = orderByPairs;
    }

    public String getDbName() {
        return dbName;
    }

    public Expr getWhereClause() {
        return whereClause;
    }

    public List<OrderByElement> getOrderByElements() {
        return orderByElements;
    }

    public ArrayList<OrderByPair> getOrderByPairs() {
        return this.orderByPairs;
    }

    public long getLimit() {
        if (limitElement != null && limitElement.hasLimit()) {
            return limitElement.getLimit();
        }
        return -1L;
    }

    public long getJobId() {
        return this.jobId;
    }

    public JobState getJobState() {
        if (Strings.isNullOrEmpty(stateValue)) {
            return null;
        }
        return jobState;
    }

    public UUID getQueryId() {
        return queryId;
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("SHOW EXPORT ");
        if (!Strings.isNullOrEmpty(dbName)) {
            sb.append("FROM `").append(dbName).append("`");
        }

        if (whereClause != null) {
            sb.append(" WHERE ").append(whereClause.toSql());
        }

        // Order By clause
        if (orderByElements != null) {
            sb.append(" ORDER BY ");
            for (int i = 0; i < orderByElements.size(); ++i) {
                sb.append(orderByElements.get(i).getExpr().toSql());
                sb.append((orderByElements.get(i).getIsAsc()) ? " ASC" : " DESC");
                sb.append((i + 1 != orderByElements.size()) ? ", " : "");
            }
        }

        if (getLimit() != -1L) {
            sb.append(" LIMIT ").append(getLimit());
        }

        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String title : ExportProcNode.TITLE_NAMES) {
            builder.addColumn(new Column(title, ScalarType.createVarchar(30)));
        }
        return builder.build();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowExportStatement(this, context);
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.FORWARD_NO_SYNC;
    }
}
