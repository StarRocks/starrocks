// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/ShowLoadStmt.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.analysis;

import com.google.common.base.Strings;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.common.UserException;
import com.starrocks.common.proc.LoadProcDir;
import com.starrocks.common.util.OrderByPair;
import com.starrocks.load.loadv2.JobState;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.sql.ast.AstVisitor;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

// SHOW LOAD STATUS statement used to get status of load job.
//
// syntax:
//      SHOW LOAD [FROM db] [LIKE mask]
public class ShowLoadStmt extends ShowStmt {

    private String dbName;
    private Expr whereClause;
    private LimitElement limitElement;
    private List<OrderByElement> orderByElements;

    private String labelValue;
    private String stateValue;
    private boolean isAccurateMatch;

    private ArrayList<OrderByPair> orderByPairs;

    public ShowLoadStmt(String db, Expr labelExpr, List<OrderByElement> orderByElements, LimitElement limitElement) {
        this.dbName = db;
        this.whereClause = labelExpr;
        this.orderByElements = orderByElements;
        this.limitElement = limitElement;

        this.labelValue = null;
        this.stateValue = null;
        this.isAccurateMatch = false;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
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

    public ArrayList<OrderByPair> getOrderByPairs() {
        return this.orderByPairs;
    }

    public void setOrderByPairs(ArrayList<OrderByPair> orderByPairs) {
        this.orderByPairs = orderByPairs;
    }

    public long getLimit() {
        if (limitElement != null && limitElement.hasLimit()) {
            return limitElement.getLimit();
        }
        return -1L;
    }

    public long getOffset() {
        if (limitElement != null && limitElement.hasOffset()) {
            return limitElement.getOffset();
        }
        return -1L;
    }

    public String getLabelValue() {
        return this.labelValue;
    }

    public void setLabelValue(String labelValue) {
        this.labelValue = labelValue;
    }

    public Set<JobState> getStates() {
        if (Strings.isNullOrEmpty(stateValue)) {
            return null;
        }

        Set<JobState> states = new HashSet<>();
        JobState state = JobState.valueOf(stateValue);
        states.add(state);

        return states;
    }

    public void setStateValue(String stateValue) {
        this.stateValue = stateValue;
    }

    public boolean isAccurateMatch() {
        return isAccurateMatch;
    }

    public void setIsAccurateMatch(boolean isAccurateMatch) {
        this.isAccurateMatch = isAccurateMatch;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowLoadStmt(this, context);
    }

    @Override
    public boolean isSupportNewPlanner() {
        return true;
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("SHOW LOAD ");
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

        if (getOffset() != -1L) {
            sb.append(" OFFSET ").append(getOffset());
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
        for (String title : LoadProcDir.TITLE_NAMES) {
            builder.addColumn(new Column(title, ScalarType.createVarchar(30)));
        }
        return builder.build();
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.FORWARD_WITH_SYNC;
    }
}
