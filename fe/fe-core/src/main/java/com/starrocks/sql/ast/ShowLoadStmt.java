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
import com.starrocks.analysis.Analyzer;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.LimitElement;
import com.starrocks.analysis.OrderByElement;
import com.starrocks.analysis.RedirectStatus;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.common.UserException;
import com.starrocks.common.proc.LoadProcDir;
import com.starrocks.common.util.OrderByPair;
import com.starrocks.load.loadv2.JobState;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.sql.parser.NodePosition;

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
    private final Expr whereClause;
    private final LimitElement limitElement;
    private final List<OrderByElement> orderByElements;

    private String labelValue;
    private String stateValue;
    private boolean isAccurateMatch;

    private ArrayList<OrderByPair> orderByPairs;

    public ShowLoadStmt(String db, Expr labelExpr, List<OrderByElement> orderByElements, LimitElement limitElement) {
        this(db, labelExpr, orderByElements, limitElement, NodePosition.ZERO);
    }

    public ShowLoadStmt(String db, Expr labelExpr, List<OrderByElement> orderByElements,
                        LimitElement limitElement, NodePosition pos) {
        super(pos);
        this.dbName = db;
        this.whereClause = labelExpr;
        this.orderByElements = orderByElements;
        this.limitElement = limitElement;
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
        return visitor.visitShowLoadStatement(this, context);
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
