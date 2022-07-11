// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/ShowAlterStmt.java

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
import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.common.proc.ProcNodeInterface;
import com.starrocks.common.proc.RollupProcDir;
import com.starrocks.common.proc.SchemaChangeProcDir;
import com.starrocks.common.util.OrderByPair;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.sql.ast.AstVisitor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/*
 * ShowAlterStmt: used to show process state of alter statement.
 * Syntax:
 *      SHOW ALTER TABLE [COLUMN | ROLLUP | MATERIALIZED_VIEW] [FROM dbName] [WHERE TableName="xxx"] [ORDER BY CreateTime DESC] [LIMIT [offset,]rows]
 */
public class ShowAlterStmt extends ShowStmt {
    private static final Logger LOG = LogManager.getLogger(ShowAlterStmt.class);

    public static enum AlterType {
        COLUMN, ROLLUP, MATERIALIZED_VIEW
    }

    private AlterType type;
    private String dbName;
    private Expr whereClause;
    private HashMap<String, Expr> filterMap;
    private List<OrderByElement> orderByElements;
    private ArrayList<OrderByPair> orderByPairs;
    private LimitElement limitElement;

    private ProcNodeInterface node;

    public AlterType getType() {
        return type;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public HashMap<String, Expr> getFilterMap() {
        return filterMap;
    }

    public LimitElement getLimitElement() {
        return limitElement;
    }

    public ArrayList<OrderByPair> getOrderPairs() {
        return orderByPairs;
    }

    public ProcNodeInterface getNode() {
        return this.node;
    }

    public Expr getWhereClause() {
        return whereClause;
    }

    public List<OrderByElement> getOrderByElements() {
        return orderByElements;
    }

    public void setNode(ProcNodeInterface node) {
        this.node = node;
    }

    public void setFilter(HashMap<String, Expr> filterMap) {
        this.filterMap = filterMap;
    }

    public void setOrderByPairs(ArrayList<OrderByPair> orderByPairs) {
        this.orderByPairs = orderByPairs;
    }

    public ShowAlterStmt(AlterType type, String dbName, Expr whereClause, List<OrderByElement> orderByElements,
                         LimitElement limitElement) {
        this.type = type;
        this.dbName = dbName;
        this.whereClause = whereClause;
        this.orderByElements = orderByElements;
        this.limitElement = limitElement;
    }

    @Override
    public void analyze(Analyzer analyzer) {}

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("SHOW ALTER TABLE ").append(type.name()).append(" ");
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
                sb.append(orderByElements.get(i).toSql());
                sb.append((i + 1 != orderByElements.size()) ? ", " : "");
            }
        }

        if (limitElement != null) {
            sb.append(limitElement.toSql());
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

        ImmutableList<String> titleNames = null;
        if (type == AlterType.ROLLUP || type == AlterType.MATERIALIZED_VIEW) {
            titleNames = RollupProcDir.TITLE_NAMES;
        } else if (type == AlterType.COLUMN) {
            titleNames = SchemaChangeProcDir.TITLE_NAMES;
        }

        for (String title : titleNames) {
            builder.addColumn(new Column(title, ScalarType.createVarchar(30)));
        }

        return builder.build();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowAlterStmt(this, context);
    }

    @Override
    public boolean isSupportNewPlanner() {
        return true;
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.FORWARD_NO_SYNC;
    }
}
