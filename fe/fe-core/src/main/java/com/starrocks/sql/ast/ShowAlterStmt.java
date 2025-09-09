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

import com.starrocks.common.proc.ProcNodeInterface;
import com.starrocks.common.util.OrderByPair;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.LimitElement;
import com.starrocks.sql.parser.NodePosition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static com.starrocks.common.util.Util.normalizeName;

/*
 * ShowAlterStmt: used to show process state of alter statement.
 * Syntax:
 *      SHOW ALTER TABLE [COLUMN | ROLLUP | MATERIALIZED_VIEW | OPTIMIZE ] [FROM dbName] [WHERE TableName="xxx"]
 *      [ORDER BY CreateTime DESC] [LIMIT [offset,]rows]
 */
public class ShowAlterStmt extends ShowStmt {

    public enum AlterType {
        COLUMN, ROLLUP, MATERIALIZED_VIEW, OPTIMIZE
    }

    private final AlterType type;
    private String dbName;
    private final Expr whereClause;
    private HashMap<String, Expr> filterMap;
    private final List<OrderByElement> orderByElements;
    private ArrayList<OrderByPair> orderByPairs;
    private final LimitElement limitElement;
    private ProcNodeInterface node;

    public ShowAlterStmt(AlterType type, String dbName, Expr whereClause, List<OrderByElement> orderByElements,
                         LimitElement limitElement) {
        this(type, dbName, whereClause, orderByElements, limitElement, NodePosition.ZERO);
    }

    public ShowAlterStmt(AlterType type, String dbName, Expr whereClause, List<OrderByElement> orderByElements,
                         LimitElement limitElement, NodePosition pos) {
        super(pos);
        this.type = type;
        this.dbName = normalizeName(dbName);
        this.whereClause = whereClause;
        this.orderByElements = orderByElements;
        this.limitElement = limitElement;
    }

    public AlterType getType() {
        return type;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = normalizeName(dbName);
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

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return ((AstVisitorExtendInterface<R, C>) visitor).visitShowAlterStatement(this, context);
    }
}
