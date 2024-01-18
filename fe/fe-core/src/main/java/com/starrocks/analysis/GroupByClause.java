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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/GroupByClause.java

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

import com.google.common.base.Preconditions;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Table;
import com.starrocks.common.exception.AnalysisException;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.parser.NodePosition;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Wraps all information of group by clause. support normal GROUP BY clause and extended GROUP BY clause like
 * ROLLUP, GROUPING SETS, CUBE syntax like
 * SELECT a, b, SUM( c ) FROM tab1 GROUP BY GROUPING SETS ( (a, b), (a), (b), ( ) );
 * SELECT a, b,c, SUM( d ) FROM tab1 GROUP BY ROLLUP(a,b,c)
 * SELECT a, b,c, SUM( d ) FROM tab1 GROUP BY CUBE(a,b,c)
 * GROUP BY `GROUPING SETS` | `CUBE` | `ROLLUP` is an extension to  GROUP BY clause.
 * This syntax lets you define multiple groupings in the same query.
 * GROUPING SETS produce a single result set that is equivalent to a UNION ALL of differently grouped rows.
 * In this class we produce the rule of generating rows base on the group by clause.
 */
public class GroupByClause implements ParseNode {
    private static final Logger LOG = LogManager.getLogger(GroupByClause.class);
    private boolean exprGenerated = false;
    private GroupingType groupingType;
    private ArrayList<Expr> groupingExprs;
    private ArrayList<Expr> oriGroupingExprs;
    // reserve this info for toSQL
    private List<ArrayList<Expr>> groupingSetList;

    private Table<Integer, Integer, Integer> groupingSetIndexToGroupingExprs;

    protected boolean needToSql = false;

    private final NodePosition pos;

    public GroupByClause(List<ArrayList<Expr>> groupingSetList, GroupingType type) {
        this(groupingSetList, type, NodePosition.ZERO);
    }

    public GroupByClause(List<ArrayList<Expr>> groupingSetList, GroupingType type, NodePosition pos) {
        this.pos = pos;
        this.groupingType = type;
        this.groupingSetList = groupingSetList;
        Preconditions.checkState(type == GroupingType.GROUPING_SETS);
    }

    public GroupByClause(ArrayList<Expr> groupingExprs, GroupingType type) {
        this(groupingExprs, type, NodePosition.ZERO);
    }

    public GroupByClause(ArrayList<Expr> groupingExprs, GroupingType type, NodePosition pos) {
        this.pos = pos;
        this.groupingType = type;
        this.oriGroupingExprs = groupingExprs;
        this.groupingExprs = new ArrayList<>();
        this.groupingExprs.addAll(oriGroupingExprs);
        Preconditions.checkState(type != GroupingType.GROUPING_SETS);
    }

    protected GroupByClause(GroupByClause other) {
        this.pos = other.pos;
        this.groupingType = other.groupingType;
        this.groupingExprs = (other.groupingExprs != null) ? Expr.cloneAndResetList(other.groupingExprs) : null;
        this.oriGroupingExprs =
                (other.oriGroupingExprs != null) ? Expr.cloneAndResetList(other.oriGroupingExprs) : null;

        if (other.groupingSetList != null) {
            this.groupingSetList = new ArrayList<>();
            for (List<Expr> exprList : other.groupingSetList) {
                this.groupingSetList.add(Expr.cloneAndResetList(exprList));
            }
        }
    }

    public List<ArrayList<Expr>> getGroupingSetList() {
        return groupingSetList;
    }

    public GroupingType getGroupingType() {
        return groupingType;
    }

    public void reset() {
        groupingExprs = new ArrayList<>();
        exprGenerated = false;
        if (oriGroupingExprs != null) {
            Expr.resetList(oriGroupingExprs);
            groupingExprs.addAll(oriGroupingExprs);
        }
        if (groupingSetList != null) {
            for (List<Expr> s : groupingSetList) {
                for (Expr e : s) {
                    if (e != null) {
                        e.reset();
                    }
                }
            }
        }
    }

    public List<Expr> getOriGroupingExprs() {
        return oriGroupingExprs;
    }

    public ArrayList<Expr> getGroupingExprs() {
        if (!exprGenerated) {
            try {
                genGroupingExprs();
            } catch (AnalysisException e) {
                LOG.error("gen grouping expr error:", e);
                return null;
            }
        }
        return groupingExprs;
    }

    // generate grouping exprs from group by, grouping sets, cube, rollup cluase
    public void genGroupingExprs() throws AnalysisException {
        if (exprGenerated) {
            return;
        }
        if (CollectionUtils.isNotEmpty(groupingExprs)) {
            // remove repeated element
            Set<Expr> groupingExprSet = new LinkedHashSet<>(groupingExprs);
            groupingExprs.clear();
            groupingExprs.addAll(groupingExprSet);
        }
        if (groupingType == GroupingType.CUBE || groupingType == GroupingType.ROLLUP) {
            if (CollectionUtils.isEmpty(groupingExprs)) {
                throw new AnalysisException(
                        "The expresions in GROUPING CUBE or ROLLUP can not be empty");
            }
        } else if (groupingType == GroupingType.GROUPING_SETS) {
            if (CollectionUtils.isEmpty(groupingSetList)) {
                throw new AnalysisException("The expresions in GROUPINGING SETS can not be empty");
            }
            // collect all Expr elements
            groupingSetIndexToGroupingExprs = HashBasedTable.create();
            groupingExprs = new ArrayList<>();
            for (int i = 0; i < groupingSetList.size(); i++) {
                for (int j = 0; j < groupingSetList.get(i).size(); j++) {
                    if (!groupingExprs.contains(groupingSetList.get(i).get(j))) {
                        groupingSetIndexToGroupingExprs.put(i, j, groupingExprs.size());
                        groupingExprs.add(groupingSetList.get(i).get(j));
                    } else {
                        groupingSetIndexToGroupingExprs.put(i, j, groupingExprs.indexOf(groupingSetList.get(i).get(j)));
                    }
                }
            }
        }
        exprGenerated = true;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException {
    }

    @Override
    public String toSql() {
        if (needToSql) {
            return toViewSql();
        }

        StringBuilder strBuilder = new StringBuilder();
        switch (groupingType) {
            case GROUP_BY:
                if (oriGroupingExprs != null) {
                    for (int i = 0; i < oriGroupingExprs.size(); ++i) {
                        strBuilder.append(oriGroupingExprs.get(i).toSql());
                        strBuilder.append((i + 1 != oriGroupingExprs.size()) ? ", " : "");
                    }
                }
                break;
            case GROUPING_SETS:
                if (groupingSetList != null) {
                    strBuilder.append("GROUPING SETS (");
                    boolean first = true;
                    for (List<Expr> groupingExprs : groupingSetList) {
                        if (first) {
                            strBuilder.append("(");
                            first = false;
                        } else {
                            strBuilder.append(", (");
                        }
                        for (int i = 0; i < groupingExprs.size(); ++i) {
                            strBuilder.append(groupingExprs.get(i).toSql());
                            strBuilder.append((i + 1 != groupingExprs.size()) ? ", " : "");
                        }
                        strBuilder.append(")");
                    }
                    strBuilder.append(")");
                }
                break;
            case CUBE:
                if (oriGroupingExprs != null) {
                    strBuilder.append("CUBE (");
                    for (int i = 0; i < oriGroupingExprs.size(); ++i) {
                        strBuilder.append(oriGroupingExprs.get(i).toSql());
                        strBuilder.append((i + 1 != oriGroupingExprs.size()) ? ", " : "");
                    }
                    strBuilder.append(")");
                }
                break;
            case ROLLUP:
                if (oriGroupingExprs != null) {
                    strBuilder.append("ROLLUP (");
                    for (int i = 0; i < oriGroupingExprs.size(); ++i) {
                        strBuilder.append(oriGroupingExprs.get(i).toSql());
                        strBuilder.append((i + 1 != oriGroupingExprs.size()) ? ", " : "");
                    }
                    strBuilder.append(")");
                }
                break;
            default:
                break;
        }
        return strBuilder.toString();
    }

    @Override
    public NodePosition getPos() {
        return pos;
    }

    private String toViewSql() {
        StringBuilder strBuilder = new StringBuilder();
        switch (groupingType) {
            case GROUP_BY:
                if (groupingExprs != null) {
                    strBuilder.append(groupingExprs.stream().map(Expr::toSql).collect(Collectors.joining(", ")));
                }
                break;
            case GROUPING_SETS:
                if (groupingSetList != null) {
                    Preconditions.checkState(groupingSetIndexToGroupingExprs != null);
                    strBuilder.append("GROUPING SETS (");
                    List<String> allExprs = Lists.newArrayList();
                    List<String> rows = Lists.newArrayList();
                    for (int i = 0; i < groupingSetList.size(); i++) {
                        rows.clear();
                        for (int j = 0; j < groupingSetList.get(i).size(); j++) {
                            Expr e = groupingExprs.get(groupingSetIndexToGroupingExprs.get(i, j));
                            rows.add(e.toSql());
                        }
                        allExprs.add("(" + String.join(", ", rows) + ")");
                    }
                    strBuilder.append(String.join(", ", allExprs));
                    strBuilder.append(")");
                }
                break;
            case CUBE:
                if (groupingExprs != null) {
                    strBuilder.append("CUBE (");
                    strBuilder.append(groupingExprs.stream().filter(e -> !(e instanceof VirtualSlotRef))
                            .map(Expr::toSql).collect(Collectors.joining(", ")));
                    strBuilder.append(")");
                }
                break;
            case ROLLUP:
                if (groupingExprs != null) {
                    strBuilder.append("ROLLUP (");
                    strBuilder.append(groupingExprs.stream().filter(e -> !(e instanceof VirtualSlotRef))
                            .map(Expr::toSql).collect(Collectors.joining(", ")));
                    strBuilder.append(")");
                }
                break;
            default:
                break;
        }
        return strBuilder.toString();
    }

    @Override
    public GroupByClause clone() {
        return new GroupByClause(this);
    }

    public boolean isEmpty() {
        return CollectionUtils.isEmpty(groupingExprs);
    }

    public enum GroupingType {
        GROUP_BY,
        GROUPING_SETS,
        ROLLUP,
        CUBE
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitGroupByClause(this, context);
    }
}
