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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/planner/MysqlScanNode.java

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

package com.starrocks.planner;

import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.collect.Lists;
import com.starrocks.analysis.Analyzer;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.ExprSubstitutionMap;
import com.starrocks.analysis.SlotDescriptor;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.MysqlTable;
import com.starrocks.common.UserException;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TMySQLScanNode;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.thrift.TScanRangeLocations;

import java.util.ArrayList;
import java.util.List;

/**
 * Full scan of an MySQL table.
 */
public class MysqlScanNode extends ScanNode {
    private final List<String> columns = new ArrayList<String>();
    private final List<String> filters = new ArrayList<String>();
    private final String tblName;
    private String temporalClause; // optional temporal clause for historical queries

    /**
     * Constructs node to scan given data files of table 'tbl'.
     */
    public MysqlScanNode(PlanNodeId id, TupleDescriptor desc, MysqlTable tbl) {
        super(id, desc, "SCAN MYSQL");
        tblName = "`" + tbl.getMysqlTableName() + "`";
    }

    public void setTemporalClause(String temporalClause) {
        this.temporalClause = temporalClause;
    }

    public String getTemporalClause() {
        return temporalClause;
    }

    @Override
    protected String debugString() {
        MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper(this);
        return helper.addValue(super.debugString()).toString();
    }

    @Override
    public void finalizeStats(Analyzer analyzer) throws UserException {
        computeColumnsAndFilters();
    }

    public void computeColumnsAndFilters() {
        createMySQLColumns();
        createMySQLFilters();
    }

    @Override
    protected String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        return prefix + "TABLE: " + tblName + "\n" +
                prefix + "Query: " + getMysqlQueryStr() + "\n";
    }

    private String getMysqlQueryStr() {
        StringBuilder sql = new StringBuilder("SELECT ");
        sql.append(Joiner.on(", ").join(columns));
        sql.append(" FROM ").append(tblName);

        if (!filters.isEmpty()) {
            sql.append(" WHERE (");
            sql.append(Joiner.on(") AND (").join(filters));
            sql.append(")");
        }

        if (temporalClause != null && !temporalClause.isEmpty()) {
            sql.append(" ");
            sql.append(temporalClause);
        }

        return sql.toString();
    }

    private void createMySQLColumns() {
        for (SlotDescriptor slot : desc.getSlots()) {
            if (!slot.isMaterialized()) {
                continue;
            }
            Column col = slot.getColumn();
            columns.add("`" + col.getName() + "`");
        }
        // this happens when count(*)
        if (0 == columns.size()) {
            columns.add("*");
        }
    }

    // We convert predicates of the form <slotref> op <constant> to MySQL filters
    private void createMySQLFilters() {
        if (conjuncts.isEmpty()) {
            return;

        }
        List<SlotRef> slotRefs = Lists.newArrayList();
        Expr.collectList(conjuncts, SlotRef.class, slotRefs);
        ExprSubstitutionMap sMap = new ExprSubstitutionMap();
        for (SlotRef slotRef : slotRefs) {
            SlotRef tmpRef = (SlotRef) slotRef.clone();
            tmpRef.setTblName(null);

            sMap.put(slotRef, tmpRef);
        }
        ArrayList<Expr> mysqlConjuncts = Expr.cloneList(conjuncts, sMap);
        for (Expr p : mysqlConjuncts) {
            filters.add(p.toMySql());
        }
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.MYSQL_SCAN_NODE;
        msg.mysql_scan_node = new TMySQLScanNode(desc.getId().asInt(), tblName, columns, filters);
        msg.mysql_scan_node.setLimit(limit);
        if (temporalClause != null && !temporalClause.isEmpty()) {
            msg.mysql_scan_node.setTemporal_clause(temporalClause);
        }
    }

    /**
     * We query MySQL Meta to get request's data location
     * extra result info will pass to backend ScanNode
     */
    @Override
    public List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength) {
        return null;
    }

    @Override
    public int getNumInstances() {
        return 1;
    }

    @Override
    public void computeStats(Analyzer analyzer) {
        super.computeStats(analyzer);
        // this is just to avoid mysql scan node's cardinality being -1. So that we can calculate the join cost
        // normally.
        // We assume that the data volume of all mysql tables is very small, so set cardinality directly to 1.
        cardinality = cardinality == -1 ? 1 : cardinality;
    }

    @Override
    public boolean canUsePipeLine() {
        return true;
    }

    @Override
    public boolean canUseRuntimeAdaptiveDop() {
        return true;
    }
}
