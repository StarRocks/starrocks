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

package com.starrocks.planner;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.Analyzer;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.ExprSubstitutionMap;
import com.starrocks.analysis.SlotDescriptor;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.RedisTable;
import com.starrocks.sql.analyzer.AstToStringBuilder;
import com.starrocks.thrift.TRedisScanNode;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.thrift.TScanRangeLocations;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class RedisScanNode extends ScanNode {

    private final List<TScanRangeLocations> scanRangeLocationsList = new ArrayList<>();
    private final List<String> columns = new ArrayList<>();
    private final List<String> filters = new ArrayList<>();
    private String tableName;
    private RedisTable table;
    Map<String, String> properties = Maps.newHashMap();

    public RedisScanNode(PlanNodeId id, TupleDescriptor desc, RedisTable tbl) {
        super(id, desc, "SCAN Redis");
        table = tbl;
        tableName = tbl.getName();
    }

    @Override
    public List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength) {
        return scanRangeLocationsList;
    }

    @Override
    public void computeStats(Analyzer analyzer) {
        super.computeStats(analyzer);
    }

    public void computeColumnsAndFilters() {
        createJDBCTableColumns();
        createJDBCTableFilters();
    }

    private String getIdentifierSymbol() {
        //TODO: for other jdbc table we need different objectIdentifier to support reserved key words
        return "`";
    }

    private void createJDBCTableColumns() {
        String objectIdentifier = getIdentifierSymbol();
        for (SlotDescriptor slot : desc.getSlots()) {
            if (!slot.isMaterialized()) {
                continue;
            }
            Column col = slot.getColumn();
            columns.add(objectIdentifier + col.getName() + objectIdentifier);
        }
        // this happends when count(*)
        if (0 == columns.size()) {
            columns.add("*");
        }
    }

    private void createJDBCTableFilters() {
        if (conjuncts.isEmpty()) {
            return;
        }
        List<SlotRef> slotRefs = Lists.newArrayList();
        Expr.collectList(conjuncts, SlotRef.class, slotRefs);
        ExprSubstitutionMap sMap = new ExprSubstitutionMap();
        String identifier = getIdentifierSymbol();
        for (SlotRef slotRef : slotRefs) {
            SlotRef tmpRef = (SlotRef) slotRef.clone();
            tmpRef.setTblName(null);
            tmpRef.setLabel(identifier + tmpRef.getLabel() + identifier);
            sMap.put(slotRef, tmpRef);
        }

        ArrayList<Expr> jdbcConjuncts = Expr.cloneList(conjuncts, sMap);
        for (Expr p : jdbcConjuncts) {
            filters.add(AstToStringBuilder.toString(p));
        }
    }

    @Override
    protected void toThrift(TPlanNode msg) {

        msg.node_type = TPlanNodeType.REDIS_SCAN_NODE;
        msg.redis_scan_node = new TRedisScanNode();
        msg.redis_scan_node.setTuple_id(desc.getId().asInt());
        msg.redis_scan_node.setTable_name(tableName);
        msg.redis_scan_node.setColumns(columns);
        msg.redis_scan_node.setFilters(filters);
        msg.redis_scan_node.setLimit(limit);
        msg.redis_scan_node.setProperties(properties);
    }
}
