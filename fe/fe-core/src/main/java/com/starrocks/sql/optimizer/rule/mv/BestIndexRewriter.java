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

package com.starrocks.sql.optimizer.rule.mv;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.starrocks.analysis.SlotRef;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.OlapTable;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class BestIndexRewriter extends OptExpressionVisitor<OptExpression, Long> {
    LogicalOlapScanOperator scanOperator;

    public BestIndexRewriter(LogicalOlapScanOperator scanOperator) {
        this.scanOperator = scanOperator;
    }

    public OptExpression rewrite(OptExpression optExpression, Long context) {
        return optExpression.getOp().accept(this, optExpression, context);
    }

    @Override
    public OptExpression visit(OptExpression optExpression, Long context) {
        for (int childIdx = 0; childIdx < optExpression.arity(); ++childIdx) {
            optExpression.setChild(childIdx, rewrite(optExpression.inputAt(childIdx), context));
        }

        return OptExpression.create(optExpression.getOp(), optExpression.getInputs());
    }

    @Override
    public OptExpression visitLogicalTableScan(OptExpression optExpression, Long bestIndex) {
        if (!OperatorType.LOGICAL_OLAP_SCAN.equals(optExpression.getOp().getOpType())) {
            return optExpression;
        }

        LogicalOlapScanOperator olapScanOperator = (LogicalOlapScanOperator) optExpression.getOp();
        if (olapScanOperator.equals(scanOperator)) {
            Map<ColumnRefOperator, Column> newColumnRefOperatorColumnMap =
                    getColumnRefOperatorColumnMapByIndexMeta(scanOperator, bestIndex);
            Map<ColumnRefOperator, Column> columnRefOperatorColumnMap = Optional.ofNullable(newColumnRefOperatorColumnMap)
                    .orElse(scanOperator.getColRefToColumnMetaMap());
            LogicalOlapScanOperator newScanOperator = LogicalOlapScanOperator.builder()
                    .withOperator(olapScanOperator)
                    .setSelectedIndexId(bestIndex)
                    .setColRefToColumnMetaMap(columnRefOperatorColumnMap)
                    .build();
            optExpression = OptExpression.create(newScanOperator);
        }
        return optExpression;
    }

    private Map<ColumnRefOperator, Column> getColumnRefOperatorColumnMapByIndexMeta(LogicalOlapScanOperator scanOperator,
                                                                                    Long bestIndex) {
        OlapTable olapTable = (OlapTable) scanOperator.getTable();
        long oldSelectedIndexId = scanOperator.getSelectedIndexId();
        if (oldSelectedIndexId != olapTable.getBaseIndexId() || bestIndex == oldSelectedIndexId) {
            return null;
        }
        MaterializedIndexMeta indexMeta = olapTable.getIndexMetaByIndexId(bestIndex);
        List<Column> mvMetaColumns = indexMeta.getSchema();
        Map<String, Column> nameToColumnMap = Maps.newHashMap();
        for (Column mvColumn : mvMetaColumns) {
            List<SlotRef> baseColumns = mvColumn.getRefColumns();
            if (baseColumns == null) {
                nameToColumnMap.put(mvColumn.getName(), mvColumn);
            } else {
                Preconditions.checkState(baseColumns.size() == 1);
                nameToColumnMap.put(baseColumns.get(0).getColumnName(), mvColumn);
            }
        }

        // Table:
        // CREATE TABLE `t1_agg` (
        //  `c_1_0` datetime NULL COMMENT "",
        //  `c_1_1` decimal128(24, 8) NOT NULL COMMENT "",
        //  `c_1_2` double SUM NOT NULL COMMENT ""
        //) ENGINE=OLAP AGGREGATE KEY(`c_1_0`, `c_1_1`)
        // MV:
        // CREATE MATERIALIZED VIEW v0
        // AS SELECT t1_17.c_1_0, t1_17.c_1_1, SUM(t1_17.c_1_2);
        //
        // Query: select * from t1_agg;
        // If query can be rewritten by mv, `SUM(t1_17.c_1_2)` must have an alias because mv's sum column name is
        // different from base table since 3.1.0.
        Map<ColumnRefOperator, Column> newColumnRefOperatorColumnMap = Maps.newHashMap();
        Map<ColumnRefOperator, Column> oldColumnRefOperatorColumnMap = scanOperator.getColRefToColumnMetaMap();
        for (Map.Entry<ColumnRefOperator, Column> e : oldColumnRefOperatorColumnMap.entrySet()) {
            ColumnRefOperator colRef = e.getKey();
            String columnName = colRef.getName();
            // This may happen like this:
            // MV:
            //  create materialized view mv0
            //  as select time, empid, bitmap_union(to_bitmap(deptno)),hll_union(hll_hash(salary)) from
            //  emps group by time, empid;
            // Query:
            //  select unnest, count(distinct deptno) from
            //  (select deptno, unnest from emps,unnest(split(name, \",\"))) t
            //  group by unnest";
            if (!nameToColumnMap.containsKey(columnName)) {
                return null;
            }
            newColumnRefOperatorColumnMap.put(colRef, nameToColumnMap.get(columnName));
        }
        return newColumnRefOperatorColumnMap;
    }
}
