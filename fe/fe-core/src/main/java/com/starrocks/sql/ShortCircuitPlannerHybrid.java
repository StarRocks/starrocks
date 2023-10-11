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

package com.starrocks.sql;

import com.starrocks.analysis.Analyzer;
import com.starrocks.analysis.SlotDescriptor;
import com.starrocks.analysis.SlotId;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.common.UserException;
import com.starrocks.planner.OlapScanNode;
import com.starrocks.planner.PlanNode;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.IDictManager;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.ScalarOperatorToExpr;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.starrocks.sql.common.ErrorType.INTERNAL_ERROR;

public class ShortCircuitPlannerHybrid {

    public static class LogicalPlanChecker extends ShortCircuitPlanner.LogicalPlanChecker {

        public LogicalPlanChecker(boolean allowFilter, boolean allowLimit, boolean allowProject, boolean allowSort,
                                  ScalarOperator predicate, List<String> orderByColumns, long limit) {
            super(allowFilter, allowLimit, allowProject, allowSort, predicate, orderByColumns, limit);
        }

        @Override
        public Boolean visitLogicalTableScan(OptExpression optExpression, Void context) {
            LogicalScanOperator scanOp = optExpression.getOp().cast();
            Table table = scanOp.getTable();
            if (!(table instanceof OlapTable) || !((OlapTable) table).hasRowStorageType()) {
                return false;
            }

            for (Column column : table.getFullSchema()) {
                if (IDictManager.getInstance().hasGlobalDict(table.getId(), column.getName())) {
                    return false;
                }
            }

            List<String> keyColumns = ((OlapTable) table).getKeyColumns().stream().map(Column::getName).collect(
                    Collectors.toList());
            List<ScalarOperator> conjuncts = Utils.extractConjuncts(predicate);
            return isPointScan(table, keyColumns, conjuncts);
        }
    }

    public static class ExecPlanNodeBuilder extends ShortCircuitPlanner.BaseExecPlanNodeBuilder {
        public ExecPlanNodeBuilder(LogicalProjectOperator projection, ScalarOperator predicate,
                                   long limit, ColumnRefSet requiredOutputColumns) {
            super(projection, predicate, limit, requiredOutputColumns);
        }

        @Override
        public PlanNode visitLogicalTableScan(OptExpression optExpression, ExecPlan context) {
            LogicalScanOperator scan = optExpression.getOp().cast();

            if (scan instanceof LogicalOlapScanOperator) {
                Table referenceTable = scan.getTable();
                context.getDescTbl().addReferencedTable(referenceTable);
                TupleDescriptor tupleDescriptor = context.getDescTbl().createTupleDescriptor();
                tupleDescriptor.setTable(referenceTable);

                for (Map.Entry<ColumnRefOperator, Column> entry : scan.getColRefToColumnMetaMap().entrySet()) {
                    SlotDescriptor slotDescriptor =
                            context.getDescTbl().addSlotDescriptor(tupleDescriptor, new SlotId(entry.getKey().getId()));
                    slotDescriptor.setColumn(entry.getValue());
                    slotDescriptor.setIsNullable(entry.getValue().isAllowNull());
                    slotDescriptor.setIsMaterialized(true);
                    context.getColRefToExpr()
                            .put(entry.getKey(), new SlotRef(entry.getKey().getName(), slotDescriptor));
                }
                tupleDescriptor.computeMemLayout();

                //set tablet
                Analyzer analyzer = new Analyzer(GlobalStateMgr.getCurrentState(), context.getConnectContext());
                OlapScanNode scanNode =
                        new OlapScanNode(context.getNextNodeId(), tupleDescriptor, "COLUMN_WITH_ROW");
                scanNode.selectBestRollupByRollupSelector();
                context.getScanNodes().add(scanNode);
                try {
                    LogicalOlapScanOperator olapScanOperator = (LogicalOlapScanOperator) optExpression.getOp();
                    List<Long> selectedPartitionIds = olapScanOperator.getTable().getPartitions().stream()
                            .filter(Partition::hasData).map(Partition::getId).collect(
                                    Collectors.toList());
                    LogicalOlapScanOperator.Builder builder = new LogicalOlapScanOperator.Builder();
                    builder.withOperator(olapScanOperator)
                            .setSelectedPartitionId(selectedPartitionIds).setPredicate(predicate);
                    olapScanOperator.buildColumnFilters(predicate);
                    olapScanOperator = builder.build();
                    scanNode.setColumnFilters(olapScanOperator.getColumnFilters());
                    scanNode.setSelectedPartitionIds(selectedPartitionIds);
                    scanNode.finalizeStats(analyzer);
                } catch (UserException e) {
                    throw new StarRocksPlannerException(
                            "Build Exec FileScanNode fail, scan info is invalid," + e.getMessage(),
                            INTERNAL_ERROR);
                }

                // set predicate
                if (predicate != null) {
                    List<ScalarOperator> predicates = Utils.extractConjuncts(predicate);
                    ScalarOperatorToExpr.FormatterContext formatterContext =
                            new ScalarOperatorToExpr.FormatterContext(context.getColRefToExpr());
                    for (ScalarOperator predicate : predicates) {
                        scanNode.getConjuncts()
                                .add(ScalarOperatorToExpr.buildExecExpression(predicate, formatterContext));
                    }
                }

                // set limit
                scanNode.setLimit(limit);
                scanNode.computePointScanRangeLocations();
                return addProject(scanNode, context);
            }
            return visit(optExpression, context);
        }
    }
}
