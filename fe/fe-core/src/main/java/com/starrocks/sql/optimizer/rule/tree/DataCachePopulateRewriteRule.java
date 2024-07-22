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

package com.starrocks.sql.optimizer.rule.tree;

import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.datacache.DataCacheOptions;
import com.starrocks.datacache.DataCachePopulateMode;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.ScanOperatorPredicates;
import com.starrocks.sql.optimizer.operator.physical.PhysicalScanOperator;
import com.starrocks.sql.optimizer.task.TaskContext;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class DataCachePopulateRewriteRule implements TreeRewriteRule {
    private static final Logger LOG = LogManager.getLogger(DataCachePopulateRewriteRule.class);

    private final ConnectContext connectContext;

    public DataCachePopulateRewriteRule(ConnectContext connectContext) {
        this.connectContext = connectContext;
    }

    @Override
    public OptExpression rewrite(OptExpression root, TaskContext taskContext) {
        SessionVariable sessionVariable = connectContext.getSessionVariable();
        if (!sessionVariable.isEnableScanDataCache()) {
            return root;
        }
        DataCachePopulateMode populateMode = sessionVariable.getDataCachePopulateMode();
        if (!sessionVariable.isEnablePopulateDataCache()) {
            // be compatible with old parameter
            populateMode = DataCachePopulateMode.NEVER;
        }

        // for NEVER or ALWAYS mode, we don't need to rewrite it
        if (populateMode == DataCachePopulateMode.NEVER || populateMode == DataCachePopulateMode.ALWAYS) {
            return root;
        }

        // check is analyze sql
        if (connectContext.isStatisticsJob()) {
            return root;
        }

        // check is query statement
        if (connectContext.getExecutor() == null) {
            return root;
        }
        if (!(connectContext.getExecutor().getParsedStmt() instanceof QueryStatement)) {
            return root;
        }

        DataCachePopulateRewriteVisitor visitor = new DataCachePopulateRewriteVisitor();
        root.getOp().accept(visitor, root, taskContext);
        return root;
    }

    private static class DataCachePopulateRewriteVisitor extends OptExpressionVisitor<Void, TaskContext> {
        @Override
        public Void visit(OptExpression optExpression, TaskContext context) {
            for (OptExpression input : optExpression.getInputs()) {
                input.getOp().accept(this, input, context);
            }
            return null;
        }

        @Override
        public Void visitPhysicalScan(OptExpression optExpression, TaskContext context) {
            PhysicalScanOperator scanOperator = (PhysicalScanOperator) optExpression.getOp();
            Table table = scanOperator.getTable();

            if (!isValidScanOperatorType(scanOperator.getOpType())) {
                return null;
            }

            // ignore full table scan
            if (checkIsFullColumnScan(table, scanOperator)) {
                rewritePhysicalScanOperator(scanOperator, false);
                return null;
            }

            ScanOperatorPredicates predicates = null;
            try {
                // ScanOperatorPredicates maybe nullptr, we have to check it
                predicates = scanOperator.getScanOperatorPredicates();
            } catch (AnalysisException e) {
                LOG.warn("Failed to get ScanOperatorPredicates", e);
            }

            if (predicates == null) {
                LOG.warn("ScanOperatorPredicates can't be null");
                return null;
            }

            // ignore full partition scan
            if (checkIsFullPartitionScan(predicates)) {
                rewritePhysicalScanOperator(scanOperator, false);
                return null;
            }

            rewritePhysicalScanOperator(scanOperator, true);

            return null;
        }

        private boolean isValidScanOperatorType(OperatorType operatorType) {
            if (operatorType == OperatorType.PHYSICAL_HIVE_SCAN || operatorType == OperatorType.PHYSICAL_ICEBERG_SCAN ||
                    operatorType == OperatorType.PHYSICAL_FILE_SCAN ||
                    operatorType == OperatorType.PHYSICAL_HUDI_SCAN ||
                    operatorType == OperatorType.PHYSICAL_DELTALAKE_SCAN ||
                    operatorType == OperatorType.PHYSICAL_PAIMON_SCAN) {
                return true;
            } else {
                return false;
            }
        }

        private boolean checkIsFullColumnScan(Table table, PhysicalScanOperator scanOperator) {
            int totalColumns = table.getColumns().size();
            // If it has only one column, ignore check
            if (totalColumns == 1) {
                return false;
            }
            int usedColumns = scanOperator.getUsedColumns().size();
            return usedColumns == totalColumns;
        }

        private boolean checkIsFullPartitionScan(ScanOperatorPredicates scanOperatorPredicates) {
            if (scanOperatorPredicates.getSelectedPartitionIds().size() == 1) {
                // for none-partition table, it has one partition id
                return false;
            }
            return scanOperatorPredicates.getSelectedPartitionIds().size() ==
                    scanOperatorPredicates.getIdToPartitionKey().size();
        }

        private void rewritePhysicalScanOperator(PhysicalScanOperator op, boolean enablePopulate) {
            op.setDataCacheOptions(
                    DataCacheOptions.DataCacheOptionsBuilder.builder().setEnablePopulate(enablePopulate).build());
        }
    }

}
