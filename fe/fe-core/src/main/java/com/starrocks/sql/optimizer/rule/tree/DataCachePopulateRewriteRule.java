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
        DataCachePopulateMode populateMode = getPopulateMode(sessionVariable);

        DataCachePopulateRewriteVisitor visitor = new DataCachePopulateRewriteVisitor(populateMode);
        root.getOp().accept(visitor, root, taskContext);
        return root;
    }

    private DataCachePopulateMode getPopulateMode(SessionVariable sessionVariable) {
        DataCachePopulateMode populateMode = sessionVariable.getDataCachePopulateMode();
        if (!sessionVariable.isEnablePopulateDataCache()) {
            // be compatible with old parameter
            populateMode = DataCachePopulateMode.NEVER;
        }

        // if populateMode is auto, we have to check further
        if (populateMode == DataCachePopulateMode.AUTO) {
            if (!isSatisfiedStatement()) {
                populateMode = DataCachePopulateMode.NEVER;
            }
        }
        return populateMode;
    }

    private boolean isSatisfiedStatement() {
        // check is analyze sql
        if (connectContext.isStatisticsJob()) {
            return false;
        }
        // check is query statement
        // make sure executor is not nullptr first
        if (connectContext.getExecutor() == null) {
            return false;
        }
        if (!(connectContext.getExecutor().getParsedStmt() instanceof QueryStatement)) {
            return false;
        }
        return true;
    }

    private static class DataCachePopulateRewriteVisitor extends OptExpressionVisitor<Void, TaskContext> {
        private final DataCachePopulateMode populateMode;

        private DataCachePopulateRewriteVisitor(DataCachePopulateMode populateMode) {
            this.populateMode = populateMode;
        }

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

            switch (populateMode) {
                case NEVER:
                    return rewritePhysicalScanOperator(scanOperator, false);
                case ALWAYS:
                    return rewritePhysicalScanOperator(scanOperator, true);
                default: {
                    Table table = scanOperator.getTable();

                    if (!isValidScanOperatorType(scanOperator.getOpType())) {
                        return rewritePhysicalScanOperator(scanOperator, false);
                    }

                    // ignore full table scan
                    if (checkIsFullColumnScan(table, scanOperator)) {
                        return rewritePhysicalScanOperator(scanOperator, false);
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
                        return rewritePhysicalScanOperator(scanOperator, false);
                    }

                    // ignore full partition scan
                    if (checkIsFullPartitionScan(predicates)) {
                        return rewritePhysicalScanOperator(scanOperator, false);
                    }

                    return rewritePhysicalScanOperator(scanOperator, true);
                }
            }
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
            if (scanOperatorPredicates.getIdToPartitionKey().size() <= 1) {
                // for none-partition table, it has one partition id
                // but delta lake's none-partition table, it has none partition id
                return false;
            }
            return scanOperatorPredicates.getSelectedPartitionIds().size() ==
                    scanOperatorPredicates.getIdToPartitionKey().size();
        }

        private Void rewritePhysicalScanOperator(PhysicalScanOperator op, boolean enablePopulate) {
            op.setDataCacheOptions(
                    DataCacheOptions.DataCacheOptionsBuilder.builder().setEnablePopulate(enablePopulate).build());
            return null;
        }
    }

}
