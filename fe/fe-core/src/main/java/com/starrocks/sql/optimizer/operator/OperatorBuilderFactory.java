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

package com.starrocks.sql.optimizer.operator;

import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalAssertOneRowOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEAnchorOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEConsumeOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalDeltaLakeScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalEsScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalExceptOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFileScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalHiveScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalHudiScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalIcebergScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalIntersectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJDBCScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalLimitOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalMetaScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalMysqlScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalRepeatOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalSchemaScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTableFunctionOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTopNOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalValuesOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalWindowOperator;
import com.starrocks.sql.optimizer.operator.stream.LogicalBinlogScanOperator;

public class OperatorBuilderFactory {
    public static Operator.Builder<?, ?> build(Operator operator) {
        if (operator instanceof LogicalJoinOperator) {
            return new LogicalJoinOperator.Builder();
        } else if (operator instanceof LogicalAggregationOperator) {
            return new LogicalAggregationOperator.Builder();
        } else if (operator instanceof LogicalTopNOperator) {
            return new LogicalTopNOperator.Builder();
        } else if (operator instanceof LogicalOlapScanOperator) {
            return new LogicalOlapScanOperator.Builder();
        } else if (operator instanceof LogicalHiveScanOperator) {
            return new LogicalHiveScanOperator.Builder();
        } else if (operator instanceof LogicalFileScanOperator) {
            return new LogicalFileScanOperator.Builder();
        } else if (operator instanceof LogicalIcebergScanOperator) {
            return new LogicalIcebergScanOperator.Builder();
        } else if (operator instanceof LogicalHudiScanOperator) {
            return new LogicalHudiScanOperator.Builder();
        } else if (operator instanceof LogicalDeltaLakeScanOperator) {
            return new LogicalDeltaLakeScanOperator.Builder();
        } else if (operator instanceof LogicalEsScanOperator) {
            return new LogicalEsScanOperator.Builder();
        } else if (operator instanceof LogicalMysqlScanOperator) {
            return new LogicalMysqlScanOperator.Builder();
        } else if (operator instanceof LogicalSchemaScanOperator) {
            return new LogicalSchemaScanOperator.Builder();
        } else if (operator instanceof LogicalMetaScanOperator) {
            return new LogicalMetaScanOperator.Builder();
        } else if (operator instanceof LogicalJDBCScanOperator) {
            return new LogicalJDBCScanOperator.Builder();
        } else if (operator instanceof LogicalValuesOperator) {
            return new LogicalValuesOperator.Builder();
        } else if (operator instanceof LogicalTableFunctionOperator) {
            return new LogicalTableFunctionOperator.Builder();
        } else if (operator instanceof LogicalWindowOperator) {
            return new LogicalWindowOperator.Builder();
        } else if (operator instanceof LogicalUnionOperator) {
            return new LogicalUnionOperator.Builder();
        } else if (operator instanceof LogicalExceptOperator) {
            return new LogicalExceptOperator.Builder();
        } else if (operator instanceof LogicalIntersectOperator) {
            return new LogicalIntersectOperator.Builder();
        } else if (operator instanceof LogicalFilterOperator) {
            return new LogicalFilterOperator.Builder();
        } else if (operator instanceof LogicalAssertOneRowOperator) {
            return new LogicalAssertOneRowOperator.Builder();
        } else if (operator instanceof LogicalRepeatOperator) {
            return new LogicalRepeatOperator.Builder();
        } else if (operator instanceof LogicalLimitOperator) {
            return new LogicalLimitOperator.Builder();
        } else if (operator instanceof LogicalCTEConsumeOperator) {
            return new LogicalCTEConsumeOperator.Builder();
        } else if (operator instanceof LogicalCTEAnchorOperator) {
            return new LogicalCTEAnchorOperator.Builder();
        } else if (operator instanceof LogicalProjectOperator) {
            return new LogicalProjectOperator.Builder();
        } else if (operator instanceof LogicalBinlogScanOperator) {
            return new LogicalBinlogScanOperator.Builder();
        } else {
            throw new StarRocksPlannerException("not implement builder: " + operator.getOpType(),
                    ErrorType.INTERNAL_ERROR);
        }
    }
}