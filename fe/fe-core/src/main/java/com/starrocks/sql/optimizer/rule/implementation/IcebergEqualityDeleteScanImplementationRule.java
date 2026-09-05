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

package com.starrocks.sql.optimizer.rule.implementation;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalIcebergEqualityDeleteScanOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.physical.PhysicalIcebergEqualityDeleteScanOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;
public class IcebergEqualityDeleteScanImplementationRule extends ImplementationRule {

    public IcebergEqualityDeleteScanImplementationRule() {
        super(RuleType.IMP_ICEBERG_EQUALITY_DELETE_LSCAN_TO_PSCAN,
                Pattern.create(OperatorType.LOGICAL_ICEBERG_EQUALITY_DELETE_SCAN));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalIcebergEqualityDeleteScanOperator scan = (LogicalIcebergEqualityDeleteScanOperator) input.getOp();
        PhysicalIcebergEqualityDeleteScanOperator physicalIcebergScan = new PhysicalIcebergEqualityDeleteScanOperator(scan);
        physicalIcebergScan.setOriginPredicate(scan.getOriginPredicate());
        physicalIcebergScan.setTableFullMORParams(scan.getTableFullMORParams());
        physicalIcebergScan.setMORParams(scan.getMORParams());
        OptExpression result = new OptExpression(physicalIcebergScan);
        return Lists.newArrayList(result);
    }
}
