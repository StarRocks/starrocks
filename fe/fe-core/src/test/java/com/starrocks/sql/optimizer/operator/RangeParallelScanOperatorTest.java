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

import com.starrocks.catalog.OlapTable;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.optimizer.base.DistributionSpec;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class RangeParallelScanOperatorTest {
    @Test
    public void parameterNormalizesAndRejectsInvalidModes() {
        SessionVariable variables = new SessionVariable();
        Assertions.assertEquals("colocate", variables.getRangeScanExecutionMode());
        Assertions.assertFalse(variables.isParallelRangeScan());
        variables.setRangeScanExecutionMode("PaRaLlEl");
        Assertions.assertEquals("parallel", variables.getRangeScanExecutionMode());
        Assertions.assertTrue(variables.isParallelRangeScan());
        for (String invalid : new String[] {"auto", "invalid", "", " parallel", null}) {
            Assertions.assertThrows(IllegalArgumentException.class, () -> variables.setRangeScanExecutionMode(invalid));
            Assertions.assertTrue(variables.isParallelRangeScan());
        }
        variables.setRangeScanExecutionMode("COLOCATE");
        Assertions.assertFalse(variables.isParallelRangeScan());
    }

    @Test
    public void modeIsPartOfLogicalAndPhysicalIdentityAndCopies() {
        LogicalOlapScanOperator ordinary = new LogicalOlapScanOperator(new OlapTable());
        LogicalOlapScanOperator parallel = LogicalOlapScanOperator.builder().withOperator(ordinary)
                .setDistributionSpec(DistributionSpec.createAnyDistributionSpec()).setParallelRangeScan(true).build();
        LogicalOlapScanOperator sameDistribution = LogicalOlapScanOperator.builder().withOperator(parallel)
                .setParallelRangeScan(false).build();
        Assertions.assertNotEquals(sameDistribution, parallel);
        Assertions.assertNotEquals(sameDistribution.hashCode(), parallel.hashCode());
        LogicalOlapScanOperator copy = LogicalOlapScanOperator.builder().withOperator(parallel).build();
        Assertions.assertTrue(copy.isParallelRangeScan());
        Assertions.assertEquals(parallel, copy);
        Assertions.assertEquals(parallel.hashCode(), copy.hashCode());
        PhysicalOlapScanOperator physical = new PhysicalOlapScanOperator(parallel);
        PhysicalOlapScanOperator physicalCopy = PhysicalOlapScanOperator.builder().withOperator(physical).build();
        Assertions.assertTrue(physicalCopy.isParallelRangeScan());
        Assertions.assertEquals(physical, physicalCopy);
        Assertions.assertEquals(physical.hashCode(), physicalCopy.hashCode());
        physicalCopy.setParallelRangeScan(false);
        Assertions.assertNotEquals(physical, physicalCopy);
        Assertions.assertNotEquals(physical.hashCode(), physicalCopy.hashCode());
    }
}
