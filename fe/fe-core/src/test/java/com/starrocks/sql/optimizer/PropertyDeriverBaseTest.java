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

package com.starrocks.sql.optimizer;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.base.DistributionCol;
import com.starrocks.sql.optimizer.base.DistributionProperty;
import com.starrocks.sql.optimizer.base.DistributionSpec;
import com.starrocks.sql.optimizer.base.HashDistributionDesc;
import com.starrocks.sql.optimizer.base.HashDistributionSpec;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.starrocks.sql.optimizer.base.HashDistributionDesc.SourceType.SHUFFLE_JOIN;

class PropertyDeriverBaseTest {

    @Test
    void testComputeShuffleJoinRequiredPropertiesDoNotReorderOnPermutationMatch() {
        // Parent requires SHUFFLE_JOIN(303, 304)
        HashDistributionSpec parentSpec = DistributionSpec.createHashDistributionSpec(
                new HashDistributionDesc(Lists.newArrayList(
                        new DistributionCol(303, true),
                        new DistributionCol(304, true)), SHUFFLE_JOIN));
        PhysicalPropertySet requiredFromParent = new PhysicalPropertySet(DistributionProperty.createProperty(parentSpec));

        // Current join's eq predicates produce left keys (304, 303) and right keys (332, 333)
        List<DistributionCol> leftShuffleColumns = Lists.newArrayList(
                new DistributionCol(304, true),
                new DistributionCol(303, true));
        List<DistributionCol> rightShuffleColumns = Lists.newArrayList(
                new DistributionCol(332, true),
                new DistributionCol(333, true));

        List<PhysicalPropertySet> required =
                PropertyDeriverBase.computeShuffleJoinRequiredProperties(requiredFromParent, leftShuffleColumns,
                        rightShuffleColumns);

        HashDistributionSpec leftReqSpec =
                (HashDistributionSpec) required.get(0).getDistributionProperty().getSpec();
        HashDistributionSpec rightReqSpec =
                (HashDistributionSpec) required.get(1).getDistributionProperty().getSpec();

        // Must keep the join key order (304,303)/(332,333). Reordering would make the optimizer's
        // required properties disagree with the join conjunct order unless an Exchange is inserted.
        Assertions.assertEquals(Lists.newArrayList(304, 303), leftReqSpec.getHashDistributionDesc().getExplainInfo());
        Assertions.assertEquals(Lists.newArrayList(332, 333), rightReqSpec.getHashDistributionDesc().getExplainInfo());
    }
}

