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

package com.starrocks.lake;

import com.starrocks.common.Status;
import com.starrocks.thrift.TStatusCode;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.OptionalLong;

public class LakeMetaVersionNotFoundExceptionTest {

    @Test
    public void testStatusPredicateSelectsOnlyTheDedicatedCode() {
        Status status = new Status(TStatusCode.LAKE_META_VERSION_NOT_FOUND,
                "lake tablet metadata version not found, tablet_id=10001, partition_id=10002, version=144847");
        Assertions.assertTrue(status.isLakeMetaVersionNotFound());
        Assertions.assertEquals(status.getErrorMsg(),
                new LakeMetaVersionNotFoundException(status.getErrorMsg()).getMessage());

        // The whole point of the dedicated code: a generic NOT_FOUND stays outside this failure class.
        Assertions.assertFalse(new Status(TStatusCode.NOT_FOUND, "Not found").isLakeMetaVersionNotFound());
        Assertions.assertFalse(new Status(TStatusCode.CANCELLED, "Cancelled").isLakeMetaVersionNotFound());
    }

    @Test
    public void testExtractsIdentifiersFromBackendMessage() {
        // The shape LakeDataSource produces, with starlet's own message appended.
        LakeMetaVersionNotFoundException e = new LakeMetaVersionNotFoundException(
                "lake tablet metadata version not found, tablet_id=10001, partition_id=10002, version=144847: "
                        + "Not found: The specified key does not exist.");

        Assertions.assertEquals(OptionalLong.of(10001), e.getTabletId());
        Assertions.assertEquals(OptionalLong.of(10002), e.getPartitionId());
        Assertions.assertEquals(OptionalLong.of(144847), e.getScanVersion());
    }

    @Test
    public void testUnparsableMessageYieldsEmptyFields() {
        // Logging must degrade instead of breaking the retry when a message changes shape.
        LakeMetaVersionNotFoundException e = new LakeMetaVersionNotFoundException("Not found");

        Assertions.assertEquals(OptionalLong.empty(), e.getTabletId());
        Assertions.assertEquals(OptionalLong.empty(), e.getPartitionId());
        Assertions.assertEquals(OptionalLong.empty(), e.getScanVersion());
    }

    @Test
    public void testPartitionIdIsNotMistakenForTabletId() {
        LakeMetaVersionNotFoundException e = new LakeMetaVersionNotFoundException(
                "lake tablet metadata version not found, tablet_id=7, partition_id=8, version=9: Not found");

        Assertions.assertEquals(OptionalLong.of(7), e.getTabletId());
        Assertions.assertEquals(OptionalLong.of(8), e.getPartitionId());
        Assertions.assertEquals(OptionalLong.of(9), e.getScanVersion());
    }
}
