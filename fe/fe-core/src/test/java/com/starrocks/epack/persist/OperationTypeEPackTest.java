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

package com.starrocks.epack.persist;

import com.starrocks.persist.OperationType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Set;

public class OperationTypeEPackTest {
    /**
     * The replay-failure skip check consults OperationTypeEPack.IGNORABLE_OPERATIONS only, so it
     * must contain everything the community set contains — otherwise switching the consumer from
     * OperationType.IGNORABLE_OPERATIONS would silently drop community ops.
     */
    @Test
    public void testEPackSetIsSupersetOfCommunitySet() {
        Assertions.assertTrue(
                OperationTypeEPack.IGNORABLE_OPERATIONS.containsAll(OperationType.IGNORABLE_OPERATIONS));
    }

    /**
     * Every op declared in OperationTypeEPack must carry @IgnorableOnReplayFailed, except the
     * deliberate exceptions pinned below.
     */
    @Test
    public void testAllEPackOpsAreIgnorable() throws IllegalAccessException {
        // Restoring from a cluster snapshot is a critical recovery step; a failed replay must
        // halt the FE rather than continue with a half-applied restore.
        Set<String> notIgnorable = Set.of("OP_RESTORE_FROM_SNAPSHOT");
        int checked = 0;
        for (Field field : OperationTypeEPack.class.getDeclaredFields()) {
            if (!field.getName().startsWith("OP_")) {
                continue;
            }
            short op = (short) field.get(null);
            if (notIgnorable.contains(field.getName())) {
                Assertions.assertFalse(OperationTypeEPack.IGNORABLE_OPERATIONS.contains(op),
                        field.getName() + " is deliberately not ignorable");
            } else {
                Assertions.assertTrue(OperationTypeEPack.IGNORABLE_OPERATIONS.contains(op),
                        field.getName() + " must be annotated with @IgnorableOnReplayFailed");
            }
            checked++;
        }
        Assertions.assertTrue(checked > 0);
    }

    /** The union must not accidentally turn unannotated community ops ignorable. */
    @Test
    public void testUnannotatedCommunityOpsNotIgnorable() {
        Assertions.assertFalse(OperationTypeEPack.IGNORABLE_OPERATIONS.contains(OperationType.OP_CREATE_USER_V2));
        Assertions.assertFalse(OperationType.IGNORABLE_OPERATIONS.contains(OperationTypeEPack.OP_CREATE_MASKING_POLICY),
                "community set stays community-only; consumers must use OperationTypeEPack.IGNORABLE_OPERATIONS");
    }
}
