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

package com.starrocks.connector.iceberg;

import com.starrocks.connector.iceberg.IcebergMORParams.EqDeleteScope;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class IcebergMORParamsTest {
    private static final Schema SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()));

    // specId 0 -> unpartitioned (global), specId 1 -> partitioned by name.
    private static final Map<Integer, PartitionSpec> SPECS = Map.of(
            0, PartitionSpec.unpartitioned(),
            1, PartitionSpec.builderFor(SCHEMA).withSpecId(1).identity("name").build());

    private static DeleteFile mockDeleteFile(int specId, List<Integer> equalityIds) {
        DeleteFile file = Mockito.mock(DeleteFile.class);
        Mockito.when(file.specId()).thenReturn(specId);
        Mockito.when(file.equalityFieldIds()).thenReturn(equalityIds);
        return file;
    }

    @Test
    public void testScopeOf() {
        assertEquals(EqDeleteScope.GLOBAL, IcebergMORParams.scopeOf(mockDeleteFile(0, List.of(1)), SPECS));
        assertEquals(EqDeleteScope.PARTITIONED, IcebergMORParams.scopeOf(mockDeleteFile(1, List.of(1)), SPECS));
    }

    @Test
    public void testScopeParticipatesInIdentity() {
        List<Integer> ids = List.of(1, 2);
        // The whole point of the split: GLOBAL and PARTITIONED legs of one equality-id set must NOT
        // collapse, or the queues map / remote-file cache key would overwrite one with the other.
        assertNotEquals(IcebergMORParams.ofEqDelete(ids, EqDeleteScope.GLOBAL),
                IcebergMORParams.ofEqDelete(ids, EqDeleteScope.PARTITIONED));
        assertNotEquals(IcebergMORParams.ofEqDelete(ids, EqDeleteScope.GLOBAL).hashCode(),
                IcebergMORParams.ofEqDelete(ids, EqDeleteScope.PARTITIONED).hashCode());
        // Same equality ids + same scope are equal (so distinct() dedups them into one leg).
        assertEquals(IcebergMORParams.ofEqDelete(ids, EqDeleteScope.GLOBAL),
                IcebergMORParams.ofEqDelete(ids, EqDeleteScope.GLOBAL));
        // A scopeless EQ_DELETE param differs from a scoped one.
        assertNotEquals(IcebergMORParams.of(IcebergMORParams.ScanTaskType.EQ_DELETE, ids),
                IcebergMORParams.ofEqDelete(ids, EqDeleteScope.GLOBAL));
    }

    @Test
    public void testMatchesEqDeleteFileByEqualityIdsAndScope() {
        IcebergMORParams partitionLeg = IcebergMORParams.ofEqDelete(List.of(1, 2), EqDeleteScope.PARTITIONED);
        IcebergMORParams globalLeg = IcebergMORParams.ofEqDelete(List.of(1, 2), EqDeleteScope.GLOBAL);

        DeleteFile partitionedFile = mockDeleteFile(1, List.of(1, 2));
        DeleteFile globalFile = mockDeleteFile(0, List.of(1, 2));
        DeleteFile otherIdsPartitionedFile = mockDeleteFile(1, List.of(1));

        // Right equality ids and right scope.
        assertTrue(partitionLeg.matchesEqDeleteFile(partitionedFile, SPECS));
        assertTrue(globalLeg.matchesEqDeleteFile(globalFile, SPECS));

        // Scope mismatch: a partitioned-spec file must not be applied by the global leg, and vice versa.
        assertFalse(partitionLeg.matchesEqDeleteFile(globalFile, SPECS));
        assertFalse(globalLeg.matchesEqDeleteFile(partitionedFile, SPECS));

        // Equality-id mismatch: a [1] file must not be scanned by a [1,2] leg. This is the latent gap
        // that turns harmful under null-safe equals (NULL <=> NULL would over-delete).
        assertFalse(partitionLeg.matchesEqDeleteFile(otherIdsPartitionedFile, SPECS));

        // Non-EQ_DELETE params never match an equality-delete file.
        assertFalse(IcebergMORParams.DATA_FILE_WITH_EQ_DELETE.matchesEqDeleteFile(partitionedFile, SPECS));
    }
}
