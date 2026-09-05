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

package com.starrocks.alter.reshard.presplit;

import com.starrocks.sql.ast.AddPartitionClause;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.mockito.Mockito.mock;

/**
 * Unit tests for the {@link PartitionSamples} compact-constructor invariants.
 *
 * <p>The record encodes two states — existing-in-catalog (carries stable
 * partition + tablet ids, no clause) and not-existing (carries an analyzed
 * {@code AddPartitionClause} for pre-create). The compact constructor rejects
 * every combination that contradicts those states so a malformed grouper output
 * fails fast at construction instead of producing a wrong reshard later.
 */
public class PartitionSamplesTest {

    private static final List<String> PARTITION_VALUES = List.of("v");
    private static final List<SampleRow> SAMPLES =
            List.of(SampleRow.ofSortKey(List.of()));

    @Test
    public void existingPartitionWithNonPositiveIdsRejected() {
        // existsInCatalog=true requires both partitionId and oldTabletId > 0.
        IllegalArgumentException badPartitionId = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> new PartitionSamples(PARTITION_VALUES, "p", /*existsInCatalog*/ true,
                        /*partitionIdIfExists*/ 0L, /*oldTabletIdIfExists*/ 21_001L,
                        /*analyzedClause*/ null, SAMPLES, /*estimatedBytes*/ 0L));
        Assertions.assertTrue(badPartitionId.getMessage().contains("partitionId+oldTabletId > 0"));

        IllegalArgumentException badTabletId = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> new PartitionSamples(PARTITION_VALUES, "p", true,
                        11_001L, /*oldTabletIdIfExists*/ -1L, null, SAMPLES, 0L));
        Assertions.assertTrue(badTabletId.getMessage().contains("partitionId+oldTabletId > 0"));
    }

    @Test
    public void existingPartitionCarryingClauseRejected() {
        // existsInCatalog=true must NOT carry an analyzedClause — pre-create is
        // only for missing partitions.
        AddPartitionClause clause = mock(AddPartitionClause.class);
        IllegalArgumentException thrown = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> new PartitionSamples(PARTITION_VALUES, "p", /*existsInCatalog*/ true,
                        11_001L, 21_001L, /*analyzedClause*/ clause, SAMPLES, 0L));
        Assertions.assertTrue(thrown.getMessage().contains("must not carry analyzedClause"));
    }

    @Test
    public void nonExistingPartitionWithoutClauseRejected() {
        // existsInCatalog=false requires a non-null analyzedClause so the
        // coordinator can pre-create the partition.
        IllegalArgumentException thrown = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> new PartitionSamples(PARTITION_VALUES, "p", /*existsInCatalog*/ false,
                        -1L, -1L, /*analyzedClause*/ null, SAMPLES, 0L));
        Assertions.assertTrue(thrown.getMessage().contains("requires analyzedClause"));
    }

    @Test
    public void validExistingPartitionConstructs() {
        // Positive control: a well-formed existing-partition record constructs and
        // exposes its fields unchanged.
        PartitionSamples samples = new PartitionSamples(PARTITION_VALUES, "p", true,
                11_001L, 21_001L, null, SAMPLES, 4096L);
        Assertions.assertTrue(samples.existsInCatalog());
        Assertions.assertEquals(11_001L, samples.partitionIdIfExists());
        Assertions.assertEquals(21_001L, samples.oldTabletIdIfExists());
        Assertions.assertNull(samples.analyzedClause());
        Assertions.assertEquals(4096L, samples.estimatedBytes());
    }

    @Test
    public void validMissingPartitionConstructs() {
        // Positive control: a well-formed missing-partition record carries its
        // clause and constructs without throwing.
        AddPartitionClause clause = mock(AddPartitionClause.class);
        PartitionSamples samples = new PartitionSamples(PARTITION_VALUES, "p", false,
                -1L, -1L, clause, SAMPLES, 0L);
        Assertions.assertFalse(samples.existsInCatalog());
        Assertions.assertSame(clause, samples.analyzedClause());
    }
}
