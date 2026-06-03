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

import java.util.List;

/**
 * One target partition predicted by {@link PartitionSampleGrouper} from a
 * load's sample set. Carries everything the downstream coordinator needs to
 * either pre-create the partition ({@code !existsInCatalog}) or to submit a
 * reshard for an existing empty single-tablet partition.
 *
 * <p>Invariants:
 * <ul>
 *   <li>{@code existsInCatalog} implies {@code partitionIdIfExists > 0},
 *       {@code oldTabletIdIfExists > 0}, and {@code analyzedClause == null}.</li>
 *   <li>{@code !existsInCatalog} implies {@code analyzedClause != null}
 *       (caller pre-creates via {@code LocalMetastore.addPartitions}).</li>
 * </ul>
 */
public record PartitionSamples(
        List<String> partitionValues,
        String partitionName,
        boolean existsInCatalog,
        long partitionIdIfExists,
        long oldTabletIdIfExists,
        AddPartitionClause analyzedClause,
        List<SampleRow> samples,
        long estimatedBytes) {

    public PartitionSamples {
        if (existsInCatalog) {
            if (partitionIdIfExists <= 0 || oldTabletIdIfExists <= 0) {
                throw new IllegalArgumentException(
                        "existing partition requires partitionId+oldTabletId > 0 (got "
                                + partitionIdIfExists + ", " + oldTabletIdIfExists + ")");
            }
            if (analyzedClause != null) {
                throw new IllegalArgumentException(
                        "existing partition must not carry analyzedClause");
            }
        } else {
            if (analyzedClause == null) {
                throw new IllegalArgumentException(
                        "non-existing partition requires analyzedClause");
            }
        }
    }
}
