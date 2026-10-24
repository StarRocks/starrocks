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

package com.starrocks.sql.optimizer.base;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

public class QueryGroupDistributionSpecTest {
    private QueryGroupDistributionSpec spec() {
        List<DistributionCol> columns = List.of(new DistributionCol(1, true),
                new DistributionCol(2, true), new DistributionCol(3, true));
        EquivalentDescriptor descriptor = new EquivalentDescriptor(17, List.of(21L, 22L));
        descriptor.initDistributionUnionFind(columns);
        return new QueryGroupDistributionSpec(17, columns, List.of(4, 5, 6), descriptor);
    }

    @Test
    public void onlyCompleteAggregateCoverageIsSatisfied() {
        QueryGroupDistributionSpec spec = spec();
        Assertions.assertTrue(spec.isSatisfy(DistributionSpec.createAnyDistributionSpec()));
        for (HashDistributionDesc.SourceType type : HashDistributionDesc.SourceType.values()) {
            Assertions.assertEquals(type == HashDistributionDesc.SourceType.SHUFFLE_AGG,
                    spec.isSatisfy(new HashDistributionSpec(new HashDistributionDesc(List.of(3, 1, 2), type))),
                    type.toString());
        }
        Assertions.assertFalse(spec.isSatisfy(new HashDistributionSpec(new HashDistributionDesc(
                List.of(1, 2), HashDistributionDesc.SourceType.SHUFFLE_AGG))));
        Assertions.assertFalse(spec.isSatisfy(DistributionSpec.createGatherDistributionSpec()));
    }

    @Test
    public void aliasEquivalenceIsNullStrictAndExcludedFromIdentity() {
        QueryGroupDistributionSpec spec = spec();
        QueryGroupDistributionSpec copy = spec();
        int hash = spec.hashCode();
        spec.getEquivalentDescriptor().unionDistributionCols(new DistributionCol(1, true), new DistributionCol(11, true));
        Assertions.assertTrue(spec.isSatisfy(new HashDistributionSpec(new HashDistributionDesc(
                List.of(11, 2, 3), HashDistributionDesc.SourceType.SHUFFLE_AGG))));
        Assertions.assertEquals(copy, spec);
        Assertions.assertEquals(hash, spec.hashCode());
        Assertions.assertNotEquals(spec, new QueryGroupDistributionSpec(17, spec.getOrderedCColumns(),
                List.of(4, 5, 7), spec.getEquivalentDescriptor()));
        Assertions.assertNotEquals(spec, new QueryGroupDistributionSpec(18, spec.getOrderedCColumns(),
                List.of(4, 5, 6), spec.getEquivalentDescriptor()));
    }

    @Test
    public void metadataIsImmutableAndNeverAnExchangeContract() {
        QueryGroupDistributionSpec spec = spec();
        List<Integer> ids = new ArrayList<>(List.of(4, 5, 6));
        QueryGroupDistributionSpec copy = new QueryGroupDistributionSpec(17, spec.getOrderedCColumns(), ids,
                spec.getEquivalentDescriptor());
        ids.clear();
        Assertions.assertEquals(List.of(4, 5, 6), copy.getOrderedPhysicalCUniqueIds());
        Assertions.assertThrows(UnsupportedOperationException.class, () -> copy.getOrderedCColumns().clear());
        Assertions.assertThrows(IllegalStateException.class, () -> spec.getType().toThrift());
        Assertions.assertThrows(IllegalStateException.class,
                () -> DistributionProperty.createProperty(spec).appendEnforcers(null));
    }
    @Test
    public void relaxedAliasDoesNotProveAnAggregateKey() {
        QueryGroupDistributionSpec spec = spec();
        spec.getEquivalentDescriptor().unionNullRelaxCols(new DistributionCol(1, true), new DistributionCol(11, true));
        Assertions.assertFalse(spec.isSatisfy(new HashDistributionSpec(new HashDistributionDesc(
                List.of(11, 2, 3), HashDistributionDesc.SourceType.SHUFFLE_AGG))));
    }
}
