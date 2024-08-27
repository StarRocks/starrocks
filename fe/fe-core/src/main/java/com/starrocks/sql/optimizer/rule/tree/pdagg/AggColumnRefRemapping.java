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

package com.starrocks.sql.optimizer.rule.tree.pdagg;

import com.google.common.collect.Maps;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

// After rewrite, the post-rewrite tree must replace the old slotId with the new one,
// ColumnRefRemapping is used to keep the mapping: old slotId->new slotId
public class AggColumnRefRemapping {
    private final Map<ColumnRefOperator, ColumnRefOperator> remapping;
    private Optional<ReplaceColumnRefRewriter> cachedReplacer = Optional.empty();
    private Optional<ColumnRefSet> cachedColumnRefSet = Optional.empty();
    public static final AggColumnRefRemapping EMPTY_REMAPPING = new AggColumnRefRemapping();

    public AggColumnRefRemapping() {
        remapping = Maps.newHashMap();
    }

    public AggColumnRefRemapping(Map<ColumnRefOperator, ColumnRefOperator> remapping) {
        this.remapping = remapping;
    }

    public void combine(AggColumnRefRemapping other) {
        remapping.putAll(other.remapping);
        cachedReplacer = Optional.empty();
        cachedColumnRefSet = Optional.empty();
    }

    public ReplaceColumnRefRewriter getReplacer() {
        if (!cachedReplacer.isPresent()) {
            ReplaceColumnRefRewriter replacer =
                    new ReplaceColumnRefRewriter(remapping.entrySet().stream().collect(
                            Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)), true);
            cachedReplacer = Optional.of(replacer);
        }
        return cachedReplacer.get();
    }

    public ColumnRefSet getColumnRefSet() {
        if (!cachedColumnRefSet.isPresent()) {
            cachedColumnRefSet = Optional.of(new ColumnRefSet(remapping.keySet()));
        }
        return cachedColumnRefSet.get();
    }

    public boolean isEmpty() {
        return remapping.isEmpty();
    }

    public Map<ColumnRefOperator, ColumnRefOperator> getRemapping() {
        return remapping;
    }
}
