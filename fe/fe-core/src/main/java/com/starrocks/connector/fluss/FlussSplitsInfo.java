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

package com.starrocks.connector.fluss;

import org.apache.fluss.flink.source.split.SourceSplitBase;
import org.apache.fluss.predicate.Predicate;

import java.util.List;

public class FlussSplitsInfo {
    private final List<Predicate> predicates;
    private final List<SourceSplitBase> flussSplits;

    public FlussSplitsInfo(List<Predicate> predicates, List<SourceSplitBase> flussSplits) {
        this.predicates = predicates;
        this.flussSplits = flussSplits;
    }

    public List<Predicate> getPredicates() {
        return predicates;
    }

    public List<SourceSplitBase> getFlussSplits() {
        return flussSplits;
    }
}
