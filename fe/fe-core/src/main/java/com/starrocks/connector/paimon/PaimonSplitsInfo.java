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

package com.starrocks.connector.paimon;

import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.source.Split;

import java.util.List;

public class PaimonSplitsInfo {
    private final List<Split> paimonSplits;
    private final List<Predicate> predicates;
    public PaimonSplitsInfo(List<Predicate> predicates, List<Split> paimonSplits) {
        this.predicates = predicates;
        this.paimonSplits = paimonSplits;
    }

    public List<Split> getPaimonSplits() {
        return paimonSplits;
    }

    public List<Predicate> getPredicate() {
        return predicates;
    }
}
