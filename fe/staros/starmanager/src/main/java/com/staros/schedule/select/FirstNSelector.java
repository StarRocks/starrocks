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


package com.staros.schedule.select;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * The most naive way to choose n elements from map according to the scores ranking.
 *   Pros: simple, predictable
 *   Cons: easy to cause peak load of black hole when new worker added.
 */
public class FirstNSelector implements Selector {
    @Override
    public List<Long> selectHighEnd(Map<Long, Double> scores, int nSelect) {
        return selectInternal(scores, nSelect, Comparator.reverseOrder());
    }

    @Override
    public List<Long> selectLowEnd(Map<Long, Double> scores, int nSelect) {
        return selectInternal(scores, nSelect, Comparator.naturalOrder());
    }

    private List<Long> selectInternal(Map<Long, Double> scores, int nSelect, Comparator<? super Double> cmp) {
        List<Map.Entry<Long, Double>> sortedLists = scores.entrySet().stream()
                .sorted(Map.Entry.comparingByValue(cmp))
                .collect(Collectors.toList());
        List<Map.Entry<Long, Double>> finalLists = sortedLists;
        if (sortedLists.size() > nSelect) {
            finalLists = sortedLists.subList(0, nSelect);
        }
        return finalLists.stream().map(Map.Entry::getKey).collect(Collectors.toList());
    }
}
