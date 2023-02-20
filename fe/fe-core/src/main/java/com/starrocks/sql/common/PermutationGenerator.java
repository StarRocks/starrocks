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


package com.starrocks.sql.common;

import com.clearspring.analytics.util.Lists;

import java.util.List;

/**
 * Return permutaion of a list of candidates. eg: inputs: [a, b], [c, d], [e, f], the result will be:
 * [a, c, e]
 * [a, c, f]
 * [a, d, e]
 * [a, d, f]
 * [b, c, e]
 * [b, c, f]
 * [b, d, e]
 * [b, d, f]
 *
 * @param <T>
 */
public class PermutationGenerator<T> {
    private List<List<T>> lists;
    private int totalCount = 1;
    private int iTotal = 0;

    private List<Generator<T>> generators = Lists.newArrayList();

    public PermutationGenerator(List<List<T>> lists) {
        this.lists = lists;
        for (List<T> list : lists) {
            totalCount *= list.size();
        }
        if (totalCount > 0) {
            int multiplier = 1;
            for (List<T> list : lists) {
                multiplier *= list.size();
                generators.add(new Generator<T>(list, totalCount, totalCount / multiplier));
            }
        }
    }

    public boolean hasNext() {
        return iTotal < totalCount;
    }

    public List<T> next() {
        List<T> n = Lists.newArrayList();
        for (Generator<T> generator : generators) {
            assert (generator.hasNext());
            n.add(generator.next());
        }
        iTotal++;
        return n;
    }
}
