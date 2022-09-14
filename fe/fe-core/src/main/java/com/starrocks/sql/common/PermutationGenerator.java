// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.common;

import com.clearspring.analytics.util.Lists;

import java.util.List;

/**
 * Return permutaion of a list of candidates. eg: inputs: [a, b], [c, d], [e, f], the result will be:
 *    [a, c, e]
 *    [a, c, f]
 *    [a, d, e]
 *    [a, d, f]
 *    [b, c, e]
 *    [b, c, f]
 *    [b, d, e]
 *    [b, d, f]
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
