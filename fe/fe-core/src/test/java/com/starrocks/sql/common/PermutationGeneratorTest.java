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

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class PermutationGeneratorTest {
    @Test
    public void testPermutation() {
        List<List<Long>> ids = Lists.newArrayList();
        for (long i = 0; i < 5; i++) {
            List<Long> subIds = Lists.newArrayList();
            for (long j = 0; j < 10; j++) {
                subIds.add(j);
            }
            ids.add(subIds);
        }
        int count = 0;
        PermutationGenerator generator = new PermutationGenerator(ids);
        while (generator.hasNext()) {
            generator.next();
            ++count;
        }
        Assert.assertEquals(100000, count);
    }
}
