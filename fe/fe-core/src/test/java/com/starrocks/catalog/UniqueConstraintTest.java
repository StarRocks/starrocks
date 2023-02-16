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

package com.starrocks.catalog;

import jersey.repackaged.com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class UniqueConstraintTest {
    @Test
    public void testParse() {
        String constraintDescs = "col1, col2  , col3 ";
        List<UniqueConstraint> results = UniqueConstraint.parse(constraintDescs);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(Lists.newArrayList("col1", "col2", "col3"), results.get(0).getUniqueColumns());

        String constraintDescs2 = "col1, col2  , col3 ; col4, col5, col6,   col7  ; col8,;";
        List<UniqueConstraint> results2 = UniqueConstraint.parse(constraintDescs2);
        Assert.assertEquals(3, results2.size());
        Assert.assertEquals(Lists.newArrayList("col1", "col2", "col3"), results2.get(0).getUniqueColumns());
        Assert.assertEquals(Lists.newArrayList("col4", "col5", "col6", "col7"), results2.get(1).getUniqueColumns());
        Assert.assertEquals(Lists.newArrayList("col8"), results2.get(2).getUniqueColumns());
    }
}
