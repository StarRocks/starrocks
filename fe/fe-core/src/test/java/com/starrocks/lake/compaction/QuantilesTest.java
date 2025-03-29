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

package com.starrocks.lake.compaction;

import org.junit.Assert;
import org.junit.Test;

public class QuantilesTest {
    @Test
    public void testBasic() {
        Quantiles q1 = new Quantiles(1.0, 2.0, 3.0);
        Quantiles q2 = new Quantiles(q1);
        Assert.assertTrue(q1.compareTo(q2) == 0);
    }

    @Test
    public void testCompare() {
        // avg
        {
            Quantiles q1 = new Quantiles(1.0, 0.0, 0.0);
            Quantiles q2 = new Quantiles(1.0, 0.0, 0.0);
            Assert.assertTrue(q1.compareTo(q2) == 0);
        }
        {
            Quantiles q1 = new Quantiles(1.0, 0.0, 0.0);
            Quantiles q2 = new Quantiles(2.0, 0.0, 0.0);
            Assert.assertTrue(q1.compareTo(q2) < 0);
        }
        {
            Quantiles q1 = new Quantiles(2.0, 0.0, 0.0);
            Quantiles q2 = new Quantiles(1.0, 0.0, 0.0);
            Assert.assertTrue(q1.compareTo(q2) > 0);
        }
        // p50
        {
            Quantiles q1 = new Quantiles(0.0, 1.0, 0.0);
            Quantiles q2 = new Quantiles(0.0, 1.0, 0.0);
            Assert.assertTrue(q1.compareTo(q2) == 0);
        }
        {
            Quantiles q1 = new Quantiles(0.0, 1.0, 0.0);
            Quantiles q2 = new Quantiles(0.0, 2.0, 0.0);
            Assert.assertTrue(q1.compareTo(q2) < 0);
        }
        {
            Quantiles q1 = new Quantiles(0.0, 2.0, 0.0);
            Quantiles q2 = new Quantiles(0.0, 1.0, 0.0);
            Assert.assertTrue(q1.compareTo(q2) > 0);
        }
        // max
        {
            Quantiles q1 = new Quantiles(0.0, 0.0, 1.0);
            Quantiles q2 = new Quantiles(0.0, 0.0, 1.0);
            Assert.assertTrue(q1.compareTo(q2) == 0);
        }
        {
            Quantiles q1 = new Quantiles(0.0, 0.0, 1.0);
            Quantiles q2 = new Quantiles(0.0, 0.0, 2.0);
            Assert.assertTrue(q1.compareTo(q2) < 0);
        }
        {
            Quantiles q1 = new Quantiles(0.0, 0.0, 2.0);
            Quantiles q2 = new Quantiles(0.0, 0.0, 1.0);
            Assert.assertTrue(q1.compareTo(q2) > 0);
        }

        {
            Quantiles q1 = new Quantiles(1.0, 1.0, 1.0);
            Quantiles q2 = new Quantiles(1.0, 1.0, 1.0);
            Assert.assertTrue(q1.compareTo(q2) == 0);
        }
    }
}
