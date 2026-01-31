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

package com.staros.schedule;

import com.staros.schedule.select.FirstNSelector;
import com.staros.schedule.select.Selector;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ScoreSelectionTest {

    @Test
    public void testFirstNSelector() {
        Selector selector = new FirstNSelector();
        Map<Long, Double> scores = new HashMap<>();
        int nSelect = 0;
        // 1: 10.0, 2: 8.0, 3: 12.5, 4: 10.0
        scores.put(1L, 10.0);
        scores.put(2L, 8.0);
        scores.put(3L, 12.5);
        scores.put(4L, 10.0);
        List<Long> highExpects = new ArrayList<>();
        List<Long> lowExpects = new ArrayList<>();
        { // choose 0 element
            List<Long> highSelects = selector.selectHighEnd(scores, nSelect);
            Assert.assertTrue(highSelects.isEmpty());
            List<Long> lowSelects = selector.selectHighEnd(scores, nSelect);
            Assert.assertTrue(lowSelects.isEmpty());
        }
        { // choose 1 element
            nSelect = 1;
            highExpects.add(3L);
            List<Long> highSelects = selector.selectHighEnd(scores, nSelect);
            Assert.assertEquals(highExpects, highSelects);
            lowExpects.add(2L);
            List<Long> lowSelects = selector.selectLowEnd(scores, nSelect);
            Assert.assertEquals(lowExpects, lowSelects);
        }
        { // choose 2 elements
            nSelect = 2;
            // add the 2nd selection
            highExpects.add(1L);
            List<Long> highSelects = selector.selectHighEnd(scores, nSelect);
            Assert.assertEquals(highExpects, highSelects);

            lowExpects.add(1L);
            List<Long> lowSelects = selector.selectLowEnd(scores, nSelect);
            Assert.assertEquals(lowExpects, lowSelects);
        }
        { // choose 3 elements
            nSelect = 3;
            // add the 3rd selection
            highExpects.add(4L);
            List<Long> highSelects = selector.selectHighEnd(scores, nSelect);
            Assert.assertEquals(highExpects, highSelects);

            lowExpects.add(4L);
            List<Long> lowSelects = selector.selectLowEnd(scores, nSelect);
            Assert.assertEquals(lowExpects, lowSelects);
        }
        { // choose 4 elements
            nSelect = 4;
            // add the 4th selection
            highExpects.add(2L);
            List<Long> highSelects = selector.selectHighEnd(scores, nSelect);
            Assert.assertEquals(highExpects, highSelects);

            lowExpects.add(3L);
            List<Long> lowSelects = selector.selectLowEnd(scores, nSelect);
            Assert.assertEquals(lowExpects, lowSelects);
        }
        { // choose 5 elements, only 4 elements available
            nSelect = 5;
            List<Long> highSelects = selector.selectHighEnd(scores, nSelect);
            Assert.assertEquals(highExpects, highSelects);

            List<Long> lowSelects = selector.selectLowEnd(scores, nSelect);
            Assert.assertEquals(lowExpects, lowSelects);
        }
    }
}
