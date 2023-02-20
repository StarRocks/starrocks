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


package com.starrocks.planner;

import com.clearspring.analytics.util.Lists;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.SlotDescriptor;
import com.starrocks.analysis.SlotId;
import com.starrocks.analysis.SlotRef;
import com.starrocks.catalog.Type;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class PlanNodeTest {

    @BeforeClass
    public static void beforeClass() throws Exception {
    }

    Expr createSlotRef(int idx) {
        SlotId slotId = new SlotId(idx);
        SlotDescriptor descriptor = new SlotDescriptor(slotId, Integer.toString(idx), Type.INT,true);
        return new SlotRef(Integer.toString(idx), descriptor);
    }

    List<List<Expr>> createSlotRefArray(int m, int n) {
        List<List<Expr>> slotRefs = Lists.newArrayList();
        int k = 0;
        for (int i = 0; i < m; i++) {
            slotRefs.add(Lists.newArrayList());
            for (int j = 0; j < n; j++) {
                slotRefs.get(i).add(createSlotRef(k));
                k++;
            }
        }
        return slotRefs;
    }

    List<Integer> slotRefsToInt(List<Expr> slotRefs) {
        List<Integer> result = Lists.newArrayList();
        for (Expr expr: slotRefs) {
            if (!(expr instanceof SlotRef)) {
                Assert.assertTrue(false);
            }
            result.add(((SlotRef) expr).getSlotId().asInt());
        }
        return result;
    }

    boolean slotRefsEqualTo(List<Expr> slotRefs, List<Integer> expect) {
        List<Integer> slotRefsToInt = slotRefsToInt(slotRefs);
        System.out.println("slotRefs:" + slotRefsToInt(slotRefs));
        System.out.println("expect:" + expect);
        if (slotRefsToInt.size() != expect.size()) {
            return false;
        }
        for (int i = 0; i < expect.size(); i++) {
            if (slotRefsToInt.get(i) != expect.get(i)) {
                return false;
            }
        }
        return true;
    }

    @Test
    public void testPermutaionsOfPartitionByExprs1() throws Exception {
        List<List<Expr>> slotRefs = createSlotRefArray(2, 3);
        for (List<Expr> refs: slotRefs) {
            System.out.println(slotRefsToInt(refs));
        }
        List<List<Expr>> newSlotRefs = PlanNode.candidateOfPartitionByExprs(slotRefs);
        Assert.assertTrue(newSlotRefs.size() == 8);
        for (List<Expr> candidates: newSlotRefs) {
            System.out.println(slotRefsToInt(candidates));
        }
        int k = 0;
        for (int i = 0; i < 3; i++) {
            for (int j = 3; j < 6; j++) {
                if (k >= 8) break;
                Assert.assertTrue(slotRefsEqualTo(newSlotRefs.get(k++), Arrays.asList(i, j)));
            }
        }
    }

    @Test
    public void testPermutaionsOfPartitionByExprs2() throws Exception {
        List<List<Expr>> slotRefs = createSlotRefArray(1, 3);
        for (List<Expr> refs: slotRefs) {
            System.out.println(slotRefsToInt(refs));
        }
        List<List<Expr>> newSlotRefs = PlanNode.candidateOfPartitionByExprs(slotRefs);
        Assert.assertTrue(newSlotRefs.size() == 3);
        for (List<Expr> candidates: newSlotRefs) {
            System.out.println(slotRefsToInt(candidates));
        }

        Assert.assertTrue(slotRefsEqualTo(newSlotRefs.get(0), Arrays.asList(0)));
        Assert.assertTrue(slotRefsEqualTo(newSlotRefs.get(1), Arrays.asList(1)));
        Assert.assertTrue(slotRefsEqualTo(newSlotRefs.get(2), Arrays.asList(2)));
    }

    @Test
    public void testPermutaionsOfPartitionByExprs3() throws Exception {
        List<List<Expr>> slotRefs = createSlotRefArray(4, 5);
        for (List<Expr> refs: slotRefs) {
            System.out.println(slotRefsToInt(refs));
        }
        List<List<Expr>> newSlotRefs = PlanNode.candidateOfPartitionByExprs(slotRefs);
        Assert.assertTrue (newSlotRefs.size() == 8);
        for (List<Expr> candidates: newSlotRefs) {
            System.out.println(slotRefsToInt(candidates));
        }
        Assert.assertTrue(slotRefsEqualTo(newSlotRefs.get(0), Arrays.asList(0, 5, 10, 15)));
        Assert.assertTrue(slotRefsEqualTo(newSlotRefs.get(1), Arrays.asList(0, 5, 10, 16)));
        Assert.assertTrue(slotRefsEqualTo(newSlotRefs.get(2), Arrays.asList(0, 5, 10, 17)));
        Assert.assertTrue(slotRefsEqualTo(newSlotRefs.get(3), Arrays.asList(0, 5, 10, 18)));
        Assert.assertTrue(slotRefsEqualTo(newSlotRefs.get(4), Arrays.asList(0, 5, 10, 19)));
        Assert.assertTrue(slotRefsEqualTo(newSlotRefs.get(5), Arrays.asList(0, 5, 11, 15)));
        Assert.assertTrue(slotRefsEqualTo(newSlotRefs.get(6), Arrays.asList(0, 5, 11, 16)));
        Assert.assertTrue(slotRefsEqualTo(newSlotRefs.get(7), Arrays.asList(0, 5, 11, 17)));
    }
}
