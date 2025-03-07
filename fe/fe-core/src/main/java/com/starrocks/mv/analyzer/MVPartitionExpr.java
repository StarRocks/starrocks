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
package com.starrocks.mv.analyzer;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.catalog.FunctionSet;

import java.util.List;
import java.util.Set;

/**
 * It's a helper class to keep the partition expression and the slot ref which it comes from.
 */
public class MVPartitionExpr {
    public static final Set<String> FN_NAME_TO_PARTITION = Sets.newHashSet(FunctionSet.DATE_TRUNC,
            FunctionSet.STR2DATE, FunctionSet.TIME_SLICE);
    // Functions that the first slot is the partition column, eg: str2date(dt, 'yyyy-MM-dd'), time_slice(dt, 'day')
    public static final Set<String> FN_NAMES_WITH_FIRST_SLOT = Sets.newHashSet(FunctionSet.STR2DATE,
            FunctionSet.TIME_SLICE);

    private Expr expr;
    private SlotRef slotRef;

    public MVPartitionExpr(Expr expr, SlotRef slotRef) {
        this.expr = expr;
        this.slotRef = slotRef;
    }

    public Expr getExpr() {
        return expr;
    }

    public SlotRef getSlotRef() {
        return slotRef;
    }

    @Override
    public String toString() {
        return "MVPartitionExpr{" +
                "expr=" + expr +
                ", slotRef=" + slotRef +
                '}';
    }

    public static boolean isSupportedMVPartitionExpr(Expr expr) {
        if (expr == null || !(expr instanceof FunctionCallExpr)) {
            return false;
        }
        FunctionCallExpr funcExpr = (FunctionCallExpr) expr;
        String fnName = funcExpr.getFnName().getFunction();
        if (!FN_NAME_TO_PARTITION.contains(fnName)) {
            return false;
        }
        return true;
    }

    public static MVPartitionExpr getSupportMvPartitionExpr(Expr expr) {
        if (expr instanceof SlotRef) {
            return new MVPartitionExpr(expr, (SlotRef) expr);
        }
        if (!isSupportedMVPartitionExpr(expr)) {
            return null;
        }

        FunctionCallExpr functionCallExpr = ((FunctionCallExpr) expr);
        List<SlotRef> slotRefs = Lists.newArrayList();
        functionCallExpr.collect(SlotRef.class, slotRefs);
        if (slotRefs.size() != 1) {
            return null;
        }
        return new MVPartitionExpr(functionCallExpr, slotRefs.get(0));
    }
}
