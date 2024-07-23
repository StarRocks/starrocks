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

import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.FunctionSet;

/**
 * It's a helper class to check the equality of two MV partition expressions.
 */
public class MVPartitionExprEqChecker {
    private static boolean areEqualSlotRefs(SlotRef s1, SlotRef s2) {
        if (!s1.getColumnName().equalsIgnoreCase(s2.getColumnName())) {
            return false;
        }
        if (!areEqualTableNames(s1.getTblNameWithoutAnalyzed(), s2.getTblNameWithoutAnalyzed())) {
            return false;
        }
        return true;
    }

    private static boolean areEqualTableNames(TableName t1, TableName t2) {
        if (t1 == null || t2 == null) {
            return false;
        }
        if (!t1.equals(t2)) {
            return false;
        }
        return true;
    }

    private static boolean areEqualFuncExprWithSlot(FunctionCallExpr f1, FunctionCallExpr f2,
                                                    int slotChildIdx,
                                                    int constChildIdx) {
        if (!f1.getChild(constChildIdx).equals(f2.getChild(constChildIdx))) {
            return false;
        }
        if (!(f1.getChild(slotChildIdx) instanceof SlotRef) || !(f2.getChild(slotChildIdx) instanceof SlotRef)) {
            return false;
        }
        return areEqualSlotRefs((SlotRef) f1.getChild(slotChildIdx), (SlotRef) f2.getChild(slotChildIdx));
    }

    public static boolean areEqualExprs(Expr expr1, Expr expr2) {
        if (expr1 instanceof SlotRef && expr2 instanceof SlotRef) {
            return areEqualSlotRefs((SlotRef) expr1, (SlotRef) expr2);
        } else {
            if (!MVPartitionExpr.isSupportedMVPartitionExpr(expr1) || !MVPartitionExpr.isSupportedMVPartitionExpr(expr2)) {
                return false;
            }
            FunctionCallExpr funcExpr1 = (FunctionCallExpr) expr1;
            FunctionCallExpr funcExpr2 = (FunctionCallExpr) expr2;
            String fnName1 = funcExpr1.getFnName().getFunction();
            String fnName2 = funcExpr2.getFnName().getFunction();
            if (!fnName1.equalsIgnoreCase(fnName2)) {
                return false;
            }
            if (fnName1.equalsIgnoreCase(FunctionSet.DATE_TRUNC)) {
                return areEqualFuncExprWithSlot(funcExpr1, funcExpr2, 1, 0);
            } else {
                return areEqualFuncExprWithSlot(funcExpr1, funcExpr2, 0, 1);
            }
        }
    }
}
