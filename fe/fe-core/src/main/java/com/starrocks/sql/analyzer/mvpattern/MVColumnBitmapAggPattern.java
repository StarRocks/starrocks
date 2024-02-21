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

package com.starrocks.sql.analyzer.mvpattern;

import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;

import static com.starrocks.catalog.FunctionSet.BITMAP_AGG_TYPE;

public class MVColumnBitmapAggPattern implements MVColumnPattern {

    @Override
    public boolean match(Expr expr) {
        if (!(expr instanceof FunctionCallExpr)) {
            return false;
        }
        FunctionCallExpr fnExpr = (FunctionCallExpr) expr;
        String fnNameString = fnExpr.getFnName().getFunction();
        if (!fnNameString.equalsIgnoreCase(FunctionSet.BITMAP_AGG)) {
            return false;
        }
        if (fnExpr.getChild(0) instanceof SlotRef) {
            SlotRef slotRef = (SlotRef) fnExpr.getChild(0);
            Type slotRefType = slotRef.getType();
            return BITMAP_AGG_TYPE.contains(slotRefType);
        } else if (fnExpr.getChild(0) instanceof Expr) {
            Expr child0FnExpr = (Expr) fnExpr.getChild(0);
            Type childType = child0FnExpr.getType();
            return BITMAP_AGG_TYPE.contains(childType);
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return FunctionSet.BITMAP_AGG + "(column), type of column only support integer. ";
    }
}
