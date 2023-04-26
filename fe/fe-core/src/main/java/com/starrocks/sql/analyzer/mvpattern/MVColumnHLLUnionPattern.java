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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/MVColumnHLLUnionPattern.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.sql.analyzer.mvpattern;

import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.PrimitiveType;

public class MVColumnHLLUnionPattern implements MVColumnPattern {
    @Override
    public boolean match(Expr expr) {
        if (!(expr instanceof FunctionCallExpr)) {
            return false;
        }
        FunctionCallExpr fnExpr = (FunctionCallExpr) expr;
        String fnNameString = fnExpr.getFnName().getFunction();
        if (!fnNameString.equalsIgnoreCase(FunctionSet.HLL_UNION)) {
            return false;
        }
        if (fnExpr.getChild(0) instanceof SlotRef) {
            SlotRef slotRef = (SlotRef) fnExpr.getChild(0);
            if (slotRef.getType().getPrimitiveType() == PrimitiveType.HLL) {
                return true;
            } else {
                return false;
            }
        } else if (fnExpr.getChild(0) instanceof FunctionCallExpr) {
            FunctionCallExpr child0FnExpr = (FunctionCallExpr) fnExpr.getChild(0);
            if (!child0FnExpr.getFnName().getFunction().equalsIgnoreCase(FunctionSet.HLL_HASH)) {
                return false;
            }
            SlotRef slotRef = child0FnExpr.getChild(0).unwrapSlotRef();
            if (slotRef == null) {
                return false;
            } else if (slotRef.getType().isDecimalOfAnyVersion()) {
                return false;
            }
            return true;
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return FunctionSet.HLL_UNION + "(" + FunctionSet.HLL_HASH + "(column)) column could not be decimal. "
                + "Or " + FunctionSet.HLL_UNION + "(hll_column) in agg table";
    }
}
