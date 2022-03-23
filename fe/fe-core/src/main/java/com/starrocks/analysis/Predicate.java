// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/Predicate.java

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

package com.starrocks.analysis;

import com.starrocks.catalog.ArrayType;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Pair;

// Our new cost based query optimizer is more powerful and stable than old query optimizer,
// The old query optimizer related codes could be deleted safely.
// TODO: Remove old query optimizer related codes before 2021-09-30
public abstract class Predicate extends Expr {
    protected boolean isEqJoinConjunct;

    public Predicate() {
        super();
        this.isEqJoinConjunct = false;
    }

    protected Predicate(Predicate other) {
        super(other);
        isEqJoinConjunct = other.isEqJoinConjunct;
    }

    public void setIsEqJoinConjunct(boolean v) {
        isEqJoinConjunct = v;
    }

    @Override
    protected void analyzeImpl(Analyzer analyzer) throws AnalysisException {
        type = Type.BOOLEAN;
        // values: true/false/null
        numDistinctValues = 3;

        for (Expr expr : children) {
            if (expr.getType().isOnlyMetricType() ||
                    (expr.getType() instanceof ArrayType && !(this instanceof IsNullPredicate))) {
                throw new AnalysisException("HLL, BITMAP, PERCENTILE and ARRAY type couldn't as Predicate");
            }
        }
    }

    /**
     * If predicate is of the form "<slotref> = <slotref>", returns both SlotRefs,
     * otherwise returns null.
     */
    public Pair<SlotId, SlotId> getEqSlots() {
        return null;
    }
}
