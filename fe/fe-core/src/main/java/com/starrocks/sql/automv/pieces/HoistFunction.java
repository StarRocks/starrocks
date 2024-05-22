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

package com.starrocks.sql.automv.pieces;

import com.google.common.base.Preconditions;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.sql.automv.pn.Op;

import java.util.List;

public interface HoistFunction {
    HoistFunction LEFT_OUTER_OR_ANTI_JOIN_HOISTER =
            (triple, lhsConjuncts, rhsConjuncts, onConjuncts, hoistConjuncts) -> {
                switch (triple.getBits()) {
                    case HoistTriple.BIT_NONE:
                        return;
                    case HoistTriple.BIT_LHS:
                    case HoistTriple.BIT_LHS_ON:
                    case HoistTriple.BIT_LHS_RHS:
                    case HoistTriple.BIT_LHS_RHS_ON:
                        hoistConjuncts.add(triple.mustGetOp(HoistTriple.BIT_LHS_SHIFT));
                        return;
                    case HoistTriple.BIT_RHS:
                        onConjuncts.add(triple.mustGetOp(HoistTriple.BIT_RHS_SHIFT));
                        return;
                    case HoistTriple.BIT_ON:
                    case HoistTriple.BIT_RHS_ON:
                        onConjuncts.add(triple.mustGetOp(HoistTriple.BIT_ON_SHIFT));
                        return;
                    default:
                        Preconditions.checkArgument(false, "Never reach here");
                }
            };
    HoistFunction INNER_OR_CROSS_JOIN_HOISTER = (triple, lhsConjuncts, rhsConjuncts, onConjuncts, hoistConjuncts) -> {
        switch (triple.getBits()) {
            case HoistTriple.BIT_NONE:
                return;
            case HoistTriple.BIT_LHS:
            case HoistTriple.BIT_LHS_ON:
            case HoistTriple.BIT_LHS_RHS:
            case HoistTriple.BIT_LHS_RHS_ON:
                hoistConjuncts.add(triple.mustGetOp(HoistTriple.BIT_LHS_SHIFT));
                return;
            case HoistTriple.BIT_RHS:
                hoistConjuncts.add(triple.mustGetOp(HoistTriple.BIT_RHS_SHIFT));
                return;
            case HoistTriple.BIT_ON:
            case HoistTriple.BIT_RHS_ON:
                hoistConjuncts.add(triple.mustGetOp(HoistTriple.BIT_ON_SHIFT));
                return;
            default:
                Preconditions.checkArgument(false, "Never reach here");
        }
    };
    HoistFunction LEFT_SEMI_JOIN_HOISTER = (triple, lhsConjuncts, rhsConjuncts, onConjuncts, hoistConjuncts) -> {
        switch (triple.getBits()) {
            case HoistTriple.BIT_NONE:
                return;
            case HoistTriple.BIT_LHS:
            case HoistTriple.BIT_LHS_ON:
            case HoistTriple.BIT_LHS_RHS:
            case HoistTriple.BIT_LHS_RHS_ON:
                hoistConjuncts.add(triple.mustGetOp(HoistTriple.BIT_LHS_SHIFT));
                return;
            case HoistTriple.BIT_RHS:
                onConjuncts.add(triple.mustGetOp(HoistTriple.BIT_RHS_SHIFT));
                return;
            case HoistTriple.BIT_ON:
            case HoistTriple.BIT_RHS_ON:
                onConjuncts.add(triple.mustGetOp(HoistTriple.BIT_ON_SHIFT));
                return;
            default:
                Preconditions.checkArgument(false, "Never reach here");
        }
    };
    HoistFunction FULL_JOIN_HOISTER = (triple, lhsConjuncts, rhsConjuncts, onConjuncts, hoistConjuncts) -> {
        triple.mayGetOp(HoistTriple.BIT_LHS_SHIFT).ifPresent(lhsConjuncts::add);
        triple.mayGetOp(HoistTriple.BIT_RHS_SHIFT).ifPresent(rhsConjuncts::add);
        triple.mayGetOp(HoistTriple.BIT_ON_SHIFT).ifPresent(onConjuncts::add);
    };

    static HoistFunction get(JoinOperator type) {
        switch (type) {
            case INNER_JOIN:
            case CROSS_JOIN:
                return INNER_OR_CROSS_JOIN_HOISTER;
            case LEFT_OUTER_JOIN:
            case NULL_AWARE_LEFT_ANTI_JOIN:
            case LEFT_ANTI_JOIN:
                return LEFT_OUTER_OR_ANTI_JOIN_HOISTER;
            case LEFT_SEMI_JOIN:
                return LEFT_SEMI_JOIN_HOISTER;
            case FULL_OUTER_JOIN:
                return FULL_JOIN_HOISTER;
            default:
                Preconditions.checkArgument(false, "Never reach here");
                return (triple, lhsConjuncts, rhsConjuncts, onConjuncts, hoistConjuncts) -> {
                };
        }
    }

    void apply(HoistTriple triple, List<Op> lhsConjuncts, List<Op> rhsConjuncts, List<Op> onConjuncts,
               List<Op> hoistConjuncts);
}
