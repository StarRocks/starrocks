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

package com.starrocks.sql.optimizer.rule.join;

import com.starrocks.analysis.JoinOperator;

/**
 * ref: On the Correct and Complete Enumeration of the Core Search Space
 */
public class JoinReorderProperty {

    private JoinReorderProperty() {
        //not called
    }

    private static final int[][] INNER_ASSOCIATIVITY_PROPERTY = new int[10][10];

    private static final int[][] OUTER_ASSOCIATIVITY_PROPERTY = new int[10][10];

    private static final int[][] LEFT_ASSCOM_PROPERTY = new int[10][10];

    public static final int UNSUPPORTED = 0;

    public static final int SUPPORTED = 1;

    // we don't support Null rejecting predicate evaluation,
    // hence cannot process scenes of CONDITIONAL_SUPPORTED.
    public static final int CONDITIONAL_SUPPORTED = 2;

    static {

        /*
         *  bottom join operator within the row and top join operator with in the column.
         *  ▷* means null aware anti join
         *   op    ⨝    ⟕   ⋉    ▷    ⋊    ◁    ⟖   ⟗   ×    ▷*
         *  ----- ---- ---- ---- ---- ---- ---- ---- ---- ---- -----
         *   ⋈     1    0    0    0    0    0    0    0    0    0
         *   ×      1    0    0    0    0    0    0    0    0    0
         */
        INNER_ASSOCIATIVITY_PROPERTY[JoinOperator.INNER_JOIN.ordinal()] = new int[] {1, 0, 0, 0, 0, 0, 0, 0, 0, 0};
        INNER_ASSOCIATIVITY_PROPERTY[JoinOperator.CROSS_JOIN.ordinal()] = new int[] {1, 0, 0, 0, 0, 0, 0, 0, 0, 0};

        /*
         *  bottom join operator within the row and top join operator with in the column.
         *  ▷* means null aware anti join
         *   op    ⨝    ⟕   ⋉    ▷    ⋊    ◁    ⟖   ⟗   ×    ▷*
         *  ----- ---- ---- ---- ---- ---- ---- ---- ---- ---- -----
         *   ⋈     0    1    0    0    0    0    0    0    0    0
         *   ⟕     0    2    0    0    0    0    0    0    0    0
         *   ⟗     0    2    0    0    0    0    0    2    0    0
         *   ×      0    1    0    0    0    0    0    0    0    0
         */
        OUTER_ASSOCIATIVITY_PROPERTY[JoinOperator.INNER_JOIN.ordinal()] = new int[] {0, 1, 0, 0, 0, 0, 0, 0, 0, 0};
        OUTER_ASSOCIATIVITY_PROPERTY[JoinOperator.LEFT_OUTER_JOIN.ordinal()] = new int[] {0, 2, 0, 0, 0, 0, 0, 0, 0, 0};
        OUTER_ASSOCIATIVITY_PROPERTY[JoinOperator.FULL_OUTER_JOIN.ordinal()] = new int[] {0, 2, 0, 0, 0, 0, 0, 2, 0, 0};
        OUTER_ASSOCIATIVITY_PROPERTY[JoinOperator.CROSS_JOIN.ordinal()] = new int[] {0, 1, 0, 0, 0, 0, 0, 0, 0, 0};


        /*
         *  bottom join operator within the row and top join operator with in the column
         *  ▷* means null aware anti join
         *   op    ⋈    ⟕   ⋉    ▷    ⋊    ◁    ⟖    ⟗   ×    ▷*
         *  ----- ---- ---- ---- ---- ---- ---- ---- ---- ---- -----
         *   ⟕     1    1    1    1    0    0    0    2    0    0
         *   ⋉     0    1    1    1    0    0    0    0    0    0
         *   ▷     0    1    1    1    0    0    0    0   0    0
         *   ⟗    0    2    0    0    0    0    0    2    0    0
         */
        LEFT_ASSCOM_PROPERTY[JoinOperator.LEFT_OUTER_JOIN.ordinal()] = new int[] {1, 1, 1, 1, 0, 0, 0, 2, 0, 0};
        LEFT_ASSCOM_PROPERTY[JoinOperator.LEFT_SEMI_JOIN.ordinal()] = new int[] {0, 1, 1, 1, 0, 0, 0, 0, 0, 0};
        LEFT_ASSCOM_PROPERTY[JoinOperator.LEFT_ANTI_JOIN.ordinal()] = new int[] {0, 1, 1, 1, 0, 0, 0, 0, 0, 0};
        LEFT_ASSCOM_PROPERTY[JoinOperator.FULL_OUTER_JOIN.ordinal()] = new int[] {0, 2, 0, 0, 0, 0, 0, 2, 0, 0};
    }

    public static int getAssociativityProperty(JoinOperator bottomJoinType, JoinOperator topJoinType, boolean isInnerMode) {
        if (isInnerMode) {
            return INNER_ASSOCIATIVITY_PROPERTY[bottomJoinType.ordinal()][topJoinType.ordinal()];
        } else {
            return OUTER_ASSOCIATIVITY_PROPERTY[bottomJoinType.ordinal()][topJoinType.ordinal()];
        }

    }

    public static int getLeftAsscomProperty(JoinOperator bottomJoinType, JoinOperator topJoinType) {
        return LEFT_ASSCOM_PROPERTY[bottomJoinType.ordinal()][topJoinType.ordinal()];
    }


}
