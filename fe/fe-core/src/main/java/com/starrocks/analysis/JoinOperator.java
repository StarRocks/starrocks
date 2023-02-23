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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/JoinOperator.java

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

import com.google.common.collect.Sets;
import com.starrocks.thrift.TJoinOp;

import java.util.Set;

public enum JoinOperator {
    INNER_JOIN("INNER JOIN", TJoinOp.INNER_JOIN),
    LEFT_OUTER_JOIN("LEFT OUTER JOIN", TJoinOp.LEFT_OUTER_JOIN),

    LEFT_SEMI_JOIN("LEFT SEMI JOIN", TJoinOp.LEFT_SEMI_JOIN),
    LEFT_ANTI_JOIN("LEFT ANTI JOIN", TJoinOp.LEFT_ANTI_JOIN),
    RIGHT_SEMI_JOIN("RIGHT SEMI JOIN", TJoinOp.RIGHT_SEMI_JOIN),
    RIGHT_ANTI_JOIN("RIGHT ANTI JOIN", TJoinOp.RIGHT_ANTI_JOIN),
    RIGHT_OUTER_JOIN("RIGHT OUTER JOIN", TJoinOp.RIGHT_OUTER_JOIN),
    FULL_OUTER_JOIN("FULL OUTER JOIN", TJoinOp.FULL_OUTER_JOIN),
    CROSS_JOIN("CROSS JOIN", TJoinOp.CROSS_JOIN),
    // Variant of the LEFT ANTI JOIN that is used for the equal of
    // NOT IN subqueries. It can have a single equality join conjunct
    // that returns TRUE when the rhs is NULL.
    NULL_AWARE_LEFT_ANTI_JOIN("NULL AWARE LEFT ANTI JOIN",
            TJoinOp.NULL_AWARE_LEFT_ANTI_JOIN);

    public static final String HINT_BUCKET = "BUCKET";
    public static final String HINT_SHUFFLE = "SHUFFLE";
    public static final String HINT_COLOCATE = "COLOCATE";
    public static final String HINT_BROADCAST = "BROADCAST";
    public static final String HINT_UNREORDER = "UNREORDER";

    private final String description;
    private final TJoinOp thriftJoinOp;

    private JoinOperator(String description, TJoinOp thriftJoinOp) {
        this.description = description;
        this.thriftJoinOp = thriftJoinOp;
    }

    @Override
    public String toString() {
        return description;
    }

    public TJoinOp toThrift() {
        return thriftJoinOp;
    }

    public boolean isOuterJoin() {
        return this == LEFT_OUTER_JOIN || this == RIGHT_OUTER_JOIN || this == FULL_OUTER_JOIN;
    }

    public boolean isSemiAntiJoin() {
        return isSemiJoin() || isAntiJoin();
    }

    public boolean isLeftSemiAntiJoin() {
        return this == JoinOperator.LEFT_SEMI_JOIN || this == JoinOperator.LEFT_ANTI_JOIN ||
                this == JoinOperator.NULL_AWARE_LEFT_ANTI_JOIN;
    }

    public boolean isRightSemiAntiJoin() {
        return this == JoinOperator.RIGHT_SEMI_JOIN || this == JoinOperator.RIGHT_ANTI_JOIN;
    }

    public boolean isSemiJoin() {
        return this == JoinOperator.LEFT_SEMI_JOIN || this == JoinOperator.RIGHT_SEMI_JOIN;
    }

    public boolean isLeftSemiJoin() {
        return this == LEFT_SEMI_JOIN;
    }

    public boolean isLeftAntiJoin() {
        return this == LEFT_ANTI_JOIN || this == NULL_AWARE_LEFT_ANTI_JOIN;
    }

    public boolean isRightSemiJoin() {
        return this == RIGHT_SEMI_JOIN;
    }

    public boolean isRightAntiJoin() {
        return this == RIGHT_ANTI_JOIN;
    }

    public boolean isInnerJoin() {
        return this == INNER_JOIN;
    }

    public boolean isAntiJoin() {
        return this == JoinOperator.LEFT_ANTI_JOIN || this == JoinOperator.RIGHT_ANTI_JOIN ||
                this == JoinOperator.NULL_AWARE_LEFT_ANTI_JOIN;
    }

    public boolean isCrossJoin() {
        return this == CROSS_JOIN;
    }

    public boolean isFullOuterJoin() {
        return this == FULL_OUTER_JOIN;
    }

    public boolean isLeftOuterJoin() {
        return this == LEFT_OUTER_JOIN;
    }

    public boolean isRightOuterJoin() {
        return this == RIGHT_OUTER_JOIN;
    }

    public boolean isRightJoin() {
        return this == RIGHT_OUTER_JOIN || this == RIGHT_ANTI_JOIN || this == RIGHT_SEMI_JOIN;
    }

    // Left transform means that the Join operation can be considered as a transformation operation
    // on left hand side in collective computation. for examples:
    // 1. INNER_JOIN:  lhs.flatMap(l->rhs.flatMap(r->if( l match r){listOf(concat(l,r))} else {emptyList()}))
    // 2. LEFT_SEMI_JOIN: lhs.flatMap(l->rhs.flatMap(r->if( l match r){listOf(l))} else {emptyList()}))
    // 3. LEFT_OUTER_JOIN: lhs.flatMap(l->rhs.flatMap(r->if( l match r){listOf(concat(l,r))} else {listOf(concat_null(l,r))}))
    // 4. LEFT_ANTI_JOIN: lhs.flatMap(l->rhs.flatMap(r->if( l match r){emptyList()} else {listOf(l)}))
    public boolean isLeftTransform() {
        return this == INNER_JOIN || this == LEFT_SEMI_JOIN || this == LEFT_OUTER_JOIN || this == LEFT_ANTI_JOIN;
    }

    public static Set<JoinOperator> semiAntiJoinSet() {
        return Sets.newHashSet(LEFT_SEMI_JOIN, LEFT_ANTI_JOIN, NULL_AWARE_LEFT_ANTI_JOIN, RIGHT_SEMI_JOIN,
                RIGHT_ANTI_JOIN);
    }

    public static Set<JoinOperator> innerCrossJoinSet() {
        return Sets.newHashSet(INNER_JOIN, CROSS_JOIN);
    }
}


