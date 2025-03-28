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

package com.starrocks.sql.optimizer.operator;

public class OpRuleBit {
    // Like LogicalJoinOperator#transformMask, add a mask to avoid one operator's dead-loop in one transform rule.
    // eg: MV's UNION-ALL RULE:
    //                 UNION                         UNION
    //               /        \                    /       \
    //  OP -->   EXTRA-OP    MV-SCAN  -->     UNION    MV-SCAN     ---> ....
    //                                       /      \
    //                                  EXTRA-OP    MV-SCAN
    // Operator has been union rewrite or not, if union all, no need to union again.
    public static final int OP_MV_UNION_REWRITE = 0;
    // Operator has been push down predicates or not, if push down predicates, no need to push down again.
    public static final int OP_MV_AGG_PUSH_DOWN_REWRITE = 1;
    // Operator has been transparent mv rewrite or not, if transparent mv rewrite, no need to rewrite again.
    public static final int OP_MV_TRANSPARENT_REWRITE = 2;
    // Operator has been partition pruned or not, if partition pruned, no need to prune again.
    public static final int OP_PARTITION_PRUNED = 3;
    // Operator has been mv transparent union rewrite and needs to prune agg columns.
    public static final int OP_MV_AGG_PRUNE_COLUMNS = 4;
}
