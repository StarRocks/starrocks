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


package com.starrocks.sql.optimizer.rule.transformation.materialization.rule;

import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rule.RuleType;

/*
 *
 * Here is the rule for pattern Join
 *
 */
public class OnlyJoinRule extends BaseMaterializedViewRewriteRule {
    private static final OnlyJoinRule INSTANCE = new OnlyJoinRule();

    public OnlyJoinRule() {
        super(RuleType.TF_MV_ONLY_JOIN_RULE, Pattern.create(OperatorType.PATTERN_MULTIJOIN));
    }

    public static OnlyJoinRule getInstance() {
        return INSTANCE;
    }
}