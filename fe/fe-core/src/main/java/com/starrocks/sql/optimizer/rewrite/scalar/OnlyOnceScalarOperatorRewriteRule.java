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

package com.starrocks.sql.optimizer.rewrite.scalar;

// OnlyOnceScalarOperatorRewriteRule is not equivalence-transform Rule. non-equivalence transform Rule
// is used to escalate or degrade predicates to make the predicates simple and applicable to index.
// it visit the expression tree in top-down style, it finishes as soon as it can not go downwards.
public class OnlyOnceScalarOperatorRewriteRule extends BaseScalarOperatorRewriteRule {
    @Override
    public boolean isOnlyOnce() {
        return true;
    }
}
