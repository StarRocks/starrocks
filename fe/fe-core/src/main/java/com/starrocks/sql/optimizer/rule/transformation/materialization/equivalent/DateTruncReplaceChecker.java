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
package com.starrocks.sql.optimizer.rule.transformation.materialization.equivalent;

import com.starrocks.catalog.PrimitiveType;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorFunctions;

public class DateTruncReplaceChecker implements PredicateReplaceChecker {

    private final CallOperator mvDateTrunc;

    public DateTruncReplaceChecker(CallOperator mvDateTrunc) {
        this.mvDateTrunc = mvDateTrunc;
    }

    @Override
    public boolean canReplace(ScalarOperator operator) {
        if (operator.isConstantRef() && operator.getType().getPrimitiveType() == PrimitiveType.DATETIME) {
            ConstantOperator sliced = ScalarOperatorFunctions.dateTrunc(
                    ((ConstantOperator) mvDateTrunc.getChild(0)),
                    (ConstantOperator) operator);
            return sliced.equals(operator);
        }
        return false;
    }
}
