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

package com.starrocks.scheduler.mv.ivm;

import com.starrocks.catalog.TableName;
import com.starrocks.sql.ast.expression.BinaryPredicate;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.ast.expression.CompoundPredicate;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.IsNullPredicate;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.ast.expression.StringLiteral;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Shape of the predicate that bounds an incremental delta away from partitions retention refused to create.
 * The behaviour these pin is invisible end to end: both a correct and a wrong escape produce a predicate that
 * is "not TRUE" for the rows an e2e run happens to exercise, so only the tree itself tells them apart.
 */
public class IvmRetentionDeltaBoundTest {

    /** {@code dt >= '2026-08-10'} -- stands in for whatever membership test the predicate builder returns. */
    private static Expr membership() {
        return new BinaryPredicate(BinaryType.GE,
                new SlotRef(new TableName("db", "mv"), "dt"), new StringLiteral("2026-08-10"));
    }

    @Test
    public void testEscapeTestsThePredicateNotTheColumn() {
        Expr inRefused = membership();
        Expr keep = MVIVMRefreshProcessor.keepUnlessRefused(inRefused);

        Assertions.assertInstanceOf(CompoundPredicate.class, keep);
        CompoundPredicate or = (CompoundPredicate) keep;
        Assertions.assertEquals(CompoundPredicate.Operator.OR, or.getOp());

        Assertions.assertInstanceOf(CompoundPredicate.class, or.getChild(0));
        CompoundPredicate not = (CompoundPredicate) or.getChild(0);
        Assertions.assertEquals(CompoundPredicate.Operator.NOT, not.getOp());
        Assertions.assertEquals(inRefused, not.getChild(0));

        // The escape has to be IS NULL over the whole membership test. Over the partition column instead it
        // reads TRUE for every null key, re-admitting rows a refused min-bounded cell already claimed.
        Assertions.assertInstanceOf(IsNullPredicate.class, or.getChild(1));
        IsNullPredicate isNull = (IsNullPredicate) or.getChild(1);
        Assertions.assertFalse(isNull.isNotNull());
        Assertions.assertEquals(inRefused, isNull.getChild(0));
        Assertions.assertFalse(isNull.getChild(0) instanceof SlotRef,
                "escape must not degrade to a bare column null test");
    }

    @Test
    public void testMembershipIsNotSharedBetweenBranches() {
        Expr inRefused = membership();
        CompoundPredicate or = (CompoundPredicate) MVIVMRefreshProcessor.keepUnlessRefused(inRefused);

        // Equal by value, distinct by identity: one analysis pass rewriting a branch in place must not reach
        // through to the other.
        Expr underNot = ((CompoundPredicate) or.getChild(0)).getChild(0);
        Expr underIsNull = or.getChild(1).getChild(0);
        Assertions.assertEquals(underNot, underIsNull);
        Assertions.assertNotSame(underNot, underIsNull);
    }
}
