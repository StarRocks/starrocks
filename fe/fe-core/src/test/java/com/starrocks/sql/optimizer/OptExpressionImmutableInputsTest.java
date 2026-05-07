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

package com.starrocks.sql.optimizer;

import com.starrocks.sql.optimizer.operator.physical.PhysicalCTEAnchorOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalCTEProduceOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalNoCTEOperator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

/**
 * Regression tests for UnsupportedOperationException in OptExpression.setChild()
 * caused by immutable inputs lists.
 *
 * <p>Root cause: {@link OptExpression#create(com.starrocks.sql.optimizer.operator.Operator, List)}
 * and {@link OptExpression.Builder#setInputs(List)} stored the caller-provided list by reference.
 * When a caller passed {@code List.of(...)} (immutable), a subsequent {@link OptExpression#setChild}
 * would throw {@link UnsupportedOperationException}.
 *
 * <p>This occurred in the following physicalRuleRewrite passes that run before
 * {@link com.starrocks.sql.optimizer.rule.tree.ApplyTuningGuideRule}:
 * <ul>
 *   <li>{@code DecodeRewriter.java:317} – {@code .setInputs(List.of(...)).build()}</li>
 *   <li>{@code GlobalLateMaterializationRewriter.java:254,1502,1576} – {@code .setInputs(List.of(...)).build()}</li>
 *   <li>{@code GlobalLateMaterializationRewriter.java:1205} – {@code OptExpression.create(op, List.of(...))}</li>
 * </ul>
 *
 * <p>Fix (Option A): make {@code create(op, List)} and {@code Builder.setInputs()} always
 * perform a defensive copy so that {@code inputs} is always a mutable {@link ArrayList}.
 *
 * <p>All tests in this class FAIL before the fix and PASS after the fix.
 */
public class OptExpressionImmutableInputsTest {

    // -------------------------------------------------------------------------
    // 1. OptExpression.create(op, List.of(...))
    // -------------------------------------------------------------------------

    /**
     * Reproduces the pattern in GlobalLateMaterializationRewriter.java:1205:
     * <pre>OptExpression.create(physicalFetchOperator, List.of(current, lookupOpt))</pre>
     * Before fix: setChild() throws UnsupportedOperationException.
     * After fix:  setChild() succeeds.
     */
    @Test
    public void testCreateWithImmutableListAllowsSetChild() {
        OptExpression leaf1 = new OptExpression(new PhysicalNoCTEOperator(1));
        OptExpression leaf2 = new OptExpression(new PhysicalNoCTEOperator(2));
        OptExpression replacement = new OptExpression(new PhysicalNoCTEOperator(99));

        OptExpression parent = OptExpression.create(
                new PhysicalCTEProduceOperator(1),
                List.of(leaf1, leaf2));

        assertDoesNotThrow(() -> parent.setChild(0, replacement),
                "setChild() must not throw when inputs were created via List.of()");
    }

    // -------------------------------------------------------------------------
    // 2. OptExpression.Builder.setInputs(List.of(...))
    // -------------------------------------------------------------------------

    /**
     * Reproduces the pattern in DecodeRewriter.java:317 and
     * GlobalLateMaterializationRewriter.java:254:
     * <pre>.setInputs(List.of(child)).build()</pre>
     */
    @Test
    public void testBuilderSetInputsImmutableAllowsSetChild() {
        OptExpression leaf = new OptExpression(new PhysicalNoCTEOperator(1));
        OptExpression replacement = new OptExpression(new PhysicalNoCTEOperator(99));

        OptExpression parent = OptExpression.builder()
                .setOp(new PhysicalCTEProduceOperator(1))
                .setInputs(List.of(leaf))
                .build();

        assertDoesNotThrow(() -> parent.setChild(0, replacement),
                "setChild() must not throw when inputs were set via Builder.setInputs(List.of(...))");
    }

    /**
     * Reproduces the pattern in GlobalLateMaterializationRewriter.java:1502:
     * <pre>OptExpression.builder().with(existing).setInputs(List.of(child)).build()</pre>
     */
    @Test
    public void testBuilderWithThenSetInputsImmutableAllowsSetChild() {
        OptExpression existing = OptExpression.create(
                new PhysicalCTEProduceOperator(1),
                new OptExpression(new PhysicalNoCTEOperator(10)));
        OptExpression newChild = new OptExpression(new PhysicalNoCTEOperator(20));
        OptExpression replacement = new OptExpression(new PhysicalNoCTEOperator(99));

        OptExpression rebuilt = OptExpression.builder()
                .with(existing)
                .setInputs(List.of(newChild))
                .build();

        assertDoesNotThrow(() -> rebuilt.setChild(0, replacement),
                "setChild() must not throw after builder.with().setInputs(List.of(...)).build()");
    }

    /**
     * Reproduces the pattern in GlobalLateMaterializationRewriter.java:1576:
     * <pre>.setInputs(List.of()).build()</pre>
     * Verifies that an empty immutable list is also copied to a mutable one.
     * (The node would not have children to set, but inputs must remain mutable
     * in case the traversal logic calls getInputs().size() and then adds children.)
     */
    @Test
    public void testBuilderSetEmptyImmutableListIsStillMutable() {
        OptExpression parent = OptExpression.builder()
                .setOp(new PhysicalNoCTEOperator(1))
                .setInputs(List.of())
                .build();

        // inputs must be a mutable list – adding to it must not throw
        assertDoesNotThrow(() -> parent.getInputs().add(new OptExpression(new PhysicalNoCTEOperator(2))),
                "inputs list must be mutable even when initialized from List.of()");
    }

    // -------------------------------------------------------------------------
    // 3. End-to-end CTE tree traversal (mimics ApplyTuningGuideRule$Visitor)
    // -------------------------------------------------------------------------

    /**
     * Simulates the exact traversal path from the bug's stack trace:
     * ApplyTuningGuideRule → visitPhysicalCTEAnchor → visitPhysicalCTEProduce → setChild().
     *
     * <p>The CTE produce node is created with an immutable input list (as
     * DecodeRewriter would do), and then the visitor tries to call setChild()
     * on it while walking the CTE anchor's children.
     */
    @Test
    public void testCteTreeTraversalWithImmutableProduceInputs() {
        OptExpression scan = new OptExpression(new PhysicalNoCTEOperator(0));
        OptExpression replacement = new OptExpression(new PhysicalNoCTEOperator(99));

        // CTE produce created with immutable list – this is the problematic node
        OptExpression cteProduceOpt = OptExpression.builder()
                .setOp(new PhysicalCTEProduceOperator(1))
                .setInputs(List.of(scan))       // immutable list
                .build();

        // CTE anchor wraps the produce node (its own inputs are mutable)
        OptExpression anchor = OptExpression.create(
                new PhysicalCTEAnchorOperator(1, 1, null),
                cteProduceOpt,
                new OptExpression(new PhysicalNoCTEOperator(2)));

        // Reproduce what ApplyTuningGuideRule$Visitor.visit() does:
        // iterate over children, process recursively, then call setChild()
        assertDoesNotThrow(() -> {
            for (int i = 0; i < anchor.getInputs().size(); i++) {
                OptExpression child = anchor.inputAt(i);
                // simulate recursive descent into the produce node
                for (int j = 0; j < child.getInputs().size(); j++) {
                    child.setChild(j, replacement);   // threw UnsupportedOperationException before fix
                }
                anchor.setChild(i, child);
            }
        }, "ApplyTuningGuideRule-style traversal must not throw on CTE plan trees");
    }

    // -------------------------------------------------------------------------
    // 4. Defensive-copy isolation
    // -------------------------------------------------------------------------

    /**
     * Verifies that the defensive copy made by create() is independent of the
     * original mutable list: mutating the caller's list must not affect the
     * OptExpression's inputs.
     *
     * <p>Before the fix, create() stored the list by reference, so clearing the
     * original list would empty the stored inputs (assertion fails with size 0).
     * After the fix, the stored inputs are a separate copy (assertion passes).
     */
    @Test
    public void testCreateDefensiveCopyIsIndependentOfOriginalList() {
        OptExpression leaf = new OptExpression(new PhysicalNoCTEOperator(1));
        ArrayList<OptExpression> callerList = new ArrayList<>();
        callerList.add(leaf);

        OptExpression parent = OptExpression.create(new PhysicalCTEProduceOperator(1), callerList);

        // mutate the caller's list after create()
        callerList.clear();

        Assertions.assertEquals(1, parent.getInputs().size(),
                "Stored inputs must be a defensive copy; clearing the caller's list must not affect them");
    }

    /**
     * Same isolation guarantee for Builder.setInputs().
     */
    @Test
    public void testBuilderSetInputsDefensiveCopyIsIndependentOfOriginalList() {
        OptExpression leaf = new OptExpression(new PhysicalNoCTEOperator(1));
        ArrayList<OptExpression> callerList = new ArrayList<>();
        callerList.add(leaf);

        OptExpression parent = OptExpression.builder()
                .setOp(new PhysicalCTEProduceOperator(1))
                .setInputs(callerList)
                .build();

        callerList.clear();

        Assertions.assertEquals(1, parent.getInputs().size(),
                "Stored inputs must be a defensive copy in Builder.setInputs() too");
    }
}
