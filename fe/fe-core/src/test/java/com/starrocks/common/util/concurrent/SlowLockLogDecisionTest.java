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

package com.starrocks.common.util.concurrent;

import com.starrocks.common.Config;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Unit tests for the single slow-lock logging decision. All cases inject their own
 * {@link AtomicLong} gates and an explicit monotonic timestamp, so the tests are fully
 * deterministic and need no reflection or static-state resets.
 */
public class SlowLockLogDecisionTest {

    private boolean origPrintStack;
    private long origStackInterval;
    private long origLogEvery;
    private long origBreadcrumbEvery;

    private static final long SENTINEL = SlowLockLogDecision.GATE_INIT_SENTINEL;
    // A realistic, large positive monotonic base so deltas behave like production.
    private static final long T0 = 1_000_000_000L;

    @BeforeEach
    public void setUp() {
        origPrintStack = Config.slow_lock_print_stack;
        origStackInterval = Config.slow_lock_log_l1_stack_interval_ms;
        origLogEvery = Config.slow_lock_log_l2_info_interval_ms;
        origBreadcrumbEvery = Config.slow_lock_log_l3_brief_interval_ms;
        // Default tiering: breadcrumb(1s) < logEvery(3s) < stackInterval(30s)
        Config.slow_lock_print_stack = true;
        Config.slow_lock_log_l1_stack_interval_ms = 30000L;
        Config.slow_lock_log_l2_info_interval_ms = 3000L;
        Config.slow_lock_log_l3_brief_interval_ms = 1000L;
    }

    @AfterEach
    public void tearDown() {
        Config.slow_lock_print_stack = origPrintStack;
        Config.slow_lock_log_l1_stack_interval_ms = origStackInterval;
        Config.slow_lock_log_l2_info_interval_ms = origLogEvery;
        Config.slow_lock_log_l3_brief_interval_ms = origBreadcrumbEvery;
    }

    private static AtomicLong gate() {
        return new AtomicLong(SENTINEL);
    }

    @Test
    public void testFirstEventWithHolderHitsL1AndCaptureStack() {
        AtomicLong stack = gate();
        AtomicLong event = gate();
        AtomicLong breadcrumb = gate();

        SlowLockLogDecision d = SlowLockLogDecision.decide(true, stack, event, breadcrumb, T0);

        Assertions.assertEquals(SlowLockTier.L1_STACK_INFO, d.tier);
        Assertions.assertTrue(d.captureStack);
        Assertions.assertTrue(d.shouldLog());
        Assertions.assertFalse(d.isBreadcrumb());
    }

    @Test
    public void testL1WinSubsumesL2AndL3Gates() {
        AtomicLong stack = gate();
        AtomicLong event = gate();
        AtomicLong breadcrumb = gate();

        SlowLockLogDecision first = SlowLockLogDecision.decide(true, stack, event, breadcrumb, T0);
        Assertions.assertEquals(SlowLockTier.L1_STACK_INFO, first.tier);

        // An L1 win must advance ALL three gates so a near-immediate second event logs nothing.
        Assertions.assertEquals(T0, stack.get());
        Assertions.assertEquals(T0, event.get(), "L1 win must subsume the L2 event gate");
        Assertions.assertEquals(T0, breadcrumb.get(), "L1 win must subsume the L3 breadcrumb gate");

        SlowLockLogDecision second = SlowLockLogDecision.decide(true, stack, event, breadcrumb, T0 + 1);
        Assertions.assertFalse(second.shouldLog(),
                "a second event 1ms after an L1 emit must be fully suppressed, not downgraded to L2/L3");
    }

    @Test
    public void testDegradeToL3BetweenL2WindowsButPastBreadcrumb() {
        AtomicLong stack = gate();
        AtomicLong event = gate();
        AtomicLong breadcrumb = gate();

        // Consume L1 at T0 (also subsumes L2 + L3 to T0).
        SlowLockLogDecision first = SlowLockLogDecision.decide(true, stack, event, breadcrumb, T0);
        Assertions.assertEquals(SlowLockTier.L1_STACK_INFO, first.tier);

        // T0+1500ms: past breadcrumb(1000) but within logEvery(3000) and stackInterval(30000).
        SlowLockLogDecision d = SlowLockLogDecision.decide(true, stack, event, breadcrumb, T0 + 1500);
        Assertions.assertEquals(SlowLockTier.L3_BREADCRUMB, d.tier);
        Assertions.assertFalse(d.captureStack);
        Assertions.assertTrue(d.isBreadcrumb());
        Assertions.assertEquals(T0 + 1500, breadcrumb.get());
        // L1/L2 gates untouched by an L3 win.
        Assertions.assertEquals(T0, stack.get());
        Assertions.assertEquals(T0, event.get());
    }

    @Test
    public void testDegradeToL2WhenStackThrottledButEventDue() {
        AtomicLong stack = gate();
        AtomicLong event = gate();
        AtomicLong breadcrumb = gate();

        // Consume L1 at T0.
        SlowLockLogDecision.decide(true, stack, event, breadcrumb, T0);
        // T0+30001ms: stack still NOT due? 30001>=30000 → actually due. Use T0+5000: stack not due
        // (5000<30000), event due (5000>=3000) → L2.
        SlowLockLogDecision d = SlowLockLogDecision.decide(true, stack, event, breadcrumb, T0 + 5000);
        Assertions.assertEquals(SlowLockTier.L2_INFO, d.tier);
        Assertions.assertFalse(d.captureStack, "L2 must not capture stack");
        Assertions.assertEquals(T0 + 5000, event.get(), "L2 win advances the event gate");
        Assertions.assertEquals(T0 + 5000, breadcrumb.get(), "L2 win subsumes the L3 gate");
        Assertions.assertEquals(T0, stack.get(), "L2 win must NOT advance the L1 stack gate");
    }

    @Test
    public void testHasHolderFalseSkipsL1ButStillReachesL2() {
        AtomicLong stack = gate();
        AtomicLong event = gate();
        AtomicLong breadcrumb = gate();

        SlowLockLogDecision d = SlowLockLogDecision.decide(false, stack, event, breadcrumb, T0);

        // No owner means no stack to dump, so L1 is skipped and the stack gate is left untouched.
        // But the L2 full-info line still carries the waiter/queue list, which is exactly the
        // signal when waiters are stuck with no holder — so an owner-less event still reaches L2
        // (just without an owner stack), rather than being downgraded to a bare breadcrumb.
        Assertions.assertEquals(SlowLockTier.L2_INFO, d.tier, "no owner → L1 skipped, but L2 still applies");
        Assertions.assertFalse(d.captureStack);
        Assertions.assertEquals(SENTINEL, stack.get(), "no-holder must not consume the L1 stack gate");
        Assertions.assertEquals(T0, event.get(), "L2 fired, so the event gate is consumed");
    }

    @Test
    public void testPrintStackOffSkipsL1() {
        Config.slow_lock_print_stack = false;
        AtomicLong stack = gate();
        AtomicLong event = gate();
        AtomicLong breadcrumb = gate();

        SlowLockLogDecision d = SlowLockLogDecision.decide(true, stack, event, breadcrumb, T0);

        Assertions.assertEquals(SlowLockTier.L2_INFO, d.tier);
        Assertions.assertEquals(SENTINEL, stack.get(), "master switch off must not touch the stack gate");
    }

    @Test
    public void testAllThrottledYieldsSuppress() {
        // Gates all just consumed at T0; an event 1ms later clears no interval.
        AtomicLong stack = new AtomicLong(T0);
        AtomicLong event = new AtomicLong(T0);
        AtomicLong breadcrumb = new AtomicLong(T0);

        SlowLockLogDecision d = SlowLockLogDecision.decide(true, stack, event, breadcrumb, T0 + 1);

        Assertions.assertFalse(d.shouldLog());
        Assertions.assertSame(SlowLockLogDecision.SUPPRESS, d);
    }

    @Test
    public void testStackIntervalZeroAlwaysCapturesL1() {
        Config.slow_lock_log_l1_stack_interval_ms = 0;
        // Stack gate just consumed; interval<=0 means "always admit" regardless.
        AtomicLong stack = new AtomicLong(T0);
        AtomicLong event = new AtomicLong(T0);
        AtomicLong breadcrumb = new AtomicLong(T0);

        SlowLockLogDecision d = SlowLockLogDecision.decide(true, stack, event, breadcrumb, T0 + 1);

        Assertions.assertEquals(SlowLockTier.L1_STACK_INFO, d.tier);
        Assertions.assertTrue(d.captureStack);
    }

    @Test
    public void testBreadcrumbIntervalZeroNeverSilent() {
        // L1 and L2 throttled hard; breadcrumb disabled-as-unthrottled must still emit (the floor
        // has no off switch — <=0 means always admit, never SUPPRESS).
        Config.slow_lock_log_l1_stack_interval_ms = 1_000_000L;
        Config.slow_lock_log_l2_info_interval_ms = 1_000_000L;
        Config.slow_lock_log_l3_brief_interval_ms = 0;
        AtomicLong stack = new AtomicLong(T0);
        AtomicLong event = new AtomicLong(T0);
        AtomicLong breadcrumb = new AtomicLong(T0);

        SlowLockLogDecision d = SlowLockLogDecision.decide(true, stack, event, breadcrumb, T0 + 1);

        Assertions.assertEquals(SlowLockTier.L3_BREADCRUMB, d.tier,
                "breadcrumb interval <= 0 must always admit, never go silent");
    }

    @Test
    public void testSentinelInitFirstEventPassesEvenWithSmallMonoNow() {
        // A JVM whose nanoTime origin is small/negative still must let the first event through.
        AtomicLong stack = gate();
        AtomicLong event = gate();
        AtomicLong breadcrumb = gate();

        SlowLockLogDecision d = SlowLockLogDecision.decide(true, stack, event, breadcrumb, 0L);
        Assertions.assertEquals(SlowLockTier.L1_STACK_INFO, d.tier,
                "sentinel init must let the first event pass at monoNow=0");
    }

    @Test
    public void testCaptureStackTrueIffL1() {
        AtomicLong stack = gate();
        AtomicLong event = gate();
        AtomicLong breadcrumb = gate();

        SlowLockLogDecision l1 = SlowLockLogDecision.decide(true, stack, event, breadcrumb, T0);
        Assertions.assertTrue(l1.captureStack);

        // next within L1/L2 window but past breadcrumb → L3, no stack
        SlowLockLogDecision l3 = SlowLockLogDecision.decide(true, stack, event, breadcrumb, T0 + 1200);
        Assertions.assertEquals(SlowLockTier.L3_BREADCRUMB, l3.tier);
        Assertions.assertFalse(l3.captureStack);

        Assertions.assertFalse(SlowLockLogDecision.SUPPRESS.captureStack);
    }
}
