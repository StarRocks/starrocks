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

import java.util.concurrent.atomic.AtomicLong;

/**
 * Immutable verdict for a single slow-lock log event: which {@link SlowLockTier} to emit (or
 * suppress) and whether to capture stacks.
 *
 * <p>This class is the <b>single place</b> that touches the stateful slow-lock throttle gates.
 * Callers decide exactly once per logical slow-lock event via {@link #decide}, then thread the
 * returned immutable verdict through all rendering code, which only reads {@link #tier} /
 * {@link #captureStack}. That separation removes the previous footguns where a stateful
 * predicate ({@code shouldCaptureStack}/{@code shouldEmitSlowLog}) was invoked from several
 * scattered sites — each invocation silently consuming throttle quota and risking inconsistent
 * (half-captured) snapshots or double consumption across a before/after pair.
 *
 * <h3>Tiering</h3>
 * <ul>
 *   <li>L1 = full info + stacks, gated by {@code slow_lock_stack_print_interval_ms} (strictest)</li>
 *   <li>L2 = full info, no stacks, gated by {@code slow_lock_log_every_ms}</li>
 *   <li>L3 = plain-text breadcrumb, gated by {@code slow_lock_breadcrumb_every_ms} (loosest floor)</li>
 * </ul>
 * {@link #decide} probes the gates in cost order and degrades downward; the chosen tier
 * <b>subsumes</b> the looser gates (an L1 win force-advances the L2 and L3 gates, an L2 win
 * force-advances the L3 gate) so total emitted volume never exceeds the loosest admitted tier's
 * rate — the refactor cannot make logging noisier than a single outer throttle would.
 */
public final class SlowLockLogDecision {
    /**
     * "Sufficiently in the past" gate initializer so the first event always wins regardless of
     * where {@link System#nanoTime()} anchors its origin (the JLS allows an arbitrary, possibly
     * negative, origin). The {@code / 2} keeps {@code monoNowMs - GATE_INIT_SENTINEL} clear of
     * signed-long overflow for any plausible JVM uptime.
     */
    public static final long GATE_INIT_SENTINEL = Long.MIN_VALUE / 2;

    /** Verdict for "do not log this event at all". */
    public static final SlowLockLogDecision SUPPRESS = new SlowLockLogDecision(null, false);

    /** The tier to emit, or {@code null} to suppress. */
    public final SlowLockTier tier;
    /** True iff the event should capture thread stacks (only at {@link SlowLockTier#L1_STACK_INFO}). */
    public final boolean captureStack;

    private SlowLockLogDecision(SlowLockTier tier, boolean captureStack) {
        this.tier = tier;
        this.captureStack = captureStack;
    }

    public boolean shouldLog() {
        return tier != null;
    }

    public boolean isBreadcrumb() {
        return tier == SlowLockTier.L3_BREADCRUMB;
    }

    /**
     * Decide the tier for one slow-lock event, consuming the appropriate throttle gate(s).
     * Must be called at most once per logical event. The caller samples the monotonic clock
     * once and passes it in (keeps the logic deterministic and unit-testable).
     *
     * @param hasHolder  whether the snapshot has an actual lock holder/reader to dump a stack for;
     *                   when false the L1 stack gate is never touched (avoids burning quota on
     *                   empty snapshots)
     * @param lastStackMs      L1 gate (slow_lock_stack_print_interval_ms)
     * @param lastEventMs      L2 gate (slow_lock_log_every_ms)
     * @param lastBreadcrumbMs L3 gate (slow_lock_breadcrumb_every_ms)
     * @param monoNowMs        current time from a monotonic source, in ms
     */
    public static SlowLockLogDecision decide(boolean hasHolder,
                                             AtomicLong lastStackMs,
                                             AtomicLong lastEventMs,
                                             AtomicLong lastBreadcrumbMs,
                                             long monoNowMs) {
        // L1: full stack + info, strictest. Master switch + hasHolder short-circuit so empty
        // snapshots never consume the stack quota.
        if (hasHolder && Config.slow_lock_print_stack
                && tryAdvance(lastStackMs, Config.slow_lock_stack_print_interval_ms, monoNowMs)) {
            // Tier subsumption: an L1 emit must also push the looser gates forward so a
            // near-immediate second event cannot additionally emit L2/L3.
            forceAdvance(lastEventMs, monoNowMs);
            forceAdvance(lastBreadcrumbMs, monoNowMs);
            return new SlowLockLogDecision(SlowLockTier.L1_STACK_INFO, true);
        }
        // L2: full info, no stack.
        if (tryAdvance(lastEventMs, Config.slow_lock_log_every_ms, monoNowMs)) {
            forceAdvance(lastBreadcrumbMs, monoNowMs);
            return new SlowLockLogDecision(SlowLockTier.L2_INFO, false);
        }
        // L3: plain-text breadcrumb, the always-leaves-evidence floor.
        if (tryAdvance(lastBreadcrumbMs, Config.slow_lock_breadcrumb_every_ms, monoNowMs)) {
            return new SlowLockLogDecision(SlowLockTier.L3_BREADCRUMB, false);
        }
        return SUPPRESS;
    }

    /**
     * CAS-advance {@code gate} to {@code monoNowMs} iff at least {@code intervalMs} has elapsed.
     * {@code intervalMs <= 0} disables the gate (always admit). Returns whether this caller won.
     */
    private static boolean tryAdvance(AtomicLong gate, long intervalMs, long monoNowMs) {
        if (intervalMs <= 0) {
            return true;
        }
        long last = gate.get();
        return monoNowMs - last >= intervalMs && gate.compareAndSet(last, monoNowMs);
    }

    /** Best-effort monotonic push-forward used for tier subsumption; never moves a gate backward. */
    private static void forceAdvance(AtomicLong gate, long monoNowMs) {
        long last;
        do {
            last = gate.get();
            if (last >= monoNowMs) {
                return;
            }
        } while (!gate.compareAndSet(last, monoNowMs));
    }
}
