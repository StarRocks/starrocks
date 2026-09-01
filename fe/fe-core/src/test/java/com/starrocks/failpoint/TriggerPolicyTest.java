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

package com.starrocks.failpoint;

import com.starrocks.thrift.TUpdateFailPointRequest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

public class TriggerPolicyTest {

    // Long enough that a parked thread stays parked for the duration of a test.
    private static final int PAUSE_TIMEOUT_SECOND = 300;

    // Spin until the policy reports the expected number of parked threads, so the tests synchronise
    // on real state rather than on a sleep.
    private static boolean waitForParked(TriggerPolicy policy, int expected) throws InterruptedException {
        for (int waited = 0; waited < 10_000; waited += 5) {
            if (policy.getPausedThreadCount() == expected) {
                return true;
            }
            Thread.sleep(5);
        }
        return false;
    }

    // Daemon threads so a failed assertion cannot leave a non-daemon thread parked for the whole
    // pause timeout.
    private static Thread startParker(TriggerPolicy policy, String name, AtomicBoolean triggered) {
        return startParker(policy, name, triggered, () -> policy.shouldTrigger(name));
    }

    private static Thread startParker(TriggerPolicy policy, String name, AtomicBoolean triggered,
                                      BooleanSupplier trigger) {
        Thread t = new Thread(() -> triggered.set(trigger.getAsBoolean()), "parker-" + name);
        t.setDaemon(true);
        t.start();
        return t;
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    public void testPauseReleasedByRelease() throws Exception {
        TriggerPolicy policy = TriggerPolicy.pausePolicy(PAUSE_TIMEOUT_SECOND);
        AtomicBoolean triggered = new AtomicBoolean(true);
        Thread t = startParker(policy, "fp_pause", triggered);
        try {
            Assertions.assertTrue(waitForParked(policy, 1));
        } finally {
            policy.release();
        }
        t.join(10_000);
        Assertions.assertFalse(t.isAlive());
        // A released pause continues normally and never injects.
        Assertions.assertFalse(triggered.get());
        Assertions.assertEquals(0, policy.getPausedThreadCount());
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    public void testPauseReleasesAllWaiters() throws Exception {
        TriggerPolicy policy = TriggerPolicy.pausePolicy(PAUSE_TIMEOUT_SECOND);
        AtomicBoolean a = new AtomicBoolean(true);
        AtomicBoolean b = new AtomicBoolean(true);
        Thread t1 = startParker(policy, "fp_multi", a);
        Thread t2 = startParker(policy, "fp_multi", b);
        try {
            Assertions.assertTrue(waitForParked(policy, 2));
        } finally {
            policy.release();
        }
        t1.join(10_000);
        t2.join(10_000);
        Assertions.assertFalse(a.get());
        Assertions.assertFalse(b.get());
    }

    // The armed timeout is honoured and a misconfigured value clamps to 1s rather than meaning
    // "never wait" or "wait forever". No global config is touched: the timeout now travels with the
    // policy, which is exactly the property that keeps frontends and backends in agreement.
    @ParameterizedTest
    @ValueSource(ints = {1, 0, -5})
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    public void testPauseTimesOutAfterItsArmedTimeout(int armedTimeout) {
        TriggerPolicy policy = TriggerPolicy.pausePolicy(armedTimeout);
        long start = System.nanoTime();
        Assertions.assertFalse(policy.shouldTrigger("fp_timeout"));
        long elapsedMs = (System.nanoTime() - start) / 1_000_000L;

        Assertions.assertTrue(elapsedMs >= 900, "resumed after " + elapsedMs + "ms, expected >= 900ms");
        Assertions.assertEquals(0, policy.getPausedThreadCount());
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    public void testInterruptResumesAndRestoresFlag() throws Exception {
        TriggerPolicy policy = TriggerPolicy.pausePolicy(PAUSE_TIMEOUT_SECOND);
        AtomicBoolean triggered = new AtomicBoolean(true);
        AtomicBoolean interruptFlagSeen = new AtomicBoolean(false);
        Thread t = new Thread(() -> {
            triggered.set(policy.shouldTrigger("fp_interrupt"));
            interruptFlagSeen.set(Thread.currentThread().isInterrupted());
        }, "parker-interrupt");
        t.setDaemon(true);
        t.start();
        try {
            Assertions.assertTrue(waitForParked(policy, 1));
        } finally {
            t.interrupt();
        }
        t.join(10_000);
        Assertions.assertFalse(t.isAlive());
        Assertions.assertFalse(triggered.get());
        // The interrupt status must be restored rather than swallowed.
        Assertions.assertTrue(interruptFlagSeen.get());
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    public void testReleaseIsIdempotentAndPreRelease() {
        TriggerPolicy policy = TriggerPolicy.pausePolicy(PAUSE_TIMEOUT_SECOND);
        policy.release();
        policy.release();
        // Already released: returns immediately rather than waiting out the timeout.
        long start = System.nanoTime();
        Assertions.assertFalse(policy.shouldTrigger("fp_prereleased"));
        Assertions.assertTrue((System.nanoTime() - start) / 1_000_000L < 5_000);
    }

    // Both ADMIN DISABLE FAILPOINT (remove) and a re-arm (replace) must release whatever is parked on
    // the superseded policy, or a paused thread is stranded until its timeout.
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    public void testRemoveTriggerPolicyReleasesPausedThread() throws Exception {
        assertReleasedBy("fp_remove", () -> FailPoint.removeTriggerPolicy("fp_remove"));
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    public void testReArmReleasesPausedThread() throws Exception {
        assertReleasedBy("fp_rearm", () -> FailPoint.setTriggerPolicy("fp_rearm", TriggerPolicy.enablePolicy()));
        FailPoint.removeTriggerPolicy("fp_rearm");
    }

    private void assertReleasedBy(String name, Runnable release) throws Exception {
        TriggerPolicy policy = TriggerPolicy.pausePolicy(PAUSE_TIMEOUT_SECOND);
        FailPoint.setTriggerPolicy(name, policy);
        AtomicBoolean triggered = new AtomicBoolean(true);
        Thread t = startParker(policy, name, triggered, () -> FailPoint.shouldTrigger(name));
        try {
            Assertions.assertTrue(waitForParked(policy, 1));
        } finally {
            release.run();
        }
        t.join(10_000);
        Assertions.assertFalse(t.isAlive());
        Assertions.assertFalse(triggered.get());
    }

    // A timed-out pause must DISARM the failpoint, not just let one thread through: otherwise every
    // newly arriving thread parks for a fresh full timeout and the node never recovers.
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    public void testTimeoutDisarmsTheFailpoint() {
        FailPoint.setTriggerPolicy("fp_selfheal", TriggerPolicy.pausePolicy(1));
        Assertions.assertFalse(FailPoint.shouldTrigger("fp_selfheal"));
        // The policy is gone, so a later arrival passes straight through instead of parking again.
        long start = System.nanoTime();
        Assertions.assertFalse(FailPoint.shouldTrigger("fp_selfheal"));
        long elapsedMs = (System.nanoTime() - start) / 1_000_000L;
        Assertions.assertTrue(elapsedMs < 500, "second arrival parked again after " + elapsedMs + "ms");
    }

    // A pause that times out must disarm ITSELF, never a policy that replaced it in the meantime --
    // otherwise the operator's newly armed mode is silently discarded. Exercised directly rather than
    // by racing a real timeout: setTriggerPolicy releases the superseded policy, so a parked thread
    // always observes a release rather than a timeout and never reaches the conditional removal.
    @Test
    public void testConditionalRemovalLeavesAReplacementArmed() {
        TriggerPolicy expiring = TriggerPolicy.pausePolicy(1);
        FailPoint.setTriggerPolicy("fp_replaced", expiring);
        TriggerPolicy replacement = TriggerPolicy.enablePolicy();
        FailPoint.setTriggerPolicy("fp_replaced", replacement);

        // This is what a timed-out pause does. It must leave the replacement alone.
        Assertions.assertFalse(FailPoint.removeTriggerPolicyIf("fp_replaced", expiring));
        Assertions.assertTrue(FailPoint.shouldTrigger("fp_replaced"), "replacement was removed");

        // The still-installed policy is removed, so a genuine self-disarm does take effect.
        Assertions.assertTrue(FailPoint.removeTriggerPolicyIf("fp_replaced", replacement));
        Assertions.assertFalse(FailPoint.shouldTrigger("fp_replaced"));
    }

    @Test
    public void testFromThriftPreferPauseOverIsEnable() {
        // A pause request sets is_enable = false so an old FE disables; a new FE must still see PAUSE.
        TUpdateFailPointRequest request = new TUpdateFailPointRequest();
        request.setName("fp");
        request.setIs_enable(false);
        request.setPause(true);
        request.setPause_timeout_second(42);
        Assertions.assertEquals(TriggerMode.PAUSE, TriggerPolicy.fromThrift(request).getMode());
    }

    @Test
    public void testIsArmingTreatsPauseAsArm() {
        TUpdateFailPointRequest pause = new TUpdateFailPointRequest();
        pause.setName("fp");
        pause.setIs_enable(false);
        pause.setPause(true);
        // The trap: is_enable is false, but this must NOT be read as a removal.
        Assertions.assertTrue(TriggerPolicy.isArming(pause));

        TUpdateFailPointRequest disable = new TUpdateFailPointRequest();
        disable.setName("fp");
        disable.setIs_enable(false);
        Assertions.assertFalse(TriggerPolicy.isArming(disable));

        TUpdateFailPointRequest enable = new TUpdateFailPointRequest();
        enable.setName("fp");
        enable.setIs_enable(true);
        Assertions.assertTrue(TriggerPolicy.isArming(enable));
    }

    @Test
    public void testNonPauseModesUnchanged() {
        Assertions.assertTrue(TriggerPolicy.enablePolicy().shouldTrigger("fp"));

        TriggerPolicy times = TriggerPolicy.timesPolicy(2);
        Assertions.assertTrue(times.shouldTrigger("fp"));
        Assertions.assertTrue(times.shouldTrigger("fp"));
        Assertions.assertFalse(times.shouldTrigger("fp"));

        Assertions.assertFalse(TriggerPolicy.probabilityPolicy(0.0).shouldTrigger("fp"));
    }
}
