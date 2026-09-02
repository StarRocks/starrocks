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

package com.starrocks.server;

import com.starrocks.common.Config;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class GracefulExitFlagTest {

    // GRACEFUL_EXIT / BEGIN_NANO are static final Atomic*; the references can't be replaced, but the
    // atomic values can be reset through the live references via plain reflection.
    private static void resetFlag() throws Exception {
        Field flagField = GracefulExitFlag.class.getDeclaredField("GRACEFUL_EXIT");
        flagField.setAccessible(true);
        ((AtomicBoolean) flagField.get(null)).set(false);
        Field beginField = GracefulExitFlag.class.getDeclaredField("BEGIN_NANO");
        beginField.setAccessible(true);
        ((AtomicLong) beginField.get(null)).set(0L);
        GracefulExitFlag.setPreSignalTxnIds(null);
    }

    @BeforeEach
    public void setUp() throws Exception {
        resetFlag();
    }

    @AfterEach
    public void tearDown() throws Exception {
        // Leave no residue for other tests in the same JVM.
        resetFlag();
    }

    private static AtomicBoolean gracefulExitFlag() throws Exception {
        Field flagField = GracefulExitFlag.class.getDeclaredField("GRACEFUL_EXIT");
        flagField.setAccessible(true);
        return (AtomicBoolean) flagField.get(null);
    }

    private static AtomicLong beginNano() throws Exception {
        Field beginField = GracefulExitFlag.class.getDeclaredField("BEGIN_NANO");
        beginField.setAccessible(true);
        return (AtomicLong) beginField.get(null);
    }

    @Test
    public void testInitiallyNotInGracefulExit() {
        Assertions.assertFalse(GracefulExitFlag.isGracefulExit());
        Assertions.assertTrue(GracefulExitFlag.shouldAcceptNewRequest());
    }

    @Test
    public void testMarkGracefulExitReturnsTrueOnlyOnce() {
        Assertions.assertTrue(GracefulExitFlag.markGracefulExit());
        // A second caller loses the CAS and must not reset BEGIN_NANO.
        Assertions.assertFalse(GracefulExitFlag.markGracefulExit());
        Assertions.assertTrue(GracefulExitFlag.isGracefulExit());
    }

    @Test
    public void testMarkGracefulExitDoesNotResetBeginNanoOnSecondCall() throws Exception {
        Assertions.assertTrue(GracefulExitFlag.markGracefulExit());
        Assertions.assertTrue(beginNano().get() > 0L);

        GracefulExitFlag.markGracefulExit();
        Assertions.assertTrue(beginNano().get() > 0L);
        // The begin time from the first (winning) caller must be preserved.
        long firstBegin = beginNano().get();
        GracefulExitFlag.markGracefulExit();
        Assertions.assertEquals(firstBegin, beginNano().get());
    }

    @Test
    public void testShouldAcceptNewRequestWithinWindow() {
        GracefulExitFlag.markGracefulExit();
        // Default accept-new window is 60s, far longer than any test execution.
        Assertions.assertTrue(GracefulExitFlag.shouldAcceptNewRequest());
    }

    @Test
    public void testShouldRejectNewRequestAfterWindow() throws Exception {
        GracefulExitFlag.markGracefulExit();
        long windowNanos = TimeUnit.MILLISECONDS.toNanos(Config.graceful_exit_accept_new_window_ms);
        // Force the recorded begin time far into the past (beyond the window).
        beginNano().set(System.nanoTime() - windowNanos - 1L);
        Assertions.assertFalse(GracefulExitFlag.shouldAcceptNewRequest());
    }
    @Test
    public void testShouldAcceptNewRequestWhenMarkedButBeginNanoZero() throws Exception {
        // GRACEFUL_EXIT is set but BEGIN_NANO is still 0 (a marker set without a begin timestamp,
        // e.g. restored state): the accept-new window is treated as not yet started, so the request
        // is accepted.
        gracefulExitFlag().set(true);
        beginNano().set(0L);
        Assertions.assertTrue(GracefulExitFlag.shouldAcceptNewRequest());
    }

    @Test
    public void testDrainWindowNotElapsedWhenMarkedButBeginNanoZero() throws Exception {
        // Same marker-without-timestamp state: the drain window cannot be considered elapsed when
        // there is no begin timestamp to measure from.
        gracefulExitFlag().set(true);
        beginNano().set(0L);
        Assertions.assertFalse(GracefulExitFlag.isDrainWindowElapsed());
    }

    @Test
    public void testDrainWindowNotElapsedBeforeGracefulExit() {
        Assertions.assertFalse(GracefulExitFlag.isDrainWindowElapsed());
    }

    @Test
    public void testDrainWindowNotElapsedWithinAcceptWindow() {
        GracefulExitFlag.markGracefulExit();
        // Default accept-new window (60s) far exceeds any test execution; drain must not be
        // considered elapsed while requests are still being accepted.
        Assertions.assertFalse(GracefulExitFlag.isDrainWindowElapsed());
    }

    @Test
    public void testDrainWindowNotElapsedBeforeMinElapses() throws Exception {
        GracefulExitFlag.markGracefulExit();
        long windowNanos = TimeUnit.MILLISECONDS.toNanos(Config.graceful_exit_accept_new_window_ms);
        // Accept-new window has just ended; min_graceful_exit_time_second has not yet elapsed.
        beginNano().set(System.nanoTime() - windowNanos - 1L);
        Assertions.assertFalse(GracefulExitFlag.isDrainWindowElapsed());
    }

    @Test
    public void testDrainWindowElapsedAfterAcceptWindowAndMin() throws Exception {
        GracefulExitFlag.markGracefulExit();
        long windowNanos = TimeUnit.MILLISECONDS.toNanos(Config.graceful_exit_accept_new_window_ms);
        long minNanos = TimeUnit.SECONDS.toNanos(Config.min_graceful_exit_time_second);
        // Both the accept-new window and the post-window minimum drain time have elapsed.
        beginNano().set(System.nanoTime() - windowNanos - minNanos - 1L);
        Assertions.assertTrue(GracefulExitFlag.isDrainWindowElapsed());
    }
}
