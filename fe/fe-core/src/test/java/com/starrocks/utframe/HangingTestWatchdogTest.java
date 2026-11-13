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

package com.starrocks.utframe;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.platform.engine.TestExecutionResult;
import org.junit.platform.engine.UniqueId;
import org.junit.platform.engine.support.descriptor.EngineDescriptor;
import org.junit.platform.launcher.TestIdentifier;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

public class HangingTestWatchdogTest {
    private PrintStream originalErr;
    private ByteArrayOutputStream err;

    @BeforeEach
    public void setUp() {
        originalErr = System.err;
        err = new ByteArrayOutputStream();
        System.setErr(new PrintStream(err, true, StandardCharsets.UTF_8));
    }

    @AfterEach
    public void tearDown() {
        System.setErr(originalErr);
    }

    private String takeErr() {
        String s = err.toString(StandardCharsets.UTF_8);
        err.reset();
        return s;
    }

    private static TestIdentifier identifier(String name) {
        return TestIdentifier.from(new EngineDescriptor(UniqueId.forEngine(name), name));
    }

    @Test
    public void testDumpOncePerStall() throws Exception {
        HangingTestWatchdog watchdog = new HangingTestWatchdog(1);
        TestIdentifier stuck = identifier("stuck");
        watchdog.executionStarted(stuck);

        watchdog.check();
        Assertions.assertEquals("", takeErr(), "must not dump before the threshold");

        Thread.sleep(1100);
        watchdog.check();
        String dump = takeErr();
        Assertions.assertTrue(dump.contains("no test started or finished for"), dump);
        Assertions.assertTrue(dump.contains("[engine:stuck]"), dump);
        Assertions.assertTrue(dump.contains("thread dump:"), dump);
        Assertions.assertTrue(dump.contains("testDumpOncePerStall"), "the dump must include this thread: " + dump);
        // the Gradle build forwards the output by this prefix
        dump.lines().forEach(line -> Assertions.assertTrue(line.startsWith(HangingTestWatchdog.OUTPUT_PREFIX), line));

        Thread.sleep(1100);
        watchdog.check();
        Assertions.assertEquals("", takeErr(), "a stall is dumped once");

        // progress starts a new stall
        TestIdentifier next = identifier("next");
        watchdog.executionFinished(stuck, TestExecutionResult.successful());
        watchdog.executionStarted(next);
        watchdog.check();
        Assertions.assertEquals("", takeErr());
        Thread.sleep(1100);
        watchdog.check();
        dump = takeErr();
        Assertions.assertTrue(dump.contains("[engine:next]"), dump);
        Assertions.assertFalse(dump.contains("[engine:stuck]"), dump);
    }

    @Test
    public void testNoDumpWhenNothingIsRunning() throws Exception {
        HangingTestWatchdog watchdog = new HangingTestWatchdog(1);
        TestIdentifier test = identifier("done");
        watchdog.executionStarted(test);
        watchdog.executionFinished(test, TestExecutionResult.successful());

        Thread.sleep(1100);
        watchdog.check();
        Assertions.assertEquals("", takeErr());
    }
}
