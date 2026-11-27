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

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ExitTraceHookTest {

    private static StackTraceElement frame(String cls, String method) {
        return new StackTraceElement(cls, method, cls.substring(cls.lastIndexOf('.') + 1) + ".java", 10);
    }

    // the stack of a thread blocked in System.exit while the shutdown hooks run, top frame first
    private static StackTraceElement[] exitStack(StackTraceElement... callers) {
        StackTraceElement[] head = {
                frame("java.lang.Object", "wait"),
                frame("java.lang.Thread", "join"),
                frame("java.lang.ApplicationShutdownHooks", "runHooks"),
                frame("java.lang.Shutdown", "runHooks"),
                frame("java.lang.Shutdown", "exit"),
                frame("java.lang.Runtime", "exit"),
                frame("java.lang.System", "exit"),
        };
        StackTraceElement[] stack = new StackTraceElement[head.length + callers.length];
        System.arraycopy(head, 0, stack, 0, head.length);
        System.arraycopy(callers, 0, stack, head.length, callers.length);
        return stack;
    }

    private static StackTraceElement[] idleStack() {
        return new StackTraceElement[] {
                frame("java.lang.Thread", "sleep"),
                frame("com.starrocks.common.util.FrontendDaemon", "run"),
        };
    }

    @Test
    public void testFindExitCaller() {
        StackTraceElement caller = frame("com.starrocks.server.GlobalStateMgr", "replayJournal");
        assertEquals(caller, ExitTraceHook.findExitCaller(
                exitStack(caller, frame("java.lang.Thread", "run"))));

        // Runtime.getRuntime().exit() has no System.exit frame
        StackTraceElement[] runtimeExit = {
                frame("java.lang.Shutdown", "exit"),
                frame("java.lang.Runtime", "exit"),
                caller,
        };
        assertEquals(caller, ExitTraceHook.findExitCaller(runtimeExit));

        // SIGTERM: Shutdown.exit is called from the signal handler
        StackTraceElement terminator = frame("java.lang.Terminator$1", "handle");
        assertEquals(terminator, ExitTraceHook.findExitCaller(
                new StackTraceElement[] {frame("java.lang.Shutdown", "exit"), terminator}));

        assertNull(ExitTraceHook.findExitCaller(idleStack()));
        assertNull(ExitTraceHook.findExitCaller(new StackTraceElement[0]));
        assertNull(ExitTraceHook.findExitCaller(null));
        // other Shutdown methods do not count
        assertNull(ExitTraceHook.findExitCaller(
                new StackTraceElement[] {frame("java.lang.Shutdown", "shutdown"), caller}));
    }

    @Test
    public void testNoDumpWithoutExit() {
        Map<Thread, StackTraceElement[]> stacks = new LinkedHashMap<>();
        stacks.put(new Thread("main"), idleStack());
        stacks.put(new Thread("worker"), idleStack());
        assertNull(ExitTraceHook.buildDump(stacks, 1, 0, List.of()));
    }

    @Test
    public void testNoDumpForTestRunnerExit() {
        Map<Thread, StackTraceElement[]> stacks = new LinkedHashMap<>();
        stacks.put(new Thread("main"),
                exitStack(frame("org.apache.maven.surefire.booter.ForkedBooter", "acknowledgedExit"),
                        frame("org.apache.maven.surefire.booter.ForkedBooter", "main")));
        stacks.put(new Thread("worker"), idleStack());
        assertNull(ExitTraceHook.buildDump(stacks, 1, 0, List.of()));

        stacks.put(new Thread("Test worker"),
                exitStack(frame("worker.org.gradle.process.internal.worker.GradleWorkerMain", "main")));
        assertNull(ExitTraceHook.buildDump(stacks, 1, 0, List.of()));
    }

    @Test
    public void testDumpForUnexpectedExit() {
        Map<Thread, StackTraceElement[]> stacks = new LinkedHashMap<>();
        Thread replayer = new Thread("replayer");
        Thread main = new Thread("main");
        stacks.put(replayer, exitStack(frame("com.starrocks.server.GlobalStateMgr", "replayJournalInner"),
                frame("com.starrocks.server.GlobalStateMgr$4", "runOneCycle")));
        stacks.put(main, new StackTraceElement[] {
                frame("com.starrocks.pseudocluster.PseudoCluster", "build"),
                frame("org.apache.maven.surefire.booter.ForkedBooter", "main"),
        });

        String dump = ExitTraceHook.buildDump(stacks, 4242, 0,
                List.of("[class:com.starrocks.transaction.ConcurrentTxnTest]", "[engine:junit-jupiter]"));
        assertNotNull(dump);
        assertTrue(dump.startsWith("JVM exit through System.exit/Runtime.exit, pid=4242, time="), dump);
        // running tests are sorted
        assertTrue(dump.contains("running tests:\n\t[class:com.starrocks.transaction.ConcurrentTxnTest]\n"
                + "\t[engine:junit-jupiter]\n"), dump);
        assertTrue(dump.contains("exit called by thread \"replayer\" at "
                + "com.starrocks.server.GlobalStateMgr.replayJournalInner(GlobalStateMgr.java:10)\n"), dump);
        // every thread with its full stack, sorted by name
        int mainIdx = dump.indexOf("\"main\" id=" + main.getId() + " NEW\n");
        int replayerIdx = dump.indexOf("\"replayer\" id=" + replayer.getId() + " NEW\n");
        assertTrue(mainIdx > 0 && replayerIdx > mainIdx, dump);
        assertTrue(dump.contains("\tat com.starrocks.pseudocluster.PseudoCluster.build(PseudoCluster.java:10)\n"
                + "\tat org.apache.maven.surefire.booter.ForkedBooter.main(ForkedBooter.java:10)\n"), dump);
        assertTrue(dump.contains("\tat java.lang.Shutdown.exit(Shutdown.java:10)\n"), dump);
        assertTrue(dump.contains("\tat com.starrocks.server.GlobalStateMgr$4.runOneCycle(GlobalStateMgr$4.java:10)\n"),
                dump);
    }

    @Test
    public void testDumpWithoutRunningTests() {
        Map<Thread, StackTraceElement[]> stacks = new LinkedHashMap<>();
        stacks.put(new Thread("SIGTERM handler"), new StackTraceElement[] {
                frame("java.lang.Shutdown", "exit"),
                frame("java.lang.Terminator$1", "handle"),
        });
        String dump = ExitTraceHook.buildDump(stacks, 1, 0, List.of());
        assertNotNull(dump);
        assertTrue(dump.contains("running tests: (unknown)\n"), dump);
        assertTrue(dump.contains("exit called by thread \"SIGTERM handler\" at java.lang.Terminator$1.handle"), dump);
    }

    @Test
    public void testInstallIsIdempotent() {
        ExitTraceHook.install();
        ExitTraceHook.install();
    }
}
