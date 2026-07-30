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

package com.starrocks.context;

import com.google.common.collect.ImmutableMap;
import com.starrocks.epack.persist.EditLogEPack;
import com.starrocks.journal.JournalTask;
import com.starrocks.persist.ContextOpLog;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.WALApplier;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.concurrent.BlockingQueue;

/**
 * Verifies that {@link ContextMgr} writes the EditLog entry <strong>before</strong> mutating
 * its in-memory map. The pre-fix order (mutate, then log) meant any failure inside
 * {@code EditLog.log*} would leave the leader carrying state that no follower would ever see,
 * and the next image dump would silently persist the drift.
 *
 * <p>Implementation note: we subclass the real {@link EditLogEPack} reusing the shared
 * {@code journalQueue} that {@link UtFrameUtils#setUpForPersistTest()} wired up, then override
 * only the three contextbase log methods to throw. Unrelated journal calls — notably
 * {@code logSaveNextId} triggered by {@code getNextId()} — still flow through the real journal
 * pipeline and complete normally, so the test exercises just the order-of-operations contract.
 */
public class ContextMgrPersistOrderTest {

    @BeforeEach
    public void setUp() {
        UtFrameUtils.setUpForPersistTest();
        // Pre-warm the id generator so the test path doesn't try to journal a SaveNextId
        // entry through our throwing EditLog. The first getNextId() reserves a batch range
        // via logSaveNextId; subsequent calls within the batch are pure CAS in-memory.
        GlobalStateMgr.getCurrentState().getNextId();
    }

    @AfterEach
    public void tearDown() {
        UtFrameUtils.tearDownForPersisTest();
    }

    /** Returns the {@code journalQueue} field of an existing {@link EditLog} via reflection. */
    @SuppressWarnings("unchecked")
    private static BlockingQueue<JournalTask> queueOf(EditLog editLog) throws Exception {
        Field f = EditLog.class.getDeclaredField("journalQueue");
        f.setAccessible(true);
        return (BlockingQueue<JournalTask>) f.get(editLog);
    }

    /** EditLogEPack subclass that re-uses the existing journal queue but throws from the three context paths. */
    private static EditLog throwingFor(EditLog real) throws Exception {
        BlockingQueue<JournalTask> queue = queueOf(real);
        return new EditLogEPack(queue) {
            @Override
            public void logCreateContextBase(ContextOpLog log, WALApplier walApplier) {
                throw new RuntimeException("simulated journal write failure on create");
            }
            @Override
            public void logAlterContextBase(ContextOpLog log, WALApplier walApplier) {
                throw new RuntimeException("simulated journal write failure on alter");
            }
            @Override
            public void logDropContextBase(ContextOpLog log, WALApplier walApplier) {
                throw new RuntimeException("simulated journal write failure on drop");
            }
        };
    }

    @Test
    public void createDoesNotMutateMapIfEditLogThrows() throws Exception {
        ContextMgr mgr = new ContextMgr();
        GlobalStateMgr realState = GlobalStateMgr.getCurrentState();
        EditLog prior = realState.getEditLog();
        try {
            realState.setEditLog(throwingFor(prior));
            Assertions.assertThrows(RuntimeException.class,
                    () -> mgr.createContextBase("persist_order_test_cb",
                            ImmutableMap.of("default_consistency", "STRICT"), false));
            Assertions.assertNull(mgr.getContextBase("persist_order_test_cb"),
                    "in-memory map must stay empty when EditLog write fails — otherwise " +
                            "leader/follower metadata diverge silently");
        } finally {
            realState.setEditLog(prior);
        }
    }

    @Test
    public void alterDoesNotMutateMapIfEditLogThrows() throws Exception {
        ContextMgr mgr = new ContextMgr();
        mgr.replayCreateContextBase(ContextOpLog.forContextBase(100L, "alter_order_cb",
                ImmutableMap.of("default_consistency", "PRIMARY_CONSISTENT")));

        GlobalStateMgr realState = GlobalStateMgr.getCurrentState();
        EditLog prior = realState.getEditLog();
        try {
            realState.setEditLog(throwingFor(prior));
            Assertions.assertThrows(RuntimeException.class,
                    () -> mgr.alterContextBase("alter_order_cb",
                            ImmutableMap.of("default_consistency", "STRICT"), false));
            ContextMgr.ContextBaseMeta meta = mgr.getContextBase("alter_order_cb");
            Assertions.assertNotNull(meta);
            Assertions.assertEquals("PRIMARY_CONSISTENT", meta.getProperties().get("default_consistency"),
                    "alter must not partially apply when EditLog write fails");
        } finally {
            realState.setEditLog(prior);
        }
    }

    @Test
    public void dropDoesNotMutateMapIfEditLogThrows() throws Exception {
        ContextMgr mgr = new ContextMgr();
        mgr.replayCreateContextBase(ContextOpLog.forContextBase(200L, "drop_order_cb",
                ImmutableMap.of()));

        GlobalStateMgr realState = GlobalStateMgr.getCurrentState();
        EditLog prior = realState.getEditLog();
        try {
            realState.setEditLog(throwingFor(prior));
            Assertions.assertThrows(RuntimeException.class,
                    () -> mgr.dropContextBase("drop_order_cb", false));
            Assertions.assertNotNull(mgr.getContextBase("drop_order_cb"),
                    "drop must not remove the in-memory entry when EditLog write fails");
        } finally {
            realState.setEditLog(prior);
        }
    }
}
