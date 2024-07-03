// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.qe.events;

import com.starrocks.qe.events.listener.LineageStmtEventListener;
import com.starrocks.qe.events.listener.MyLineageStmtEventListener;
import org.junit.Test;

import java.util.Set;

import static com.starrocks.common.Config.stmt_event_listeners;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class StmtEventProcessorTest {
    static final String DEFAULT_LISTENER_NAME = "com.starrocks.qe.events.listener.LineageStmtEventListener";

    @Test
    public void testEvenProcessorWithSuccess() {
        assertFalse(StmtEventProcessor.isStatementListenerEnabled());
        stmt_event_listeners = DEFAULT_LISTENER_NAME + ",java.util.Date";
        StmtEventProcessor.startProcessor();
        StmtEvent event = new StmtEvent()
                .clientIp("127.0.0.1")
                .queryId("12")
                .user("root")
                .timestamp(123L);
        StmtEventProcessor.postEvent(event);
        LineageStmtEventListener listener = new LineageStmtEventListener();
        assertTrue(StmtEventProcessor.isStatementListenerEnabled());
        assertTrue(StmtEventProcessor.deregisterListener(listener));
        StmtEventProcessor.forceStop();
        StmtEventProcessor.stopProcessor();
    }

    @Test
    public void testGetListenerClassNames() {
        // default value of Config.stmt_event_listeners
        Set<String> listenerClassNames = StmtEventProcessor.getListenerClassNames();
        assertTrue(listenerClassNames.contains(DEFAULT_LISTENER_NAME));

        stmt_event_listeners = "java.util.ArrayList , java.util.HashSet ,  ,, , java.text.NumberFormat";
        listenerClassNames = StmtEventProcessor.getListenerClassNames();
        // empty ones are filtered
        assertEquals(3, listenerClassNames.size());
        assertTrue(listenerClassNames.contains("java.util.ArrayList"));
        assertTrue(listenerClassNames.contains("java.util.HashSet"));
        assertTrue(listenerClassNames.contains("java.text.NumberFormat"));

        stmt_event_listeners = null;
        listenerClassNames = StmtEventProcessor.getListenerClassNames();
        assertTrue(listenerClassNames.isEmpty());
    }

    @Test
    public void testNewStatementListenerInstance() {
        assertNull(StmtEventProcessor.newStatementListenerInstance("java.util.ArrayList"));
        assertNotNull(StmtEventProcessor.newStatementListenerInstance(DEFAULT_LISTENER_NAME));
        assertNull(StmtEventProcessor.newStatementListenerInstance(MyLineageStmtEventListener.class.getCanonicalName()));
    }
}
