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

import com.starrocks.common.Config;
import com.starrocks.qe.events.listener.StmtEventListener;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * processing stmt events, loading listeners when startProcessor
 */
public class StmtEventProcessor {
    private static final Logger LOG = LogManager.getLogger(StmtEventProcessor.class);

    private final BlockingQueue<StmtEvent> eventQueue = new ArrayBlockingQueue<>(Config.stmt_event_processor_queue_size);

    private final AtomicBoolean running = new AtomicBoolean();

    private final List<StmtEventListener> listeners = new ArrayList<>();

    private final AtomicBoolean inited = new AtomicBoolean(false);

    private Thread workerThread;

    private static final StmtEventProcessor INSTANCE = new StmtEventProcessor();

    private StmtEventProcessor() {
    }

    public static void startProcessor() {
        LOG.info("Initializing StmtEventProcessor.");
        if (INSTANCE != null) {
            INSTANCE.start();
        }
    }

    public static void stopProcessor() {
        if (INSTANCE != null) {
            INSTANCE.stop();
        }
    }

    // visible for testing only
    public static void forceStop() {
        if (INSTANCE != null && INSTANCE.workerThread != null) {
            INSTANCE.running.compareAndSet(true, false);
            INSTANCE.workerThread.interrupt();
        }
    }

    /**
     * register listener to processor
     */
    public static boolean registerListener(StmtEventListener listener) {
        //only register once
        if (INSTANCE != null) {
            return INSTANCE.register(listener);
        }
        return false;
    }

    /**
     * deregister listener from processor
     */
    public static boolean deregisterListener(StmtEventListener listener) {
        if (INSTANCE != null) {
            return INSTANCE.deregister(listener);
        }
        return true;
    }

    /**
     * post event to processor
     */
    public static void postEvent(StmtEvent event) {
        try {
            boolean success = INSTANCE.eventQueue.offer(event);
            if (!success) {
                LOG.warn("stmt event queue is full, queryId {}", event.getQueryId());
            }
        } catch (Exception e) {
            LOG.warn("encounter exception when handle stmt event, queryId {}", event.getQueryId(), e);
        }
    }

    /**
     * Check if any statement listener is registered
     *
     * @return true on any listener registered, false on none registered
     */
    public static boolean isStatementListenerEnabled() {
        return !INSTANCE.listeners.isEmpty();
    }

    public boolean register(StmtEventListener listener) {
        //only register once
        boolean isPresent = listeners.stream().anyMatch(current ->
                current.getClass().getCanonicalName().equals(listener.getClass().getCanonicalName()));
        if (!isPresent) {
            LOG.info("Registering listener of {}", listener.getClass().getCanonicalName());
            return listeners.add(listener);
        } else {
            return false;
        }
    }

    public boolean deregister(StmtEventListener listener) {
        if (listeners.contains(listener)) {
            return listeners.remove(listener);
        }
        return true;
    }

    public void start() {
        LOG.info("Starting StmtEventProcessor.");
        if (running.get()) {
            LOG.info("worker is running");
            return;
        }
        if (!inited.get()) {
            LOG.info("Try loading and registering statement listeners.");
            loadAndRegisterListeners();
            inited.set(true);
            LOG.info("Total {} listeners are registered, they are: {}", listeners.size(),
                    listeners.stream().map(c -> c.getClass().getCanonicalName()).collect(Collectors.joining(",")));
        }
        if (listeners.isEmpty()) {
            LOG.info("No statement listener was registered, skip starting event processing thread.");
            return;
        }
        workerThread = new Thread(new Worker(), "StmtEventProcessor");
        workerThread.setDaemon(true);
        workerThread.start();
        running.set(true);
        LOG.info("StmtEventProcessor with worker thread ({}) had been started.", workerThread.getName());
    }

    public void stop() {
        running.compareAndSet(true, false);
        if (workerThread != null) {
            try {
                //block until the end of consumption
                workerThread.join();
            } catch (InterruptedException e) {
                LOG.warn("join worker join failed.", e);
            }
        }
    }

    /**
     * load all listener implements and register to processor
     */
    private void loadAndRegisterListeners() {
        Set<String> listenerNames = getListenerClassNames();
        LOG.info("Being registered listeners are: {}", listenerNames);
        for (String className : listenerNames) {
            StmtEventListener listener = newStatementListenerInstance(className);
            if (null == listener) {
                continue;
            }
            register(listener);
        }
    }

    @NotNull
    protected static Set<String> getListenerClassNames() {
        if (StringUtils.isBlank(Config.stmt_event_listeners)) {
            return Collections.emptySet();
        }
        return Arrays.stream(StringUtils.split(Config.stmt_event_listeners, ','))
                .map(StringUtils::trim).filter(StringUtils::isNotBlank).collect(Collectors.toSet());
    }

    protected static StmtEventListener newStatementListenerInstance(String listenerClassName) {
        try {
            Class<?> clazz = Class.forName(listenerClassName);
            if (!isValidStmtListener(clazz)) {
                LOG.warn("Listener class {} is NOT a sub class of {}", listenerClassName,
                        StmtEventListener.class.getCanonicalName());
                return null;
            }
            return (StmtEventListener) clazz.getDeclaredConstructor().newInstance();
        } catch (Exception ex) {
            LOG.error("Failed initializing statement listener: {}", listenerClassName, ex);
            return null;
        }
    }

    private static boolean isValidStmtListener(Class<?> origin) {
        return Arrays.stream(origin.getInterfaces())
                .anyMatch(i -> i.getCanonicalName().equals(StmtEventListener.class.getCanonicalName()));
    }

    public class Worker implements Runnable {
        @Override
        public void run() {
            while (running.get()) {
                StmtEvent stmtEvent = null;
                try {
                    stmtEvent = eventQueue.take();
                    for (StmtEventListener listener : listeners) {
                        listener.onEvent(stmtEvent);
                    }
                } catch (Exception e) {
                    LOG.warn("encounter exception when getting stmt listener event from queue, "
                            + "queryId {}", stmtEvent == null ? "" : stmtEvent.getQueryId(), e);
                }
            }
        }
    }
}
