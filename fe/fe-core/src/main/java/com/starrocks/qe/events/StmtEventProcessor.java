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
import com.starrocks.plugin.ExtendedPluginsClassLoader;
import com.starrocks.qe.events.listener.StmtEventListener;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
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

    private static final String DEFAULT_PACKAGE = "com.starrocks.qe.events.listener";

    private static final String PACKAGE_PREFIX = "com.starrocks";

    private final BlockingQueue<StmtEvent> eventQueue = new ArrayBlockingQueue<>(Config.stmt_event_processor_queue_size);

    private final AtomicBoolean running = new AtomicBoolean();

    private final List<StmtEventListener> listeners = new ArrayList<>();

    private final AtomicBoolean inited = new AtomicBoolean(false);
    private Thread workerThread;

    private static final StmtEventProcessor INSTANCE = new StmtEventProcessor();

    private StmtEventProcessor() {
    }

    public static void startProcessor() {
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
     * unregister listener from processor
     */
    public static boolean unRegisterListener(StmtEventListener listener) {
        if (INSTANCE != null) {
            return INSTANCE.unRegister(listener);
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
            LOG.warn("encounter exception when handle stmt event, "
                    + "queryId {}", event.getQueryId(), e);
        }
    }

    public boolean register(StmtEventListener listener) {
        //only register once
        boolean isPresent = listeners.stream()
                .filter(current ->
                        current.getClass().getCanonicalName()
                                .equals(listener.getClass().getCanonicalName()))
                .findFirst().isPresent();
        if (!isPresent) {
            return listeners.add(listener);
        } else {
            return false;
        }
    }

    /**
     * load all listener implements and register to processor
     */
    private void loadListeners() {
        List<String> packages = new ArrayList<>();
        packages.add(DEFAULT_PACKAGE);
        String extendedPackages = Config.event_listener_packages;
        if (StringUtils.isNotEmpty(extendedPackages)) {
            Arrays.stream(extendedPackages.split(","))
                    .filter(s -> s.startsWith(PACKAGE_PREFIX))
                    .forEach(packages::add);
        }

        for (String packagePath : packages) {
            Set<Class> classes = findAllClassesByPackage(packagePath);
            classes.stream().filter(clazz -> isInterfaceOf(clazz, StmtEventListener.class))
                    .forEach(clazz -> {
                        try {
                            StmtEventListener listener = (StmtEventListener) clazz.newInstance();
                            register(listener);
                        } catch (Exception e) {
                            LOG.error("instance class error, class {}", clazz.getCanonicalName(), e);
                        }
                    });
        }
    }

    public boolean unRegister(StmtEventListener listener) {
        if (listeners.contains(listener)) {
            return listeners.remove(listener);
        }
        return true;
    }


    public void start() {
        if (running.get()) {
            LOG.info("worker is running");
            return;
        }
        if (!inited.get()) {
            loadListeners();
            inited.set(true);
        }
        workerThread = new Thread(new Worker(), "StmtEventProcessor");
        workerThread.setDaemon(true);
        workerThread.start();
        running.set(true);
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

    private Set<Class> findAllClassesByPackage(String packageName) {
        ClassLoader parentLoader = ExtendedPluginsClassLoader
                .create(getClass().getClassLoader(), Collections.EMPTY_LIST);
        InputStream stream = parentLoader.getResourceAsStream(packageName.replaceAll("[.]", "/"));
        BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
        return reader.lines()
                .filter(line -> line.endsWith(".class"))
                .map(line -> getClass(line, packageName))
                .filter(clazz -> clazz != null)
                .collect(Collectors.toSet());
    }

    private Class getClass(String className, String packageName) {
        try {
            return Class.forName(packageName + "."
                    + className.substring(0, className.lastIndexOf('.')));
        } catch (ClassNotFoundException e) {
            LOG.error("load class error, class {}, package {}", className, packageName, e);
        }
        return null;
    }

    private static boolean isInterfaceOf(Class origin, Class interfaceClass) {
        return Arrays.stream(origin.getInterfaces())
                .filter(i -> i.getCanonicalName().equals(interfaceClass.getCanonicalName()))
                .findFirst()
                .isPresent();
    }

    public class Worker implements Runnable {
        @Override
        public void run() {
            while (running.get()) {
                StmtEvent stmtEvent = null;
                try {
                    stmtEvent = eventQueue.take();
                    if (stmtEvent == null) {
                        continue;
                    }
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
