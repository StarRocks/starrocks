// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.utils.loader;

import java.io.Closeable;

public class ThreadContextClassLoader
        implements Closeable {
    private final ClassLoader originalThreadContextClassLoader;

    public ThreadContextClassLoader(ClassLoader newThreadContextClassLoader) {
        this.originalThreadContextClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(newThreadContextClassLoader);
    }

    @Override
    public void close() {
        Thread.currentThread().setContextClassLoader(originalThreadContextClassLoader);
    }
}
