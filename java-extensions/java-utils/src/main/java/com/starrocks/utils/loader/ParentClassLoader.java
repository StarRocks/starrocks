// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.utils.loader;

/**
 * Reference to Apache Spark with some customization
 * A class loader which makes some protected methods in ClassLoader accessible.
 */
public class ParentClassLoader extends ClassLoader {

    static {
        ClassLoader.registerAsParallelCapable();
    }

    public ParentClassLoader(ClassLoader parent) {
        super(parent);
    }

    @Override
    public Class<?> findClass(String name) throws ClassNotFoundException {
        return super.findClass(name);
    }

    @Override
    public Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        return super.loadClass(name, resolve);
    }
}