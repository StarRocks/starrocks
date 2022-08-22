// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.utils.loader;

import com.starrocks.utils.NativeMethodHelper;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;

/**
 * Reference to Apache Spark with some customization
 * A mutable class loader that gives preference to its own URLs over the parent class loader
 * when loading classes and resources.
 */
public class ChildFirstClassLoader extends URLClassLoader {
    static {
        ClassLoader.registerAsParallelCapable();
    }

    private ParentClassLoader parent;
    private ArrayList<String> parentFirstClass;

    public ChildFirstClassLoader(URL[] urls, ClassLoader parent) {
        super(urls, null);
        this.parent = new ParentClassLoader(parent);
        // load native method class from parent
        this.parentFirstClass = new ArrayList<>(Collections.singleton(NativeMethodHelper.class.getName()));
    }

    @Override
    public Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        if (!parentFirstClass.isEmpty() && parentFirstClass.stream().anyMatch(c -> c.equals(name))) {
            return parent.loadClass(name, resolve);
        }
        try {
            return super.loadClass(name, resolve);
        } catch (ClassNotFoundException cnf) {
            return parent.loadClass(name, resolve);
        }
    }

    @Override
    public Enumeration<URL> getResources(String name) throws IOException {
        ArrayList<URL> urls = Collections.list(super.getResources(name));
        urls.addAll(Collections.list(parent.getResources(name)));
        return Collections.enumeration(urls);
    }

    @Override
    public URL getResource(String name) {
        URL url = super.getResource(name);
        if (url != null) {
            return url;
        } else {
            return parent.getResource(name);
        }
    }

    @Override
    public void addURL(URL url) {
        super.addURL(url);
    }
}
