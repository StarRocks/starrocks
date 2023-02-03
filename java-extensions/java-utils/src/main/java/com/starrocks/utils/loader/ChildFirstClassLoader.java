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

package com.starrocks.utils.loader;

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

    private ParentClassLoader parentLoader;
    private ArrayList<String> parentFirstClass;

    public ChildFirstClassLoader(URL[] urls, ClassLoader parent) {
        super(urls, null);
        this.parentLoader = new ParentClassLoader(parent);
        // load native method class from parent
        this.parentFirstClass = new ArrayList<>(Collections.singleton("com.starrocks.utils.NativeMethodHelper"));
    }

    @Override
    public Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        if (!parentFirstClass.isEmpty() && parentFirstClass.stream().anyMatch(c -> c.equals(name))) {
            return parentLoader.loadClass(name, resolve);
        }
        try {
            return super.loadClass(name, resolve);
        } catch (ClassNotFoundException cnf) {
            return parentLoader.loadClass(name, resolve);
        }
    }

    @Override
    public Enumeration<URL> getResources(String name) throws IOException {
        ArrayList<URL> urls = Collections.list(super.getResources(name));
        urls.addAll(Collections.list(parentLoader.getResources(name)));
        return Collections.enumeration(urls);
    }

    @Override
    public URL getResource(String name) {
        URL url = super.getResource(name);
        if (url != null) {
            return url;
        } else {
            return parentLoader.getResource(name);
        }
    }
}
