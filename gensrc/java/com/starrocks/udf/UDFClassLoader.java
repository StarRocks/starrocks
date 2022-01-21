// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.udf;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;

// UDF ClassLoader
// Used to isolate classes between UDFs to avoid the influence between static variables.
// BE UDF's Expr creates a new instance of ClassLoader 
// each time it opens. It can ensure no influence on each other.

public class UDFClassLoader extends ClassLoader {
    private String UDFPath;
    private URLClassLoader loader;


    public UDFClassLoader(String UDFPath) throws IOException {
        this.UDFPath = UDFPath;
        URL[] urls = {new URL("file://" + this.UDFPath)};
        loader = URLClassLoader.newInstance(urls);
    }

    @Override
    protected Class<?> findClass(String name) throws ClassNotFoundException {
        return loader.loadClass(name.replace("/", "."));
    }
}

