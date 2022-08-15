// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.jdbcbridge;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;

/*
 * In order to simplify the implementation of jni cpp code, we add JDBCBridge as a bridge,
 * encapsulate some complex logic, and only provide the simplest interface for C++ calls.
 *
 * Currently, the implementation is relatively simple.
 * Each query will create a separate thread, load the JDBC driver class and create a short connection.
 * In fact, this has additional overhead.
 *
 * @TODO(silverbullet233):
 *   We can consider letting JDBCBridge manage the driver class loading and JDBC connection pool in the future.
 * */
public class JDBCBridge {
    private String driverLocation = null;

    public void setClassLoader(String driverLocation) throws Exception {
        URLClassLoader loader = URLClassLoader.newInstance(new URL[] {
                new File(driverLocation).toURI().toURL(),
        });
        Thread.currentThread().setContextClassLoader(loader);
        this.driverLocation = driverLocation;
    }

    public JDBCScanner getScanner(JDBCScanContext scanContext) throws Exception {
        return new JDBCScanner(this.driverLocation, scanContext);
    }
}
