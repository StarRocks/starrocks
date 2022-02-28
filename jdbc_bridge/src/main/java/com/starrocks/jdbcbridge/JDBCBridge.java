// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.jdbcbridge;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;



public class JDBCBridge {

    public JDBCBridge() {
    }

    public void setClassLoader(String driverLocation) throws Exception {
        URLClassLoader loader = URLClassLoader.newInstance(new URL[] {
                new File(driverLocation).toURI().toURL(),
        });
        Thread.currentThread().setContextClassLoader(loader);
    }

    public JDBCScanner getScanner(JDBCScanContext scanContext) throws Exception {
        JDBCScanner scanner = new JDBCScanner(scanContext);
        return scanner;
    }
}
