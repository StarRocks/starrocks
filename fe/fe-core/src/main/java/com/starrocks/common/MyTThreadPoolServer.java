// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.common;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.server.TThreadPoolServer;

public class MyTThreadPoolServer extends TThreadPoolServer {
    private static final Logger LOG = LogManager.getLogger(MyTThreadPoolServer.class);

    public MyTThreadPoolServer(Args args) {
        super(args);
    }

    /**
     * override the execute method, catch any throwable and retry to avoid the accept thread exit unexpected
     */
    @Override
    protected void execute() {
        boolean shouldSleep = false;
        while (true) {
            try {
                if (shouldSleep) {
                    Thread.sleep(5000);
                }
                shouldSleep = true;

                super.execute();
            } catch (Throwable t) {
                LOG.error("thrift server accept failed, will retry", t);
            }
        }
    }
}
