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
     * override the execute method, catch OutOfMemoryError and retry to avoid the accept thread exit unexpected
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
            } catch (OutOfMemoryError error) {
                if (error.getMessage() != null &&
                        error.getMessage().toLowerCase().contains("unable to create new native thread")) {
                    LOG.error("Fail to accept new connection, " +
                            "please set the max user processes to a bigger value using `ulimit -u`. will retry", error);
                } else {
                    LOG.error("Fail to accept new connection, " +
                            "please set the jvm heap size to a bigger value. will exit", error);
                    System.exit(-1);
                }
            } catch (Throwable t) {
                LOG.error("Fail to accept new connection. will exit", t);
                System.exit(-1);
            }
        }
    }
}
