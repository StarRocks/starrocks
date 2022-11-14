// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.task;

import com.starrocks.common.PriorityRunnable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class PriorityLeaderTask extends PriorityRunnable {
    private static final Logger LOG = LogManager.getLogger(PriorityLeaderTask.class);

    protected long signature;

    public PriorityLeaderTask() {
        super(0);
    }

    public PriorityLeaderTask(int priority) {
        super(priority);
    }

    @Override
    public void run() {
        try {
            exec();
        } catch (Exception e) {
            LOG.error("task exec error ", e);
        }
    }

    public long getSignature() {
        return signature;
    }

    /**
     * implement in child
     */
    protected abstract void exec();

}
