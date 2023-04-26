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
