// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.load.pipe;

import com.starrocks.common.Config;
import com.starrocks.common.UserException;
import com.starrocks.common.util.FrontendDaemon;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * Listen event for the pipe, and generate new tasks
 * TODO: currently it's singe-thread execution, but it's very easy to extend to multi-thread style
 */
public class PipeListener extends FrontendDaemon {

    private static final Logger LOG = LogManager.getLogger(PipeListener.class);

    private PipeManager pipeManager;

    public PipeListener(PipeManager pm) {
        super("PipeListener", Config.pipe_listener_interval_millis);
        this.pipeManager = pm;
    }

    @Override
    protected void runAfterCatalogReady() {
        try {
            process();
        } catch (Throwable e) {
            LOG.warn("Failed to process one round of PipeListener", e);
        }
    }

    private void process() throws UserException {
        List<Pipe> pipes = pipeManager.getRunnablePipes();
        for (Pipe pipe : pipes) {
            try {
                pipe.poll();
            } catch (Throwable e) {
                LOG.warn("Poll pipe failed due to ", e);
            }
        }
    }

}
