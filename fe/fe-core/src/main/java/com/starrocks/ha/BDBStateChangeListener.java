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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/ha/BDBStateChangeListener.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.ha;

import com.google.common.base.Preconditions;
import com.sleepycat.je.rep.StateChangeEvent;
import com.sleepycat.je.rep.StateChangeListener;
import com.starrocks.common.util.Util;
import com.starrocks.persist.EditLog;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class BDBStateChangeListener implements StateChangeListener {
    public static final Logger LOG = LogManager.getLogger(EditLog.class);
    private FrontendNodeType newType = FrontendNodeType.UNKNOWN;
    private final boolean isElectable;

    public BDBStateChangeListener(boolean isElectable) {
        this.isElectable = isElectable;
    }

    public synchronized FrontendNodeType getNewType() {
        return newType;
    }

    @Override
    public synchronized void stateChange(StateChangeEvent sce) throws RuntimeException {
        switch (sce.getState()) {
            case MASTER: {
                newType = FrontendNodeType.LEADER;
                break;
            }
            case REPLICA: {
                if (isElectable) {
                    newType = FrontendNodeType.FOLLOWER;
                } else {
                    newType = FrontendNodeType.OBSERVER;
                }
                break;
            }
            case UNKNOWN: {
                newType = FrontendNodeType.UNKNOWN;
                break;
            }
            default: {
                String msg = "this node is " + sce.getState().name();
                LOG.warn(msg);
                Util.stdoutWithTime(msg);
                return;
            }
        }
        Preconditions.checkNotNull(newType);
        StateChangeExecutor.getInstance().notifyNewFETypeTransfer(newType);
    }

}
