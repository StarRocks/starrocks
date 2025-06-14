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

package com.starrocks.task;

import com.starrocks.common.Status;
import com.starrocks.common.util.concurrent.MarkedCountDownLatch;
import com.starrocks.thrift.TDropAutoIncrementMapReq;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TTaskType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DropAutoIncrementMapTask extends AgentTask {
    private static final Logger LOG = LogManager.getLogger(DropAutoIncrementMapTask.class);

    long tableId;

    // used for synchronous process
    private MarkedCountDownLatch<Long, Long> latch;

    public DropAutoIncrementMapTask(long backendId, long tableId, long signature) {
        super(null, backendId, TTaskType.DROP_AUTO_INCREMENT_MAP, -1L, -1L, -1L, -1L, -1L, signature);
        this.tableId = tableId;
    }

    public long tableId() {
        return tableId;
    }

    public void countDownLatch(long backendId) {
        if (this.latch != null) {
            if (latch.markedCountDown(backendId, -1L)) {
                LOG.debug("DropAutoIncrementMapTask current latch count: {}, backend: {}, table:{}",
                        latch.getCount(), backendId, tableId);
            }
        }
    }

    // call this always means one of tasks is failed. count down to zero to finish entire task
    public void countDownToZero(String errMsg) {
        if (this.latch != null) {
            latch.countDownToZero(new Status(TStatusCode.CANCELLED, errMsg));
            LOG.debug("DropAutoIncrementMapTask down to zero. error msg: {}", errMsg);
        }
    }

    public void setLatch(MarkedCountDownLatch<Long, Long> latch) {
        this.latch = latch;
    }

    public TDropAutoIncrementMapReq toThrift() {
        TDropAutoIncrementMapReq req = new TDropAutoIncrementMapReq(tableId);
        return req;
    }
}
