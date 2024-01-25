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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/task/UpdateTabletMetaInfoTask.java

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
import com.starrocks.server.RunMode;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TTabletMetaInfo;
import com.starrocks.thrift.TTabletType;
import com.starrocks.thrift.TTaskType;
import com.starrocks.thrift.TUpdateTabletMetaInfoReq;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public abstract class TabletMetadataUpdateAgentTask extends AgentTask {

    protected static final Logger LOG = LogManager.getLogger(TabletMetadataUpdateAgentTask.class);

    protected MarkedCountDownLatch<Long, Set<Long>> latch;

    protected long txnId;

    protected TabletMetadataUpdateAgentTask(long backendId, long signature) {
        super(null, backendId, TTaskType.UPDATE_TABLET_META_INFO, -1L, -1L, -1L, -1L, -1L, signature);
    }

    public void setLatch(MarkedCountDownLatch<Long, Set<Long>> latch) {
        this.latch = requireNonNull(latch, "latch is null");
    }

    public void setTxnId(long txnId) {
        this.txnId = txnId;
    }

    public void countDownLatch(long backendId, Set<Long> tablets) {
        if (this.latch != null) {
            if (latch.markedCountDown(backendId, tablets)) {
                LOG.debug("UpdateTabletMetaInfoTask current latch count: {}, backend: {}, tablets:{}", latch.getCount(),
                        backendId, tablets);
            }
        }
    }

    // call this always means one of tasks is failed. count down to zero to finish entire task
    public void countDownToZero(String errMsg) {
        if (this.latch != null) {
            latch.countDownToZero(new Status(TStatusCode.CANCELLED, errMsg));
            LOG.debug("UpdateTabletMetaInfoTask count down to zero. error msg: {}", errMsg);
        }
    }

    public abstract Set<Long> getTablets();

    public abstract List<TTabletMetaInfo> getTTabletMetaInfoList();

    public TUpdateTabletMetaInfoReq toThrift() {
        TUpdateTabletMetaInfoReq updateTabletMetaInfoReq = new TUpdateTabletMetaInfoReq();
        List<TTabletMetaInfo> metaInfos = getTTabletMetaInfoList();
        updateTabletMetaInfoReq.setTabletMetaInfos(metaInfos);
        if (RunMode.isSharedDataMode()) {
            updateTabletMetaInfoReq.setTablet_type(TTabletType.TABLET_TYPE_LAKE);
        }
        updateTabletMetaInfoReq.setTxn_id(txnId);
        return updateTabletMetaInfoReq;
    }
}