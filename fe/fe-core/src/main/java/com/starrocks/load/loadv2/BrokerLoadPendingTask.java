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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/load/loadv2/BrokerLoadPendingTask.java

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

package com.starrocks.load.loadv2;

import com.google.common.collect.Lists;
import com.starrocks.analysis.BrokerDesc;
import com.starrocks.common.exception.UserException;
import com.starrocks.common.util.BrokerUtil;
import com.starrocks.common.util.LogBuilder;
import com.starrocks.common.util.LogKey;
import com.starrocks.fs.HdfsUtil;
import com.starrocks.load.BrokerFileGroup;
import com.starrocks.load.BrokerFileGroupAggInfo.FileGroupAggKey;
import com.starrocks.load.FailMsg;
import com.starrocks.thrift.TBrokerFileStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;

public class BrokerLoadPendingTask extends LoadTask {

    private static final Logger LOG = LogManager.getLogger(BrokerLoadPendingTask.class);

    private Map<FileGroupAggKey, List<BrokerFileGroup>> aggKeyToBrokerFileGroups;
    private BrokerDesc brokerDesc;

    public BrokerLoadPendingTask(BrokerLoadJob loadTaskCallback,
                                 Map<FileGroupAggKey, List<BrokerFileGroup>> aggKeyToBrokerFileGroups,
                                 BrokerDesc brokerDesc) {
        super(loadTaskCallback, TaskType.PENDING, 0);
        this.retryTime = 3;
        this.attachment = new BrokerPendingTaskAttachment(signature);
        this.aggKeyToBrokerFileGroups = aggKeyToBrokerFileGroups;
        this.brokerDesc = brokerDesc;
        this.failMsg = new FailMsg(FailMsg.CancelType.ETL_RUN_FAIL);
    }

    @Override
    void executeTask() throws UserException {
        LOG.info("begin to execute broker pending task. job: {}", callback.getCallbackId());
        getAllFileStatus();
    }

    private void getAllFileStatus() throws UserException {
        long start = System.currentTimeMillis();
        long totalFileSize = 0;
        int totalFileNum = 0;
        for (Map.Entry<FileGroupAggKey, List<BrokerFileGroup>> entry : aggKeyToBrokerFileGroups.entrySet()) {
            FileGroupAggKey aggKey = entry.getKey();
            List<BrokerFileGroup> fileGroups = entry.getValue();

            List<List<TBrokerFileStatus>> fileStatusList = Lists.newArrayList();
            long tableTotalFileSize = 0;
            int tableTotalFileNum = 0;
            int groupNum = 0;
            for (BrokerFileGroup fileGroup : fileGroups) {
                long groupFileSize = 0;
                List<TBrokerFileStatus> fileStatuses = Lists.newArrayList();
                for (String path : fileGroup.getFilePaths()) {
                    if (brokerDesc.hasBroker()) {
                        BrokerUtil.parseFile(path, brokerDesc, fileStatuses);
                    } else {
                        HdfsUtil.parseFile(path, brokerDesc, fileStatuses);
                    }
                }
                fileStatusList.add(fileStatuses);
                for (TBrokerFileStatus fstatus : fileStatuses) {
                    groupFileSize += fstatus.getSize();
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(new LogBuilder(LogKey.LOAD_JOB, callback.getCallbackId())
                                .add("file_status", fstatus).build());
                    }
                }
                tableTotalFileSize += groupFileSize;
                tableTotalFileNum += fileStatuses.size();
                LOG.info("get {} files in file group {} for table {}. size: {}. job: {}",
                        fileStatuses.size(), groupNum, entry.getKey(), groupFileSize, callback.getCallbackId());
                groupNum++;
            }

            totalFileSize += tableTotalFileSize;
            totalFileNum += tableTotalFileNum;
            ((BrokerPendingTaskAttachment) attachment).addFileStatus(aggKey, fileStatusList);
            LOG.info("get {} files to be loaded. total size: {}. cost: {} ms, job: {}",
                    tableTotalFileNum, tableTotalFileSize, (System.currentTimeMillis() - start),
                    callback.getCallbackId());
        }

        ((BrokerLoadJob) callback).setLoadFileInfo(totalFileNum, totalFileSize);
    }
}
