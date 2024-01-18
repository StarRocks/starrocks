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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/load/loadv2/BrokerLoadPendingTaskTest.java

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
import com.google.common.collect.Maps;
import com.starrocks.analysis.BrokerDesc;
import com.starrocks.common.exception.UserException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.fs.HdfsUtil;
import com.starrocks.load.BrokerFileGroup;
import com.starrocks.load.BrokerFileGroupAggInfo.FileGroupAggKey;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TBrokerFileStatus;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class BrokerLoadPendingTaskTest {

    @Test
    public void testExecuteTask(@Injectable BrokerLoadJob brokerLoadJob,
                                @Injectable BrokerFileGroup brokerFileGroup,
                                @Injectable BrokerDesc brokerDesc,
                                @Mocked GlobalStateMgr globalStateMgr,
                                @Injectable TBrokerFileStatus tBrokerFileStatus) throws UserException {
        Map<FileGroupAggKey, List<BrokerFileGroup>> aggKeyToFileGroups = Maps.newHashMap();
        List<BrokerFileGroup> brokerFileGroups = Lists.newArrayList();
        brokerFileGroups.add(brokerFileGroup);
        FileGroupAggKey aggKey = new FileGroupAggKey(1L, null);
        aggKeyToFileGroups.put(aggKey, brokerFileGroups);
        new Expectations() {
            {
                globalStateMgr.getNextId();
                result = 1L;
                brokerFileGroup.getFilePaths();
                result = "hdfs://localhost:8900/test_column";
            }
        };
        new MockUp<HdfsUtil>() {
            @Mock
            public void parseFile(String path, BrokerDesc brokerDesc, List<TBrokerFileStatus> fileStatuses) {
                fileStatuses.add(tBrokerFileStatus);
            }
        };

        BrokerLoadPendingTask brokerLoadPendingTask =
                new BrokerLoadPendingTask(brokerLoadJob, aggKeyToFileGroups, brokerDesc);
        brokerLoadPendingTask.executeTask();
        BrokerPendingTaskAttachment brokerPendingTaskAttachment =
                Deencapsulation.getField(brokerLoadPendingTask, "attachment");
        Assert.assertEquals(1, brokerPendingTaskAttachment.getFileNumByTable(aggKey));
        Assert.assertEquals(tBrokerFileStatus, brokerPendingTaskAttachment.getFileStatusByTable(aggKey).get(0).get(0));
    }
}
