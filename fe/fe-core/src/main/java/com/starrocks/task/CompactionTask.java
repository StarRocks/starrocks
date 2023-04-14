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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/task/CheckConsistencyTask.java

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

import com.starrocks.thrift.TCompactionReq;
import com.starrocks.thrift.TResourceInfo;
import com.starrocks.thrift.TTaskType;

import java.util.List;

public class CompactionTask extends AgentTask {

    public List<Long> getTabletIds() {
        return tabletIds;
    }

    public boolean isBaseCompaction() {
        return baseCompaction;
    }

    private List<Long> tabletIds;
    private boolean baseCompaction;

    public CompactionTask(TResourceInfo resourceInfo, long backendId, long dbId,
                          long tableId, List<Long> tabletIds, boolean baseCompaction) {
        super(resourceInfo, backendId, TTaskType.COMPACTION, dbId, tableId, -1, -1, -1);

        this.tabletIds = tabletIds;
        this.baseCompaction = baseCompaction;
    }

    public TCompactionReq toThrift() {
        TCompactionReq req = new TCompactionReq();
        req.setIs_base_compaction(baseCompaction);
        req.setTablet_ids(tabletIds);
        return req;
    }
}
