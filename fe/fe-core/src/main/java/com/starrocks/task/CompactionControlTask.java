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

import com.google.common.collect.Maps;
import com.starrocks.thrift.TCompactionControlReq;
import com.starrocks.thrift.TTaskType;

import java.util.Map;

public class CompactionControlTask extends AgentTask {

    private Map<Long, Long> tableToDisableDeadline = Maps.newHashMap();

    public CompactionControlTask(long backendId, Map<Long, Long> tableToDisableDeadline) {
        super(null, backendId, TTaskType.COMPACTION_CONTROL, -1, -1, -1, -1, -1);
        this.tableToDisableDeadline = tableToDisableDeadline;
    }

    public TCompactionControlReq toThrift() {
        TCompactionControlReq req = new TCompactionControlReq();
        req.setTable_to_disable_deadline(tableToDisableDeadline);
        return req;
    }
}
