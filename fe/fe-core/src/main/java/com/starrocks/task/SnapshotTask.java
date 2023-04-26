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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/task/SnapshotTask.java

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

import com.starrocks.thrift.TResourceInfo;
import com.starrocks.thrift.TSnapshotRequest;
import com.starrocks.thrift.TTaskType;
import com.starrocks.thrift.TypesConstants;

public class SnapshotTask extends AgentTask {
    private long jobId;

    private long version;

    private int schemaHash;

    private long timeoutMs;

    private boolean isRestoreTask;

    public SnapshotTask(TResourceInfo resourceInfo, long backendId, long signature, long jobId,
                        long dbId, long tableId, long partitionId, long indexId, long tabletId,
                        long version, int schemaHash, long timeoutMs, boolean isRestoreTask) {
        super(resourceInfo, backendId, TTaskType.MAKE_SNAPSHOT, dbId, tableId, partitionId, indexId, tabletId,
                signature);

        this.jobId = jobId;

        this.version = version;
        this.schemaHash = schemaHash;

        this.timeoutMs = timeoutMs;

        this.isRestoreTask = isRestoreTask;
    }

    public long getJobId() {
        return jobId;
    }

    public long getVersion() {
        return version;
    }

    public int getSchemaHash() {
        return schemaHash;
    }

    public long getTimeoutMs() {
        return timeoutMs;
    }

    public boolean isRestoreTask() {
        return isRestoreTask;
    }

    public TSnapshotRequest toThrift() {
        TSnapshotRequest request = new TSnapshotRequest(tabletId, schemaHash);
        request.setVersion(version);
        request.setList_files(true);
        request.setPreferred_snapshot_format(TypesConstants.TPREFER_SNAPSHOT_REQ_VERSION);
        request.setTimeout(timeoutMs / 1000);
        request.setIs_restore_task(isRestoreTask);
        return request;
    }
}
