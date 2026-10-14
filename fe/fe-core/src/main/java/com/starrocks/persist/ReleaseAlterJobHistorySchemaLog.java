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

package com.starrocks.persist;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Writable;

// Journal entry for releasing the history schema of a done alter job. The job id locates the job; the db and
// table ids identify it in logs.
public class ReleaseAlterJobHistorySchemaLog implements Writable {

    @SerializedName(value = "jobId")
    private long jobId;
    @SerializedName(value = "dbId")
    private long dbId;
    @SerializedName(value = "tableId")
    private long tableId;

    public ReleaseAlterJobHistorySchemaLog(long jobId, long dbId, long tableId) {
        this.jobId = jobId;
        this.dbId = dbId;
        this.tableId = tableId;
    }

    public long getJobId() {
        return jobId;
    }

    public long getDbId() {
        return dbId;
    }

    public long getTableId() {
        return tableId;
    }
}
