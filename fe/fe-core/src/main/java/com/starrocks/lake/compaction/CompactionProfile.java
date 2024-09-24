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

package com.starrocks.lake.compaction;

import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import com.starrocks.proto.CompactStat;

import javax.validation.constraints.NotNull;

class CompactionProfile {
    @SerializedName(value = "sub_task_count")
    private int subTaskCount;
    @SerializedName(value = "read_local_sec")
    private long readLocalSec;
    @SerializedName(value = "read_local_mb")
    private long readLocalMb;
    @SerializedName(value = "read_remote_sec")
    private long readRemoteSec;
    @SerializedName(value = "read_remote_mb")
    private long readRemoteMb;
    @SerializedName(value = "in_queue_sec")
    private int inQueueSec;

    public CompactionProfile(@NotNull CompactStat stat) {
        subTaskCount = stat.subTaskCount;

        readLocalSec = stat.readTimeLocal / 1000000000L;
        readLocalMb = stat.readBytesLocal / 1048576;
        readRemoteSec = stat.readTimeRemote / 1000000000L;
        readRemoteMb = stat.readBytesRemote / 1048576;
        inQueueSec = stat.inQueueTimeSec;
    }

    @Override
    public String toString() {
        return new Gson().toJson(this);
    }
}
