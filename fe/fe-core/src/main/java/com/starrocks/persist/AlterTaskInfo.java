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
import com.starrocks.scheduler.Constants;
import com.starrocks.scheduler.persist.TaskSchedule;

public class AlterTaskInfo implements Writable {
    @SerializedName("name")
    private String name;

    @SerializedName("type")
    private Constants.TaskType type;

    @SerializedName("schedule")
    private TaskSchedule schedule;

    public AlterTaskInfo() {
        // for persist
    }

    public AlterTaskInfo(String name, Constants.TaskType type, TaskSchedule schedule) {
        this.name = name;
        this.type = type;
        this.schedule = schedule;
    }

    public String getName() {
        return name;
    }

    public Constants.TaskType getType() {
        return type;
    }

    public TaskSchedule getSchedule() {
        return schedule;
    }
}
