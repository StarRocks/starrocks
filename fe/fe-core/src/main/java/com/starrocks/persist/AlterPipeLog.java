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
import com.starrocks.load.pipe.Pipe;
import com.starrocks.load.pipe.PipeId;

import java.util.Map;

public class AlterPipeLog implements Writable {

    @SerializedName(value = "id")
    private final PipeId pipeId;

    @SerializedName(value = "state")
    private Pipe.State state;

    @SerializedName(value = "changeProps")
    private Map<String, String> changeProps;

    @SerializedName(value = "load_status")
    private Pipe.LoadStatus loadStatus;

    public AlterPipeLog(PipeId pipeId) {
        this.pipeId = pipeId;
    }

    public PipeId getPipeId() {
        return pipeId;
    }

    public Pipe.State getState() {
        return state;
    }

    public void setState(Pipe.State state) {
        this.state = state;
    }

    public Map<String, String> getChangeProps() {
        return changeProps;
    }

    public void setChangeProps(Map<String, String> changeProps) {
        this.changeProps = changeProps;
    }

    public Pipe.LoadStatus getLoadStatus() {
        return loadStatus;
    }

    public void setLoadStatus(Pipe.LoadStatus loadStatus) {
        this.loadStatus = loadStatus;
    }
}
