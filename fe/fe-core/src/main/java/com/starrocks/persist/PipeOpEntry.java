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
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.load.pipe.Pipe;
import com.starrocks.persist.gson.GsonUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PipeOpEntry implements Writable {

    @SerializedName(value = "pipe")
    Pipe pipe;
    @SerializedName(value = "opType")
    PipeOpType opType;

    public Pipe getPipe() {
        return pipe;
    }

    public void setPipe(Pipe pipe) {
        this.pipe = pipe;
    }

    public PipeOpType getOpType() {
        return opType;
    }

    public void setOpType(PipeOpType opType) {
        this.opType = opType;
    }

    public static PipeOpEntry read(DataInput input) throws IOException {
        String json = Text.readString(input);
        return GsonUtils.GSON.fromJson(json, PipeOpEntry.class);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public enum PipeOpType {
        PIPE_OP_CREATE(0),
        PIPE_OP_ALTER(1),
        PIPE_OP_DROP(2);

        private final int value;

        private PipeOpType(int value) {
            this.value = value;
        }
    }
}
