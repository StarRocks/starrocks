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

package com.starrocks.statistic;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;

import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

public class BatchRemoveBasicStatsMetaLog implements Writable {
    @SerializedName("metas")
    private List<BasicStatsMeta> metas;

    // For Gson
    public BatchRemoveBasicStatsMetaLog() {
    }

    public BatchRemoveBasicStatsMetaLog(List<BasicStatsMeta> metas) {
        this.metas = metas;
    }

    public List<BasicStatsMeta> getMetas() {
        return metas;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }
}

