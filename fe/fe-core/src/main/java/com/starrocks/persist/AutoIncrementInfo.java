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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/persist/AutoIncrementInfo.java

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

package com.starrocks.persist;

import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class AutoIncrementInfo implements Writable {
    @SerializedName(value = "tableIdToIncrementId")
    private Map<Long, Long> tableIdToIncrementId = Maps.newHashMap();

    public AutoIncrementInfo(ConcurrentHashMap<Long, Long> tableIdToIncrementId) {
        if (tableIdToIncrementId != null) {
            // for persist
            for (Map.Entry<Long, Long> entry : tableIdToIncrementId.entrySet()) {
                Long tableId = entry.getKey();
                Long id = entry.getValue();

                this.tableIdToIncrementId.put(tableId, id);
            }
        }
    }

    public Map<Long, Long> tableIdToIncrementId() {
        return this.tableIdToIncrementId;
    }

    public AutoIncrementInfo read(DataInput input) throws IOException {
        AutoIncrementInfo info = GsonUtils.GSON.fromJson(Text.readString(input), AutoIncrementInfo.class);
        for (Map.Entry<Long, Long> entry : info.tableIdToIncrementId().entrySet()) {
            Long tableId = entry.getKey();
            Long id = entry.getValue();

            this.tableIdToIncrementId.put(tableId, id);
        }
        return this;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }
}
