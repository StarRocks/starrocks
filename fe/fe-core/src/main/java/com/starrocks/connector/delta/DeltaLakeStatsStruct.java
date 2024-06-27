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

package com.starrocks.connector.delta;

import com.google.gson.annotations.SerializedName;

import java.util.Map;

public class DeltaLakeStatsStruct {
    @SerializedName(value = "numRecords")
    public long numRecords;

    @SerializedName(value = "minValues")
    public Map<String, Object> minValues;

    @SerializedName(value = "maxValues")
    public Map<String, Object> maxValues;

    @SerializedName(value = "nullCount")
    public Map<String, Object> nullCount;

    public DeltaLakeStatsStruct(long numRecords) {
        this.numRecords = numRecords;
    }
}
