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
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.Partition;

import java.util.List;

public class ListPartitionPersistInfo extends PartitionPersistInfoV2 {

    @SerializedName("values")
    private List<String> values;
    @SerializedName("multiValues")
    private List<List<String>> multiValues;

    public ListPartitionPersistInfo(Long dbId, Long tableId, Partition partition,
                                    DataProperty dataProperty, short replicationNum,
                                    boolean isInMemory, boolean isTempPartition,
                                    List<String> values, List<List<String>> multiValues) {
        super(dbId, tableId, partition, dataProperty, replicationNum, isInMemory, isTempPartition);
        this.multiValues = multiValues;
        this.values = values;
    }

    public List<String> getValues() {
        return values;
    }

    public List<List<String>> getMultiValues() {
        return multiValues;
    }

}