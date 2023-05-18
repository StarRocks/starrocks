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

package com.starrocks.sql.ast;

import com.starrocks.catalog.Type;

import java.util.Map;

public class PartitionConvertContext {

    private boolean isAutoPartitionTable = false;
    private Type firstPartitionColumnType;
    private Map<String, String> properties;
    private boolean isTempPartition = false;


    public boolean isAutoPartitionTable() {
        return isAutoPartitionTable;
    }

    public void setAutoPartitionTable(boolean autoPartitionTable) {
        isAutoPartitionTable = autoPartitionTable;
    }

    public Type getFirstPartitionColumnType() {
        return firstPartitionColumnType;
    }

    public void setFirstPartitionColumnType(Type firstPartitionColumnType) {
        this.firstPartitionColumnType = firstPartitionColumnType;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public boolean isTempPartition() {
        return isTempPartition;
    }

    public void setTempPartition(boolean tempPartition) {
        isTempPartition = tempPartition;
    }
}
