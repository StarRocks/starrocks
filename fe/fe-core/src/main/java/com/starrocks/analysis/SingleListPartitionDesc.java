// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/SingleRangePartitionDesc.java

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

package com.starrocks.analysis;

import com.starrocks.catalog.ListPartitionItemDesc;
import com.starrocks.common.util.PrintableMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SingleListPartitionDesc extends ListPartitionItemDesc {

    private final boolean ifNotExists;
    private final String partitionName;
    private final List<String> values;
    private final Map<String, String> partitionProperties;

    public SingleListPartitionDesc(boolean ifNotExists, String partitionName, List<String> values,
                                 Map<String, String> partitionProperties) {
        this.ifNotExists = ifNotExists;
        this.partitionName = partitionName ;
        this.values = values;
        this.partitionProperties = partitionProperties;
    }

    public List<String> getValues(){
        return this.values;
    }

    @Override
    public String getPartitionName(){
        return this.partitionName;
    }

    @Override
    public Map<String, String> getPartitionProperties() {
        return this.partitionProperties;
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("PARTITION ").append(super.getPartitionName()).append(" VALUES IN (");
        sb.append(this.values.stream().map(value -> "\"" + value + "\"")
                .collect(Collectors.joining(",")));
        sb.append(")");
        if (partitionProperties != null && !partitionProperties.isEmpty()) {
            sb.append(" (");
            sb.append(new PrintableMap(partitionProperties, "=", true, false));
            sb.append(")");
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }
}
