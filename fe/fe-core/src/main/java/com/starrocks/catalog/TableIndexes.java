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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/catalog/TableIndexes.java

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

package com.starrocks.catalog;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Internal representation of table index, including indexes and index properties for future features
 */
public class TableIndexes implements Writable {
    @SerializedName(value = "indexes")
    private List<Index> indexes;
    @SerializedName(value = "properties")
    private Map<String, String> properties;

    public TableIndexes() {
        this.indexes = Lists.newArrayList();
        this.properties = Maps.newHashMap();
    }

    public TableIndexes(List<Index> indexes) {
        this.indexes = indexes;
        this.properties = Maps.newHashMap();
    }

    public TableIndexes(List<Index> indexes, Map<String, String> properties) {
        this.indexes = indexes;
        this.properties = properties;
    }

    public List<Index> getIndexes() {
        if (indexes == null) {
            indexes = Lists.newArrayList();
        }
        return indexes;
    }

    public List<Index> getCopiedIndexes() {
        if (indexes == null || indexes.size() == 0) {
            return Lists.newArrayList();
        } else {
            return Lists.newArrayList(indexes);
        }
    }

    public void setIndexes(List<Index> indexes) {
        this.indexes = indexes;
    }

    public Map<String, String> getProperties() {
        if (properties == null) {
            properties = Maps.newHashMap();
        }
        return properties;
    }

    public Map<String, String> getCopiedProperties() {
        if (properties == null || properties.size() == 0) {
            return new HashMap<>();
        } else {
            return new HashMap<>(properties);
        }
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }


    public TableIndexes shallowCopy() {
        return new TableIndexes(getCopiedIndexes(), getCopiedProperties());
    }
    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static TableIndexes read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, TableIndexes.class);
    }
}
