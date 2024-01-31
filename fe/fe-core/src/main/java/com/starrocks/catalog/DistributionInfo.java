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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/catalog/DistributionInfo.java

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

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.sql.ast.DistributionDesc;
import org.apache.commons.lang.NotImplementedException;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

public abstract class DistributionInfo implements Writable {

    public enum DistributionInfoType {
        HASH,
        RANDOM
    }

    // for Gson runtime type adaptor
    @SerializedName(value = "typeStr")
    protected String typeStr;
    @SerializedName(value = "type")
    protected DistributionInfoType type;

    public DistributionInfo() {
        // for persist
    }

    public DistributionInfo(DistributionInfoType type) {
        this.type = type;
        this.typeStr = this.type.name();
    }

    public DistributionInfoType getType() {
        return type;
    }

    public int getBucketNum() {
        // should override in sub class
        throw new NotImplementedException("not implemented");
    }

    public abstract boolean supportColocate();

    public abstract List<Column> getDistributionColumns();

    public String getDistributionKey() {
        return "";
    }

    public void setBucketNum(int bucketNum) {
        throw new NotImplementedException("not implemented");
    }

    public DistributionDesc toDistributionDesc() {
        throw new NotImplementedException();
    }

    public DistributionInfo copy() {
        throw new NotImplementedException();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, type.name());
    }

    public void readFields(DataInput in) throws IOException {
        type = DistributionInfoType.valueOf(Text.readString(in));
    }

    public String toSql() {
        return "";
    }
}
