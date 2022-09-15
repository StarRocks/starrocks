// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/DistributionDesc.java

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

import com.starrocks.catalog.Column;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.DistributionInfo.DistributionInfoType;
import com.starrocks.common.DdlException;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import org.apache.commons.lang.NotImplementedException;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Set;

public class DistributionDesc implements ParseNode, Writable {
    protected DistributionInfoType type;

    public DistributionDesc() {

    }

    public void analyze(Set<String> colSet) {
        throw new NotImplementedException();
    }

    public int getBuckets() {
        throw new NotImplementedException();
    }

    public String toSql() {
        throw new NotImplementedException();
    }

    public DistributionInfo toDistributionInfo(List<Column> columns) throws DdlException {
        throw new NotImplementedException();
    }

    public static DistributionDesc read(DataInput in) throws IOException {
        DistributionInfoType type = DistributionInfoType.valueOf(Text.readString(in));
        if (type == DistributionInfoType.HASH) {
            DistributionDesc desc = new HashDistributionDesc();
            desc.readFields(in);
            return desc;
        } else {
            throw new IOException("Unknow distribution type: " + type);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, type.name());
    }

    public void readFields(DataInput in) throws IOException {
        throw new NotImplementedException();
    }
}
