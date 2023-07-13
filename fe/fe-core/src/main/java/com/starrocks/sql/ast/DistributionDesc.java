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

import com.starrocks.analysis.ParseNode;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.DistributionInfo.DistributionInfoType;
import com.starrocks.common.DdlException;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.sql.parser.NodePosition;
import org.apache.commons.lang.NotImplementedException;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Set;

public class DistributionDesc implements ParseNode, Writable {
    protected DistributionInfoType type;

    protected final NodePosition pos;

    public DistributionDesc() {
        this(NodePosition.ZERO);
    }

    public DistributionDesc(NodePosition pos) {
        this.pos = pos;
    }

    public void analyze(Set<String> colSet) {
        throw new NotImplementedException();
    }

    public int getBuckets() {
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
        } else if (type == DistributionInfoType.RANDOM) {
            DistributionDesc desc = new RandomDistributionDesc();
            desc.readFields(in);
            return desc;
        } else {
            throw new IOException("Unknown distribution type: " + type);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, type.name());
    }

    public void readFields(DataInput in) throws IOException {
        throw new NotImplementedException();
    }

    @Override
    public NodePosition getPos() {
        return pos;
    }
}
