// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.analysis.ParseNode;
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
