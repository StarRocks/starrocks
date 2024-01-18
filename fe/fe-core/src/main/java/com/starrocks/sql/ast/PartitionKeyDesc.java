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

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.starrocks.analysis.ParseNode;
import com.starrocks.common.exception.AnalysisException;
import com.starrocks.sql.parser.NodePosition;

import java.util.List;

// Describe the partition key values in create table or add partition clause
public class PartitionKeyDesc implements ParseNode {
    public static final PartitionKeyDesc MAX_VALUE = new PartitionKeyDesc();

    public enum PartitionRangeType {
        INVALID,
        LESS_THAN,
        FIXED
    }

    private List<PartitionValue> lowerValues;
    private List<PartitionValue> upperValues;
    private final PartitionRangeType partitionType;

    private final NodePosition pos;

    public static PartitionKeyDesc createMaxKeyDesc() {
        return MAX_VALUE;
    }

    private PartitionKeyDesc() {
        this.pos = NodePosition.ZERO;
        partitionType = PartitionRangeType.LESS_THAN; // LESS_THAN is default type.
    }

    // values less than
    public PartitionKeyDesc(List<PartitionValue> upperValues) {
        this(upperValues, NodePosition.ZERO);
    }

    public PartitionKeyDesc(List<PartitionValue> upperValues, NodePosition pos) {
        this.pos = pos;
        this.upperValues = upperValues;
        partitionType = PartitionRangeType.LESS_THAN;
    }

    // fixed range
    public PartitionKeyDesc(List<PartitionValue> lowerValues, List<PartitionValue> upperValues) {
        this(lowerValues, upperValues, NodePosition.ZERO);
    }

    public PartitionKeyDesc(List<PartitionValue> lowerValues, List<PartitionValue> upperValues, NodePosition pos) {
        this.pos = pos;
        this.lowerValues = lowerValues;
        this.upperValues = upperValues;
        partitionType = PartitionRangeType.FIXED;
    }

    public List<PartitionValue> getLowerValues() {
        return lowerValues;
    }

    public List<PartitionValue> getUpperValues() {
        return upperValues;
    }

    public boolean isMax() {
        return this == MAX_VALUE;
    }

    public boolean hasLowerValues() {
        return lowerValues != null;
    }

    public boolean hasUpperValues() {
        return upperValues != null;
    }

    public PartitionRangeType getPartitionType() {
        return partitionType;
    }

    public void analyze(int partColNum) throws AnalysisException {
        if (!isMax()) {
            if (upperValues.isEmpty() || upperValues.size() > partColNum) {
                throw new AnalysisException("Partition values number is more than partition column number: " + this);
            }
        }

        // currently, we do not support MAXVALUE in multi partition range values. eg: ("100", "200", MAXVALUE);
        // maybe support later.
        if (lowerValues != null) {
            for (PartitionValue lowerVal : lowerValues) {
                if (lowerVal.isMax() && partColNum > 1) {
                    throw new AnalysisException("Not support MAXVALUE in multi partition range values.");
                }
            }
        }

        if (upperValues != null) {
            for (PartitionValue upperVal : upperValues) {
                if (upperVal.isMax() && partColNum > 1) {
                    throw new AnalysisException("Not support MAXVALUE in multi partition range values.");
                }
            }
        }
    }

    private String getPartitionValuesStr(List<PartitionValue> values) {
        StringBuilder sb = new StringBuilder("(");
        Joiner.on(", ").appendTo(sb, Lists.transform(values, new Function<PartitionValue, String>() {
            @Override
            public String apply(PartitionValue v) {
                if (v.isMax()) {
                    return v.getStringValue();
                } else {
                    return "'" + v.getStringValue() + "'";
                }
            }
        })).append(")");
        return sb.toString();
    }

    @Override
    public NodePosition getPos() {
        return pos;
    }

    // returns:
    // 1: MAXVALUE
    // 2: ("100", "200", MAXVALUE)
    // 3: [("100", "200"), ("300", "200"))
    public String toString() {
        if (isMax()) {
            return "MAXVALUE";
        }

        if (partitionType == PartitionRangeType.LESS_THAN) {
            return getPartitionValuesStr(upperValues);
        } else if (partitionType == PartitionRangeType.FIXED) {
            return "[" + getPartitionValuesStr(lowerValues) + ", " + getPartitionValuesStr(upperValues) + ")";
        } else {
            return "INVALID";
        }
    }
}
