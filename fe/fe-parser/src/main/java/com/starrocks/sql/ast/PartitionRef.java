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

import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.parser.NodePosition;

import java.util.ArrayList;
import java.util.List;

public class PartitionRef implements ParseNode {
    private final List<String> partitionNames;
    private final List<String> partitionColNames;
    private final List<Expr> partitionColValues;
    private final boolean isTemp;
    private final NodePosition pos;

    public PartitionRef(List<String> partitionNames, boolean isTemp, NodePosition pos) {
        this(partitionNames, isTemp, new ArrayList<>(), new ArrayList<>(), pos);
    }

    public PartitionRef(List<String> partitionNames, boolean isTemp,
                        List<String> partitionColNames, List<Expr> partitionColValues, NodePosition pos) {
        this.partitionNames = partitionNames == null ? new ArrayList<>() : new ArrayList<>(partitionNames);
        this.partitionColNames = partitionColNames == null ? new ArrayList<>() : new ArrayList<>(partitionColNames);
        this.partitionColValues = partitionColValues == null ? new ArrayList<>() : new ArrayList<>(partitionColValues);
        this.isTemp = isTemp;
        this.pos = pos;
    }

    public PartitionRef(PartitionRef other) {
        this(other.partitionNames, other.isTemp, other.partitionColNames, other.partitionColValues, other.pos);
    }

    public List<String> getPartitionNames() {
        return partitionNames;
    }

    public List<String> getPartitionColNames() {
        return partitionColNames;
    }

    public List<Expr> getPartitionColValues() {
        return partitionColValues;
    }

    public boolean isTemp() {
        return isTemp;
    }

    public boolean isKeyPartitionNames() {
        return !partitionColValues.isEmpty();
    }

    @Override
    public NodePosition getPos() {
        return pos;
    }

    @Override
    public String toString() {
        if (partitionNames.isEmpty()) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        if (isTemp) {
            sb.append("TEMPORARY ");
        }
        sb.append("PARTITIONS (");
        sb.append(String.join(", ", partitionNames));
        sb.append(")");
        return sb.toString();
    }
}
