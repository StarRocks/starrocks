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

package com.starrocks.catalog;

import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.Expr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class ExprPartitionMeta {

    private static final Logger LOG = LogManager.getLogger(ExpressionRangePartitionInfo.class);

    @SerializedName(value = "partitionExprs")
    private List<Expr> partitionExprs;

    @SerializedName(value = "automaticPartition")
    private Boolean automaticPartition = false;

    @SerializedName(value = "sourcePartitionTypes")
    private List<Type> sourcePartitionTypes;

    public List<Expr> getPartitionExprs() {
        return partitionExprs;
    }

    public void setPartitionExprs(List<Expr> partitionExprs) {
        this.partitionExprs = partitionExprs;
    }

    public Boolean getAutomaticPartition() {
        return automaticPartition;
    }

    public void setAutomaticPartition(Boolean automaticPartition) {
        this.automaticPartition = automaticPartition;
    }

    public List<Type> getSourcePartitionTypes() {
        return sourcePartitionTypes;
    }

    public void setSourcePartitionTypes(List<Type> sourcePartitionTypes) {
        this.sourcePartitionTypes = sourcePartitionTypes;
    }
}
