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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/planner/DataPartition.java

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

package com.starrocks.planner;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.analysis.Analyzer;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.ExprSubstitutionMap;
import com.starrocks.common.AnalysisException;
import com.starrocks.thrift.TDataPartition;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TPartitionType;

import java.util.List;

/**
 * Specification of the partition of a single stream of data.
 * Examples of those streams of data are: the scan of a table; the output
 * of a plan fragment; etc. (ie, this is not restricted to direct exchanges
 * between two fragments, which in the backend is facilitated by the classes
 * DataStreamSender/DataStreamMgr/DataStreamRecvr).
 * TODO: better name? just Partitioning?
 */
public class DataPartition {
    public static final DataPartition UNPARTITIONED = new DataPartition(TPartitionType.UNPARTITIONED);

    public static final DataPartition RANDOM = new DataPartition(TPartitionType.RANDOM);

    private final TPartitionType type;

    // for hash partition: exprs used to compute hash value
    private ImmutableList<Expr> partitionExprs;

    public DataPartition(TPartitionType type, List<Expr> exprs) {
        if (type != TPartitionType.UNPARTITIONED && type != TPartitionType.RANDOM) {
            Preconditions.checkNotNull(exprs);
            Preconditions.checkState(!exprs.isEmpty());
            Preconditions.checkState(
                    type == TPartitionType.HASH_PARTITIONED || type == TPartitionType.RANGE_PARTITIONED
                            || type == TPartitionType.BUCKET_SHUFFLE_HASH_PARTITIONED);
            this.type = type;
            this.partitionExprs = ImmutableList.copyOf(exprs);
        } else {
            this.type = type;
            this.partitionExprs = ImmutableList.of();
        }
    }

    public void substitute(ExprSubstitutionMap smap, Analyzer analyzer) throws AnalysisException {
        List<Expr> list = Expr.trySubstituteList(partitionExprs, smap, analyzer, false);
        partitionExprs = ImmutableList.copyOf(list);
    }

    public DataPartition(TPartitionType type) {
        Preconditions.checkState(
                type == TPartitionType.UNPARTITIONED || type == TPartitionType.RANDOM);
        this.type = type;
        this.partitionExprs = ImmutableList.of();
    }

    public static DataPartition hashPartitioned(List<Expr> exprs) {
        return new DataPartition(TPartitionType.HASH_PARTITIONED, exprs);
    }

    public boolean isPartitioned() {
        return type != TPartitionType.UNPARTITIONED;
    }

    public boolean isBucketShuffle() {
        return type == TPartitionType.BUCKET_SHUFFLE_HASH_PARTITIONED;
    }

    public TPartitionType getType() {
        return type;
    }

    public List<Expr> getPartitionExprs() {
        return partitionExprs;
    }

    public TDataPartition toThrift() {
        TDataPartition result = new TDataPartition(type);
        if (partitionExprs != null) {
            result.setPartition_exprs(Expr.treesToThrift(partitionExprs));
        }
        return result;
    }

    public String getExplainString(TExplainLevel explainLevel) {
        StringBuilder str = new StringBuilder();
        str.append(type.toString());
        if (!partitionExprs.isEmpty()) {
            List<String> strings = Lists.newArrayList();
            for (Expr expr : partitionExprs) {
                strings.add(expr.toSql());
            }
            str.append(": ").append(Joiner.on(", ").join(strings));
        }
        str.append("\n");
        return str.toString();
    }
}
