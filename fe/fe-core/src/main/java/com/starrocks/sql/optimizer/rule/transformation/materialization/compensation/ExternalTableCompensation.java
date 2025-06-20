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

package com.starrocks.sql.optimizer.rule.transformation.materialization.compensation;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.common.Config;
import com.starrocks.sql.common.PRangeCell;
import org.apache.commons.collections4.CollectionUtils;

import java.util.List;
import java.util.stream.Collectors;

public class ExternalTableCompensation extends BaseCompensation {
    public ExternalTableCompensation(List<PRangeCell> partitionKeys) {
        super(partitionKeys);
    }

    @Override
    public String toString() {
        List<PRangeCell> compensations = getCompensations();
        if (CollectionUtils.isEmpty(compensations)) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        sb.append("[size=").append(compensations.size()).append("]");
        int size = Math.min(Config.max_mv_task_run_meta_message_values_length, compensations.size());
        sb.append(" [");
        List<String> partitions = Lists.newArrayList();
        for (int i = 0; i < size; i++) {
            PRangeCell key = compensations.get(i);
            Range<PartitionKey> range = key.getRange();
            if (range.lowerEndpoint().equals(range.upperEndpoint())) {
                partitions.add(getPartitionKeyString(range.lowerEndpoint()));
            } else {
                sb.append(getPartitionKeyString(key.getRange().lowerEndpoint()))
                        .append(" - ")
                        .append(getPartitionKeyString(key.getRange().upperEndpoint()));
            }
        }
        sb.append(Joiner.on(",").join(partitions));
        sb.append("]");
        return sb.toString();
    }

    private String getPartitionKeyString(PartitionKey key) {
        List<String> keys = key.getKeys()
                .stream()
                .map(LiteralExpr::getStringValue)
                .collect(Collectors.toList());
        return "(" + Joiner.on(",").join(keys) + ")";
    }
}


