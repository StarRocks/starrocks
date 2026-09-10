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
package com.starrocks.common.util;

import com.google.common.collect.Lists;
import com.starrocks.proto.AIExecutionStatisticsPB;
import com.starrocks.proto.NodeExecStatsItemPB;
import com.starrocks.proto.PQueryStatistics;
import com.starrocks.proto.QueryStatisticsItemPB;
import com.starrocks.thrift.TAIExecutionStatistics;
import com.starrocks.thrift.TAuditStatistics;
import com.starrocks.thrift.TAuditStatisticsItem;
import org.apache.commons.collections.CollectionUtils;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class AuditStatisticsUtil {
    // Copy without normalization: a snapshot must preserve optional presence and every reported field.
    public static PQueryStatistics copyProtobuf(PQueryStatistics source) {
        if (source == null) {
            return null;
        }
        PQueryStatistics copy = new PQueryStatistics();
        copy.scanRows = source.scanRows;
        copy.scanBytes = source.scanBytes;
        copy.returnedRows = source.returnedRows;
        copy.cpuCostNs = source.cpuCostNs;
        copy.memCostBytes = source.memCostBytes;
        copy.spillBytes = source.spillBytes;
        copy.transmittedBytes = source.transmittedBytes;
        copy.readLocalCnt = source.readLocalCnt;
        copy.readRemoteCnt = source.readRemoteCnt;
        if (source.statsItems != null) {
            copy.statsItems = Lists.newArrayListWithCapacity(source.statsItems.size());
            for (QueryStatisticsItemPB item : source.statsItems) {
                QueryStatisticsItemPB copiedItem = new QueryStatisticsItemPB();
                copiedItem.scanRows = item.scanRows;
                copiedItem.scanBytes = item.scanBytes;
                copiedItem.tableId = item.tableId;
                copy.statsItems.add(copiedItem);
            }
        }
        if (source.nodeExecStatsItems != null) {
            copy.nodeExecStatsItems = Lists.newArrayListWithCapacity(source.nodeExecStatsItems.size());
            for (NodeExecStatsItemPB item : source.nodeExecStatsItems) {
                NodeExecStatsItemPB copiedItem = new NodeExecStatsItemPB();
                copiedItem.pushRows = item.pushRows;
                copiedItem.pullRows = item.pullRows;
                copiedItem.predFilterRows = item.predFilterRows;
                copiedItem.indexFilterRows = item.indexFilterRows;
                copiedItem.rfFilterRows = item.rfFilterRows;
                copiedItem.nodeId = item.nodeId;
                copy.nodeExecStatsItems.add(copiedItem);
            }
        }
        if (source.aiStatistics != null) {
            AIExecutionStatisticsPB ai = source.aiStatistics;
            AIExecutionStatisticsPB copiedAI = new AIExecutionStatisticsPB();
            copiedAI.taskCount = ai.taskCount;
            copiedAI.requestCount = ai.requestCount;
            copiedAI.retryCount = ai.retryCount;
            copiedAI.timeoutCount = ai.timeoutCount;
            copiedAI.errorCount = ai.errorCount;
            copiedAI.httpTimeNs = ai.httpTimeNs;
            copiedAI.promptTokens = ai.promptTokens;
            copiedAI.completionTokens = ai.completionTokens;
            copiedAI.totalTokens = ai.totalTokens;
            copiedAI.promptUsageCount = ai.promptUsageCount;
            copiedAI.completionUsageCount = ai.completionUsageCount;
            copiedAI.totalUsageCount = ai.totalUsageCount;
            copy.aiStatistics = copiedAI;
        }
        return copy;
    }

    public static PQueryStatistics toProtobuf(TAuditStatistics tb) {
        if (tb == null) {
            return null;
        }
        PQueryStatistics pb = new PQueryStatistics();
        pb.scanRows = tb.getScan_rows();
        pb.scanBytes = tb.getScan_bytes();
        pb.returnedRows = tb.getReturned_rows();
        pb.cpuCostNs = tb.getCpu_cost_ns();
        pb.memCostBytes = tb.getMem_cost_bytes();
        pb.spillBytes = tb.getSpill_bytes();
        pb.transmittedBytes = tb.getTransmitted_bytes();
        if (tb.isSetStats_items()) {
            pb.statsItems = Lists.newArrayList();
            for (TAuditStatisticsItem tItem : tb.getStats_items()) {
                QueryStatisticsItemPB pItem = new QueryStatisticsItemPB();
                pItem.scanBytes = tItem.getScan_bytes();
                pItem.scanRows = tItem.getScan_rows();
                pItem.tableId = tItem.getTable_id();
                pb.statsItems.add(pItem);
            }
        }
        pb.readLocalCnt = tb.getRead_local_cnt();
        pb.readRemoteCnt = tb.getRead_remote_cnt();
        if (tb.isSetAi_statistics()) {
            pb.aiStatistics = toAIProtobuf(tb.getAi_statistics());
        }
        return pb;
    }

    public static void mergeProtobuf(PQueryStatistics from, PQueryStatistics to) {
        if (from == null || to == null) {
            return;
        }
        if (from.scanRows != null) {
            if (to.scanRows == null) {
                to.scanRows = 0L;
            }
            to.scanRows += from.scanRows;
        }
        if (from.scanBytes != null) {
            if (to.scanBytes == null) {
                to.scanBytes = 0L;
            }
            to.scanBytes += from.scanBytes;
        }
        if (from.returnedRows != null) {
            if (to.returnedRows == null) {
                to.returnedRows = 0L;
            }
            to.returnedRows += from.returnedRows;
        }
        if (from.cpuCostNs != null) {
            if (to.cpuCostNs == null) {
                to.cpuCostNs = 0L;
            }
            to.cpuCostNs += from.cpuCostNs;
        }
        if (from.memCostBytes != null) {
            if (to.memCostBytes == null) {
                to.memCostBytes = 0L;
            }
            to.memCostBytes = Math.max(from.memCostBytes, to.memCostBytes);
        }
        if (from.spillBytes != null) {
            if (to.spillBytes == null) {
                to.spillBytes = 0L;
            }
            to.spillBytes += from.spillBytes;
        }
        if (from.transmittedBytes != null) {
            if (to.transmittedBytes == null) {
                to.transmittedBytes = 0L;
            }
            to.transmittedBytes += from.transmittedBytes;
        }
        if (CollectionUtils.isNotEmpty(from.statsItems)) {
            if (to.statsItems == null) {
                to.statsItems = Lists.newArrayList();
            }
            Map<Long, QueryStatisticsItemPB> itemMap = to.statsItems.stream()
                    .collect(Collectors.toMap(item -> item.tableId, Function.identity()));
            for (QueryStatisticsItemPB fromItem : from.statsItems) {
                if (fromItem.tableId == null) {
                    continue;
                }
                QueryStatisticsItemPB existToItem = itemMap.get(fromItem.tableId);
                if (existToItem != null) {
                    if (fromItem.scanBytes != null) {
                        if (existToItem.scanBytes == null) {
                            existToItem.scanBytes = 0L;
                        }
                        existToItem.scanBytes += fromItem.scanBytes;
                    }
                    if (fromItem.scanRows != null) {
                        if (existToItem.scanRows == null) {
                            existToItem.scanRows = 0L;
                        }
                        existToItem.scanRows += fromItem.scanRows;
                    }
                } else {
                    to.statsItems.add(fromItem);
                    itemMap.put(fromItem.tableId, fromItem);
                }
            }
        }
        if (from.readLocalCnt != null) {
            if (to.readLocalCnt == null) {
                to.readLocalCnt = 0L;
            }
            to.readLocalCnt += from.readLocalCnt;
        }
        if (from.readRemoteCnt != null) {
            if (to.readRemoteCnt == null) {
                to.readRemoteCnt = 0L;
            }
            to.readRemoteCnt += from.readRemoteCnt;
        }
        if (from.aiStatistics != null) {
            if (to.aiStatistics == null) {
                to.aiStatistics = new AIExecutionStatisticsPB();
            }
            mergeAIStatistics(from.aiStatistics, to.aiStatistics);
        }
    }

    public static TAuditStatistics toThrift(PQueryStatistics pb) {
        if (pb == null) {
            return null;
        }
        TAuditStatistics tb = new TAuditStatistics();
        if (pb.scanRows != null) {
            tb.setScan_rows(pb.scanRows);
        }
        if (pb.scanBytes != null) {
            tb.setScan_bytes(pb.scanBytes);
        }
        if (pb.returnedRows != null) {
            tb.setReturned_rows(pb.returnedRows);
        }
        if (pb.cpuCostNs != null) {
            tb.setCpu_cost_ns(pb.cpuCostNs);
        }
        if (pb.memCostBytes != null) {
            tb.setMem_cost_bytes(pb.memCostBytes);
        }
        if (pb.spillBytes != null) {
            tb.setSpill_bytes(pb.spillBytes);
        }
        if (pb.transmittedBytes != null) {
            tb.setTransmitted_bytes(pb.transmittedBytes);
        }
        if (CollectionUtils.isNotEmpty(pb.statsItems)) {
            for (QueryStatisticsItemPB pItem : pb.statsItems) {
                TAuditStatisticsItem tItem = new TAuditStatisticsItem();
                if (pItem.scanBytes != null) {
                    tItem.setScan_bytes(pItem.scanBytes);
                }
                if (pItem.scanRows != null) {
                    tItem.setScan_rows(pItem.scanRows);
                }
                if (pItem.tableId != null) {
                    tItem.setTable_id(pItem.tableId);
                }
                tb.addToStats_items(tItem);
            }
        }
        if (pb.readLocalCnt != null) {
            tb.setRead_local_cnt(pb.readLocalCnt);
        }
        if (pb.readRemoteCnt != null) {
            tb.setRead_remote_cnt(pb.readRemoteCnt);
        }
        if (pb.aiStatistics != null) {
            tb.setAi_statistics(toAIThrift(pb.aiStatistics));
        }
        return tb;
    }

    private static AIExecutionStatisticsPB toAIProtobuf(TAIExecutionStatistics thrift) {
        AIExecutionStatisticsPB protobuf = new AIExecutionStatisticsPB();
        if (thrift.isSetTask_count()) {
            protobuf.taskCount = nonnegative(thrift.getTask_count());
        }
        if (thrift.isSetRequest_count()) {
            protobuf.requestCount = nonnegative(thrift.getRequest_count());
        }
        if (thrift.isSetRetry_count()) {
            protobuf.retryCount = nonnegative(thrift.getRetry_count());
        }
        if (thrift.isSetTimeout_count()) {
            protobuf.timeoutCount = nonnegative(thrift.getTimeout_count());
        }
        if (thrift.isSetError_count()) {
            protobuf.errorCount = nonnegative(thrift.getError_count());
        }
        if (thrift.isSetHttp_time_ns()) {
            protobuf.httpTimeNs = nonnegative(thrift.getHttp_time_ns());
        }
        if (thrift.isSetPrompt_tokens()) {
            protobuf.promptTokens = nonnegative(thrift.getPrompt_tokens());
        }
        if (thrift.isSetCompletion_tokens()) {
            protobuf.completionTokens = nonnegative(thrift.getCompletion_tokens());
        }
        if (thrift.isSetTotal_tokens()) {
            protobuf.totalTokens = nonnegative(thrift.getTotal_tokens());
        }
        if (thrift.isSetPrompt_usage_count()) {
            protobuf.promptUsageCount = nonnegative(thrift.getPrompt_usage_count());
        }
        if (thrift.isSetCompletion_usage_count()) {
            protobuf.completionUsageCount = nonnegative(thrift.getCompletion_usage_count());
        }
        if (thrift.isSetTotal_usage_count()) {
            protobuf.totalUsageCount = nonnegative(thrift.getTotal_usage_count());
        }
        return protobuf;
    }

    private static TAIExecutionStatistics toAIThrift(AIExecutionStatisticsPB protobuf) {
        TAIExecutionStatistics thrift = new TAIExecutionStatistics();
        if (protobuf.taskCount != null && protobuf.taskCount >= 0) {
            thrift.setTask_count(protobuf.taskCount);
        }
        if (protobuf.requestCount != null && protobuf.requestCount >= 0) {
            thrift.setRequest_count(protobuf.requestCount);
        }
        if (protobuf.retryCount != null && protobuf.retryCount >= 0) {
            thrift.setRetry_count(protobuf.retryCount);
        }
        if (protobuf.timeoutCount != null && protobuf.timeoutCount >= 0) {
            thrift.setTimeout_count(protobuf.timeoutCount);
        }
        if (protobuf.errorCount != null && protobuf.errorCount >= 0) {
            thrift.setError_count(protobuf.errorCount);
        }
        if (protobuf.httpTimeNs != null && protobuf.httpTimeNs >= 0) {
            thrift.setHttp_time_ns(protobuf.httpTimeNs);
        }
        if (protobuf.promptTokens != null && protobuf.promptTokens >= 0) {
            thrift.setPrompt_tokens(protobuf.promptTokens);
        }
        if (protobuf.completionTokens != null && protobuf.completionTokens >= 0) {
            thrift.setCompletion_tokens(protobuf.completionTokens);
        }
        if (protobuf.totalTokens != null && protobuf.totalTokens >= 0) {
            thrift.setTotal_tokens(protobuf.totalTokens);
        }
        if (protobuf.promptUsageCount != null && protobuf.promptUsageCount >= 0) {
            thrift.setPrompt_usage_count(protobuf.promptUsageCount);
        }
        if (protobuf.completionUsageCount != null && protobuf.completionUsageCount >= 0) {
            thrift.setCompletion_usage_count(protobuf.completionUsageCount);
        }
        if (protobuf.totalUsageCount != null && protobuf.totalUsageCount >= 0) {
            thrift.setTotal_usage_count(protobuf.totalUsageCount);
        }
        return thrift;
    }

    private static void mergeAIStatistics(AIExecutionStatisticsPB from, AIExecutionStatisticsPB to) {
        to.taskCount = addAICounter(to.taskCount, from.taskCount);
        to.requestCount = addAICounter(to.requestCount, from.requestCount);
        to.retryCount = addAICounter(to.retryCount, from.retryCount);
        to.timeoutCount = addAICounter(to.timeoutCount, from.timeoutCount);
        to.errorCount = addAICounter(to.errorCount, from.errorCount);
        to.httpTimeNs = addAICounter(to.httpTimeNs, from.httpTimeNs);
        // Validate both sides before merging: incomplete reports must not combine into fabricated usage.
        Long fromPromptCount = observedUsageCount(from.promptTokens, from.promptUsageCount);
        Long toPromptCount = observedUsageCount(to.promptTokens, to.promptUsageCount);
        to.promptTokens = addAICounter(
                hasUsage(toPromptCount) ? to.promptTokens : null,
                hasUsage(fromPromptCount) ? from.promptTokens : null);
        to.promptUsageCount = addAICounter(toPromptCount, fromPromptCount);

        Long fromCompletionCount = observedUsageCount(from.completionTokens, from.completionUsageCount);
        Long toCompletionCount = observedUsageCount(to.completionTokens, to.completionUsageCount);
        to.completionTokens = addAICounter(
                hasUsage(toCompletionCount) ? to.completionTokens : null,
                hasUsage(fromCompletionCount) ? from.completionTokens : null);
        to.completionUsageCount = addAICounter(toCompletionCount, fromCompletionCount);

        Long fromTotalCount = observedUsageCount(from.totalTokens, from.totalUsageCount);
        Long toTotalCount = observedUsageCount(to.totalTokens, to.totalUsageCount);
        to.totalTokens = addAICounter(
                hasUsage(toTotalCount) ? to.totalTokens : null,
                hasUsage(fromTotalCount) ? from.totalTokens : null);
        to.totalUsageCount = addAICounter(toTotalCount, fromTotalCount);
    }

    private static Long observedUsageCount(Long tokens, Long usageCount) {
        if (usageCount != null && usageCount == 0) {
            return 0L;
        }
        return tokens != null && tokens >= 0 && hasUsage(usageCount) ? usageCount : null;
    }

    private static boolean hasUsage(Long usageCount) {
        return usageCount != null && usageCount > 0;
    }

    private static Long nonnegative(Long value) {
        return value != null && value >= 0 ? value : null;
    }

    // Keep absent counters absent and prevent malformed or overflowing telemetry from becoming negative.
    private static Long addAICounter(Long left, Long right) {
        left = nonnegative(left);
        right = nonnegative(right);
        if (left == null) {
            return right;
        }
        if (right == null) {
            return left;
        }
        return Long.MAX_VALUE - left < right ? Long.MAX_VALUE : left + right;
    }
}
