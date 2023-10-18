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
import com.starrocks.proto.PQueryStatistics;
import com.starrocks.proto.QueryStatisticsItemPB;
import com.starrocks.thrift.TAuditStatistics;
import com.starrocks.thrift.TAuditStatisticsItem;
import org.apache.commons.collections.CollectionUtils;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class AuditStatisticsUtil {
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
            to.memCostBytes += from.memCostBytes;
        }
        if (from.spillBytes != null) {
            if (to.spillBytes == null) {
                to.spillBytes = 0L;
            }
            to.spillBytes += from.spillBytes;
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
        return tb;
    }
}
