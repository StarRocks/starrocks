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

package com.starrocks.common.proc;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.LimitElement;
import com.starrocks.catalog.PaimonTable;
import com.starrocks.catalog.PartitionType;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.util.ListComparator;
import com.starrocks.common.util.OrderByPair;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.connector.ConnectorMetadatRequestContext;
import com.starrocks.connector.PartitionInfo;
import com.starrocks.connector.paimon.Partition;
import com.starrocks.server.GlobalStateMgr;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class PaimonTablePartitionsProcDir extends PartitionsProcDir {
    private ImmutableList<String> titleNames;
    private Table table;

    public PaimonTablePartitionsProcDir(Table table, PartitionType partitionType) {
        super(partitionType);
        this.table = table;
        this.createTitleNames();
    }

    private void createTitleNames() {
        ImmutableList.Builder<String> builder =
                new ImmutableList.Builder<String>().add("PartitionName").add("VisibleVersionTime").add("State")
                        .add("PartitionKey").add("RowCount").add("DataSize").add("FileCount");
        this.titleNames = builder.build();

    }
    public ProcResult fetchResultByFilter(Map<String, Expr> filterMap, List<OrderByPair> orderByPairs,
                                          LimitElement limitElement) throws AnalysisException {
        List<List<Comparable>> partitionInfos = getPartitionInfos();
        List<List<Comparable>> filterPartitionInfos;

        // where
        if (filterMap == null || filterMap.isEmpty()) {
            filterPartitionInfos = partitionInfos;
        } else {
            filterPartitionInfos = Lists.newArrayList();
            for (List<Comparable> partitionInfo : partitionInfos) {
                if (partitionInfo.size() != this.titleNames.size()) {
                    throw new AnalysisException(
                            "PartitionInfos.size() " + partitionInfos.size() + " not equal TITLE_NAMES.size() " +
                                    this.titleNames.size());
                }
                boolean isNeed = true;
                for (int i = 0; i < partitionInfo.size(); i++) {
                    isNeed = filter(this.titleNames.get(i), partitionInfo.get(i), filterMap);
                    if (!isNeed) {
                        break;
                    }
                }

                if (isNeed) {
                    filterPartitionInfos.add(partitionInfo);
                }
            }
        }

        // order by
        if (orderByPairs != null && !orderByPairs.isEmpty()) {
            ListComparator<List<Comparable>> comparator;
            OrderByPair[] orderByPairArr = new OrderByPair[orderByPairs.size()];
            comparator = new ListComparator<>(orderByPairs.toArray(orderByPairArr));
            filterPartitionInfos.sort(comparator);
        }

        // limit
        if (limitElement != null && limitElement.hasLimit()) {
            int beginIndex = (int) limitElement.getOffset();
            int endIndex = (int) (beginIndex + limitElement.getLimit());
            if (endIndex > filterPartitionInfos.size()) {
                endIndex = filterPartitionInfos.size();
            }
            filterPartitionInfos = filterPartitionInfos.subList(beginIndex, endIndex);
        }

        return getBasicProcResult(filterPartitionInfos);
    }

    public BaseProcResult getBasicProcResult(List<List<Comparable>> partitionInfos) {
        // set result
        BaseProcResult result = new BaseProcResult();
        result.setNames(this.titleNames);
        for (List<Comparable> info : partitionInfos) {
            List<String> row = new ArrayList<String>(info.size());
            for (Comparable comparable : info) {
                row.add(comparable.toString());
            }
            result.addRow(row);
        }

        return result;
    }

    public int analyzeColumn(String columnName) {
        for (int i = 0; i < this.titleNames.size(); ++i) {
            if (this.titleNames.get(i).equalsIgnoreCase(columnName)) {
                return i;
            }
        }
        ErrorReport.reportSemanticException(ErrorCode.ERR_WRONG_COLUMN_NAME, columnName);
        return -1;
    }

    private List<List<Comparable>> getPartitionInfos() {
        Preconditions.checkNotNull(table);
        PaimonTable paimonTable = (PaimonTable) table;
        // get info
        List<List<Comparable>> partitionInfos = new ArrayList<List<Comparable>>();
        List<String> listPartitionNames = GlobalStateMgr.getCurrentState().getMetadataMgr()
                .listPartitionNames(table.getCatalogName(), paimonTable.getCatalogDBName(),
                        paimonTable.getCatalogTableName(), ConnectorMetadatRequestContext.DEFAULT);

        List<PartitionInfo> tblPartitionInfo = GlobalStateMgr.getCurrentState().getMetadataMgr()
                .getPartitions(table.getCatalogName(), table, listPartitionNames);

        for (PartitionInfo partitionInfo : tblPartitionInfo) {
            partitionInfos.add(getPartitionInfo(partitionInfo));
        }
        return partitionInfos;
    }

    private List<Comparable> getPartitionInfo(PartitionInfo tblPartitionInfo) {
        List<Comparable> partitionInfo = new ArrayList<Comparable>();
        Partition partition = (Partition) tblPartitionInfo;
        // PartitionName
        partitionInfo.add(partition.getPartitionName());
        // VisibleVersionTime
        partitionInfo.add(TimeUtils.longToTimeString(partition.getModifiedTime()));
        // State
        partitionInfo.add("Normal");
        // partition key
        partitionInfo.add(table.isUnPartitioned() ? "" : String.join(",", table.getPartitionColumnNames()));

        partitionInfo.add(partition.getRecordCount());
        partitionInfo.add(partition.getFileSizeInBytes());
        partitionInfo.add(partition.getFileCount());

        return partitionInfo;
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        List<List<Comparable>> partitionInfos = getPartitionInfos();
        return getBasicProcResult(partitionInfos);
    }

    @Override
    public boolean register(String name, ProcNodeInterface node) {
        return false;
    }

    @Override
    public ProcNodeInterface lookup(String partitionIdOrName) throws AnalysisException {
        return null;
    }
}
