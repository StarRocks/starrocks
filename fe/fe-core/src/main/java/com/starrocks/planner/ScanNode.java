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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/planner/ScanNode.java

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
import com.google.common.base.MoreObjects;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.SlotDescriptor;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.catalog.ColumnAccessPath;
import com.starrocks.common.StarRocksException;
import com.starrocks.connector.RemoteFilesSampleStrategy;
import com.starrocks.datacache.DataCacheOptions;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.optimizer.ScanOptimzeOption;
import com.starrocks.thrift.TColumnAccessPath;
import com.starrocks.thrift.TScanRangeLocations;
import org.jetbrains.annotations.TestOnly;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Representation of the common elements of all scan nodes.
 */
public abstract class ScanNode extends PlanNode {
    protected final TupleDescriptor desc;
    protected Map<String, PartitionColumnFilter> columnFilters;
    protected String sortColumn = null;
    protected List<ColumnAccessPath> columnAccessPaths;
    protected DataCacheOptions dataCacheOptions = null;
    protected long warehouseId = WarehouseManager.DEFAULT_WAREHOUSE_ID;
    protected ScanOptimzeOption scanOptimzeOption;
    // The column names applied dict optimization
    // used for explain
    protected final List<String> appliedDictStringColumns = new ArrayList<>();

    public ScanNode(PlanNodeId id, TupleDescriptor desc, String planNodeName) {
        super(id, desc.getId().asList(), planNodeName);
        this.desc = desc;
    }

    public void setColumnFilters(Map<String, PartitionColumnFilter> columnFilters) {
        this.columnFilters = columnFilters;
    }

    public void setColumnAccessPaths(List<ColumnAccessPath> columnAccessPaths) {
        this.columnAccessPaths = columnAccessPaths;
    }

    @TestOnly
    public List<ColumnAccessPath> getColumnAccessPaths() {
        return this.columnAccessPaths;
    }

    public void setDataCacheOptions(DataCacheOptions dataCacheOptions) {
        this.dataCacheOptions = dataCacheOptions;
    }

    public void setWarehouseId(long warehouseId) {
        this.warehouseId = warehouseId;
    }

    public void setScanOptimzeOption(ScanOptimzeOption opt) {
        this.scanOptimzeOption = opt.copy();
    }

    public ScanOptimzeOption getScanOptimzeOption() {
        return scanOptimzeOption;
    }

    public void updateAppliedDictStringColumns(Set<Integer> appliedColumnIds) {
        for (SlotDescriptor slot : desc.getSlots()) {
            if (appliedColumnIds.contains(slot.getId().asInt())) {
                appliedDictStringColumns.add(slot.getColumn().getName());
            }
        }
    }

    public TupleDescriptor getDesc() {
        return desc;
    }

    public String getTableName() {
        return desc.getTable().getName();
    }

    public boolean isLocalNativeTable() {
        return false;
    }

    public boolean hasMoreScanRanges() {
        return false;
    }

    /**
     * cast expr to SlotDescriptor type
     */
    protected Expr castToSlot(SlotDescriptor slotDesc, Expr expr) throws StarRocksException {
        if (!slotDesc.getType().matchesType(expr.getType())) {
            return expr.castTo(slotDesc.getType());
        } else {
            return expr;
        }
    }

    /**
     * Returns all scan ranges plus their locations. Needs to be preceded by a call to
     * finalize().
     *
     * @param maxScanRangeLength The maximum number of bytes each scan range should scan;
     *                           only applicable to HDFS; less than or equal to zero means no
     *                           maximum.
     */
    public abstract List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength);

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("tid", desc.getId().asInt()).add("tblName",
                desc.getTable().getName()).add("keyRanges", "").addValue(
                super.debugString()).toString();
    }

    protected String explainColumnAccessPath(String prefix) {
        String result = "";
        if (columnAccessPaths.stream().anyMatch(c -> !c.isFromPredicate())) {
            result += prefix + "ColumnAccessPath: [" + columnAccessPaths.stream()
                    .filter(c -> !c.isFromPredicate())
                    .map(ColumnAccessPath::explain)
                    .sorted()
                    .collect(Collectors.joining(", ")) + "]\n";
        }
        if (columnAccessPaths.stream().anyMatch(ColumnAccessPath::isFromPredicate)) {
            result += prefix + "PredicateAccessPath: [" + columnAccessPaths.stream()
                    .filter(ColumnAccessPath::isFromPredicate)
                    .map(ColumnAccessPath::explain)
                    .sorted()
                    .collect(Collectors.joining(", ")) + "]\n";
        }
        return result;
    }

    protected List<TColumnAccessPath> columnAccessPathToThrift() {
        if (columnAccessPaths == null) {
            return Collections.emptyList();
        }

        return columnAccessPaths.stream().map(ColumnAccessPath::toThrift).collect(Collectors.toList());
    }

    protected boolean supportTopNRuntimeFilter() {
        return false;
    }

    @Override
    public boolean needCollectExecStats() {
        return true;
    }

    // We use this flag to know how many connector scan nodes at BE side, and connector framework
    // will use this number to fair share memory usage between those scan nodes.
    public boolean isRunningAsConnectorOperator() {
        return true;
    }

    public void setScanSampleStrategy(RemoteFilesSampleStrategy strategy) {
    }

    public boolean isConnectorScanNode() {
        return this instanceof HdfsScanNode || this instanceof IcebergScanNode ||
                this instanceof HudiScanNode || this instanceof DeltaLakeScanNode ||
                this instanceof FileTableScanNode || this instanceof PaimonScanNode ||
                this instanceof OdpsScanNode || this instanceof IcebergMetadataScanNode;
    }

    protected String explainColumnDict(String prefix) {
        StringBuilder output = new StringBuilder();
        if (!appliedDictStringColumns.isEmpty()) {
            int maxSize = Math.min(appliedDictStringColumns.size(), 5);
            List<String> printList = appliedDictStringColumns.subList(0, maxSize);
            String format_template = "dict_col=%s";
            if (appliedDictStringColumns.size() > 5) {
                format_template = format_template + "...";
            }
            output.append(prefix).append(String.format(format_template, Joiner.on(",").join(printList)));
            output.append("\n");
        }
        return output.toString();
    }
}
