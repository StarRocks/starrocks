// This file is made available under Elastic License 2.0.
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

import com.google.common.base.MoreObjects;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.SlotDescriptor;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.common.UserException;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TScanRangeLocations;

import java.util.List;
import java.util.Map;

/**
 * Representation of the common elements of all scan nodes.
 */
// Our new cost based query optimizer is more powerful and stable than old query optimizer,
// The old query optimizer related codes could be deleted safely.
// TODO: Remove old query optimizer related codes before 2021-09-30
public abstract class ScanNode extends PlanNode {
    protected final TupleDescriptor desc;
    protected Map<String, PartitionColumnFilter> columnFilters;
    protected String sortColumn = null;

    public ScanNode(PlanNodeId id, TupleDescriptor desc, String planNodeName) {
        super(id, desc.getId().asList(), planNodeName);
        this.desc = desc;
    }

    /**
     * Helper function to parse a "host:port" address string into TNetworkAddress
     * This is called with ipaddress:port when doing scan range assigment.
     */
    protected static TNetworkAddress addressToTNetworkAddress(String address) {
        TNetworkAddress result = new TNetworkAddress();
        String[] hostPort = address.split(":");
        result.hostname = hostPort[0];
        result.port = Integer.parseInt(hostPort[1]);
        return result;
    }

    public void setColumnFilters(Map<String, PartitionColumnFilter> columnFilters) {
        this.columnFilters = columnFilters;
    }

    public void setSortColumn(String column) {
        sortColumn = column;
    }

    /**
     * cast expr to SlotDescriptor type
     */
    protected Expr castToSlot(SlotDescriptor slotDesc, Expr expr) throws UserException {
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
}
