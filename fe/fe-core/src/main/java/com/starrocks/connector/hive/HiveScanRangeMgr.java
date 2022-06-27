// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.connector.hive;

import com.google.common.base.Preconditions;
import com.starrocks.analysis.DescriptorTable;
import com.starrocks.connector.ConnectorScanRangeMgr;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHiveScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalScanOperator;
import com.starrocks.thrift.TScanRangeLocations;

public class HiveScanRangeMgr implements ConnectorScanRangeMgr {

    @Override
    public TScanRangeLocations getScanRanges(DescriptorTable descTbl, PhysicalScanOperator scanOperator)
    {
        Preconditions.checkArgument(scanOperator instanceof PhysicalHiveScanOperator);

        return null;
    }
}
