// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.connector.hive;

import com.google.common.base.Preconditions;
import com.starrocks.connector.ConnectorScanRangeMgr;
import com.starrocks.connector.ConnectorTableHandle;
import com.starrocks.thrift.TScanRangeLocations;

import java.util.List;

public class HiveScanRangeMgr implements ConnectorScanRangeMgr {

    @Override
    public List<TScanRangeLocations> getScanRanges(ConnectorTableHandle tableHandle)
    {
        Preconditions.checkArgument(tableHandle instanceof HiveTableHandle);
        HiveTableHandle hiveTableHandle = (HiveTableHandle) tableHandle;

        return null;
    }
}
